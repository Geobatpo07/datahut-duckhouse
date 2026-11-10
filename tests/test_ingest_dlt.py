"""Tests for the ``dhd ingest-dlt`` exploitation layer (Vague 1b, task 3):
run log, write_disposition dispatch (replace/append/merge), schema contracts,
and the "one Iceberg commit per table per run" invariant.

Fully offline: a local ``file://`` Iceberg warehouse in a temp dir, a fake
``pipeline`` stub (no real sources.yml/connectors/Docker/network involved --
those are tests/test_registry.py, tests/test_dlt_pipeline.py and
tests/integration/test_multi_engine.py's job).
"""

from __future__ import annotations

import importlib.util
import sqlite3
import sys
import types
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

REPO_ROOT = Path(__file__).parent.parent


def _load_registry_module():
    spec = importlib.util.spec_from_file_location(
        "dlt_registry_for_ingest_dlt_test",
        REPO_ROOT / "ingestion" / "dlt" / "registry.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def registry():
    return _load_registry_module()


@pytest.fixture
def pyiceberg_env(monkeypatch, tmp_path):
    monkeypatch.setenv("ICEBERG_WAREHOUSE", "")
    monkeypatch.setenv("ICEBERG_WAREHOUSE_PATH", str(tmp_path / "wh"))
    monkeypatch.setenv("ICEBERG_NAMESPACE", "default")
    monkeypatch.delenv("ICEBERG_CATALOG_URI", raising=False)
    monkeypatch.delenv("DHD_ENGINE", raising=False)


def _make_source(registry, **overrides):
    defaults = {
        "name": "t",
        "family": "files",
        "connector": "local_file",
        "path": "x.csv",
        "format": "csv",
    }
    defaults.update(overrides)
    return registry.SourceConfig(**defaults)


def _stub_pipeline(registry, sources_by_name: dict, extracted_by_source: dict):
    """A minimal stand-in for ingestion/dlt/pipeline.py: no registry.yml, no
    connectors, no sys.path tricks -- just load_sources()/extract_source()."""
    stub = types.SimpleNamespace()
    stub.SourcesConfigError = registry.SourcesConfigError
    stub.MissingDriverError = RuntimeError  # never raised in these tests
    stub.ConnectorError = RuntimeError

    def load_sources():
        return list(sources_by_name.values())

    def extract_source(source, work_dir=None):
        return extracted_by_source[source.name]

    stub.load_sources = load_sources
    stub.extract_source = extract_source
    return stub


def _write_parquet(tmp_path: Path, name: str, table: pa.Table) -> Path:
    path = tmp_path / f"{name}.parquet"
    papq.write_table(table, path)
    return path


class TestWriteDispositionDispatch:
    def test_replace_overwrites_on_rerun(
        self, pyiceberg_env, registry, tmp_path, monkeypatch
    ):
        from datahut_duckhouse import ingest_dlt

        source = _make_source(registry, name="s", write_disposition="replace")
        t1 = pa.table({"id": [1, 2], "name": ["a", "b"]})
        t2 = pa.table({"id": [3], "name": ["c"]})
        monkeypatch.setattr(
            ingest_dlt,
            "_load_pipeline",
            lambda: _stub_pipeline(
                registry,
                {"s": source},
                {"s": {"s_t": _write_parquet(tmp_path, "r1", t1)}},
            ),
        )
        ingest_dlt.run_ingest_dlt("s")

        monkeypatch.setattr(
            ingest_dlt,
            "_load_pipeline",
            lambda: _stub_pipeline(
                registry,
                {"s": source},
                {"s": {"s_t": _write_parquet(tmp_path, "r2", t2)}},
            ),
        )
        ingest_dlt.run_ingest_dlt("s")

        from datahut_duckhouse.ingest import connect

        backend = connect()
        rows = backend.catalog.load_table("default.s_t").scan().to_arrow().to_pylist()
        assert rows == [{"id": 3, "name": "c"}]  # fully replaced, not merged

    def test_append_accumulates_on_rerun(
        self, pyiceberg_env, registry, tmp_path, monkeypatch
    ):
        from datahut_duckhouse import ingest_dlt

        source = _make_source(registry, name="s", write_disposition="append")
        t1 = pa.table({"id": [1], "name": ["a"]})
        t2 = pa.table({"id": [2], "name": ["b"]})
        for i, t in enumerate([t1, t2]):
            monkeypatch.setattr(
                ingest_dlt,
                "_load_pipeline",
                lambda t=t, i=i: _stub_pipeline(
                    registry,
                    {"s": source},
                    {"s": {"s_t": _write_parquet(tmp_path, f"a{i}", t)}},
                ),
            )
            ingest_dlt.run_ingest_dlt("s")

        from datahut_duckhouse.ingest import connect

        backend = connect()
        rows = backend.catalog.load_table("default.s_t").scan().to_arrow().to_pylist()
        assert {r["id"] for r in rows} == {1, 2}  # both runs' rows present

    def test_merge_upserts_by_primary_key(
        self, pyiceberg_env, registry, tmp_path, monkeypatch
    ):
        from datahut_duckhouse import ingest_dlt

        source = _make_source(
            registry, name="s", write_disposition="merge", primary_key=["id"]
        )
        t1 = pa.table({"id": [1, 2], "name": ["a", "b"]})
        # Second run: id=1 updated, id=3 new -- merge must not duplicate id=1
        # or id=2, and must pick up the new id=3 (task 5's incremental story).
        t2 = pa.table({"id": [1, 3], "name": ["a-updated", "c"]})
        for i, t in enumerate([t1, t2]):
            monkeypatch.setattr(
                ingest_dlt,
                "_load_pipeline",
                lambda t=t, i=i: _stub_pipeline(
                    registry,
                    {"s": source},
                    {"s": {"s_t": _write_parquet(tmp_path, f"m{i}", t)}},
                ),
            )
            ingest_dlt.run_ingest_dlt("s")

        from datahut_duckhouse.ingest import connect

        backend = connect()
        rows = backend.catalog.load_table("default.s_t").scan().to_arrow().to_pylist()
        by_id = {r["id"]: r["name"] for r in rows}
        assert by_id == {1: "a-updated", 2: "b", 3: "c"}


class TestSchemaContract:
    def test_evolve_widens_the_table(
        self, pyiceberg_env, registry, tmp_path, monkeypatch
    ):
        from datahut_duckhouse import ingest_dlt

        source = _make_source(
            registry, name="s", schema_contract="evolve", write_disposition="append"
        )
        t1 = pa.table({"id": [1], "name": ["a"]})
        t2 = pa.table({"id": [2], "name": ["b"], "extra": [9]})
        for i, t in enumerate([t1, t2]):
            monkeypatch.setattr(
                ingest_dlt,
                "_load_pipeline",
                lambda t=t, i=i: _stub_pipeline(
                    registry,
                    {"s": source},
                    {"s": {"s_t": _write_parquet(tmp_path, f"e{i}", t)}},
                ),
            )
            ingest_dlt.run_ingest_dlt("s")

        from datahut_duckhouse.ingest import connect

        backend = connect()
        rows = backend.catalog.load_table("default.s_t").scan().to_arrow().to_pylist()
        assert {"id": 1, "name": "a", "extra": None} in rows
        assert {"id": 2, "name": "b", "extra": 9} in rows

    def test_freeze_rejects_new_columns(
        self, pyiceberg_env, registry, tmp_path, monkeypatch
    ):
        from datahut_duckhouse import ingest_dlt

        source = _make_source(registry, name="s", schema_contract="freeze")
        t1 = pa.table({"id": [1], "name": ["a"]})
        t2 = pa.table({"id": [2], "name": ["b"], "extra": [9]})
        monkeypatch.setattr(
            ingest_dlt,
            "_load_pipeline",
            lambda: _stub_pipeline(
                registry,
                {"s": source},
                {"s": {"s_t": _write_parquet(tmp_path, "f0", t1)}},
            ),
        )
        ingest_dlt.run_ingest_dlt("s")

        monkeypatch.setattr(
            ingest_dlt,
            "_load_pipeline",
            lambda: _stub_pipeline(
                registry,
                {"s": source},
                {"s": {"s_t": _write_parquet(tmp_path, "f1", t2)}},
            ),
        )
        with pytest.raises(ingest_dlt.IngestDltError, match="extra"):
            ingest_dlt.run_ingest_dlt("s")

    def test_discard_value_drops_new_columns(
        self, pyiceberg_env, registry, tmp_path, monkeypatch
    ):
        from datahut_duckhouse import ingest_dlt

        source = _make_source(registry, name="s", schema_contract="discard_value")
        t1 = pa.table({"id": [1], "name": ["a"]})
        t2 = pa.table({"id": [2], "name": ["b"], "extra": [9]})
        monkeypatch.setattr(
            ingest_dlt,
            "_load_pipeline",
            lambda: _stub_pipeline(
                registry,
                {"s": source},
                {"s": {"s_t": _write_parquet(tmp_path, "d0", t1)}},
            ),
        )
        ingest_dlt.run_ingest_dlt("s")

        monkeypatch.setattr(
            ingest_dlt,
            "_load_pipeline",
            lambda: _stub_pipeline(
                registry,
                {"s": source},
                {"s": {"s_t": _write_parquet(tmp_path, "d1", t2)}},
            ),
        )
        ingest_dlt.run_ingest_dlt("s")  # must not raise

        from datahut_duckhouse.ingest import connect

        backend = connect()
        table = backend.catalog.load_table("default.s_t")
        assert "extra" not in table.schema().as_arrow().names


class TestOneCommitPerTablePerRun:
    def test_snapshot_count_increases_by_one_per_run(
        self, pyiceberg_env, registry, tmp_path, monkeypatch
    ):
        from datahut_duckhouse import ingest_dlt

        source = _make_source(registry, name="s", write_disposition="append")
        for i in range(3):
            t = pa.table({"id": [i]})
            monkeypatch.setattr(
                ingest_dlt,
                "_load_pipeline",
                lambda t=t, i=i: _stub_pipeline(
                    registry,
                    {"s": source},
                    {"s": {"s_t": _write_parquet(tmp_path, f"snap{i}", t)}},
                ),
            )
            ingest_dlt.run_ingest_dlt("s")

        from datahut_duckhouse.ingest import connect

        backend = connect()
        snapshots = list(backend.catalog.load_table("default.s_t").snapshots())
        # 1 create (run 0) + 2 appends (runs 1, 2) = 3 commits for 3 runs,
        # regardless of how many Parquet chunks a connector produced per run.
        assert len(snapshots) == 3


class TestRunLog:
    def test_successful_run_is_logged_to_iceberg(
        self, pyiceberg_env, registry, tmp_path, monkeypatch
    ):
        from datahut_duckhouse import ingest_dlt
        from datahut_duckhouse.ingest_run_log import RUN_LOG_TABLE

        source = _make_source(registry, name="s")
        t1 = pa.table({"id": [1], "name": ["a"]})
        monkeypatch.setattr(
            ingest_dlt,
            "_load_pipeline",
            lambda: _stub_pipeline(
                registry,
                {"s": source},
                {"s": {"s_t": _write_parquet(tmp_path, "l0", t1)}},
            ),
        )
        ingest_dlt.run_ingest_dlt("s")

        from datahut_duckhouse.ingest import connect

        backend = connect()
        assert RUN_LOG_TABLE in set(backend.list_tables())
        rows = (
            backend.catalog.load_table(f"default.{RUN_LOG_TABLE}")
            .scan()
            .to_arrow()
            .to_pylist()
        )
        assert len(rows) == 1
        assert rows[0]["source"] == "s"
        assert rows[0]["table_name"] == "s_t"
        assert rows[0]["status"] == "ok"
        assert rows[0]["rows"] == 1

    def test_failed_table_is_logged_with_error(
        self, pyiceberg_env, registry, tmp_path, monkeypatch
    ):
        from datahut_duckhouse import ingest_dlt
        from datahut_duckhouse.ingest_run_log import RUN_LOG_TABLE

        source = _make_source(registry, name="s", schema_contract="freeze")
        t1 = pa.table({"id": [1], "name": ["a"]})
        t2 = pa.table({"id": [2], "name": ["b"], "extra": [1]})
        monkeypatch.setattr(
            ingest_dlt,
            "_load_pipeline",
            lambda: _stub_pipeline(
                registry,
                {"s": source},
                {"s": {"s_t": _write_parquet(tmp_path, "le0", t1)}},
            ),
        )
        ingest_dlt.run_ingest_dlt("s")

        monkeypatch.setattr(
            ingest_dlt,
            "_load_pipeline",
            lambda: _stub_pipeline(
                registry,
                {"s": source},
                {"s": {"s_t": _write_parquet(tmp_path, "le1", t2)}},
            ),
        )
        with pytest.raises(ingest_dlt.IngestDltError):
            ingest_dlt.run_ingest_dlt("s")

        from datahut_duckhouse.ingest import connect

        backend = connect()
        rows = (
            backend.catalog.load_table(f"default.{RUN_LOG_TABLE}")
            .scan()
            .to_arrow()
            .to_pylist()
        )
        statuses = [r["status"] for r in rows]
        assert statuses.count("ok") == 1
        assert statuses.count("error") == 1

    def test_falls_back_to_sqlite_when_iceberg_logging_fails(
        self, tmp_path, monkeypatch
    ):
        from datahut_duckhouse import ingest_run_log
        from datahut_duckhouse.ingest_run_log import RunLog

        fallback_db = tmp_path / "fallback_runs.db"
        monkeypatch.setattr(ingest_run_log, "FALLBACK_DB", fallback_db)

        class _BrokenBackend:
            def list_tables(self):
                raise RuntimeError("catalog is down, simulated")

        run_log = RunLog(_BrokenBackend())  # __init__ hits list_tables() -> falls back
        assert run_log._use_iceberg is False
        assert fallback_db.exists()

        run_log.record("s", "s_t", 0.0, 1.0, 5, "ok")

        with sqlite3.connect(fallback_db) as conn:
            logged = conn.execute(
                "SELECT source, table_name, rows FROM ingest_runs"
            ).fetchall()
        assert logged == [("s", "s_t", 5)]
