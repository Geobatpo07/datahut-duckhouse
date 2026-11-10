"""Tests for ``ingestion.engine.dlt_engine.DltIngestor`` (Vague 1b, task 3;
rewritten for the follow-up review's step 0, then moved here in step A's
restructuring): run log, write_disposition dispatch (replace/append via
Flight, merge direct via pyiceberg), schema contracts, and the "one
Iceberg commit per table per run" invariant.

Uses the real in-process Flight server (``flight_server`` fixture, shared
with ``tests/test_cli.py`` via ``tests/conftest.py``) -- no Docker, no S3
credentials, but a real ``HybridBackend`` and real pyiceberg writes,
because ``DltIngestor`` needs a reachable Flight server unconditionally
(``replace``/``append`` write through it; ``merge`` still writes direct,
but only once ``catalog_guard`` proves the client and server share one
catalog). A fake connector stands in for ``ingestion.connectors.registry``
(no real sources.yml/network involved -- that's
``tests/ingestion/test_connectors_*.py`` and
``tests/integration/test_multi_engine.py``'s job).
"""

from __future__ import annotations

import sqlite3
import types
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

from ingestion.config.models import SourceConfig
from ingestion.engine import dlt_engine as dlt_engine_module
from ingestion.engine.dlt_engine import ConnectorError, DltIngestor
from ingestion.sinks.base import SchemaContractViolationError
from ingestion.sinks.catalog_guard import CatalogMismatchError


@pytest.fixture(autouse=True)
def _matching_pyiceberg_env(flight_server, monkeypatch):
    """Point this process's own pyiceberg client (used for ``merge`` /
    ``DirectSink``, and for direct-catalog assertions below) at the exact
    same local warehouse/catalog the ``flight_server`` fixture just
    started, so ``catalog_guard.check_same_catalog`` sees a real match for
    the 'merge' tests. Also drops this module's fixed table names before
    each test: ``flight_server`` is module-scoped, so every test in this
    module shares one real catalog and would otherwise see the previous
    test's leftover rows."""
    monkeypatch.delenv("DHD_ENGINE", raising=False)
    monkeypatch.setenv("ICEBERG_NAMESPACE", "default")

    from datahut_duckhouse.connection import connect as flight_connect

    backend = flight_connect()
    for table_name in backend.list_tables():
        backend.drop_table(table_name)


def _make_source(**overrides):
    defaults = {
        "name": "t",
        "family": "files",
        "connector": "local_file",
        "path": "x.csv",
        "format": "csv",
    }
    defaults.update(overrides)
    return SourceConfig(**defaults)


def _write_parquet(tmp_path: Path, name: str, table: pa.Table) -> Path:
    path = tmp_path / f"{name}.parquet"
    papq.write_table(table, path)
    return path


def _fake_connector(written: dict[str, Path]):
    return types.SimpleNamespace(build_resources=lambda source, work_dir: written)


def _engine(flight_backend, monkeypatch, written: dict[str, Path]) -> DltIngestor:
    monkeypatch.setattr(
        dlt_engine_module.connector_registry,
        "get",
        lambda family: _fake_connector(written),
    )
    from datahut_duckhouse.ingest import connect as pyiceberg_connect

    return DltIngestor(flight_backend, pyiceberg_connect)


def _rows_via_flight(table_name: str) -> list[dict]:
    """Read back a table through the real Flight server -- proves the data
    actually landed where `dhd query` already looks, not just somewhere
    pyiceberg can see."""
    from datahut_duckhouse.connection import connect

    backend = connect()
    return backend.sql(f"SELECT * FROM {table_name}").to_pyarrow().to_pylist()


class TestWriteDispositionDispatch:
    def test_replace_overwrites_on_rerun(self, flight_server, tmp_path, monkeypatch):
        from datahut_duckhouse.connection import connect as flight_connect

        source = _make_source(name="s", write_disposition="replace")
        t1 = pa.table({"id": [1, 2], "name": ["a", "b"]})
        engine = _engine(
            flight_connect(), monkeypatch, {"s_t": _write_parquet(tmp_path, "r1", t1)}
        )
        engine.run(source)

        t2 = pa.table({"id": [3], "name": ["c"]})
        engine2 = _engine(
            flight_connect(), monkeypatch, {"s_t": _write_parquet(tmp_path, "r2", t2)}
        )
        engine2.run(source)

        rows = _rows_via_flight("s_t")
        assert rows == [{"id": 3, "name": "c"}]  # fully replaced, not merged

    def test_append_accumulates_on_rerun(self, flight_server, tmp_path, monkeypatch):
        from datahut_duckhouse.connection import connect as flight_connect

        source = _make_source(name="s", write_disposition="append")
        for i, t in enumerate(
            [pa.table({"id": [1], "name": ["a"]}), pa.table({"id": [2], "name": ["b"]})]
        ):
            engine = _engine(
                flight_connect(),
                monkeypatch,
                {"s_t": _write_parquet(tmp_path, f"a{i}", t)},
            )
            engine.run(source)

        rows = _rows_via_flight("s_t")
        assert {r["id"] for r in rows} == {1, 2}  # both runs' rows present

    def test_merge_upserts_by_primary_key(self, flight_server, tmp_path, monkeypatch):
        from datahut_duckhouse.connection import connect as flight_connect

        source = _make_source(name="s", write_disposition="merge", primary_key=["id"])
        t1 = pa.table({"id": [1, 2], "name": ["a", "b"]})
        # Second run: id=1 updated, id=3 new -- merge must not duplicate id=1
        # or id=2, and must pick up the new id=3 (task 5's incremental story).
        t2 = pa.table({"id": [1, 3], "name": ["a-updated", "c"]})
        for i, t in enumerate([t1, t2]):
            engine = _engine(
                flight_connect(),
                monkeypatch,
                {"s_t": _write_parquet(tmp_path, f"m{i}", t)},
            )
            engine.run(source)

        rows = _rows_via_flight("s_t")
        by_id = {r["id"]: r["name"] for r in rows}
        assert by_id == {1: "a-updated", 2: "b", 3: "c"}


class TestSchemaContract:
    def test_evolve_widens_the_table(self, flight_server, tmp_path, monkeypatch):
        from datahut_duckhouse.connection import connect as flight_connect

        source = _make_source(
            name="s", schema_contract="evolve", write_disposition="append"
        )
        t1 = pa.table({"id": [1], "name": ["a"]})
        t2 = pa.table({"id": [2], "name": ["b"], "extra": [9]})
        for i, t in enumerate([t1, t2]):
            engine = _engine(
                flight_connect(),
                monkeypatch,
                {"s_t": _write_parquet(tmp_path, f"e{i}", t)},
            )
            engine.run(source)

        rows = _rows_via_flight("s_t")
        assert {"id": 1, "name": "a", "extra": None} in rows
        assert {"id": 2, "name": "b", "extra": 9} in rows

    def test_freeze_rejects_new_columns(self, flight_server, tmp_path, monkeypatch):
        from datahut_duckhouse.connection import connect as flight_connect

        source = _make_source(name="s", schema_contract="freeze")
        t1 = pa.table({"id": [1], "name": ["a"]})
        t2 = pa.table({"id": [2], "name": ["b"], "extra": [9]})
        engine = _engine(
            flight_connect(), monkeypatch, {"s_t": _write_parquet(tmp_path, "f0", t1)}
        )
        engine.run(source)

        engine2 = _engine(
            flight_connect(), monkeypatch, {"s_t": _write_parquet(tmp_path, "f1", t2)}
        )
        with pytest.raises(SchemaContractViolationError, match="extra"):
            engine2.run(source)

    def test_discard_value_drops_new_columns(
        self, flight_server, tmp_path, monkeypatch
    ):
        from datahut_duckhouse.connection import connect as flight_connect

        source = _make_source(name="s", schema_contract="discard_value")
        t1 = pa.table({"id": [1], "name": ["a"]})
        t2 = pa.table({"id": [2], "name": ["b"], "extra": [9]})
        engine = _engine(
            flight_connect(), monkeypatch, {"s_t": _write_parquet(tmp_path, "d0", t1)}
        )
        engine.run(source)

        engine2 = _engine(
            flight_connect(), monkeypatch, {"s_t": _write_parquet(tmp_path, "d1", t2)}
        )
        engine2.run(source)  # must not raise

        rows = _rows_via_flight("s_t")
        assert "extra" not in rows[0]


class TestOneCommitPerTablePerRun:
    def test_snapshot_count_increases_by_one_per_run(
        self, flight_server, tmp_path, monkeypatch
    ):
        from datahut_duckhouse.connection import connect as flight_connect
        from datahut_duckhouse.ingest import connect as ingest_connect

        source = _make_source(name="s", write_disposition="append")
        for i in range(3):
            t = pa.table({"id": [i]})
            engine = _engine(
                flight_connect(),
                monkeypatch,
                {"s_t": _write_parquet(tmp_path, f"snap{i}", t)},
            )
            engine.run(source)

        engine_r = ingest_connect()
        snapshots = list(engine_r.catalog.load_table("default.s_t").snapshots())
        # 1 create (run 0) + 2 appends (runs 1, 2) = 3 commits for 3 runs,
        # regardless of how many Parquet chunks a connector produced per run.
        assert len(snapshots) == 3


class TestRunLog:
    def test_successful_run_is_logged_and_visible_via_flight(
        self, flight_server, tmp_path, monkeypatch
    ):
        from datahut_duckhouse.connection import connect as flight_connect
        from ingestion.runs.models import RUN_LOG_TABLE

        source = _make_source(name="s")
        t1 = pa.table({"id": [1], "name": ["a"]})
        engine = _engine(
            flight_connect(), monkeypatch, {"s_t": _write_parquet(tmp_path, "l0", t1)}
        )
        engine.run(source)

        rows = _rows_via_flight(RUN_LOG_TABLE)
        assert len(rows) == 1
        assert rows[0]["source"] == "s"
        assert rows[0]["table_name"] == "s_t"
        assert rows[0]["status"] == "ok"
        assert rows[0]["rows"] == 1

    def test_failed_table_is_logged_with_error(
        self, flight_server, tmp_path, monkeypatch
    ):
        from datahut_duckhouse.connection import connect as flight_connect
        from ingestion.runs.models import RUN_LOG_TABLE

        source = _make_source(name="s", schema_contract="freeze")
        t1 = pa.table({"id": [1], "name": ["a"]})
        t2 = pa.table({"id": [2], "name": ["b"], "extra": [1]})
        engine = _engine(
            flight_connect(), monkeypatch, {"s_t": _write_parquet(tmp_path, "le0", t1)}
        )
        engine.run(source)

        engine2 = _engine(
            flight_connect(), monkeypatch, {"s_t": _write_parquet(tmp_path, "le1", t2)}
        )
        with pytest.raises(SchemaContractViolationError):
            engine2.run(source)

        rows = _rows_via_flight(RUN_LOG_TABLE)
        statuses = [r["status"] for r in rows]
        assert statuses.count("ok") == 1
        assert statuses.count("error") == 1

    def test_falls_back_to_sqlite_when_iceberg_logging_fails(
        self, tmp_path, monkeypatch
    ):
        from ingestion.runs.store import RunLog

        fallback_db = tmp_path / "fallback_runs.db"
        monkeypatch.setenv("DHD_INGEST_RUN_LOG_DB", str(fallback_db))

        class _BrokenBackend:
            def list_tables(self):
                raise RuntimeError("catalog is down, simulated")

        messages: list[str] = []
        run_log = RunLog(_BrokenBackend(), echo=messages.append)
        assert run_log._use_sqlite is True
        assert fallback_db.exists()
        assert any("catalog is down" in m for m in messages)

        run_log.record("s", "s_t", 0.0, 1.0, 5, "ok")

        with sqlite3.connect(fallback_db) as conn:
            logged = conn.execute(
                "SELECT source, table_name, rows FROM ingest_runs"
            ).fetchall()
        assert logged == [("s", "s_t", 5)]


class TestConnectorErrorWrapping:
    def test_connector_failure_is_wrapped_with_the_source_name(
        self, flight_server, monkeypatch
    ):
        from datahut_duckhouse.connection import connect as flight_connect

        def _boom(source, work_dir):
            raise RuntimeError("boom")

        monkeypatch.setattr(
            dlt_engine_module.connector_registry,
            "get",
            lambda family: types.SimpleNamespace(build_resources=_boom),
        )
        from datahut_duckhouse.ingest import connect as pyiceberg_connect

        engine = DltIngestor(flight_connect(), pyiceberg_connect)
        with pytest.raises(ConnectorError, match="boom_source"):
            engine.run(_make_source(name="boom_source"))


class TestCatalogGuard:
    """``merge`` refuses to write direct-pyiceberg when the client and the
    Flight server don't share a catalog -- the follow-up review's step 0
    decision. ``_matching_pyiceberg_env`` (autouse) makes every other test
    in this module a *matching* client; this one deliberately breaks the
    match to prove the refusal is real, not just untested."""

    def test_merge_is_refused_on_a_catalog_mismatch(
        self, flight_server, tmp_path, monkeypatch
    ):
        from datahut_duckhouse.connection import connect as flight_connect

        # A different warehouse path than the one `flight_server` is using
        # -- same mechanism as a host-run `dhd ingest-dlt` against a
        # container Flight server, reproduced without Docker.
        monkeypatch.setenv("ICEBERG_WAREHOUSE_PATH", str(tmp_path / "other_wh"))

        source = _make_source(name="s", write_disposition="merge", primary_key=["id"])
        t1 = pa.table({"id": [1], "name": ["a"]})
        engine = _engine(
            flight_connect(), monkeypatch, {"s_t": _write_parquet(tmp_path, "cg0", t1)}
        )
        with pytest.raises(CatalogMismatchError, match="different Iceberg"):
            engine.run(source)


class TestCrashThenResume:
    """A process killed mid-run (step 5's crash-resume experiment,
    docs/DLT_EVAL.md) leaves no partial commit -- Iceberg only exposes a
    snapshot once a write fully completes -- so a resumed run never
    duplicates or loses data under ``replace``/``merge``. Simulated here
    as a connector that raises instead of returning (the "crash" point:
    before any table's data is even handed to a sink), proving the engine
    itself adds no partial state of its own on top of Iceberg's atomicity."""

    def _crashing_connector(self):
        def _boom(source, work_dir):
            raise RuntimeError("simulated crash mid-extraction")

        return types.SimpleNamespace(build_resources=_boom)

    def _run_with_crash_then_resume(
        self, flight_server, tmp_path, monkeypatch, write_disposition, primary_key=None
    ):
        from datahut_duckhouse.connection import connect as flight_connect

        source = _make_source(
            name="s", write_disposition=write_disposition, primary_key=primary_key
        )
        t1 = pa.table({"id": [1, 2], "name": ["a", "b"]})

        monkeypatch.setattr(
            dlt_engine_module.connector_registry,
            "get",
            lambda family: self._crashing_connector(),
        )
        from datahut_duckhouse.ingest import connect as pyiceberg_connect

        crashing_engine = DltIngestor(flight_connect(), pyiceberg_connect)
        with pytest.raises(RuntimeError, match="simulated crash"):
            crashing_engine.run(source)
        assert "s_t" not in flight_connect().list_tables()  # no partial commit

        resumed = _engine(
            flight_connect(),
            monkeypatch,
            {"s_t": _write_parquet(tmp_path, "resume", t1)},
        )
        resumed.run(source)
        rows = _rows_via_flight("s_t")
        assert {r["id"] for r in rows} == {1, 2}
        return rows

    def test_replace_resumes_without_duplication(
        self, flight_server, tmp_path, monkeypatch
    ):
        rows = self._run_with_crash_then_resume(
            flight_server, tmp_path, monkeypatch, "replace"
        )
        assert len(rows) == 2  # not 4 -- the crashed run committed nothing

    def test_merge_resumes_without_duplication(
        self, flight_server, tmp_path, monkeypatch
    ):
        rows = self._run_with_crash_then_resume(
            flight_server, tmp_path, monkeypatch, "merge", primary_key=["id"]
        )
        assert len(rows) == 2  # not 4 -- the crashed run committed nothing
