"""Tests for ``batch_rows`` (fix/ingest-batching-grpc, part A): the engine
splits a table's Parquet file into ``batch_rows``-sized Arrow batches for
``append``/``merge`` (one commit per batch), forces exactly one commit for
``replace`` regardless of ``batch_rows`` (with a warning), and the run log
records the real commit count -- see ``ingestion/sinks/base.py``'s
docstring for why ``replace`` can't be split safely.

Same real in-process Flight server as ``test_engine.py`` (no Docker, no S3
credentials, real ``HybridBackend``, real pyiceberg snapshots).
"""

from __future__ import annotations

import types
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as papq
import pytest

from ingestion.config.models import SourceConfig
from ingestion.engine import dlt_engine as dlt_engine_module
from ingestion.engine.dlt_engine import DltIngestor


@pytest.fixture(autouse=True)
def _matching_pyiceberg_env(flight_server, monkeypatch):
    monkeypatch.delenv("DHD_ENGINE", raising=False)
    monkeypatch.setenv("ICEBERG_NAMESPACE", "default")

    from datahut_duckhouse.ingest import connect as ingest_connect
    from ingestion.runs.models import RUN_LOG_TABLE

    engine = ingest_connect()
    for table_name in ("s_t", RUN_LOG_TABLE):
        if table_name in engine.list_tables():
            engine.drop_table(table_name)


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


def _engine(flight_backend, monkeypatch, written: dict[str, Path], echo=None) -> DltIngestor:
    monkeypatch.setattr(
        dlt_engine_module.connector_registry,
        "get",
        lambda family: _fake_connector(written),
    )
    from datahut_duckhouse.ingest import connect as pyiceberg_connect

    kwargs = {"run_log_echo": echo} if echo is not None else {}
    return DltIngestor(flight_backend, pyiceberg_connect, **kwargs)


def _rows_via_flight(table_name: str) -> list[dict]:
    from datahut_duckhouse.connection import connect

    backend = connect()
    return backend.sql(f"SELECT * FROM {table_name}").to_pyarrow().to_pylist()


def _snapshot_count(table_name: str) -> int:
    from datahut_duckhouse.ingest import connect as ingest_connect

    return len(list(ingest_connect().catalog.load_table(f"default.{table_name}").snapshots()))


class TestAppendChunking:
    def test_splits_into_batch_rows_sized_commits(self, flight_server, tmp_path, monkeypatch):
        from datahut_duckhouse.connection import connect as flight_connect

        source = _make_source(name="s", write_disposition="append", batch_rows=2)
        t = pa.table({"id": [1, 2, 3, 4, 5]})
        engine = _engine(
            flight_connect(), monkeypatch, {"s_t": _write_parquet(tmp_path, "a", t)}
        )
        result = engine.run(source)

        # 5 rows / 2 per batch = 3 batches (2, 2, 1) = 3 commits.
        assert result.tables[0].commits == 3
        assert result.tables[0].rows == 5
        assert _snapshot_count("s_t") == 3
        rows = _rows_via_flight("s_t")
        assert {r["id"] for r in rows} == {1, 2, 3, 4, 5}

    def test_run_log_records_the_real_commit_count(self, flight_server, tmp_path, monkeypatch):
        from datahut_duckhouse.connection import connect as flight_connect
        from ingestion.runs.models import RUN_LOG_TABLE

        source = _make_source(name="s", write_disposition="append", batch_rows=2)
        t = pa.table({"id": [1, 2, 3, 4, 5]})
        engine = _engine(
            flight_connect(), monkeypatch, {"s_t": _write_parquet(tmp_path, "a", t)}
        )
        engine.run(source)

        log_rows = _rows_via_flight(RUN_LOG_TABLE)
        assert log_rows[0]["commits"] == 3

    def test_default_batch_rows_is_one_commit_for_a_small_table(
        self, flight_server, tmp_path, monkeypatch
    ):
        """500_000 default: a handful of rows still fits in a single batch,
        matching the pre-fix "one commit per table per run" behavior."""
        from datahut_duckhouse.connection import connect as flight_connect

        source = _make_source(name="s", write_disposition="append")
        assert source.batch_rows == 500_000
        t = pa.table({"id": [1, 2, 3]})
        engine = _engine(
            flight_connect(), monkeypatch, {"s_t": _write_parquet(tmp_path, "a", t)}
        )
        result = engine.run(source)
        assert result.tables[0].commits == 1

    def test_null_batch_rows_disables_chunking(self, flight_server, tmp_path, monkeypatch):
        from datahut_duckhouse.connection import connect as flight_connect

        source = _make_source(name="s", write_disposition="append", batch_rows=None)
        t = pa.table({"id": [1, 2, 3, 4, 5]})
        engine = _engine(
            flight_connect(), monkeypatch, {"s_t": _write_parquet(tmp_path, "a", t)}
        )
        result = engine.run(source)
        assert result.tables[0].commits == 1
        assert _snapshot_count("s_t") == 1


class TestMergeChunking:
    def test_splits_into_batch_rows_sized_commits_and_upserts_correctly(
        self, flight_server, tmp_path, monkeypatch
    ):
        from datahut_duckhouse.connection import connect as flight_connect

        source = _make_source(
            name="s", write_disposition="merge", primary_key=["id"], batch_rows=2
        )
        t = pa.table({"id": [1, 2, 3, 4, 5], "v": ["a", "b", "c", "d", "e"]})
        engine = _engine(
            flight_connect(), monkeypatch, {"s_t": _write_parquet(tmp_path, "m", t)}
        )
        result = engine.run(source)

        assert result.tables[0].commits == 3
        rows = _rows_via_flight("s_t")
        by_id = {r["id"]: r["v"] for r in rows}
        assert by_id == {1: "a", 2: "b", 3: "c", 4: "d", 5: "e"}


class TestReplaceIgnoresBatchRows:
    def test_forces_a_single_commit_and_warns(self, flight_server, tmp_path, monkeypatch):
        from datahut_duckhouse.connection import connect as flight_connect

        messages: list[str] = []
        source = _make_source(name="s", write_disposition="replace", batch_rows=2)
        t = pa.table({"id": [1, 2, 3, 4, 5]})
        engine = _engine(
            flight_connect(),
            monkeypatch,
            {"s_t": _write_parquet(tmp_path, "r", t)},
            echo=messages.append,
        )
        result = engine.run(source)

        assert result.tables[0].commits == 1
        assert _snapshot_count("s_t") == 1
        rows = _rows_via_flight("s_t")
        assert {r["id"] for r in rows} == {1, 2, 3, 4, 5}
        assert any(
            "replace" in m and "batch_rows=2" in m and "ignored" in m for m in messages
        )

    def test_no_warning_when_batch_rows_is_null(self, flight_server, tmp_path, monkeypatch):
        from datahut_duckhouse.connection import connect as flight_connect

        messages: list[str] = []
        source = _make_source(name="s", write_disposition="replace", batch_rows=None)
        t = pa.table({"id": [1, 2]})
        engine = _engine(
            flight_connect(),
            monkeypatch,
            {"s_t": _write_parquet(tmp_path, "r", t)},
            echo=messages.append,
        )
        engine.run(source)
        assert not any("batch_rows" in m for m in messages)


class TestKillMidBatch:
    """A process killed between two batches (not between two whole tables,
    which ``TestCrashThenResume`` in ``test_engine.py`` already covers):
    simulated by making the sink's underlying write blow up on the second
    batch, after the first has already committed."""

    def _engine_failing_on_nth_write(
        self, flight_backend, monkeypatch, written, fail_on_call: int, source
    ):
        engine = _engine(flight_backend, monkeypatch, written)
        sink = engine._sink_for(source)  # DirectSink for merge, FlightSink otherwise
        calls = {"n": 0}
        original_write = sink.write

        def _write(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] == fail_on_call:
                raise RuntimeError("simulated kill mid-batch")
            return original_write(*args, **kwargs)

        monkeypatch.setattr(sink, "write", _write)
        return engine

    def test_merge_resumes_without_duplication_after_a_mid_batch_kill(
        self, flight_server, tmp_path, monkeypatch
    ):
        from datahut_duckhouse.connection import connect as flight_connect

        source = _make_source(
            name="s", write_disposition="merge", primary_key=["id"], batch_rows=2
        )
        t = pa.table({"id": [1, 2, 3, 4, 5], "v": ["a", "b", "c", "d", "e"]})

        crashing_engine = self._engine_failing_on_nth_write(
            flight_connect(),
            monkeypatch,
            {"s_t": _write_parquet(tmp_path, "m0", t)},
            fail_on_call=2,
            source=source,
        )
        with pytest.raises(RuntimeError, match="simulated kill mid-batch"):
            crashing_engine.run(source)

        # First batch's commit is durable even though the run as a whole failed.
        rows_after_crash = _rows_via_flight("s_t")
        assert {r["id"] for r in rows_after_crash} == {1, 2}

        resumed = _engine(
            flight_connect(),
            monkeypatch,
            {"s_t": _write_parquet(tmp_path, "m1", t)},
        )
        resumed.run(source)
        rows = _rows_via_flight("s_t")
        by_id = {r["id"]: r["v"] for r in rows}
        # All 5 rows present exactly once -- no duplicates from re-merging
        # the first two rows that had already landed.
        assert by_id == {1: "a", 2: "b", 3: "c", 4: "d", 5: "e"}

    def test_replace_cannot_leave_a_half_replaced_table(
        self, flight_server, tmp_path, monkeypatch
    ):
        """replace forces a single commit, so there is no "mid-batch" point
        to kill at -- a kill either happens before the one commit (old data
        intact) or after it (new data fully in place), never in between."""
        from datahut_duckhouse.connection import connect as flight_connect

        source = _make_source(name="s", write_disposition="replace", batch_rows=2)
        t0 = pa.table({"id": [100]})
        seed = _engine(
            flight_connect(), monkeypatch, {"s_t": _write_parquet(tmp_path, "r0", t0)}
        )
        seed.run(source)

        t = pa.table({"id": [1, 2, 3, 4, 5]})
        crashing_engine = self._engine_failing_on_nth_write(
            flight_connect(),
            monkeypatch,
            {"s_t": _write_parquet(tmp_path, "r1", t)},
            fail_on_call=1,
            source=source,
        )
        with pytest.raises(RuntimeError, match="simulated kill mid-batch"):
            crashing_engine.run(source)

        # The single commit never happened -- previous snapshot untouched.
        rows = _rows_via_flight("s_t")
        assert {r["id"] for r in rows} == {100}
