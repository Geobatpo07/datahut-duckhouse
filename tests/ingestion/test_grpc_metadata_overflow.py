"""Part B: the "gRPC metadata size" bug (fix/ingest-batching-grpc).

Root cause, confirmed by reading ``xorq/flight/server.py`` (third-party,
not modified) and reproducing live against the real in-process Flight
server (no mocks): xorq's ``do_put`` is not wrapped in the
``bounded_status`` decorator ``do_exchange`` has, so a server-side
exception whose message+traceback exceeds gRPC's status-metadata budget
(~4KB on the wire, per that module's own comment) reaches the client as
a bare ``grpc_status=8 RESOURCE_EXHAUSTED, "received initial metadata
size exceeds limit"`` instead of the real Python exception -- the real
cause is masked, not just truncated.

Triggered here by a genuine schema mismatch across many columns (not
fault injection): a table with N int64 columns, then an append of the
same N column names as strings. This is a real scenario a misconfigured
or drifted source could hit, and reproduces the exact gRPC message
reported live (see docs/DLT_EVAL.md's part B section).

Not fixable here (xorq is a third-party dependency; HybridBackend is
off-limits for this task) -- what this project's own code CAN do, and
what these tests verify:

1. ``ingestion.engine.dlt_engine`` catches this specific gRPC failure
   and re-raises it as a ``SinkError`` with an actionable message
   (instead of an opaque ``ArrowInvalid`` crashing the whole run
   uncaught and unlogged).
2. ``ingestion.runs.store.RunLog`` bounds how long an ``error`` string
   it logs can be, regardless of cause.
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
from ingestion.sinks.base import SinkError

N_COLS = 4000


@pytest.fixture(autouse=True)
def _matching_pyiceberg_env(flight_server, monkeypatch):
    monkeypatch.delenv("DHD_ENGINE", raising=False)
    monkeypatch.setenv("ICEBERG_NAMESPACE", "default")

    from datahut_duckhouse.ingest import connect as ingest_connect
    from ingestion.runs.models import RUN_LOG_TABLE

    engine = ingest_connect()
    for table_name in ("wide_t", RUN_LOG_TABLE):
        if table_name in engine.list_tables():
            engine.drop_table(table_name)


def _write_parquet(tmp_path: Path, name: str, table: pa.Table) -> Path:
    path = tmp_path / f"{name}.parquet"
    papq.write_table(table, path)
    return path


def _engine(flight_backend, monkeypatch, written: dict[str, Path]) -> DltIngestor:
    monkeypatch.setattr(
        dlt_engine_module.connector_registry,
        "get",
        lambda family: types.SimpleNamespace(
            build_resources=lambda source, work_dir: written
        ),
    )
    from datahut_duckhouse.ingest import connect as pyiceberg_connect

    return DltIngestor(flight_backend, pyiceberg_connect)


def _wide_source(**overrides) -> SourceConfig:
    defaults = {
        "name": "wide",
        "family": "files",
        "connector": "local_file",
        "path": "x.parquet",
        "format": "parquet",
        "write_disposition": "append",
        "batch_rows": None,
    }
    defaults.update(overrides)
    return SourceConfig(**defaults)


class TestGrpcMetadataOverflow:
    def test_schema_mismatch_across_many_columns_becomes_a_clear_sink_error(
        self, flight_server, tmp_path, monkeypatch
    ):
        from datahut_duckhouse.connection import connect as flight_connect

        int_cols = pa.table({f"c{i}": [1] for i in range(N_COLS)})
        seed = _engine(
            flight_connect(),
            monkeypatch,
            {"wide_t": _write_parquet(tmp_path, "seed", int_cols)},
        )
        seed.run(_wide_source())

        string_cols = pa.table({f"c{i}": ["x"] for i in range(N_COLS)})
        mismatched = _engine(
            flight_connect(),
            monkeypatch,
            {"wide_t": _write_parquet(tmp_path, "mismatch", string_cols)},
        )
        with pytest.raises(SinkError, match="metadata size exceeds limit") as exc_info:
            mismatched.run(_wide_source())

        message = str(exc_info.value)
        assert "wide_t" in message
        assert "server logs" in message.lower() or "server's own logs" in message.lower()

    def test_the_failure_is_logged_not_lost(self, flight_server, tmp_path, monkeypatch):
        from datahut_duckhouse.connection import connect as flight_connect
        from ingestion.runs.models import RUN_LOG_TABLE

        int_cols = pa.table({f"c{i}": [1] for i in range(N_COLS)})
        seed = _engine(
            flight_connect(),
            monkeypatch,
            {"wide_t": _write_parquet(tmp_path, "seed", int_cols)},
        )
        seed.run(_wide_source())

        string_cols = pa.table({f"c{i}": ["x"] for i in range(N_COLS)})
        mismatched = _engine(
            flight_connect(),
            monkeypatch,
            {"wide_t": _write_parquet(tmp_path, "mismatch", string_cols)},
        )
        with pytest.raises(SinkError):
            mismatched.run(_wide_source())

        from datahut_duckhouse.connection import connect as flight_connect2

        backend = flight_connect2()
        rows = backend.sql(f"SELECT * FROM {RUN_LOG_TABLE}").to_pyarrow().to_pylist()
        error_rows = [r for r in rows if r["status"] == "error"]
        assert len(error_rows) == 1
        assert "metadata size exceeds limit" in error_rows[0]["error"]


class TestRunLogBoundsErrorLength:
    def test_a_huge_error_string_is_truncated_not_rejected(self, tmp_path, monkeypatch):
        from ingestion.runs.store import RunLog

        fallback_db = tmp_path / "fallback.db"
        monkeypatch.setenv("DHD_INGEST_RUN_LOG_DB", str(fallback_db))

        class _BrokenBackend:
            def list_tables(self):
                raise RuntimeError("catalog is down, simulated")

        run_log = RunLog(_BrokenBackend(), echo=lambda m: None)
        huge_error = "x" * 50_000
        run_log.record("s", "t", 0.0, 1.0, 0, "error", huge_error)

        import sqlite3

        with sqlite3.connect(fallback_db) as conn:
            (stored,) = conn.execute("SELECT error FROM ingest_runs").fetchone()
        assert len(stored) < len(huge_error)
        assert "truncated" in stored

    def test_a_short_error_string_is_untouched(self, tmp_path, monkeypatch):
        from ingestion.runs.store import RunLog

        fallback_db = tmp_path / "fallback.db"
        monkeypatch.setenv("DHD_INGEST_RUN_LOG_DB", str(fallback_db))

        class _BrokenBackend:
            def list_tables(self):
                raise RuntimeError("catalog is down, simulated")

        run_log = RunLog(_BrokenBackend(), echo=lambda m: None)
        run_log.record("s", "t", 0.0, 1.0, 0, "error", "short message")

        import sqlite3

        with sqlite3.connect(fallback_db) as conn:
            (stored,) = conn.execute("SELECT error FROM ingest_runs").fetchone()
        assert stored == "short message"
