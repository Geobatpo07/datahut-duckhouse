"""Run log for ``dhd ingest`` (Vague 1b, task 3; moved here from
``datahut_duckhouse/ingest_run_log.py`` in the ``feat/dhd-ingest``
restructuring, step A).

Logged through the Flight server (the same single-writer path
``ingestion.sinks.flight_sink.FlightSink`` uses for ``replace``/
``append``) as ``_ingest_runs``, so the log itself is visible to ``dhd
query`` too -- previously it was written pyiceberg-direct and suffered
the exact same "invisible from Flight" bug as the ingested data (step 0
of the follow-up review). One append per table per run, the same "one
Iceberg commit" discipline as the data itself (see
``tests/ingestion/test_engine.py``'s snapshot-count test). If that table
can't be prepared or appended to -- the catalog accepted the data writes
but rejects this one, or anything else goes wrong logging it -- falls back
to a local SQLite file and says so on stderr, rather than silently
dropping the row.

The fallback path is configurable (``DHD_INGEST_RUN_LOG_DB``), same
reasoning as ``engine/state.py``'s ``DHD_INGEST_STATE_DIR``.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pyarrow as pa

from ingestion.runs.models import RUN_LOG_TABLE, SCHEMA

_DEFAULT_FALLBACK_DB = (
    Path(__file__).resolve().parent.parent / "_state" / "ingest_runs.db"
)

# fix/ingest-batching-grpc, part B: a long error message is itself a row
# *value* here (travels in the do_put body, not gRPC metadata) and that
# alone doesn't trigger the "metadata size exceeds limit" failure this
# bounds against -- but an unbounded error string is still bad hygiene for
# a log table, and keeping it short removes one more variable the next
# time this class of gRPC failure needs diagnosing. See
# docs/DLT_EVAL.md's part B section and dlt_engine.py's own handling of
# the gRPC-side failure for the actual fix.
_MAX_ERROR_CHARS = 4000


def _bounded_error(error: str | None) -> str | None:
    if error is None or len(error) <= _MAX_ERROR_CHARS:
        return error
    elided = len(error) - _MAX_ERROR_CHARS
    return f"{error[:_MAX_ERROR_CHARS]} [... {elided} more chars truncated ...]"


def _fallback_db() -> Path:
    override = os.getenv("DHD_INGEST_RUN_LOG_DB")
    return Path(override) if override else _DEFAULT_FALLBACK_DB


def _iso(epoch_s: float) -> str:
    import datetime

    return datetime.datetime.fromtimestamp(epoch_s, tz=datetime.UTC).isoformat()


def read_runs(
    flight_backend=None,
    *,
    source: str | None = None,
    status: str | None = None,
    last: int | None = None,
) -> list[dict]:
    """``dhd ingest runs``: read back logged rows, newest first. Tries
    ``flight_backend`` (if given and the table exists there) first, then
    the local SQLite fallback -- whichever one a given run actually landed
    in."""
    rows: list[dict] = []
    if flight_backend is not None:
        try:
            if RUN_LOG_TABLE in set(flight_backend.list_tables()):
                rows = (
                    flight_backend.sql(f"SELECT * FROM {RUN_LOG_TABLE}")
                    .to_pyarrow()
                    .to_pylist()
                )
        except Exception:  # noqa: BLE001 - fall back to the local file below
            rows = []

    fallback_db = _fallback_db()
    if fallback_db.exists():
        with sqlite3.connect(fallback_db) as conn:
            conn.row_factory = sqlite3.Row
            local_rows = [
                dict(r) for r in conn.execute("SELECT * FROM ingest_runs").fetchall()
            ]
        # Rows can legitimately exist in both places (an early run fell
        # back, a later one didn't) -- keep both, de-duplicated by run_id.
        seen = {r["run_id"] for r in rows}
        rows.extend(r for r in local_rows if r["run_id"] not in seen)

    if source is not None:
        rows = [r for r in rows if r["source"] == source]
    if status is not None:
        rows = [r for r in rows if r["status"] == status]
    rows.sort(key=lambda r: r["started_at"], reverse=True)
    if last is not None:
        rows = rows[:last]
    return rows


class RunLog:
    """``backend`` is a connected Flight backend (duck-typed: anything with
    ``list_tables``/``create_table``/``con.upload_table``) -- not a
    :class:`~ingestion.sinks.base.Sink`: that keeps the log on the Flight
    single-writer path independently of which sink wrote the table it's
    logging."""

    def __init__(self, backend, echo=print) -> None:
        self.backend = backend
        self._echo = echo
        self.run_id = str(uuid.uuid4())
        self._use_sqlite = False
        try:
            if RUN_LOG_TABLE not in set(backend.list_tables()):
                backend.create_table(
                    RUN_LOG_TABLE, pa.Table.from_pylist([], schema=SCHEMA)
                )
        except Exception as exc:  # noqa: BLE001 - fall back, never block ingestion
            self._fall_back_to_sqlite(
                f"could not prepare the '{RUN_LOG_TABLE}' Iceberg table ({exc})"
            )

    def _fall_back_to_sqlite(self, reason: str) -> None:
        self._use_sqlite = True
        fallback_db = _fallback_db()
        self._echo(
            f"warning: {reason}; logging runs to a local SQLite file instead "
            f"({fallback_db})."
        )
        fallback_db.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(fallback_db) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS ingest_runs ("
                "run_id TEXT, source TEXT, table_name TEXT, started_at TEXT, "
                "finished_at TEXT, duration_s REAL, rows INTEGER, "
                "commits INTEGER, status TEXT, error TEXT, "
                "sink TEXT, sink_reason TEXT)"
            )

    def record(
        self,
        source: str,
        table_name: str,
        started_at: float,
        finished_at: float,
        rows: int,
        status: str,
        error: str | None = None,
        commits: int = 1,
        sink: str | None = None,
        sink_reason: str | None = None,
    ) -> None:
        row = {
            "run_id": self.run_id,
            "source": source,
            "table_name": table_name,
            "started_at": _iso(started_at),
            "finished_at": _iso(finished_at),
            "duration_s": finished_at - started_at,
            "rows": rows,
            "commits": commits,
            "status": status,
            "error": _bounded_error(error),
            "sink": sink,
            "sink_reason": sink_reason,
        }
        if not self._use_sqlite:
            try:
                self.backend.con.upload_table(
                    RUN_LOG_TABLE,
                    pa.Table.from_pylist([row], schema=SCHEMA),
                    mode="append",
                )
                return
            except Exception as exc:  # noqa: BLE001 - fall back, don't lose the row
                self._fall_back_to_sqlite(
                    f"could not append to '{RUN_LOG_TABLE}' ({exc})"
                )
        with sqlite3.connect(_fallback_db()) as conn:
            conn.execute(
                "INSERT INTO ingest_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    row["run_id"],
                    row["source"],
                    row["table_name"],
                    row["started_at"],
                    row["finished_at"],
                    row["duration_s"],
                    row["rows"],
                    row["commits"],
                    row["status"],
                    row["error"],
                    row["sink"],
                    row["sink_reason"],
                ),
            )
