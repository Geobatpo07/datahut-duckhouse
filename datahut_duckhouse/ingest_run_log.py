"""Run log for ``dhd ingest-dlt`` (Vague 1b, task 3): one row per run and per
table (source, table, start, end, duration, rows, status, error).

Logged into DataHut's own Iceberg catalog as ``_ingest_runs``, through the
same ``backend`` connection ``run_ingest_dlt`` already has open -- one
append per table per run, the same "one Iceberg commit" discipline as the
data itself (see ``tests/test_ingest_dlt.py``'s snapshot-count test). If
that table can't be prepared or appended to -- the catalog accepted the
data writes but rejects this one, or anything else goes wrong logging it --
falls back to a local SQLite file and says so on stderr, rather than
silently dropping the log.
"""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path

import click
import pyarrow as pa

RUN_LOG_TABLE = "_ingest_runs"
FALLBACK_DB = (
    Path(__file__).resolve().parent.parent
    / "ingestion"
    / "dlt"
    / "_state"
    / "ingest_runs.db"
)

_SCHEMA = pa.schema(
    [
        ("run_id", pa.string()),
        ("source", pa.string()),
        ("table_name", pa.string()),
        ("started_at", pa.string()),
        ("finished_at", pa.string()),
        ("duration_s", pa.float64()),
        ("rows", pa.int64()),
        ("status", pa.string()),
        ("error", pa.string()),
    ]
)


def _iso(epoch_s: float) -> str:
    import datetime

    return datetime.datetime.fromtimestamp(epoch_s, tz=datetime.UTC).isoformat()


class RunLog:
    def __init__(self, backend) -> None:
        self.backend = backend
        self.run_id = str(uuid.uuid4())
        self._use_iceberg = True
        try:
            if RUN_LOG_TABLE not in set(backend.list_tables()):
                backend.create_table(
                    RUN_LOG_TABLE, pa.Table.from_pylist([], schema=_SCHEMA)
                )
        except Exception as exc:  # noqa: BLE001 - fall back, never block ingestion
            self._fall_back_to_sqlite(
                f"could not prepare the '{RUN_LOG_TABLE}' Iceberg table ({exc})"
            )

    def _fall_back_to_sqlite(self, reason: str) -> None:
        self._use_iceberg = False
        click.echo(
            f"warning: {reason}; logging runs to a local SQLite file instead "
            f"({FALLBACK_DB}).",
            err=True,
        )
        FALLBACK_DB.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(FALLBACK_DB) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS ingest_runs ("
                "run_id TEXT, source TEXT, table_name TEXT, started_at TEXT, "
                "finished_at TEXT, duration_s REAL, rows INTEGER, status TEXT, error TEXT)"
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
    ) -> None:
        row = {
            "run_id": self.run_id,
            "source": source,
            "table_name": table_name,
            "started_at": _iso(started_at),
            "finished_at": _iso(finished_at),
            "duration_s": finished_at - started_at,
            "rows": rows,
            "status": status,
            "error": error,
        }
        if self._use_iceberg:
            try:
                self.backend.upload_table(
                    RUN_LOG_TABLE,
                    pa.Table.from_pylist([row], schema=_SCHEMA),
                    mode="append",
                )
                return
            except Exception as exc:  # noqa: BLE001 - fall back, don't lose the row
                self._fall_back_to_sqlite(
                    f"could not append to '{RUN_LOG_TABLE}' ({exc})"
                )
        with sqlite3.connect(FALLBACK_DB) as conn:
            conn.execute(
                "INSERT INTO ingest_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    row["run_id"],
                    row["source"],
                    row["table_name"],
                    row["started_at"],
                    row["finished_at"],
                    row["duration_s"],
                    row["rows"],
                    row["status"],
                    row["error"],
                ),
            )
