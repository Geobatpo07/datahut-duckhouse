"""The run log's row shape: one row per table per run (source, table,
start, end, duration, rows, status, error)."""

from __future__ import annotations

from dataclasses import asdict, dataclass

import pyarrow as pa

RUN_LOG_TABLE = "_ingest_runs"

SCHEMA = pa.schema(
    [
        ("run_id", pa.string()),
        ("source", pa.string()),
        ("table_name", pa.string()),
        ("started_at", pa.string()),
        ("finished_at", pa.string()),
        ("duration_s", pa.float64()),
        ("rows", pa.int64()),
        ("commits", pa.int64()),
        ("status", pa.string()),
        ("error", pa.string()),
    ]
)


@dataclass
class RunLogRow:
    run_id: str
    source: str
    table_name: str
    started_at: str
    finished_at: str
    duration_s: float
    rows: int
    status: str
    commits: int = 1
    error: str | None = None

    def as_dict(self) -> dict:
        return asdict(self)
