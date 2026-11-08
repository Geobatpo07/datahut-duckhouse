"""The sink contract: where a table's rows actually land.

``write_disposition`` fixes both the *semantics* and the *failure-recovery
behavior* of a write:

- ``replace``: a full create-or-overwrite. If the write fails partway
  through (the process is killed mid-upload), the table keeps its
  *previous* snapshot -- Iceberg only exposes a commit once it completes,
  so a half-sent upload is simply never committed. Safe to re-run.
- ``append``: adds rows with no dedup key. If a run is killed *after* its
  commit lands but before the caller observes success, a naive re-run
  duplicates every row from that commit -- there is no way to tell "did
  the last commit happen" from the data alone. Not safe to blindly re-run
  without deduplicating downstream (see ``ingestion.config.rules`` for why
  ``incremental`` + ``append`` is rejected at validation time instead of
  relying on this).
- ``merge``: upserts by primary key. Idempotent regardless of how many
  times the same extracted rows get re-applied -- a crash-then-resume (or
  a deliberate re-run) never duplicates and never loses a row already
  committed. The one gap: a row deleted at the source is never detected
  (there is no cursor value a deleted row could report) -- verified live,
  see docs/DLT_EVAL.md.
"""

from __future__ import annotations

from typing import Protocol

import pyarrow as pa


class SinkError(RuntimeError):
    """A table write failed in a way the caller should present cleanly."""


class SchemaContractViolationError(RuntimeError):
    """A ``schema_contract: freeze`` source tried to write a new column."""


class Sink(Protocol):
    """A destination for one table's worth of extracted rows."""

    write_dispositions: tuple[str, ...]

    def list_tables(self) -> list[str]: ...

    def write(
        self,
        table_name: str,
        data: pa.Table,
        write_disposition: str,
        primary_key: list[str] | None = None,
    ) -> str:
        """Write ``data`` to ``table_name``; return a verb: "created",
        "overwritten", "appended" or "merged"."""
        ...
