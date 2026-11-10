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

``batch_rows`` (fix/ingest-batching-grpc, part A) splits one table's data
into several Arrow batches, each written through :meth:`Sink.write` as its
own call -- so each batch is its own Iceberg commit, not one commit for
the whole table. The engine (``ingestion.engine.dlt_engine``), not this
module, owns the batch loop; a ``Sink`` only ever sees one ``pa.Table`` at
a time and has no idea whether it's the only one or one of many. What
that means per disposition, if the process is killed between two batches:

- ``append``: every batch committed before the kill is durable and stays.
  Re-running from scratch re-extracts and re-appends *all* batches,
  including the ones that already landed -- the same "not safe to
  blindly re-run" gap as a single-batch append, just with partial credit
  instead of all-or-nothing.
- ``merge``: every batch committed before the kill is durable, and
  because each batch upserts by primary key, re-running from scratch
  re-merges every batch (committed or not) without duplicating or losing
  a row. Composes safely across batches for exactly the reason a single
  merge is idempotent.
- ``replace``: deliberately **not** batched. A multi-commit replace would
  mean each batch *overwrites* the table with just that batch's rows,
  leaving only the last batch's data once the run finishes -- there is no
  safe multi-commit replace strategy without a staging table this project
  doesn't have. So the engine always forces exactly one commit for
  ``replace`` regardless of ``batch_rows``, and warns that the setting
  was ignored when it isn't ``null``.
"""

from __future__ import annotations

import os
import random
import time
from collections.abc import Callable
from typing import Protocol, TypeVar

import pyarrow as pa
from pyiceberg.exceptions import CommitFailedException

T = TypeVar("T")

_DEFAULT_MAX_ATTEMPTS = 5
_BASE_DELAY_SECONDS = 0.5


class SinkError(RuntimeError):
    """A table write failed in a way the caller should present cleanly."""


class SchemaContractViolationError(RuntimeError):
    """A ``schema_contract: freeze`` source tried to write a new column."""


class CommitConflictError(SinkError):
    """A write kept conflicting with a concurrent writer past the retry budget."""


def _max_attempts() -> int:
    """``DHD_COMMIT_RETRY_ATTEMPTS`` (ADR-0006, decision 6) -- not a
    ``Sink``-level parameter, since every table write in a run should use
    the same budget."""
    raw = os.getenv("DHD_COMMIT_RETRY_ATTEMPTS")
    if not raw:
        return _DEFAULT_MAX_ATTEMPTS
    try:
        attempts = int(raw)
    except ValueError:
        return _DEFAULT_MAX_ATTEMPTS
    return attempts if attempts > 0 else _DEFAULT_MAX_ATTEMPTS


def with_commit_retry(write: Callable[[], T], *, table_name: str) -> T:
    """Call ``write()`` (a whole reload-mutate-commit operation -- a stale
    ``Table`` handle always re-conflicts, so retrying means calling
    ``write()`` again from scratch, not re-committing the same handle),
    retrying on :class:`~pyiceberg.exceptions.CommitFailedException` --
    pyiceberg does not replay a conflicting commit itself (ADR-0006,
    decision 6) -- with exponential backoff and jitter, up to
    ``DHD_COMMIT_RETRY_ATTEMPTS`` attempts (default 5).

    ``CommitStateUnknownException`` (the commit's outcome is ambiguous --
    e.g. the request succeeded server-side but the response was lost) is
    deliberately NOT retried here: blindly retrying an unknown-state write
    risks double-applying it. It propagates to the caller unchanged.
    """
    attempts = _max_attempts()
    last_exc: CommitFailedException | None = None
    for attempt in range(1, attempts + 1):
        try:
            return write()
        except CommitFailedException as exc:
            last_exc = exc
            if attempt == attempts:
                break
            delay = _BASE_DELAY_SECONDS * (2 ** (attempt - 1)) + random.uniform(
                0, _BASE_DELAY_SECONDS
            )
            time.sleep(delay)
    raise CommitConflictError(
        f"'{table_name}': commit kept conflicting with a concurrent writer "
        f"after {attempts} attempt(s) ({type(last_exc).__name__}: {last_exc}). "
        f"Retry the run; if this keeps happening, something is writing to "
        f"'{table_name}' continuously (see ADR-0006, decision 6)."
    ) from last_exc


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
