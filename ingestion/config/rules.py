"""Cross-field validation rules for a ``SourceConfig`` (Vague 1b follow-up
review, point 1 -- factored out of ``models.py`` so the *shape* of a valid
entry and the *reasons* a combination is rejected/warned about can be read
separately).

Each function raises :class:`ValueError` (fatal -- ``models.py``'s
validator lets pydantic turn it into a field error) or emits a
:class:`UserWarning` (non-fatal -- surfaced by ``registry.load_sources``'s
``collect_warnings``). Never imports ``dlt``, a connector, a sink, or the
engine -- see ``docs/INGESTION_ARCHITECTURE.md``'s dependency rules.
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ingestion.config.models import SourceConfig

SQL_DIALECTS = ("sqlite", "postgresql", "mysql", "sqlserver")
FILE_FORMATS = ("csv", "json", "parquet")


def check_write_disposition(source: SourceConfig) -> None:
    """``merge`` needs a ``primary_key``. Point 1 of the follow-up review:
    ``incremental`` + ``append`` is always a duplicate risk (no dedup key),
    rejected outright. ``incremental`` + ``replace`` is a different,
    non-duplicating but still surprising failure mode (silent data loss of
    unchanged rows): warned, not rejected."""
    if source.write_disposition == "merge" and not source.primary_key:
        raise ValueError(
            f"'{source.name}': write_disposition 'merge' needs 'primary_key'."
        )
    if source.incremental is None:
        return
    if source.write_disposition == "append":
        # Verified live (docs/DLT_EVAL.md, experiment 1): the incremental
        # cursor's lower bound is inclusive, so any row sharing the exact
        # cursor value already recorded gets re-extracted on every
        # subsequent run, not skipped. Under 'merge' that's a harmless
        # no-op upsert; under 'append' (which has no dedup key at all) it's
        # a guaranteed duplicate on every single re-run, not just after a
        # crash. There is no explicit-dedup escape hatch for 'append' in
        # this schema, so this is always rejected, not just warned about.
        raise ValueError(
            f"'{source.name}': incremental + write_disposition 'append' "
            f"duplicates rows on every re-run (the cursor's inclusive "
            f"lower bound re-extracts rows already committed, and "
            f"'append' has no primary key to deduplicate them against). "
            f"Use write_disposition 'merge' with a primary_key instead."
        )
    if source.write_disposition == "replace":
        # Not rejected (no duplicate risk), but still a real correctness
        # trap: 'replace' overwrites the *entire* table with only the
        # newly-extracted incremental subset, discarding every row from
        # earlier runs that didn't change this time -- silent data loss,
        # not duplication. Warn rather than block: a source genuinely
        # re-pulling its whole incremental window at once (rare) might
        # intend exactly this.
        warnings.warn(
            f"'{source.name}': incremental + write_disposition 'replace' "
            f"overwrites the whole table with just this run's "
            f"incrementally-extracted subset every time -- likely "
            f"silent data loss for rows unchanged since the last run. "
            f"Use 'merge' with a primary_key unless this is intentional.",
            stacklevel=2,
        )


def check_batch_rows(source: SourceConfig) -> None:
    """``batch_rows`` must be a positive count or ``null`` (chunking off).
    Caught here, not left to blow up later as a ``ValueError`` from
    ``pyarrow.parquet.ParquetFile.iter_batches(batch_size=...)``."""
    if source.batch_rows is not None and source.batch_rows <= 0:
        raise ValueError(
            f"'{source.name}': batch_rows must be a positive integer or "
            f"null (got {source.batch_rows!r})."
        )


def check_family_fields(source: SourceConfig) -> None:
    """Each family's own required fields -- the part of validation that is
    specific to ``sql``/``files``/``rest``, not shared across families."""
    if source.family == "sql":
        if not source.dialect:
            raise ValueError(f"'{source.name}': family 'sql' needs 'dialect'.")
        if source.dialect not in SQL_DIALECTS:
            raise ValueError(
                f"'{source.name}': unknown dialect '{source.dialect}'. Choose "
                f"from: {', '.join(SQL_DIALECTS)}."
            )
        if not source.secret_ref:
            raise ValueError(f"'{source.name}': family 'sql' needs 'secret_ref'.")
    elif source.family == "files":
        if not source.path and not source.http_url:
            raise ValueError(
                f"'{source.name}': family 'files' needs 'path' or 'http_url'."
            )
        if not source.format:
            raise ValueError(f"'{source.name}': family 'files' needs 'format'.")
        if source.format not in FILE_FORMATS:
            raise ValueError(
                f"'{source.name}': unknown format '{source.format}'. Choose "
                f"from: {', '.join(FILE_FORMATS)}."
            )
        if not source.resources:
            # files family: exactly one resource, named after the source.
            source.resources = [source.name]
    elif source.family == "rest":
        if not source.base_url:
            raise ValueError(f"'{source.name}': family 'rest' needs 'base_url'.")
        if not source.resources:
            raise ValueError(f"'{source.name}': family 'rest' needs 'resources'.")
