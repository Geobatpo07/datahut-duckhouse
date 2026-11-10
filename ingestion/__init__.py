"""The ingestion package's public API -- the only thing a caller (``dhd``,
``ingestion.cli``, a script) should ever need to import from this package.
Everything else (``config``, ``connectors``, ``engine``, ``sinks``,
``runs``) is an implementation detail; see
``docs/INGESTION_ARCHITECTURE.md`` for the full layer map and its
dependency rules, enforced by ``tests/ingestion/test_dependency_rules.py``.

``dlt`` never appears in this module, nor in anything that imports only
this module -- it is an implementation detail confined to
``connectors/_dlt_sources.py`` and (allowed, but unused in practice)
``engine/dlt_engine.py``.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pyarrow.parquet as papq

from ingestion.config.models import SourceConfig, SourcesConfigError
from ingestion.config.registry import SOURCES_YML, get_source, load_sources
from ingestion.connectors import registry as _connector_registry
from ingestion.connectors.sql import MissingDriverError
from ingestion.engine import state as _state
from ingestion.engine.base import RunResult, TableResult
from ingestion.engine.dlt_engine import DEFAULT_WORK_DIR, ConnectorError, DltIngestor
from ingestion.runs.store import read_runs
from ingestion.sinks.base import SchemaContractViolationError, SinkError
from ingestion.sinks.catalog_guard import CatalogMismatchError, check_same_catalog
from ingestion.sinks.router import DirectSinkUnavailableError, UnknownSinkError

__all__ = [
    "SOURCES_YML",
    "SourceConfig",
    "SourcesConfigError",
    "RunResult",
    "TableResult",
    "ConnectorError",
    "MissingDriverError",
    "SchemaContractViolationError",
    "SinkError",
    "CatalogMismatchError",
    "DirectSinkUnavailableError",
    "UnknownSinkError",
    "list_sources",
    "validate_registry",
    "extract_source",
    "dry_run_source",
    "run_source",
    "read_runs",
    "check_same_catalog",
]


def list_sources() -> list[SourceConfig]:
    """Every configured source, as validated ``SourceConfig`` objects."""
    return load_sources()


def validate_registry() -> tuple[list[SourceConfig], list[str]]:
    """Validate ``sources.yml``. Returns ``(sources, warnings)`` on
    success; raises :class:`SourcesConfigError` (one line per problem,
    with its YAML line number) otherwise."""
    warnings: list[str] = []
    sources = load_sources(collect_warnings=warnings)
    return sources, warnings


def extract_source(
    source: SourceConfig, work_dir: Path = DEFAULT_WORK_DIR
) -> dict[str, Path]:
    """Extract ``source`` to Parquet via its family's connector, with no
    sink/run-log involved -- dispatch-only, for scripts and tests that just
    need the extracted data (``run_source`` is the full load-into-Iceberg
    path)."""
    connector = _connector_registry.get(source.family)
    return connector.build_resources(source, work_dir)


def _with_overrides(
    source: SourceConfig, tables: list[str] | None, sink: str | None = None
) -> SourceConfig:
    update: dict = {}
    if tables:
        update["resources"] = list(tables)
    if sink is not None:
        update["sink"] = sink
    return source.model_copy(update=update) if update else source


def dry_run_source(name: str, tables: list[str] | None = None) -> RunResult:
    """Resolve ``name``, extract it (a real connection -- for ``sql``,
    actually querying the database; for ``rest``, actually hitting the
    API), and report what *would* be written, without touching any sink
    or the run log at all. Verified (``tests/ingestion/test_cli.py``) by
    comparing a table's Iceberg snapshot count before and after: unchanged.
    """
    source = _with_overrides(get_source(name), tables)
    written = extract_source(source)
    result = RunResult(source=source.name)
    for table_name, parquet_path in written.items():
        rows = papq.read_table(parquet_path).num_rows
        result.tables.append(TableResult(table=table_name, verb="planned", rows=rows))
    return result


def run_source(
    name: str,
    *,
    flight_backend,
    pyiceberg_connect: Callable[[], object],
    run_log_echo: Callable[[str], None] = print,
    tables: list[str] | None = None,
    full_refresh: bool = False,
    sink: str | None = None,
) -> RunResult:
    """Extract and load ``name`` (one entry from ``sources.yml``).

    Which sink writes each table is resolved per ADR-0006, decision 5
    (``ingestion.sinks.router``) from ``source.sink`` (default ``auto``),
    overridable here with ``sink`` (``dhd ingest run --sink``). ``merge``,
    and now ``replace``/``append`` when the direct sink is chosen, write
    direct via pyiceberg, connected lazily through ``pyiceberg_connect``
    (only called the first time a direct write is actually needed) and
    gated by a catalog-identity check against ``flight_backend`` -- see
    ``sinks/catalog_guard.py``.

    ``tables`` narrows extraction to specific resources, same as editing
    ``resources`` in ``sources.yml`` for just this run. ``full_refresh``
    drops this source's incremental cursor first (``engine.state.
    clear_source``), so extraction starts from scratch exactly like a
    brand-new source's first run -- safe for every ``write_disposition``
    that can carry ``incremental`` (``merge``, or ``replace`` with its
    warning): `incremental` + `append` is rejected at validation time, so
    that unsafe combination can't reach here.
    """
    source = _with_overrides(get_source(name), tables, sink)
    if full_refresh:
        _state.clear_source(source.name)
    engine = DltIngestor(flight_backend, pyiceberg_connect, run_log_echo=run_log_echo)
    return engine.run(source)
