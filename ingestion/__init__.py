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

from ingestion.config.models import SourceConfig, SourcesConfigError
from ingestion.config.registry import SOURCES_YML, get_source, load_sources
from ingestion.connectors import registry as _connector_registry
from ingestion.connectors.sql import MissingDriverError
from ingestion.engine.base import RunResult, TableResult
from ingestion.engine.dlt_engine import DEFAULT_WORK_DIR, ConnectorError, DltIngestor
from ingestion.sinks.base import SchemaContractViolationError, SinkError
from ingestion.sinks.catalog_guard import CatalogMismatchError

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
    "list_sources",
    "validate_registry",
    "extract_source",
    "run_source",
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


def run_source(
    name: str,
    *,
    flight_backend,
    pyiceberg_connect: Callable[[], object],
    run_log_echo: Callable[[str], None] = print,
) -> RunResult:
    """Extract and load ``name`` (one entry from ``sources.yml``).

    ``replace``/``append`` write through ``flight_backend`` (already
    connected -- see ``datahut_duckhouse.connection.connect``). ``merge``
    writes direct via pyiceberg, connected lazily through
    ``pyiceberg_connect`` (only called if a 'merge' source is actually
    encountered) and gated by a catalog-identity check against
    ``flight_backend`` -- see ``sinks/catalog_guard.py``.
    """
    source = get_source(name)
    engine = DltIngestor(flight_backend, pyiceberg_connect, run_log_echo=run_log_echo)
    return engine.run(source)
