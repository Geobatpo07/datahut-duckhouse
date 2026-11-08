"""``dhd ingest-dlt``: load a configured source into Iceberg.

Thin CLI-facing adapter over the ``ingestion`` package's public API
(``ingestion.run_source``/``list_sources``) -- nothing here reaches into
``ingestion``'s engine/connectors/sinks internals, only its top-level
``ingestion/__init__.py``. Translates ``ingestion``'s domain exceptions
into CLI-facing ones with the right exit code (0 success, 1 run failure,
2 configuration error -- see ``cli.py``).
"""

from __future__ import annotations

import click

from ingestion import (
    CatalogMismatchError,
    ConnectorError,
    MissingDriverError,
    SchemaContractViolationError,
    SinkError,
    SourcesConfigError,
    list_sources,
    run_source,
)


class IngestDltError(RuntimeError):
    """A clean, CLI-facing error for ``dhd ingest-dlt`` failures (exit code 1)."""


class IngestConfigError(IngestDltError):
    """A clean, CLI-facing *configuration* error (exit code 2): bad
    ``sources.yml``, a missing secret, or a catalog-identity mismatch for a
    ``merge`` source (``ingestion.CatalogMismatchError``)."""


def _resolve_source_names(source_name: str) -> list[str]:
    """Shared by :func:`run_ingest_dlt` and :func:`plan_ingest_dlt`: resolve
    ``source_name`` ('all' for every source) against the registry."""
    try:
        sources = list_sources()
    except SourcesConfigError as exc:
        raise IngestConfigError(str(exc)) from exc

    if source_name == "all":
        return [s.name for s in sources]
    if source_name not in {s.name for s in sources}:
        raise IngestConfigError(
            f"No source named '{source_name}' in ingestion/config/sources.yml."
        )
    return [source_name]


def plan_ingest_dlt(source_name: str) -> list:
    """Validate ``sources.yml`` and resolve ``source_name`` without extracting,
    connecting to Iceberg, or writing anything -- the ``--dry-run`` path."""
    try:
        sources = list_sources()
    except SourcesConfigError as exc:
        raise IngestConfigError(str(exc)) from exc
    names = set(_resolve_source_names(source_name))
    return [s for s in sources if s.name in names]


def run_ingest_dlt(source_name: str) -> dict[str, tuple[str, int]]:
    """Extract ``source_name`` ('all' for every source) and load it into
    Iceberg, applying each source's ``write_disposition`` and
    ``schema_contract`` (task 3) via ``ingestion.run_source``.

    Returns ``{table_name: (verb, row_count)}`` where ``verb`` is "created"
    (table did not exist yet) or "overwritten"/"appended"/"merged" (per
    ``write_disposition``, it did).
    """
    from datahut_duckhouse.connection import FlightUnavailableError
    from datahut_duckhouse.connection import connect as flight_connect
    from datahut_duckhouse.ingest import IngestUnavailableError
    from datahut_duckhouse.ingest import connect as ingest_connect

    names = _resolve_source_names(source_name)

    try:
        flight_backend = flight_connect()
    except FlightUnavailableError as exc:
        raise IngestDltError(str(exc)) from exc

    def pyiceberg_connect():
        try:
            return ingest_connect()
        except IngestUnavailableError as exc:
            raise IngestConfigError(str(exc)) from exc

    results: dict[str, tuple[str, int]] = {}
    for name in names:
        try:
            run_result = run_source(
                name,
                flight_backend=flight_backend,
                pyiceberg_connect=pyiceberg_connect,
                run_log_echo=lambda msg: click.echo(msg, err=True),
            )
        except CatalogMismatchError as exc:
            raise IngestConfigError(str(exc)) from exc
        except (MissingDriverError, SourcesConfigError) as exc:
            raise IngestConfigError(str(exc)) from exc
        except (ConnectorError, SchemaContractViolationError, SinkError) as exc:
            raise IngestDltError(str(exc)) from exc
        for table in run_result.tables:
            results[table.table] = (table.verb, table.rows)
    return results
