"""``dhd ingest-dlt``: load dlt-extracted sources into Iceberg (Vague 1 step 4;
exploitation layer -- run log, write_disposition dispatch, schema contracts
-- added in Vague 1b task 3).

Bridges ``ingestion/dlt/pipeline.py`` (extraction) to a :mod:`ingest_sinks`
``Sink`` (step 0 of the follow-up review): ``replace``/``append`` go
through the Flight server (``FlightSink``, the exact path ``dhd
create-table``/``dhd insert`` use) so a table lands exactly where ``dhd
query``/``dhd list-tables`` already look -- a real Flight server is now
required for every run, not just ``merge``. ``merge`` still writes
directly via pyiceberg (``DirectSink``), gated by
``catalog_guard.check_same_catalog``. Each table's write is one Iceberg
commit (create/append/overwrite/upsert), whatever the source's
``write_disposition`` -- see ``tests/test_ingest_dlt.py``'s
snapshot-count test.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow.parquet as papq

if TYPE_CHECKING:
    from datahut_duckhouse.ingest_sinks import DirectSink, FlightSink

_DLT_DIR = Path(__file__).resolve().parent.parent / "ingestion" / "dlt"


class IngestDltError(RuntimeError):
    """A clean, CLI-facing error for ``dhd ingest-dlt`` failures (exit code 1)."""


class IngestConfigError(IngestDltError):
    """A clean, CLI-facing *configuration* error (exit code 2): bad
    ``sources.yml``, a missing secret, or a catalog-identity mismatch for a
    ``merge`` source (``catalog_guard.CatalogMismatchError``)."""


def _load_pipeline():
    """Import ``ingestion/dlt/pipeline.py`` by path.

    Not ``import ingestion.dlt.pipeline``: see that module's own docstring --
    ``ingestion/__init__.py`` is unrelated pre-existing code that imports
    submodules which do not exist on disk and raises ``ModuleNotFoundError``
    on import. Going straight at ``ingestion/dlt/`` avoids triggering it.
    """
    dlt_dir = str(_DLT_DIR)
    if dlt_dir not in sys.path:
        sys.path.insert(0, dlt_dir)
    import pipeline  # type: ignore[import-not-found]

    return pipeline


def _resolve_sources(pipeline, source_name: str):
    """Shared by :func:`run_ingest_dlt` and :func:`plan_ingest_dlt`: parse
    ``sources.yml`` and narrow it to ``source_name`` ('all' for every
    source)."""
    try:
        sources = pipeline.load_sources()
    except pipeline.SourcesConfigError as exc:
        raise IngestConfigError(str(exc)) from exc

    if source_name != "all":
        sources = [s for s in sources if s.name == source_name]
        if not sources:
            raise IngestConfigError(
                f"No source named '{source_name}' in ingestion/dlt/sources.yml."
            )
    return sources


def plan_ingest_dlt(source_name: str) -> list:
    """Validate ``sources.yml`` and resolve ``source_name`` without extracting,
    connecting to Iceberg, or writing anything -- the ``--dry-run`` path."""
    pipeline = _load_pipeline()
    return _resolve_sources(pipeline, source_name)


def _sink_for(source, flight_backend, _direct_cache: dict) -> DirectSink | FlightSink:
    """Return the :class:`~datahut_duckhouse.ingest_sinks.Sink` that writes
    ``source``'s ``write_disposition``. ``DirectSink`` is built lazily, once
    per run (not per source), since building it runs the catalog-identity
    guard -- a network round trip worth paying at most once."""
    from datahut_duckhouse.ingest_sinks import DirectSink, FlightSink

    if source.write_disposition == "merge":
        if "direct" not in _direct_cache:
            from datahut_duckhouse.ingest import IngestUnavailableError
            from datahut_duckhouse.ingest import connect as ingest_connect

            try:
                pyiceberg_engine = ingest_connect()
            except IngestUnavailableError as exc:
                raise IngestConfigError(str(exc)) from exc
            _direct_cache["direct"] = DirectSink(pyiceberg_engine, flight_backend)
        return _direct_cache["direct"]
    if "flight" not in _direct_cache:
        _direct_cache["flight"] = FlightSink(flight_backend)
    return _direct_cache["flight"]


def run_ingest_dlt(source_name: str) -> dict[str, tuple[str, int]]:
    """Extract ``source_name`` ('all' for every source) and load it into
    Iceberg, applying each source's ``write_disposition`` and
    ``schema_contract`` (task 3) -- ``replace``/``append`` via the Flight
    server (:class:`~datahut_duckhouse.ingest_sinks.FlightSink`), ``merge``
    direct via pyiceberg (:class:`~datahut_duckhouse.ingest_sinks.DirectSink`),
    see this module's docstring.

    Returns ``{table_name: (verb, row_count)}`` where ``verb`` is "created"
    (table did not exist yet) or "overwritten"/"appended"/"merged" (per
    ``write_disposition``, it did).
    """
    from datahut_duckhouse.catalog_guard import CatalogMismatchError
    from datahut_duckhouse.connection import FlightUnavailableError
    from datahut_duckhouse.connection import connect as flight_connect
    from datahut_duckhouse.ingest_run_log import RunLog
    from datahut_duckhouse.ingest_sinks import SchemaContractViolationError, SinkError

    pipeline = _load_pipeline()
    sources = _resolve_sources(pipeline, source_name)

    try:
        flight_backend = flight_connect()
    except FlightUnavailableError as exc:
        raise IngestDltError(str(exc)) from exc

    run_log = RunLog(flight_backend)
    sink_cache: dict = {}
    results: dict[str, tuple[str, int]] = {}
    for source in sources:
        t0 = time.time()
        try:
            sink = _sink_for(source, flight_backend, sink_cache)
        except CatalogMismatchError as exc:
            run_log.record(source.name, "*", t0, time.time(), 0, "error", str(exc))
            raise IngestConfigError(str(exc)) from exc

        try:
            written = pipeline.extract_source(source)
        except (pipeline.MissingDriverError, pipeline.SourcesConfigError) as exc:
            run_log.record(source.name, "*", t0, time.time(), 0, "error", str(exc))
            raise IngestConfigError(str(exc)) from exc
        except pipeline.ConnectorError as exc:
            run_log.record(source.name, "*", t0, time.time(), 0, "error", str(exc))
            raise IngestDltError(str(exc)) from exc

        for table_name, parquet_path in written.items():
            table_t0 = time.time()
            try:
                data = papq.read_table(parquet_path)
                if table_name in sink.list_tables() and source.schema_contract:
                    data = sink.apply_schema_contract(
                        table_name, data, source.schema_contract
                    )
                verb = sink.write(
                    table_name,
                    data,
                    write_disposition=source.write_disposition,
                    primary_key=source.primary_key,
                )
            except (SchemaContractViolationError, SinkError) as exc:
                run_log.record(
                    source.name, table_name, table_t0, time.time(), 0, "error", str(exc)
                )
                raise IngestDltError(str(exc)) from exc
            results[table_name] = (verb, data.num_rows)
            run_log.record(
                source.name, table_name, table_t0, time.time(), data.num_rows, "ok"
            )
    return results
