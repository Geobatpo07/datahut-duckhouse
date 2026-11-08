"""The one :class:`~ingestion.engine.base.Ingestor` implementation: extracts
a source via its family's connector (``connectors.registry``) and writes
each table through the right sink (``replace``/``append`` via
:class:`~ingestion.sinks.flight_sink.FlightSink`, ``merge`` via
:class:`~ingestion.sinks.direct_sink.DirectSink` -- see ``sinks/base.py``'s
docstring for what each ``write_disposition`` means for crash recovery),
logging every table through :class:`~ingestion.runs.store.RunLog`.

Takes an already-connected Flight backend and a lazy pyiceberg-connect
callable rather than connecting either itself: this module (and the rest
of ``ingestion``) stays independent of how a connection is actually
established (host/port resolution, the Python-version guard, environment
variables) -- that is the caller's job (``datahut_duckhouse``/``dhd``).

This, not ``connectors/_dlt_sources.py``, is the engine's own "may import
dlt" module (the dependency rule allows both) -- in practice it doesn't
need to, since dlt stays entirely inside the connectors it dispatches to.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path

import pyarrow.parquet as papq

from ingestion.config.models import SourceConfig
from ingestion.connectors import registry as connector_registry
from ingestion.connectors.sql import MissingDriverError
from ingestion.engine.base import RunResult, TableResult
from ingestion.runs.store import RunLog
from ingestion.sinks.base import SchemaContractViolationError, SinkError
from ingestion.sinks.catalog_guard import CatalogMismatchError
from ingestion.sinks.direct_sink import DirectSink
from ingestion.sinks.flight_sink import FlightSink

DEFAULT_WORK_DIR = Path(__file__).resolve().parent.parent / "_pipeline_data"


class ConnectorError(RuntimeError):
    """Wraps any connector-level failure with the source name attached."""


class DltIngestor:
    def __init__(
        self,
        flight_backend,
        pyiceberg_connect: Callable[[], object],
        *,
        work_dir: Path = DEFAULT_WORK_DIR,
        run_log_echo: Callable[[str], None] = print,
    ) -> None:
        self.flight_backend = flight_backend
        self._pyiceberg_connect = pyiceberg_connect
        self.work_dir = work_dir
        self.run_log = RunLog(flight_backend, echo=run_log_echo)
        self._flight_sink = FlightSink(flight_backend)
        self._direct_sink: DirectSink | None = None

    def _sink_for(self, source: SourceConfig):
        """``DirectSink`` is built lazily, once per engine instance (not per
        source): building it runs the catalog-identity guard, a network
        round trip worth paying at most once per run."""
        if source.write_disposition != "merge":
            return self._flight_sink
        if self._direct_sink is None:
            pyiceberg_engine = self._pyiceberg_connect()
            self._direct_sink = DirectSink(pyiceberg_engine, self.flight_backend)
        return self._direct_sink

    def run(self, source: SourceConfig) -> RunResult:
        t0 = time.time()
        connector = connector_registry.get(source.family)

        try:
            sink = self._sink_for(source)
        except CatalogMismatchError as exc:
            self.run_log.record(source.name, "*", t0, time.time(), 0, "error", str(exc))
            raise

        try:
            written = connector.build_resources(source, self.work_dir)
        except MissingDriverError:
            raise
        except Exception as exc:  # noqa: BLE001 - attach the source name, re-raise
            self.run_log.record(source.name, "*", t0, time.time(), 0, "error", str(exc))
            raise ConnectorError(f"'{source.name}' ({source.family}): {exc}") from exc

        result = RunResult(source=source.name)
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
                self.run_log.record(
                    source.name, table_name, table_t0, time.time(), 0, "error", str(exc)
                )
                raise
            result.tables.append(
                TableResult(table=table_name, verb=verb, rows=data.num_rows)
            )
            self.run_log.record(
                source.name, table_name, table_t0, time.time(), data.num_rows, "ok"
            )
        return result
