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

import pyarrow as pa
import pyarrow.parquet as papq

from ingestion.config.models import SourceConfig, SourcesConfigError
from ingestion.connectors import registry as connector_registry
from ingestion.connectors.sql import MissingDriverError
from ingestion.engine.base import RunResult, TableResult
from ingestion.runs.store import RunLog
from ingestion.sinks import router
from ingestion.sinks.base import SchemaContractViolationError, SinkError
from ingestion.sinks.catalog_guard import CatalogMismatchError
from ingestion.sinks.direct_sink import DirectSink
from ingestion.sinks.flight_sink import FlightSink
from ingestion.sinks.router import DirectSinkUnavailableError, UnknownSinkError

DEFAULT_WORK_DIR = Path(__file__).resolve().parent.parent / "_pipeline_data"

# fix/ingest-batching-grpc, part B: xorq's do_put doesn't bound its error
# messages the way do_exchange does (xorq/flight/server.py's
# bounded_status) -- a server-side exception whose message+traceback
# exceeds gRPC's status-metadata budget (~4KB on the wire) reaches the
# client as this exact, stable gRPC-level message instead of the real
# Python exception. Reproduced live (docs/DLT_EVAL.md's part B section)
# via a genuine schema mismatch across many columns; not something this
# project's code can fix (xorq is a third-party dependency) -- caught
# below so it at least becomes an actionable, logged SinkError instead of
# an opaque crash.
_GRPC_METADATA_OVERFLOW_MARKER = "metadata size exceeds limit"


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
        self._echo = run_log_echo
        self.run_log = RunLog(flight_backend, echo=run_log_echo)
        self._flight_sink = FlightSink(flight_backend)
        self._direct_sink: DirectSink | None = None

    def _sink_for(self, source: SourceConfig) -> tuple[object, str, str]:
        """Resolves which sink this source uses (ADR-0006, decision 5),
        fresh per source (``source.sink`` is per-source config) --
        ``DirectSink`` itself is still built lazily, once per engine
        instance (not per source): building it runs the catalog-identity
        guard, a network round trip worth paying at most once per run.
        Returns ``(sink, sink_name, sink_reason)``.
        """
        sink_name, sink_reason = router.resolve_sink(source.sink)
        if sink_name == "flight":
            return self._flight_sink, sink_name, sink_reason
        if self._direct_sink is None:
            pyiceberg_engine = self._pyiceberg_connect()
            self._direct_sink = DirectSink(pyiceberg_engine, self.flight_backend)
        return self._direct_sink, sink_name, sink_reason

    def run(self, source: SourceConfig) -> RunResult:
        t0 = time.time()
        connector = connector_registry.get(source.family)

        try:
            sink, sink_name, sink_reason = self._sink_for(source)
        except (
            CatalogMismatchError,
            DirectSinkUnavailableError,
            UnknownSinkError,
        ) as exc:
            message = source.redact(str(exc))
            self.run_log.record(source.name, "*", t0, time.time(), 0, "error", message)
            raise type(exc)(message) from exc

        try:
            written = connector.build_resources(source, self.work_dir)
        except (MissingDriverError, SourcesConfigError) as exc:
            # Both are configuration errors (missing secret, bad dialect,
            # ...), not run failures -- the caller (ingestion/cli.py) maps
            # them to exit code 2, not 1. Still logged, still redacted.
            message = source.redact(str(exc))
            self.run_log.record(
                source.name,
                "*",
                t0,
                time.time(),
                0,
                "error",
                message,
                sink=sink_name,
                sink_reason=sink_reason,
            )
            raise type(exc)(message) from exc
        except Exception as exc:  # noqa: BLE001 - attach the source name, re-raise
            message = source.redact(
                f"'{source.name}' ({source.family}): probable cause: {exc}. "
                f"Action: check {source.secret_ref or 'the source'}'s "
                f"connectivity/config, then retry."
            )
            self.run_log.record(
                source.name,
                "*",
                t0,
                time.time(),
                0,
                "error",
                message,
                sink=sink_name,
                sink_reason=sink_reason,
            )
            raise ConnectorError(message) from exc

        if source.sink == "flight":
            for parquet_path in written.values():
                estimated = papq.ParquetFile(parquet_path).metadata.num_rows
                warning = router.flight_size_warning(estimated)
                if warning:
                    self._echo(f"warning: '{source.name}': {warning}")

        result = RunResult(source=source.name)
        for table_name, parquet_path in written.items():
            table_t0 = time.time()
            try:
                verb, rows_written, commits = self._write_table(
                    sink, source, table_name, parquet_path, sink_name
                )
            except (SchemaContractViolationError, SinkError) as exc:
                message = source.redact(
                    f"'{source.name}'.'{table_name}': probable cause: {exc}. "
                    f"Action: check schema_contract/write_disposition for "
                    f"this source in sources.yml."
                )
                self.run_log.record(
                    source.name,
                    table_name,
                    table_t0,
                    time.time(),
                    0,
                    "error",
                    message,
                    sink=sink_name,
                    sink_reason=sink_reason,
                )
                raise type(exc)(message) from exc
            result.tables.append(
                TableResult(
                    table=table_name, verb=verb, rows=rows_written, commits=commits
                )
            )
            self.run_log.record(
                source.name,
                table_name,
                table_t0,
                time.time(),
                rows_written,
                "ok",
                commits=commits,
                sink=sink_name,
                sink_reason=sink_reason,
            )
        return result

    def _write_table(
        self,
        sink,
        source: SourceConfig,
        table_name: str,
        parquet_path: Path,
        sink_name: str,
    ) -> tuple[str, int, int]:
        """Writes one table's Parquet file through ``sink``, in
        ``source.effective_batch_rows(sink_name)``-sized Arrow batches for
        ``append``/``merge`` (one commit per batch) -- never for
        ``replace``, which always forces a single commit regardless of
        ``batch_rows`` (see ``ingestion/sinks/base.py``'s docstring for
        why). Returns ``(verb, rows_written, commits)``.
        """
        batch_rows = source.effective_batch_rows(sink_name)
        if source.write_disposition == "replace":
            if batch_rows is not None:
                self._echo(
                    f"warning: '{table_name}': write_disposition 'replace' "
                    f"always commits once -- batch_rows={batch_rows} is "
                    f"ignored (no safe multi-commit replace strategy; see "
                    f"ingestion/sinks/base.py)."
                )
            data = papq.read_table(parquet_path)
            verb = self._write_chunk(sink, source, table_name, data)
            return verb, data.num_rows, 1

        if batch_rows is None:
            data = papq.read_table(parquet_path)
            verb = self._write_chunk(sink, source, table_name, data)
            return verb, data.num_rows, 1

        rows_written = 0
        commits = 0
        verb = "appended" if source.write_disposition == "append" else "merged"
        for batch in papq.ParquetFile(parquet_path).iter_batches(batch_size=batch_rows):
            chunk = pa.Table.from_batches([batch])
            verb = self._write_chunk(sink, source, table_name, chunk)
            rows_written += chunk.num_rows
            commits += 1
        return verb, rows_written, commits

    def _write_chunk(
        self, sink, source: SourceConfig, table_name: str, data: pa.Table
    ) -> str:
        if table_name in sink.list_tables() and source.schema_contract:
            data = sink.apply_schema_contract(table_name, data, source.schema_contract)
        try:
            return sink.write(
                table_name,
                data,
                write_disposition=source.write_disposition,
                primary_key=source.primary_key,
            )
        except pa.lib.ArrowException as exc:
            if _GRPC_METADATA_OVERFLOW_MARKER in str(exc):
                raise SinkError(
                    f"'{table_name}': the Flight server rejected this write "
                    f"with a gRPC 'metadata size exceeds limit' error -- the "
                    f"real server-side exception was too large to fit gRPC's "
                    f"status-metadata budget and was replaced by this opaque "
                    f"one (grpc_status=8 RESOURCE_EXHAUSTED). Likely cause: a "
                    f"schema mismatch across many columns, or another error "
                    f"whose message+traceback exceeds ~4KB. Check the Flight "
                    f"server's own logs for the actual exception -- it is "
                    f"never sent to the client for this RPC (xorq's do_put "
                    f"doesn't bound its error messages the way do_exchange "
                    f"does; see xorq/flight/server.py's bounded_status)."
                ) from exc
            raise
