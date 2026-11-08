"""Where ``dhd ingest``'s extracted tables actually get written (the
single-writer decision from Vague 1b's follow-up review, step 0).

Reproduced live against the real Docker Flight server (2026-11-08): a
table written by the old pyiceberg-direct ``dhd ingest-dlt`` was invisible
to ``dhd query``/``dhd list-tables`` -- two different SQLite catalog
files, see ``docs/INGESTION.md``'s "single writer" section. Fix: route
``replace``/``append`` through the Flight server's own ``create-table``/
``insert`` actions (:class:`FlightSink`) -- the exact path ``dhd
create-table``/``dhd insert`` already use, so a table lands exactly where
reads already look.

``merge`` (pyiceberg's native upsert) has no Flight-level equivalent today:
``HybridBackend`` inherits a working ``upsert()`` *method* unmodified from
xorq's ``PyIcebergBackend``, but no Flight action or ``UploadMode`` reaches
it over the wire (``xorq.flight.server.put_table`` only dispatches
create/append/replace). :class:`DirectSink` still writes it straight via
pyiceberg from the client -- but only once ``catalog_guard.check_same_catalog``
has proven the client and the Flight server share one catalog.
"""

from __future__ import annotations

from typing import Protocol

import pyarrow as pa

from datahut_duckhouse.catalog_guard import check_same_catalog
from datahut_duckhouse.schema_evolve_action import request_schema_evolution


class SinkError(RuntimeError):
    """A table write failed in a way the caller should present cleanly."""


class SchemaContractViolationError(RuntimeError):
    """A ``schema_contract: freeze`` source tried to write a new column."""


class Sink(Protocol):
    """A destination for one table's worth of extracted rows."""

    write_dispositions: tuple[str, ...]

    def list_tables(self) -> list[str]: ...

    def write(self, table_name: str, data: pa.Table, **kwargs: object) -> str:
        """Write ``data`` to ``table_name``; return a verb: "created",
        "overwritten", "appended" or "merged"."""
        ...


def _existing_columns(names: list[str]) -> set[str]:
    return set(names)


class FlightSink:
    """Writes ``replace``/``append`` through the Flight server."""

    write_dispositions = ("replace", "append")

    def __init__(self, flight_backend) -> None:
        self.backend = flight_backend

    def list_tables(self) -> list[str]:
        return list(self.backend.list_tables())

    def existing_columns(self, table_name: str) -> set[str] | None:
        if table_name not in self.list_tables():
            return None
        return _existing_columns(list(self.backend.get_schema(table_name).names))

    def apply_schema_contract(
        self, table_name: str, data: pa.Table, contract: str
    ) -> pa.Table:
        """Mirrors ``PyIcebergEngine.apply_schema_contract``'s semantics
        exactly, split across what the client can decide on its own
        (freeze/discard_value: pure reads) and what needs a server-side
        mutation (evolve: widening the table's Iceberg schema)."""
        existing = self.existing_columns(table_name)
        if existing is None:
            return data  # brand-new table: no contract to apply yet
        new_cols = [c for c in data.column_names if c not in existing]
        if not new_cols:
            return data
        if contract == "freeze":
            raise SchemaContractViolationError(
                f"'{table_name}': incoming data has column(s) not in the "
                f"table's schema ({', '.join(new_cols)}) and this source's "
                f"schema_contract is 'freeze'."
            )
        if contract == "discard_value":
            return data.drop_columns(new_cols)
        # "evolve" (default): widen the server's Iceberg table in place, via
        # a dynamically-registered action reaching the server's own
        # `.catalog` object -- not a HybridBackend change, see
        # `schema_evolve_action.py`.
        request_schema_evolution(self.backend, table_name, data.schema)
        return data

    def write(
        self,
        table_name: str,
        data: pa.Table,
        write_disposition: str,
        primary_key: list[str] | None = None,
    ) -> str:
        del primary_key  # unused: only DirectSink's 'merge' needs it
        exists = table_name in self.list_tables()
        if write_disposition == "replace":
            self.backend.con.upload_table(table_name, data, mode="replace")
            return "created" if not exists else "overwritten"
        if write_disposition == "append":
            # mode=None: server creates on a first run, appends afterwards --
            # the exact "created"/"appended" split this module already
            # reports for the direct-write path.
            self.backend.con.upload_table(table_name, data, mode=None)
            return "created" if not exists else "appended"
        raise SinkError(
            f"FlightSink cannot write write_disposition={write_disposition!r} "
            f"(only 'replace'/'append' -- 'merge' needs DirectSink)."
        )


class DirectSink:
    """Writes ``merge`` straight to Iceberg via pyiceberg -- gated by
    ``catalog_guard.check_same_catalog``."""

    write_dispositions = ("merge",)

    def __init__(self, pyiceberg_engine, flight_backend_for_guard) -> None:
        check_same_catalog(flight_backend_for_guard, pyiceberg_engine)
        self.engine = pyiceberg_engine

    def list_tables(self) -> list[str]:
        return self.engine.list_tables()

    def apply_schema_contract(
        self, table_name: str, data: pa.Table, contract: str
    ) -> pa.Table:
        from datahut_duckhouse.ingest import SchemaContractViolationError as _Viol

        try:
            return self.engine.apply_schema_contract(table_name, data, contract)
        except _Viol as exc:
            raise SchemaContractViolationError(str(exc)) from exc

    def write(
        self,
        table_name: str,
        data: pa.Table,
        write_disposition: str,
        primary_key: list[str] | None = None,
    ) -> str:
        if write_disposition != "merge":
            raise SinkError(
                f"DirectSink only writes 'merge' (got {write_disposition!r})."
            )
        exists = table_name in self.list_tables()
        if not exists:
            self.engine.create_table(table_name, data)
            return "created"
        self.engine.upload_table(
            table_name, data, mode="merge", primary_key=primary_key
        )
        return "merged"
