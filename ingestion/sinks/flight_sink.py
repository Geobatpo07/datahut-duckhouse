"""The default sink: writes ``replace``/``append`` through the Flight
server (follow-up review, step 0) -- the same ``create-table``/``insert``
actions ``dhd create-table``/``dhd insert`` already use, so a table lands
exactly where ``dhd query``/``dhd list-tables`` already look.

Takes an already-connected Flight backend (``xorq.flight.backend.Backend``,
duck-typed -- this module never imports it) rather than connecting itself,
so it never needs to know how a connection is built (host/port, the
Python-version guard, ...) -- that stays the caller's job.
"""

from __future__ import annotations

import pyarrow as pa

from ingestion.sinks.base import SchemaContractViolationError, SinkError

_SCHEMA_EVOLVE_ACTION_NAME = "dhd_server_schema_evolve"


class _SchemaEvolveAction:
    """Registered on the server at runtime via xorq's own ``add-action``
    (same technique as ``catalog_guard.py``/``flight_version_guard.py`` --
    no ``HybridBackend``/xorq change). Widens ``table_name``'s Iceberg
    schema to include every field of the incoming data
    (``update_schema().union_by_name(...).commit()``) -- the same pyiceberg
    call ``DirectSink`` makes directly, reached here through the server's
    live ``.catalog`` object, the very same one ``HybridBackend`` already
    exposes. Body: ``(table_name, schema_bytes)`` -- the pyarrow schema
    serialized with ``schema.serialize()``-equivalent IPC bytes, not the
    live object, to keep this plain-value-safe like the other two
    dynamically-registered actions in this project."""

    name = _SCHEMA_EVOLVE_ACTION_NAME

    @classmethod
    def do_action(cls, server, context, action):
        from cloudpickle import loads
        from xorq.flight.action import make_flight_result

        table_name, schema_bytes = loads(action.body)
        schema = pa.ipc.read_schema(pa.BufferReader(schema_bytes))
        conn = server._conn
        ident = f"{conn.namespace}.{table_name}"
        iceberg_table = conn.catalog.load_table(ident)
        iceberg_table.update_schema().union_by_name(schema).commit()
        yield make_flight_result(True)


def _serialize_schema(schema: pa.Schema) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema):
        pass  # header-only stream: just the schema, no record batches
    return sink.getvalue().to_pybytes()


def _request_schema_evolution(
    flight_backend, table_name: str, schema: pa.Schema
) -> None:
    flight_backend.con.do_action("add-action", _SchemaEvolveAction)
    flight_backend.con.do_action(
        _SCHEMA_EVOLVE_ACTION_NAME, (table_name, _serialize_schema(schema))
    )


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
        return set(self.backend.get_schema(table_name).names)

    def apply_schema_contract(
        self, table_name: str, data: pa.Table, contract: str
    ) -> pa.Table:
        """Mirrors ``DirectSink``'s semantics exactly, split across what the
        client can decide on its own (freeze/discard_value: pure reads) and
        what needs a server-side mutation (evolve: widening the table's
        Iceberg schema, via ``_request_schema_evolution`` above)."""
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
        # "evolve" (default): widen the server's Iceberg table in place.
        _request_schema_evolution(self.backend, table_name, data.schema)
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
            # mode=None: server creates on a first run, appends afterwards.
            self.backend.con.upload_table(table_name, data, mode=None)
            return "created" if not exists else "appended"
        raise SinkError(
            f"FlightSink cannot write write_disposition={write_disposition!r} "
            f"(only 'replace'/'append' -- 'merge' needs DirectSink)."
        )
