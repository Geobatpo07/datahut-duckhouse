"""Widen a table's Iceberg schema over Flight (``schema_contract: evolve``
under ``write_disposition: append``, routed through :class:`FlightSink`).

Pyiceberg's own ``Table.append``/``insert`` raise ``ValueError`` on an
unseen column (verified, see ``docs/DLT_EVAL.md``'s schema-evolution
experiment) -- the direct-write path (``DirectSink``) avoids that by
calling ``iceberg_table.update_schema().union_by_name(...).commit()``
itself before writing. Over Flight there is no action for that, so this
registers one at runtime via xorq's own ``add-action`` (same technique as
``flight_version_guard.py`` / ``catalog_guard.py`` -- no ``HybridBackend``
or xorq change): it reaches the server's live ``.catalog`` object
directly, the very same object ``HybridBackend`` already exposes, and
calls the identical pyiceberg API the direct path uses.
"""

from __future__ import annotations

import pyarrow as pa

_ACTION_NAME = "dhd_server_schema_evolve"


class _SchemaEvolveAction:
    """Registered on the server at runtime. Body: ``(table_name, schema_bytes)``
    -- the incoming pyarrow schema serialized with ``schema.serialize()``,
    not the live object, to keep this plain-value-safe like the other two
    dynamically-registered actions in this project."""

    name = _ACTION_NAME

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
        pass
    return sink.getvalue().to_pybytes()


def request_schema_evolution(
    flight_backend, table_name: str, schema: pa.Schema
) -> None:
    """Ask the connected Flight server to widen ``table_name``'s Iceberg
    schema to include every field of ``schema`` (``union_by_name``)."""
    flight_backend.con.do_action("add-action", _SchemaEvolveAction)
    flight_backend.con.do_action(_ACTION_NAME, (table_name, _serialize_schema(schema)))
