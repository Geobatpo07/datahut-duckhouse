"""Single-writer guard for the one write path that cannot go through Flight.

``dhd ingest``'s ``replace``/``append`` writes go through the Flight
server's own ``create-table``/``insert`` actions (see ``ingest_sinks.py``),
so the table lands wherever ``dhd query``/``dhd list-tables`` already look.
``merge`` (pyiceberg's native upsert) has no Flight-level equivalent today:
``HybridBackend`` inherits a working ``upsert()`` *method* unmodified from
xorq's ``PyIcebergBackend``, but no Flight action or ``UploadMode`` reaches
it over the wire (``xorq.flight.server.put_table`` only dispatches
create/append/replace) -- so ``merge`` still writes directly via pyiceberg
from the client.

That direct write is only a *single* writer if the client and the Flight
server are, in fact, pointed at the same catalog. This module proves that
at runtime, by asking the server (via a tiny action registered dynamically
through xorq's own ``add-action`` -- the same technique
``flight_version_guard.py`` uses, no ``HybridBackend``/xorq change needed)
what catalog URI and warehouse it resolved, and comparing them to the
client's own (``PyIcebergEngine.uri`` / ``.warehouse_path``).
"""

from __future__ import annotations

_ACTION_NAME = "dhd_server_catalog_identity"


class _ServerCatalogIdentityAction:
    """Registered on the server at runtime via xorq's ``add-action``.

    Deliberately NOT a subclass of ``xorq.flight.action.AbstractAction`` --
    see ``flight_version_guard.py``'s identical note: that ABC's
    ``classproperty`` attributes cannot be cloudpickled at the class level.
    """

    name = _ACTION_NAME

    @classmethod
    def do_action(cls, server, context, action):
        from xorq.flight.action import make_flight_result

        conn = server._conn
        yield make_flight_result((str(conn.uri), str(conn.warehouse_path)))


class CatalogMismatchError(RuntimeError):
    """The client's pyiceberg catalog and the Flight server's are different."""


def server_catalog_identity(flight_backend) -> tuple[str, str]:
    """Ask the connected Flight server for its resolved (uri, warehouse_path)."""
    flight_backend.con.do_action("add-action", _ServerCatalogIdentityAction)
    (identity,) = flight_backend.con.do_action(_ACTION_NAME)
    uri, warehouse_path = identity
    return str(uri), str(warehouse_path)


def _normalize_warehouse(value: str) -> str:
    """``PyIcebergEngine.warehouse_path`` prefixes a local path with
    ``file://`` (it's built straight into pyiceberg's own ``warehouse``
    catalog property); ``HybridBackend.warehouse_path`` never does (that
    prefix is added separately, only inside its *own* catalog_params dict,
    never stored back onto the attribute this guard reads). Strip it so an
    identical local warehouse doesn't look like a mismatch over a cosmetic
    difference. ``s3a://`` vs ``s3://`` is the same normalization both
    classes already apply to their *catalog* ``warehouse`` property, kept
    here too for symmetry."""
    return value.removeprefix("file://").replace("s3a://", "s3://", 1)


def check_same_catalog(flight_backend, pyiceberg_engine) -> None:
    """Raise :class:`CatalogMismatchError` unless ``pyiceberg_engine`` (about
    to write directly) resolves to the exact same catalog as the connected
    ``flight_backend``."""
    server_uri, server_warehouse = server_catalog_identity(flight_backend)
    client_uri, client_warehouse = (
        pyiceberg_engine.uri,
        pyiceberg_engine.warehouse_path,
    )
    if (server_uri, _normalize_warehouse(server_warehouse)) == (
        client_uri,
        _normalize_warehouse(client_warehouse),
    ):
        return
    raise CatalogMismatchError(
        "Refusing to write 'merge' directly via pyiceberg: this client and "
        "the Flight server resolve to different Iceberg catalogs, so a "
        "direct write here would not be a single writer -- it would quietly "
        "fragment the catalog (see docs/INGESTION.md, 'single writer').\n"
        f"  this client's catalog:  {client_uri}\n"
        f"  this client's warehouse: {client_warehouse}\n"
        f"  Flight server's catalog:  {server_uri}\n"
        f"  Flight server's warehouse: {server_warehouse}\n"
        "Fix: point this client's ICEBERG_CATALOG_URI / "
        "ICEBERG_WAREHOUSE(_PATH) at the same values as the Flight server's "
        "(check the flight-server container's environment), or run this "
        "source with a write_disposition other than 'merge'."
    )
