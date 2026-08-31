"""Connection helper for the ``dhd`` CLI.

The CLI never touches ``HybridBackend`` directly - it goes through the Arrow
Flight server (``flight_server.app.app_xorq``), which is the whole point of
Phase 2: exercise the real network + Iceberg + reflected-DuckDB-views path.

``xorq==0.2.4`` exposes a Flight-backed ibis backend, but only via
``xorq.flight.connect(FlightUrl(...))`` - and ``FlightUrl`` *binds* the port on
construction (it is built for the server side). For a plain client we use the
lower-level ``xorq.flight.backend.Backend`` directly, which just wraps a
``FlightClient`` with no socket binding.

Known limitation (documented in ADR-0001): ``FlightClient`` in this xorq version
has no visible connect/RPC timeout, so an unreachable server makes ``dhd`` hang
rather than failing fast. We do a cheap TCP pre-check here to turn the common
case (server not started) into a clear error instead of a hang.
"""

from __future__ import annotations

import os
import socket
from contextlib import closing

from xorq.flight.backend import Backend as _FlightBackend

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 8815


class FlightUnavailableError(RuntimeError):
    """The Flight server could not be reached."""


def flight_target() -> tuple[str, int]:
    host = os.getenv("FLIGHT_SERVER_HOST", DEFAULT_HOST)
    # The server may legitimately bind 0.0.0.0; a client must dial a real host.
    if host in ("0.0.0.0", ""):
        host = DEFAULT_HOST
    port = int(os.getenv("FLIGHT_SERVER_PORT", str(DEFAULT_PORT)))
    return host, port


def _assert_reachable(host: str, port: int, timeout: float = 3.0) -> None:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.settimeout(timeout)
        try:
            sock.connect((host, port))
        except OSError as exc:
            raise FlightUnavailableError(
                f"No Flight server at {host}:{port} ({exc.__class__.__name__}). "
                f"Start it with `uv run python -m flight_server.app.app_xorq` "
                f"(or `make dev-server`), or set FLIGHT_SERVER_HOST / "
                f"FLIGHT_SERVER_PORT."
            ) from exc


def connect() -> _FlightBackend:
    """Return a connected Flight-backed ibis backend."""
    host, port = flight_target()
    _assert_reachable(host, port)
    backend = _FlightBackend()
    backend.do_connect(host=host, port=port)
    return backend


# Alias kept for the ingestion scripts (scripts/ingest_flight.py, pipeline.py)
# and their tests, which were written against this name.
get_connection = connect
