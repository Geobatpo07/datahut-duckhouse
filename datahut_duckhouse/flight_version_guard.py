"""Guard against the cross-Python-minor-version cloudpickle segfault.

See ``docs/adr/0001-canonical-hybrid-backend.md``'s "Segfault root cause"
note and ``docs/UPSTREAM_ISSUE.md``: a Flight client and server running
different Python *minor* versions can segfault the client when an action
round-trips a cloudpickled xorq/ibis ``Annotable`` object
(``get_schema_using_query``, used by ``create-table``'s post-write
confirmation and by ``query``). This registers a tiny custom Flight action
at runtime, via xorq's own ``add-action`` (no ``HybridBackend`` or xorq
changes needed), that reports the server's Python version as a plain
``(major, minor)`` tuple of ints -- verified to survive the exact
cross-version round trip that crashes on an ``Annotable`` payload, since
plain values don't hit the bug (see ``docs/UPSTREAM_ISSUE.md``'s "Sanity"
step, and the live cross-version check below) -- and refuses to go further
if it doesn't match the client's own version.
"""

from __future__ import annotations

import sys

from xorq.flight.action import make_flight_result

_ACTION_NAME = "dhd_server_python_version"


class _ServerPythonVersionAction:
    """Registered on the server at runtime via xorq's ``add-action``.

    Deliberately NOT a subclass of ``xorq.flight.action.AbstractAction``:
    that ABC declares ``name``/``description`` as ``classproperty`` objects,
    which cloudpickle cannot pickle at the class level (verified:
    ``TypeError: cannot pickle 'classproperty' object``, raised client-side
    before anything touches the network). A plain class attribute avoids
    that. The server's dispatch (``xorq/flight/server.py``) only needs
    ``.name`` (to key ``server.actions``) and a ``do_action`` classmethod --
    not the ABC itself.
    """

    name = _ACTION_NAME

    @classmethod
    def do_action(cls, server, context, action):
        yield make_flight_result(tuple(sys.version_info[:2]))


class FlightPythonVersionMismatchError(RuntimeError):
    """The Flight client and server are on different Python minor versions."""


def check_python_version(backend) -> None:
    """Raise :class:`FlightPythonVersionMismatchError` on a minor-version mismatch.

    Registers ``_ServerPythonVersionAction`` on ``backend.con`` (the xorq
    ``FlightClient``) and compares its answer to this process's own
    ``sys.version_info``. Call this right after ``do_connect``, before any
    action that might round-trip a Schema/``Annotable`` object.
    """
    client_version = tuple(sys.version_info[:2])
    backend.con.do_action("add-action", _ServerPythonVersionAction)
    (server_version,) = backend.con.do_action(_ACTION_NAME)
    server_version = tuple(server_version)
    if server_version != client_version:
        client_str = f"{client_version[0]}.{client_version[1]}"
        server_str = f"{server_version[0]}.{server_version[1]}"
        raise FlightPythonVersionMismatchError(
            f"Client is Python {client_str}, but the Flight server is Python "
            f"{server_str}. Running `dhd` against a server on a different "
            f"Python *minor* version segfaults the client on any "
            f"Schema-returning action (`create-table`'s confirmation, "
            f"`query`) -- see docs/adr/0001-canonical-hybrid-backend.md and "
            f"docs/UPSTREAM_ISSUE.md. Fix: rebuild/run the flight-server "
            f"image with `--build-arg PYTHON_VERSION={client_str}`, or run "
            f"this client under Python {server_str} (e.g. "
            f"`uv venv --python {server_str}`)."
        )
