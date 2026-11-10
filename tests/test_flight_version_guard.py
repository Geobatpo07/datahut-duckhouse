"""Tests for ``datahut_duckhouse.flight_version_guard`` (chore/python-matrix).

Verifies the three cases the matrix brief asked for: matching versions pass
silently, mismatched versions raise a clear, non-segfaulting error before any
risky action is sent, and an unreachable server never even reaches the
version check (``connection._assert_reachable`` runs first).

The "cross-version add-action round trip never segfaults" claim itself is
verified live against a real Dockerized flight-server, not here -- see
docs/adr/0001-canonical-hybrid-backend.md's "Guard verified end-to-end" note.
These tests only cover the guard's own comparison/error-message logic with a
stand-in client, matching the style already used by ``tests/test_cli.py``'s
``_FakeBackend``.
"""

from __future__ import annotations

import sys

import pytest

from datahut_duckhouse import connection as connection_mod
from datahut_duckhouse.flight_version_guard import (
    _ACTION_NAME,
    FlightPythonVersionMismatchError,
    check_python_version,
)


class _FakeFlightClient:
    """Stand-in for ``xorq.flight.client.FlightClient``: records calls."""

    def __init__(self, server_version):
        self.server_version = server_version
        self.calls: list[tuple[str, object]] = []

    def do_action(self, action_type, action_body=None, options=None):
        self.calls.append((action_type, action_body))
        if action_type == "add-action":
            return (None,)
        if action_type == _ACTION_NAME:
            return (self.server_version,)
        raise AssertionError(f"unexpected action: {action_type}")


class _FakeBackend:
    def __init__(self, server_version):
        self.con = _FakeFlightClient(server_version)


class TestCheckPythonVersion:
    def test_matching_versions_passes_silently(self):
        client_version = tuple(sys.version_info[:2])
        backend = _FakeBackend(server_version=client_version)
        check_python_version(backend)  # must not raise
        assert backend.con.calls[0][0] == "add-action"
        assert backend.con.calls[1][0] == _ACTION_NAME

    def test_mismatched_versions_raises_with_both_versions_named(self):
        client_version = tuple(sys.version_info[:2])
        server_version = (client_version[0], client_version[1] + 1)
        backend = _FakeBackend(server_version=server_version)

        with pytest.raises(FlightPythonVersionMismatchError) as excinfo:
            check_python_version(backend)

        message = str(excinfo.value)
        assert f"{client_version[0]}.{client_version[1]}" in message
        assert f"{server_version[0]}.{server_version[1]}" in message

    def test_mismatch_registers_the_action_before_comparing(self):
        # The guard must register its own action every call (the server is a
        # long-running process the client doesn't control the history of) --
        # never skip straight to the comparison.
        backend = _FakeBackend(server_version=(99, 0))

        with pytest.raises(FlightPythonVersionMismatchError):
            check_python_version(backend)

        assert ("add-action", None) not in (
            (t, b) for t, b in backend.con.calls if t != "add-action"
        )
        assert any(t == "add-action" for t, _ in backend.con.calls)


class TestUnreachableServerNeverReachesVersionCheck:
    def test_unreachable_server_raises_before_any_version_check(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            connection_mod,
            "_assert_reachable",
            lambda host, port, timeout=3.0: (_ for _ in ()).throw(
                connection_mod.FlightUnavailableError("No Flight server at host:1")
            ),
        )

        def _spy_check(backend):
            calls.append(backend)

        monkeypatch.setattr(connection_mod, "check_python_version", _spy_check)

        with pytest.raises(connection_mod.FlightUnavailableError):
            connection_mod.connect()

        assert calls == [], (
            "check_python_version must not run when the TCP pre-check already "
            "failed -- an unreachable server is a different, earlier failure "
            "mode than a version mismatch."
        )
