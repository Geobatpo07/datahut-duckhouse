"""Sink routing (ADR-0006, decision 5): ``auto`` picks the direct sink
whenever a direct write is actually possible from this process, and
``flight`` otherwise -- no size threshold. ``check_direct_capacity()`` is
the capacity check that decision 5 names as ``catalog_guard``'s
replacement for *that* job: it answers "can this process write directly
at all" (catalog and storage reachable), which is what ``auto`` needs to
decide.

``catalog_guard.check_same_catalog`` is kept, not removed -- see
``direct_sink.py``'s docstring for why it's still called on every direct
write, just demoted from "the reason direct is allowed" to a safety net
underneath the capacity check.
"""

from __future__ import annotations

import os
import socket
from urllib.parse import urlparse

DEFAULT_WARN_ROWS = 1_000_000


class DirectSinkUnavailableError(RuntimeError):
    """``--sink direct`` was requested but a direct write is not possible
    from this process. Never falls back silently (ADR-0006, decision 5) --
    the caller maps this to exit code 2."""


class UnknownSinkError(ValueError):
    """``source.sink`` / ``--sink`` is not one of auto|flight|direct."""


def _host_port_reachable(
    url: str, *, default_port: int, timeout: float = 2.0
) -> tuple[bool, str]:
    parsed = urlparse(url)
    host = parsed.hostname
    if not host:
        return True, ""  # e.g. a bare sqlite:///path URI -- nothing to reach
    port = parsed.port or default_port
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, ""
    except OSError as exc:
        return False, f"{host}:{port} unreachable ({type(exc).__name__}: {exc})"


def check_direct_capacity() -> tuple[bool, str]:
    """Can this process write directly? Checks that the catalog's own URI
    (only when ``ICEBERG_CATALOG_TYPE=rest`` -- Nessie) and the object
    store's endpoint (only when the resolved warehouse is actually
    ``s3://`` -- not whenever ``S3_ENDPOINT`` happens to be *set*, which
    it may be in the environment while unused by a local/solo warehouse)
    are both reachable by a plain TCP connect -- the exact gap the
    workstation rule exists for: the Windows host cannot resolve the
    Docker network's container names (docs/spikes/NESSIE_SPIKES.md, S1;
    docs/spikes/NESSIE_SPIKES_2.md, T1). Returns ``(possible, reason)``;
    ``reason`` is empty when ``possible``.

    Deliberately a reachability check, not a full write probe: an actual
    S3 write has side effects (an object landing in storage) that a
    routing decision shouldn't cause just by being asked.
    """
    from iceberg_catalog.factory import default_warehouse_location, is_remote_warehouse

    if os.getenv("ICEBERG_CATALOG_TYPE", "sql") == "rest":
        catalog_uri = os.getenv("ICEBERG_CATALOG_URI")
        if catalog_uri:
            ok, reason = _host_port_reachable(catalog_uri, default_port=80)
            if not ok:
                return False, f"catalog unreachable: {reason}"

    if is_remote_warehouse(default_warehouse_location()):
        s3_endpoint = os.getenv("S3_ENDPOINT") or os.getenv("AWS_ENDPOINT_URL_S3")
        if s3_endpoint:
            ok, reason = _host_port_reachable(s3_endpoint, default_port=9000)
            if not ok:
                return False, f"storage unreachable: {reason}"

    return True, ""


def _warn_rows() -> int:
    raw = os.getenv("DHD_SINK_WARN_ROWS")
    if not raw:
        return DEFAULT_WARN_ROWS
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_WARN_ROWS
    return value if value > 0 else DEFAULT_WARN_ROWS


def resolve_sink(source_sink: str, *, capacity_check=None) -> tuple[str, str]:
    """Decide which sink to use for one source. Returns ``(sink,
    sink_reason)`` -- ``sink`` is ``"flight"`` or ``"direct"``.

    - ``"flight"``: honored as asked.
    - ``"direct"``: honored if possible; raises
      :class:`DirectSinkUnavailableError` otherwise -- never a silent
      fallback to flight.
    - ``"auto"``: direct if possible, flight otherwise. No size threshold
      (docs/spikes/NESSIE_SPIKES_2.md, T6 found no crossover point where
      flight wins).

    Row-count estimates don't factor into this decision (capacity is a
    fixed fact about this process, independent of any one table's size)
    -- see :func:`flight_size_warning` for the separate, per-table
    "you picked flight for a big load" warning.

    ``capacity_check`` is looked up as a module global at call time, not
    bound as a default-argument value at def time: a default-argument
    binding would capture ``check_direct_capacity`` once, at import, so
    monkeypatching ``router.check_direct_capacity`` afterwards (the normal
    way a test replaces it) would silently have no effect.
    """
    if capacity_check is None:
        capacity_check = check_direct_capacity

    if source_sink == "flight":
        return "flight", "explicit --sink flight"

    if source_sink == "direct":
        possible, reason = capacity_check()
        if not possible:
            raise DirectSinkUnavailableError(
                f"--sink direct requested but not possible from this "
                f"process: {reason}. See docs/CATALOG.md's workstation "
                f"rule."
            )
        return "direct", "explicit --sink direct"

    if source_sink == "auto":
        possible, reason = capacity_check()
        if possible:
            return "direct", "auto: direct path reachable"
        return "flight", f"auto: direct path unreachable ({reason}), using Flight"

    raise UnknownSinkError(f"unknown sink {source_sink!r} (choose auto|flight|direct)")


def flight_size_warning(estimated_rows: int) -> str | None:
    """A message to surface when an explicit ``--sink flight`` write is
    large enough (``DHD_SINK_WARN_ROWS``, default 1,000,000) that the
    direct sink would likely be faster -- docs/spikes/NESSIE_SPIKES_2.md,
    T6: direct dominated flight in every cell of the routing-threshold
    test matrix, up to 7.7x faster at 10M target rows. Returns ``None``
    below the threshold.
    """
    if estimated_rows < _warn_rows():
        return None
    return (
        f"sink=flight with an estimated {estimated_rows:,} rows -- the "
        f"direct sink is faster at this scale (see "
        f"docs/spikes/NESSIE_SPIKES_2.md, T6); consider --sink direct or "
        f"--sink auto."
    )
