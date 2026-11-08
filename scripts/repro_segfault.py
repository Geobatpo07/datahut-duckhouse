"""Minimal reproducer for the ``dhd query`` / ``dhd create-table`` segfault
(fix/segfault-diagnosis, step 2).

Bypasses the `dhd` CLI and xorq's ``FlightClient``/``HybridBackend`` entirely.
Talks straight to the Flight server with a bare ``pyarrow.flight.FlightClient``
and replicates exactly what ``xorq.flight.client.FlightClient._do_action`` does
for the ``get_schema_using_query`` action (see
``xorq/flight/backend.py::_get_schema_using_query`` and
``xorq/flight/client.py::_do_action`` inside the flight-server image):

    action = pa.flight.Action("get_schema_using_query", cloudpickle.dumps(query))
    for result in client.do_action(action):
        raw = result.body.to_pybytes()   # <-- saved to disk BEFORE unpickling
        schema = cloudpickle.loads(raw)

The raw bytes are written to disk before ``cloudpickle.loads`` runs, so even if
that call segfaults, the exact payload that crashed is still on disk for
later, offline, isolated replay (``--replay``).

Usage::

    PYTHONFAULTHANDLER=1 uv run python scripts/repro_segfault.py
    uv run python scripts/repro_segfault.py --replay schema_bytes.pkl  # no network
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pyarrow.flight as paf
from cloudpickle import dumps, loads

DEFAULT_HOST = os.getenv("FLIGHT_SERVER_HOST", "localhost")
DEFAULT_PORT = int(os.getenv("FLIGHT_SERVER_PORT", "8815"))
DEFAULT_QUERY = "SELECT * FROM sales"
DEFAULT_OUT = Path(__file__).resolve().parent / "schema_bytes.pkl"


def fetch_raw_schema_bytes(host: str, port: int, query: str, out_path: Path) -> bytes:
    url = f"grpc://{host}:{port}"
    print(f"Connecting to {url} ...")
    client = paf.FlightClient(url)

    print("do_action('healthcheck') ...")
    list(client.do_action(paf.Action("healthcheck", dumps({}))))
    print("  -> OK (server reachable, this action never crashes)")

    print(f"do_action('get_schema_using_query', query={query!r}) ...")
    action = paf.Action("get_schema_using_query", dumps(query))
    results = list(client.do_action(action))
    print(f"  -> got {len(results)} Result(s) back from the server")

    raw = results[0].body.to_pybytes()
    out_path.write_bytes(raw)
    print(f"  -> wrote {len(raw)} raw (still-pickled) bytes to {out_path}")
    return raw


def replay(path: Path):
    raw = path.read_bytes()
    print(f"Read {len(raw)} bytes from {path}. Calling cloudpickle.loads(raw) ...")
    schema = loads(raw)
    print(f"  -> OK, no crash. schema = {schema!r}")
    return schema


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--query", default=DEFAULT_QUERY)
    parser.add_argument(
        "--out", type=Path, default=DEFAULT_OUT, help="Where to save the raw bytes."
    )
    parser.add_argument(
        "--replay",
        type=Path,
        default=None,
        help="Skip the network entirely; just cloudpickle.loads() a previously "
        "saved --out file. Use this to test whether the crash is in the gRPC "
        "round trip or in unpickling the schema itself.",
    )
    parser.add_argument(
        "--skip-loads",
        action="store_true",
        help="Fetch the bytes but don't call cloudpickle.loads() on them -- "
        "use this to confirm the crash is NOT in the do_action()/to_pybytes() "
        "gRPC path by itself.",
    )
    args = parser.parse_args()

    if args.replay is not None:
        replay(args.replay)
        return 0

    raw = fetch_raw_schema_bytes(args.host, args.port, args.query, args.out)
    if args.skip_loads:
        print("--skip-loads set: not calling cloudpickle.loads(). Done.")
        return 0

    print("Calling cloudpickle.loads(raw) ...")
    schema = loads(raw)
    print(f"  -> OK, no crash. schema = {schema!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
