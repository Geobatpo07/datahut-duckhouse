"""ADR-0006, condition C5 (step 6 of the implementation brief): is a table
written directly (bypassing the Flight server) visible at once to `dhd
list-tables`/`dhd query` -- and to dbt's `dev` target?

Two halves, run from different places:

1. ``write`` -- inside the Docker network, against a *separately started*
   deployment-mode Flight server (see the module docstring's exact
   commands below for how that server was started in this session: a
   background `docker compose run --rm -d` of the `flight-server` image,
   its own port published to the host, so a querying client can use it
   from host or container alike). Writes one row directly via
   ``iceberg_catalog.get_catalog()`` -- pyiceberg only, no Flight.
2. ``check`` -- from anywhere that can reach that server's published
   port: polls ``list_tables()`` and queries the table through the
   *Flight* client (``datahut_duckhouse.connection.connect()``), timing
   both.

Exact commands used to produce this spike's result (docs/spikes or the
implementation report cites this script; adjust the port/table name to
avoid colliding with a real deployment):

    # 1. a deployment-mode Flight server, in the background, its own port:
    docker compose run --rm -d --name flight-server-c5 -p 8816:8815 \\
      -e ICEBERG_CATALOG_TYPE=rest \\
      -e ICEBERG_CATALOG_URI=http://nessie:19120/iceberg \\
      -e ICEBERG_WAREHOUSE=s3://duckhouse-warehouse/shared-catalog/ \\
      -e ICEBERG_BRANCH=main -e ICEBERG_NAMESPACE=step6 \\
      -e DUCKDB_PATH=/app/ingestion/data/step6_duckhouse.duckdb \\
      -e FLIGHT_SERVER_PORT=8815 -e FLIGHT_SERVER_HOST=0.0.0.0 \\
      flight-server

    # 2. the direct write, inside the network (bypasses Flight):
    docker compose run --rm \\
      -e ICEBERG_CATALOG_TYPE=rest \\
      -e ICEBERG_CATALOG_URI=http://nessie:19120/iceberg \\
      -e ICEBERG_WAREHOUSE=s3://duckhouse-warehouse/shared-catalog/ \\
      -e ICEBERG_BRANCH=main -e ICEBERG_NAMESPACE=step6 \\
      --entrypoint /app/.venv/bin/python \\
      flight-server scripts/verify_direct_write_visibility.py write

    # 3. the check, from the HOST (Flight's own port is published; only
    #    data WRITES need the Docker network, not Flight's RPC protocol):
    FLIGHT_SERVER_PORT=8816 uv run python scripts/verify_direct_write_visibility.py check

    # 4. the dbt-visibility check (also inside the network, while the
    #    server from step 1 is NOT holding the file open -- DuckDB's
    #    single-writer lock blocks even a read-only cold open while the
    #    server is live, found during this verification): stop the
    #    server from step 1 first, then:
    docker compose run --rm --entrypoint /app/.venv/bin/python flight-server \\
      scripts/verify_direct_write_visibility.py dbt-cold-check

Result of this exact sequence (see the implementation report, step 6):
(a) list_tables/query via Flight: visible on the very first poll, no
measurable staleness beyond normal RPC/scan latency. (b) the cold DuckDB
file dbt's `dev` target opens: empty, and still would be even if dbt
could open it while the server runs (exclusive lock) -- `_reflect_views`
registers Arrow data into the *live session only*, never persisted to
the on-disk catalog; the project's own dbt models today don't query
Iceberg-backed sources at all (pre-existing, unrelated to this ADR).
"""

from __future__ import annotations

import sys
import time

import pyarrow as pa

NAMESPACE = "step6"
TABLE = "step6_direct_proof"
IDENT = f"{NAMESPACE}.{TABLE}"


def write() -> None:
    from iceberg_catalog import get_catalog

    cat = get_catalog(ensure_namespace=NAMESPACE)
    if TABLE in [t[-1] for t in cat.list_tables(NAMESPACE)]:
        cat.drop_table(IDENT)
    cat.create_table(IDENT, schema=pa.table({"id": [1], "label": ["direct"]}).schema)
    cat.load_table(IDENT).append(pa.table({"id": [1], "label": ["direct"]}))
    print("write done")


def check(timeout_s: float = 10.0) -> None:
    from datahut_duckhouse.connection import connect

    backend = connect()
    t0 = time.perf_counter()
    polls = 0
    while True:
        polls += 1
        if TABLE in backend.list_tables():
            break
        if time.perf_counter() - t0 > timeout_s:
            print(f"RESULT NOT VISIBLE after {timeout_s}s ({polls} polls)")
            raise SystemExit(1)
        time.sleep(0.05)
    t_list_ms = (time.perf_counter() - t0) * 1000
    rows = backend.sql(f"SELECT * FROM {TABLE}").to_pyarrow().to_pylist()
    t_query_ms = (time.perf_counter() - t0) * 1000
    print(
        f"RESULT list_tables saw it after {t_list_ms:.1f}ms ({polls} poll(s)); "
        f"query returned {rows} after {t_query_ms:.1f}ms total"
    )


def dbt_cold_check() -> None:
    """Opens the SAME DUCKDB_PATH file cold (exactly what dbt's `dev`
    target does -- a fresh ``duckdb.connect()``, no Flight involved) and
    reports what it sees. Only works while no Flight server currently
    holds that file open (DuckDB's single-writer lock) -- which is
    itself part of the finding."""
    import os

    import duckdb

    path = os.environ["DUCKDB_PATH"]
    con = duckdb.connect(path, read_only=True)
    tables = con.execute("SHOW TABLES").fetchall()
    print(f"RESULT tables in {path} (cold open): {tables}")
    try:
        rows = con.execute(f"SELECT * FROM {TABLE}").fetchall()
        print(f"RESULT {TABLE} rows (cold open): {rows}")
    except duckdb.Error as exc:
        print(f"RESULT {TABLE} not queryable cold: {type(exc).__name__}: {exc}")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "check"
    {"write": write, "check": check, "dbt-cold-check": dbt_cold_check}[mode]()
