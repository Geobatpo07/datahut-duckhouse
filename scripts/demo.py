"""One-command demo: ingest Chinook and run three illustrative queries via
``--engine pyiceberg`` (Vague 1, step 7).

    uv run python scripts/demo.py                    # SQLite, no Docker needed
    uv run python scripts/demo.py --source pg_chinook   # needs:
    uv run python scripts/demo.py --source mysql_chinook  #  docker compose -f
                                                            #  docker-compose.test.yml up -d

Downloads ``tests/data/Chinook_Sqlite.sqlite`` on first run if missing
(``scripts/fetch_chinook.py``) and loads the chosen source into a dedicated
local Iceberg warehouse (``scripts/_demo_warehouse/``, gitignored) via
``datahut_duckhouse.ingest_dlt.run_ingest_dlt`` -- the same code `dhd
ingest-dlt` uses, always the pyiceberg engine, never the default flight
engine (see README Known gaps). Re-running overwrites, so the demo is
idempotent.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pyarrow as pa

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

DEMO_WAREHOUSE = REPO_ROOT / "scripts" / "_demo_warehouse"

# Column casing differs by the dialect each Chinook load script targets --
# see tests/docker/chinook/*/Chinook_*.sql and tests/integration's _COLUMNS.
_COLUMNS = {
    "sqlite_chinook": {
        "invoice": "Invoice",
        "customer": "Customer",
        "invoice_line": "InvoiceLine",
        "track": "Track",
        "album": "Album",
        "artist": "Artist",
        "customer_id": "CustomerId",
        "country": "Country",
        "total": "Total",
        "invoice_date": "InvoiceDate",
        "track_id": "TrackId",
        "album_id": "AlbumId",
        "artist_id": "ArtistId",
        "unit_price": "UnitPrice",
        "quantity": "Quantity",
        "name": "Name",
    },
    "mysql_chinook": {
        "invoice": "Invoice",
        "customer": "Customer",
        "invoice_line": "InvoiceLine",
        "track": "Track",
        "album": "Album",
        "artist": "Artist",
        "customer_id": "CustomerId",
        "country": "Country",
        "total": "Total",
        "invoice_date": "InvoiceDate",
        "track_id": "TrackId",
        "album_id": "AlbumId",
        "artist_id": "ArtistId",
        "unit_price": "UnitPrice",
        "quantity": "Quantity",
        "name": "Name",
    },
    "pg_chinook": {
        "invoice": "invoice",
        "customer": "customer",
        "invoice_line": "invoice_line",
        "track": "track",
        "album": "album",
        "artist": "artist",
        "customer_id": "customer_id",
        "country": "country",
        "total": "total",
        "invoice_date": "invoice_date",
        "track_id": "track_id",
        "album_id": "album_id",
        "artist_id": "artist_id",
        "unit_price": "unit_price",
        "quantity": "quantity",
        "name": "name",
    },
}


def _build_queries(source: str) -> dict[str, str]:
    c = _COLUMNS[source]
    p = lambda table: f"{source}_{table}"  # noqa: E731 - tiny local helper
    return {
        "Sales by country": f"""
            SELECT c."{c['country']}" AS country, SUM(i."{c['total']}") AS total_sales
            FROM {p(c['invoice'])} i
            JOIN {p(c['customer'])} c ON i."{c['customer_id']}" = c."{c['customer_id']}"
            GROUP BY c."{c['country']}"
            ORDER BY total_sales DESC
            LIMIT 10
        """,
        "Top artists by revenue": f"""
            SELECT ar."{c['name']}" AS artist,
                   SUM(il."{c['unit_price']}" * il."{c['quantity']}") AS revenue
            FROM {p(c['invoice_line'])} il
            JOIN {p(c['track'])} t ON il."{c['track_id']}" = t."{c['track_id']}"
            JOIN {p(c['album'])} al ON t."{c['album_id']}" = al."{c['album_id']}"
            JOIN {p(c['artist'])} ar ON al."{c['artist_id']}" = ar."{c['artist_id']}"
            GROUP BY ar."{c['name']}"
            ORDER BY revenue DESC
            LIMIT 10
        """,
        "Monthly sales": f"""
            SELECT strftime(i."{c['invoice_date']}", '%Y-%m') AS month,
                   SUM(i."{c['total']}") AS total_sales
            FROM {p(c['invoice'])} i
            GROUP BY month
            ORDER BY month
        """,
    }


def _print_table(name: str, table: pa.Table) -> None:
    print(f"\n=== {name} ===")
    cols = table.column_names
    rows = [
        [str(v) if v is not None else "NULL" for v in row.values()]
        for row in table.to_pylist()
    ]
    widths = [
        max(len(cols[i]), *(len(r[i]) for r in rows)) if rows else len(cols[i])
        for i in range(len(cols))
    ]
    sep = "  "
    print(sep.join(c.ljust(w) for c, w in zip(cols, widths, strict=True)))
    print(sep.join("-" * w for w in widths))
    for r in rows:
        print(sep.join(c.ljust(w) for c, w in zip(r, widths, strict=True)))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        choices=sorted(_COLUMNS),
        default="sqlite_chinook",
        help="dlt source to ingest and query (default: sqlite_chinook, no "
        "Docker needed). pg_chinook/mysql_chinook need "
        "`docker compose -f docker-compose.test.yml up -d`.",
    )
    args = parser.parse_args()

    if args.source == "sqlite_chinook":
        sqlite_path = REPO_ROOT / "tests" / "data" / "Chinook_Sqlite.sqlite"
        os.environ.setdefault("CHINOOK_SQLITE_URL", f"sqlite:///{sqlite_path}")
        if not sqlite_path.exists():
            import fetch_chinook

            fetch_chinook.fetch(sqlite_path)

    # Pre-create the dir: datahut_duckhouse.ingest's _local_meta_dir() only
    # trusts DUCKDB_PATH's parent (and so keeps the catalog isolated here)
    # when that parent already exists -- otherwise it falls back to
    # ingestion/data/, which is the *shared* dev catalog, not this demo's own.
    DEMO_WAREHOUSE.mkdir(parents=True, exist_ok=True)
    os.environ["ICEBERG_WAREHOUSE"] = ""
    os.environ["ICEBERG_WAREHOUSE_PATH"] = str(DEMO_WAREHOUSE / "warehouse")
    os.environ["ICEBERG_NAMESPACE"] = "demo"
    os.environ["DUCKDB_PATH"] = str(DEMO_WAREHOUSE / "duckhouse.duckdb")
    os.environ.pop("ICEBERG_CATALOG_URI", None)

    from datahut_duckhouse.ingest_dlt import IngestDltError, run_ingest_dlt

    print(f"Ingesting source '{args.source}' via dlt + the pyiceberg engine...")
    try:
        results = run_ingest_dlt(args.source)
    except IngestDltError as exc:
        print(f"Ingestion failed: {exc}", file=sys.stderr)
        return 1
    for table, (verb, n) in sorted(results.items()):
        print(f"  {verb} {table} ({n} rows)")

    from datahut_duckhouse.ingest import connect as ingest_connect

    backend = ingest_connect()
    for name, sql in _build_queries(args.source).items():
        result = backend.sql(sql).to_pyarrow()
        _print_table(name, result)

    return 0


if __name__ == "__main__":
    sys.exit(main())
