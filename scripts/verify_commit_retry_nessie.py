"""Real two-writer commit-conflict test against Nessie (ADR-0006, decision
6; step 3 of the implementation brief). Two threads, each with its OWN
``iceberg_catalog.get_catalog()`` connection (so each has its own stale-vs-
fresh ``Table`` handle lifecycle, exactly like two separate OS processes
would -- a real conflict needs two independent catalog connections, not
two threads sharing one), each append ``ROWS_PER_WRITER`` single-row
commits to the same Nessie-backed table through
``ingestion.sinks.base.with_commit_retry``. Asserts the final row count is
exactly the sum of both writers' rows: no lost commit, no duplicate.

Must run inside the Docker network (the Windows host cannot write data to
a Nessie-backed catalog -- see docs/spikes/NESSIE_SPIKES.md, S1, and
ADR-0006's workstation rule):

    docker compose run --rm \\
      -e ICEBERG_CATALOG_TYPE=rest \\
      -e ICEBERG_CATALOG_URI=http://nessie:19120/iceberg \\
      -e ICEBERG_WAREHOUSE=s3://duckhouse-warehouse/shared-catalog/ \\
      -e ICEBERG_BRANCH=main \\
      -e ICEBERG_NAMESPACE=step3 \\
      --entrypoint /app/.venv/bin/python \\
      flight-server scripts/verify_commit_retry_nessie.py
"""

from __future__ import annotations

import os
import threading

import pyarrow as pa
from dotenv import load_dotenv

load_dotenv()

ROWS_PER_WRITER = 15
TABLE_NAME = "step3_commit_retry"


def _writer(
    namespace: str, table_ident: str, writer_id: int, errors: list, attempts: list
) -> None:
    from iceberg_catalog import get_catalog
    from ingestion.sinks.base import with_commit_retry

    catalog = get_catalog(ensure_namespace=namespace)
    for i in range(ROWS_PER_WRITER):
        row_id = writer_id * 1000 + i
        calls = {"n": 0}

        def _commit(row_id=row_id, calls=calls):
            calls["n"] += 1
            table = catalog.load_table(table_ident)
            table.append(pa.table({"id": [row_id], "writer": [writer_id]}))

        with_commit_retry(_commit, table_name=table_ident)
        attempts.append(calls["n"])


def main() -> None:
    namespace = os.getenv("ICEBERG_NAMESPACE", "step3")
    table_ident = f"{namespace}.{TABLE_NAME}"

    from iceberg_catalog import get_catalog

    setup_catalog = get_catalog(ensure_namespace=namespace)
    if TABLE_NAME in [t[-1] for t in setup_catalog.list_tables(namespace)]:
        setup_catalog.drop_table(table_ident)
    setup_catalog.create_table(
        table_ident, schema=pa.schema([("id", pa.int64()), ("writer", pa.int64())])
    )

    errors: list = []
    attempts_a: list = []
    attempts_b: list = []
    t1 = threading.Thread(
        target=_writer, args=(namespace, table_ident, 1, errors, attempts_a)
    )
    t2 = threading.Thread(
        target=_writer, args=(namespace, table_ident, 2, errors, attempts_b)
    )
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    final = setup_catalog.load_table(table_ident).scan().to_arrow()
    expected = 2 * ROWS_PER_WRITER
    retried = sum(1 for n in attempts_a + attempts_b if n > 1)
    print(f"RESULT rows={final.num_rows} expected={expected} retried_commits={retried}")
    print(f"RESULT writer1_attempts={attempts_a}")
    print(f"RESULT writer2_attempts={attempts_b}")
    assert final.num_rows == expected, f"expected {expected} rows, got {final.num_rows}"
    assert (
        len(set(final.column("id").to_pylist())) == expected
    ), "duplicate id -- data corruption"
    print(
        "PASS: exact data, no loss, no duplication, after real concurrent Nessie commits"
    )


if __name__ == "__main__":
    main()
