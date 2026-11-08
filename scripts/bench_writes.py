"""Benchmark Iceberg write/commit costs (Vague 1, step 6).

Measures wall-clock time per Iceberg commit, via the ``pyiceberg`` engine
(``datahut_duckhouse.ingest.PyIcebergEngine`` -- no Flight server, no xorq,
per this session's constraint), for:

  (a) real Chinook tables (extracted from the SQLite fixture via
      ``ingestion/dlt/pipeline.py``) -- one ``create_table`` + one
      ``insert(mode=append)`` + one ``insert(mode=overwrite)`` per table.
  (b) a synthetic dataset at two sizes (default 100_000 and 1_000_000 rows;
      two int/string/float columns), three write patterns:
        - "single append": one ``insert(mode=append)`` with all N rows
        - "single overwrite": one ``insert(mode=overwrite)`` with all N rows
        - "batched append": the same N rows split into 100 chunks, as 100
          sequential ``insert(mode=append)`` calls -- this isolates
          per-commit overhead from total throughput.

Both run against a local ``file://`` warehouse, and (with ``--minio``)
against a real MinIO bucket (``docker compose up -d minio minio-setup``
must already be running).

Not benchmarked: "batched overwrite" (re-overwriting the same full table
100 times) -- not a meaningful operational pattern; see docs/DOGFOOD.md.

This script prints its raw measurements; it does not write docs/DOGFOOD.md
itself -- transcribe the printed numbers there by hand, so the doc only
ever contains measurements that were actually observed in that run.

Usage:
    uv run python scripts/bench_writes.py                  # local warehouse only
    uv run python scripts/bench_writes.py --minio           # + MinIO
    uv run python scripts/bench_writes.py --sizes 1000 5000  # smaller/faster run
"""

from __future__ import annotations

import argparse
import os
import platform
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "ingestion" / "dlt"))

BATCH_COUNT = 100


@dataclass
class Measurement:
    scenario: str
    warehouse: str
    rows: int
    commits: int
    durations_s: list[float] = field(default_factory=list)

    @property
    def total_s(self) -> float:
        return sum(self.durations_s)

    @property
    def mean_s(self) -> float:
        return statistics.mean(self.durations_s)

    @property
    def stdev_s(self) -> float:
        return statistics.stdev(self.durations_s) if len(self.durations_s) > 1 else 0.0

    def __str__(self) -> str:
        if self.commits == 1:
            return (
                f"{self.scenario:<22} {self.warehouse:<6} rows={self.rows:<9} "
                f"1 commit: {self.total_s:.3f}s"
            )
        return (
            f"{self.scenario:<22} {self.warehouse:<6} rows={self.rows:<9} "
            f"{self.commits} commits: total={self.total_s:.3f}s "
            f"mean={self.mean_s:.4f}s stdev={self.stdev_s:.4f}s"
        )


def _env_for(warehouse: str, *, namespace: str, duckdb_path: str) -> dict[str, str]:
    env = dict(os.environ)
    env["ICEBERG_NAMESPACE"] = namespace
    env["DUCKDB_PATH"] = duckdb_path
    if warehouse == "local":
        env["ICEBERG_WAREHOUSE"] = ""
        env["ICEBERG_WAREHOUSE_PATH"] = str(Path(duckdb_path).parent / "warehouse")
        env.pop("S3_ENDPOINT", None)
    else:  # minio
        env["ICEBERG_WAREHOUSE"] = "s3://duckhouse-warehouse/"
        env["S3_ENDPOINT"] = os.getenv("S3_ENDPOINT", "http://localhost:9000")
        env["AWS_ACCESS_KEY_ID"] = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
        env["AWS_SECRET_ACCESS_KEY"] = os.getenv(
            "AWS_SECRET_ACCESS_KEY", "minioadmin123"
        )
    env.pop("ICEBERG_CATALOG_URI", None)
    return env


def _connect(env: dict[str, str]):
    old_env = dict(os.environ)
    os.environ.clear()
    os.environ.update(env)
    try:
        import importlib

        import datahut_duckhouse.ingest as ingest_mod

        importlib.reload(ingest_mod)
        return ingest_mod.connect()
    finally:
        os.environ.clear()
        os.environ.update(old_env)


def _synthetic_table(n_rows: int) -> pa.Table:
    return pa.table(
        {
            "id": pa.array(range(n_rows), type=pa.int64()),
            "name": pa.array([f"row-{i}" for i in range(n_rows)], type=pa.string()),
            "amount": pa.array(
                [float(i) * 1.5 for i in range(n_rows)], type=pa.float64()
            ),
        }
    )


def bench_single_write(
    warehouse: str, table_name: str, data: pa.Table, mode: str, work_dir: Path
) -> Measurement:
    env = _env_for(
        warehouse, namespace="bench", duckdb_path=str(work_dir / "duckhouse.duckdb")
    )
    backend = _connect(env)
    if table_name not in set(backend.list_tables()):
        backend.create_table(table_name, data.slice(0, 0))
    t0 = time.perf_counter()
    backend.upload_table(table_name, data, mode=mode)
    duration = time.perf_counter() - t0
    label = "single append" if mode == "append" else "single overwrite"
    return Measurement(label, warehouse, data.num_rows, 1, [duration])


def bench_batched_append(
    warehouse: str,
    table_name: str,
    data: pa.Table,
    work_dir: Path,
    batches: int = BATCH_COUNT,
) -> Measurement:
    env = _env_for(
        warehouse, namespace="bench", duckdb_path=str(work_dir / "duckhouse.duckdb")
    )
    backend = _connect(env)
    if table_name not in set(backend.list_tables()):
        backend.create_table(table_name, data.slice(0, 0))
    chunk_size = max(1, data.num_rows // batches)
    durations = []
    offset = 0
    while offset < data.num_rows:
        chunk = data.slice(offset, chunk_size)
        t0 = time.perf_counter()
        backend.upload_table(table_name, chunk, mode="append")
        durations.append(time.perf_counter() - t0)
        offset += chunk_size
    return Measurement(
        "batched append", warehouse, data.num_rows, len(durations), durations
    )


def bench_real_chinook(warehouse: str, work_dir: Path) -> list[Measurement]:
    import pipeline as dlt_pipeline  # ingestion/dlt/pipeline.py, loaded via sys.path above

    os.environ.setdefault(
        "CHINOOK_SQLITE_URL",
        f"sqlite:///{REPO_ROOT / 'tests' / 'data' / 'Chinook_Sqlite.sqlite'}",
    )
    source = dlt_pipeline.get_source("sqlite_chinook")
    extracted = dlt_pipeline.extract_source(
        source, work_dir=work_dir / "chinook_parquet"
    )

    env = _env_for(
        warehouse,
        namespace="bench_chinook",
        duckdb_path=str(work_dir / "duckhouse.duckdb"),
    )
    backend = _connect(env)
    existing = set(backend.list_tables())

    measurements = []
    import pyarrow.parquet as papq

    for table_name, parquet_path in sorted(extracted.items()):
        data = papq.read_table(parquet_path)
        if table_name not in existing:
            t0 = time.perf_counter()
            backend.create_table(table_name, data)
            create_s = time.perf_counter() - t0
            existing.add(table_name)
            measurements.append(
                Measurement(
                    f"chinook create ({table_name})",
                    warehouse,
                    data.num_rows,
                    1,
                    [create_s],
                )
            )
        t0 = time.perf_counter()
        backend.upload_table(table_name, data, mode="append")
        append_s = time.perf_counter() - t0
        measurements.append(
            Measurement(
                f"chinook append ({table_name})",
                warehouse,
                data.num_rows,
                1,
                [append_s],
            )
        )
        t0 = time.perf_counter()
        backend.upload_table(table_name, data, mode="overwrite")
        overwrite_s = time.perf_counter() - t0
        measurements.append(
            Measurement(
                f"chinook overwrite ({table_name})",
                warehouse,
                data.num_rows,
                1,
                [overwrite_s],
            )
        )
    return measurements


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sizes",
        type=int,
        nargs="+",
        default=[100_000, 1_000_000],
        help="Synthetic dataset sizes to benchmark (default: 100000 1000000).",
    )
    parser.add_argument(
        "--minio",
        action="store_true",
        help="Also run every scenario against MinIO (needs `docker compose up -d minio minio-setup`).",
    )
    parser.add_argument(
        "--skip-chinook",
        action="store_true",
        help="Skip the real-Chinook-tables scenario (needs tests/data/Chinook_Sqlite.sqlite).",
    )
    args = parser.parse_args()

    warehouses = ["local"] + (["minio"] if args.minio else [])

    print(f"Python {platform.python_version()} / {platform.platform()}")
    import pyarrow
    import pyiceberg

    print(f"pyarrow {pyarrow.__version__}, pyiceberg {pyiceberg.__version__}")
    print(f"Warehouses: {warehouses}")
    print()

    all_measurements: list[Measurement] = []
    work_dir = REPO_ROOT / "scripts" / "_bench_work"
    work_dir.mkdir(parents=True, exist_ok=True)

    for warehouse in warehouses:
        if not args.skip_chinook:
            print(f"--- real Chinook tables ({warehouse}) ---")
            measurements = bench_real_chinook(warehouse, work_dir / warehouse)
            for m in measurements:
                print(m)
            all_measurements.extend(measurements)
            print()

        for size in args.sizes:
            print(f"--- synthetic {size} rows ({warehouse}) ---")
            data = _synthetic_table(size)
            table_single = f"bench_single_{size}"
            table_batched = f"bench_batched_{size}"

            m = bench_single_write(
                warehouse, table_single, data, "append", work_dir / warehouse
            )
            print(m)
            all_measurements.append(m)

            m = bench_single_write(
                warehouse, table_single, data, "overwrite", work_dir / warehouse
            )
            print(m)
            all_measurements.append(m)

            m = bench_batched_append(
                warehouse, table_batched, data, work_dir / warehouse
            )
            print(m)
            all_measurements.append(m)
            print()

    print("Done. Transcribe the numbers above into docs/DOGFOOD.md by hand.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
