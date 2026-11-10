# Dogfooding notes — Vague 1 (Chinook multi-engine ingestion)

Raw findings from actually running the multi-engine ingestion benchmark
described in [README.md](../README.md#-banc-dessai-multi-moteurs). Numbers
below are measured, not estimated — see "Method" under each section for the
exact command. Where something could not be verified, it says so instead of
guessing.

## Machine / software versions (2026-11-08)

- Windows 11 Pro 10.0.22631, Python 3.12.7
- `xorq` 0.4.11, `dlt` 1.32.0, `pyiceberg` 0.11.1, `pyarrow` 20.0.0
- Docker 29.6.2, Docker Compose v5.3.1
- MinIO `RELEASE.2025-09-07T16-13-09Z`, Postgres `postgres:16-alpine`,
  MySQL `mysql:8.4`
- Chinook dataset pinned at commit
  [`7f67772503d71ba90f19283c38e93923addb43fa`](https://github.com/lerocha/chinook-database/commit/7f67772503d71ba90f19283c38e93923addb43fa)
  (see [docs/THIRD_PARTY_CHINOOK.md](THIRD_PARTY_CHINOOK.md))

## Iceberg write/commit cost (step 6)

**Method:** `uv run python scripts/bench_writes.py --sizes 100000 1000000
--minio` (local `file://` warehouse, then a real MinIO bucket — `docker
compose up -d minio minio-setup` was already running). Every number below is
one run of that script; it was not repeated for statistical confidence
beyond the 100-commit batched scenario's own stdev. All writes go through
the `pyiceberg` engine (`datahut_duckhouse.ingest.PyIcebergEngine`), per this
session's constraint — never the Flight server/xorq path.

### Real Chinook tables (11 tables, 5–8715 rows each), local warehouse

| table | rows | create | append | overwrite |
|---|---|---|---|---|
| Album | 347 | 0.321s | 0.173s | 0.297s |
| Artist | 275 | 0.101s | 0.115s | 0.295s |
| Customer | 59 | 0.131s | 0.119s | 0.267s |
| Employee | 8 | 0.146s | 0.165s | 0.252s |
| Genre | 25 | 0.111s | 0.095s | 0.282s |
| Invoice | 412 | 0.132s | 0.116s | 0.306s |
| InvoiceLine | 2240 | 0.115s | 0.121s | 0.255s |
| MediaType | 5 | 0.060s | 0.061s | 0.203s |
| Playlist | 18 | 0.088s | 0.085s | 0.165s |
| PlaylistTrack | 8715 | 0.087s | 0.095s | 0.210s |
| Track | 3503 | 0.109s | 0.126s | 0.236s |

Same tables, real MinIO bucket:

| table | rows | create | append | overwrite |
|---|---|---|---|---|
| Album | 347 | 0.955s | 0.712s | 1.004s |
| Artist | 275 | 0.806s | 0.651s | 1.053s |
| Customer | 59 | 0.872s | 0.722s | 1.115s |
| Employee | 8 | 0.825s | 0.708s | 1.108s |
| Genre | 25 | 0.824s | 0.601s | 0.994s |
| Invoice | 412 | 0.826s | 0.682s | 1.101s |
| InvoiceLine | 2240 | 0.889s | 0.757s | 0.907s |
| MediaType | 5 | 0.762s | 0.660s | 1.026s |
| Playlist | 18 | 0.831s | 0.691s | 1.014s |
| PlaylistTrack | 8715 | 0.828s | 0.649s | 0.952s |
| Track | 3503 | 0.854s | 0.672s | 0.967s |

**Observation (this run only):** for these small, real-world Chinook table
sizes, commit time does not visibly track row count (Employee at 8 rows and
PlaylistTrack at 8715 rows commit in roughly the same time). Overwrite
consistently costs more than append in both warehouses. MinIO commits are
roughly 5–8x slower than local-filesystem commits at this scale — almost
entirely per-commit overhead (network round trips to MinIO for metadata +
manifest writes), not data volume.

### Synthetic dataset (2 int/string/float columns), single write vs. 100 batched appends

| rows | warehouse | single append | single overwrite | 100 batched appends: total / mean / stdev |
|---|---|---|---|---|
| 100,000 | local | 0.228s | 0.298s | 34.609s / 0.3461s / 0.1384s |
| 1,000,000 | local | 0.792s | 0.773s | 34.047s / 0.3405s / 0.1481s |
| 100,000 | minio | 0.810s | 1.077s | 100.544s / 1.0054s / 0.2080s |
| 1,000,000 | minio | 1.738s | 1.846s | 100.410s / 1.0041s / 0.1615s |

**Observations (this run only, not extrapolated beyond it):**

- **A single commit's cost barely depends on row count** at these sizes: a
  1,000,000-row single append (0.79s local / 1.74s MinIO) costs roughly 3x a
  100,000-row one (0.23s / 0.81s), not 10x — most of the cost is Iceberg
  metadata/manifest writing, not the data volume itself (both are a single
  Parquet file either way).
- **The batched-append mean per commit is essentially flat between
  100,000 and 1,000,000 total rows** (local: 0.346s vs 0.341s per commit;
  MinIO: 1.005s vs 1.004s per commit) — each of the 100 chunks is 1,000 or
  10,000 rows respectively, and in both cases the chunk itself writes fast;
  the ~0.3s (local) / ~1.0s (MinIO) is overwhelmingly **per-commit Iceberg
  overhead** (new manifest + table-metadata file per `append`), not
  per-row cost.
- **100 small commits cost roughly 100x more wall-clock time than 1 big
  commit** of the same total data (compare "100 batched appends: total"
  above to "single append" on the same row/warehouse row): e.g. 1,000,000
  rows locally, 0.79s in one commit vs 34.0s in 100 commits. This is the
  clearest, most actionable number from this benchmark: **batch size
  directly trades off latency-per-write against total commit overhead**,
  and that overhead is not small.
- **MinIO roughly triples the per-commit cost** versus the local filesystem
  warehouse at every size tested (e.g. 100k single-append: 0.23s local vs
  0.81s MinIO). Expected — each commit round-trips to the MinIO container
  over HTTP for the manifest/metadata files — but the magnitude (not just
  the direction) is worth having measured rather than assumed.
- Stdev on the 100-commit batches (0.13–0.21s) is a meaningful fraction of
  the mean (roughly 30–40% locally, ~15–20% on MinIO) — individual commit
  times vary noticeably, not just between warehouses but run to run on the
  same warehouse. Treat the means above as directionally solid, not as a
  precise SLA number.
- **Not benchmarked: "batched overwrite"** (re-running a full-table
  `overwrite` 100 times with the same data) — this isn't a pattern anyone
  would actually run operationally (it would just re-write the same final
  state 100 times for no benefit over doing it once), so `scripts/bench_writes.py`
  does not implement it. If a real incremental-overwrite pattern is needed
  later (e.g. partition-level overwrites), that would need its own,
  differently-shaped benchmark.
- This is still small-scale dogfooding, not a load test: Chinook tops out
  at ~8,700 rows per table, and the synthetic benchmark only ever writes one
  Parquet file per commit (pyiceberg's own internal batching, not multi-file
  commits). Nothing here says what happens at 10-1000x this scale, under
  concurrent writers (there are none — single-writer SQLite/SQL catalog,
  ADR-0001), or with wide/nested schemas.

## Multi-engine extraction consistency (steps 1–5)

Covered in detail by `tests/integration/test_multi_engine.py` (10/10
passing locally, `uv run pytest -m integration`): row counts, types
(decimal totals, timestamps, accented text), and a cross-engine join result
all matched exactly across SQLite, Postgres and MySQL loads of the same
pinned Chinook dataset. No divergence was found between engines in this
pass — see that test file for the exact assertions, and the README's
"Banc d'essai multi-moteurs" section for the commands.

SQL Server was **not run or verified** — the `mssql` compose profile exists
(`tests/docker/chinook/mssql/`) but was not brought up in this pass (x86_64-only
base image, heavier startup; see `tests/docker/chinook/README.md`). No
claim is made about its row counts, types, or join results.

## Frictions encountered

- dlt's own `filesystem` destination (used for its Parquet-writing
  pipeline/state bookkeeping) does not work reliably on this Windows
  machine: `pipeline.run(..., loader_file_format="parquet")` against a
  local `file://` bucket_url **raised `FileNotFoundError` writing
  `_dlt_pipeline_state/*.jsonl` on every attempt**, even though the parquet
  data files themselves were written successfully and the directory already
  existed. Worked around by using dlt's `sql_database` source purely as the
  per-dialect SQL reader (`backend="pyarrow"`, iterate `resource` directly
  for `pyarrow.Table` chunks) and writing Parquet with `pyarrow` directly —
  see `ingestion/dlt/pipeline.py`'s module docstring. Not investigated
  further (out of scope for this pass); worth a closer look if dlt's own
  destination/load tracking is ever needed instead of a pure-extraction use.
- MySQL's official image grants `MYSQL_USER`/`MYSQL_PASSWORD` privileges
  only on `MYSQL_DATABASE`, which this stack doesn't set (the vendored
  `Chinook_MySql.sql` creates and uses its own `` `Chinook` `` database) —
  needed an extra `GRANT` init script (`tests/docker/chinook/mysql/zz_grant.sql`)
  run after the Chinook load, named to sort after it in
  `docker-entrypoint-initdb.d`'s filename ordering.
- A freshly-created MySQL container occasionally refused the very first
  connection attempt after reporting "healthy" (`Lost connection to MySQL
  server during query`) — a short retry a few seconds later always
  succeeded. Not chased further; `tests/integration/conftest.py`'s
  `require_mysql` fixture only checks the TCP port is open, not that the
  server is fully past its post-healthcheck settling time, so a flaky first
  connection in that window is possible in principle (not observed during
  the actual test runs for this pass).
- The two pyiceberg-engine write paths that already existed
  (`create_table` vs `upload_table(mode=...)`) made the idempotent-reload
  logic in `dhd ingest-dlt` straightforward (check `list_tables()` once,
  branch per table) — no friction there, noted only because it could have
  gone the other way.

## Open questions

- What does the batched-commit overhead curve look like **below** 100,000
  rows total (e.g. streaming ingestion with much smaller, more frequent
  batches), and **above** 1,000,000? Not measured in this pass.
- Does Iceberg manifest size (and therefore per-commit cost) grow with the
  *number* of historical snapshots on a table, independent of row count?
  The benchmark tables here only ever accumulate a handful of snapshots
  (≤102 commits); a table with thousands of historical snapshots was not
  tested.
- No concurrent-writer scenario was tested (single local SQL/SQLite
  catalog, ADR-0001) — multi-writer commit contention/retry cost is
  unknown.
- SQL Server: entirely unverified, as noted above. If it's ever brought
  into Vague 2+, budget time to actually bring up the `mssql` profile and
  run the same test suite against it before trusting `sources.yml`'s
  `mssql_chinook` entry.
