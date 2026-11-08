# Changelog

## Unreleased

### Works today

- Multi-engine ingestion benchmark (Vague 1): `ingestion/dlt/` extracts any
  configured source (SQLite, PostgreSQL, MySQL — one `sources.yml` entry
  each) via dlt's `sql_database` source into Parquet; `dhd ingest-dlt
  --source <name|all>` loads it into Iceberg through the `pyiceberg` engine
  (overwrite semantics, so a re-run never duplicates rows). `dlt` + DB
  drivers live in a new optional `ingest` extra (`uv sync --extra ingest`),
  not the core dependencies.
- `docker-compose.test.yml` brings up Postgres + MySQL preloaded with the
  Chinook sample database (pinned to a fixed upstream commit —
  see `docs/THIRD_PARTY_CHINOOK.md`); `scripts/fetch_chinook.py` downloads
  the SQLite version on demand, so `scripts/demo.py` needs no Docker.
- `tests/integration/` (marked `integration`, excluded from the default
  `pytest` run): verified row-count, type (decimal/timestamp/accented
  text), and cross-engine join-result consistency across SQLite, Postgres
  and MySQL loads of the same dataset — 10/10 passing.
- `scripts/bench_writes.py` + `docs/DOGFOOD.md`: measured, not estimated,
  Iceberg commit costs for real Chinook tables and a 100k/1M-row synthetic
  dataset, single write vs. 100 batched commits, local warehouse vs. MinIO.
- `scripts/demo.py`: one-command demo (SQLite, no Docker) running three
  illustrative queries (sales by country, top artists, monthly sales)
  against the ingested data; `--source pg_chinook`/`mysql_chinook` runs the
  same queries against the Docker sources.

### Known gaps

- SQL Server is scaffolded (`tests/docker/chinook/mssql/`, the `mssql`
  compose profile) but **not run or verified** — see
  `tests/docker/chinook/README.md`.
- Chinook is small (~15k rows across 11 tables): this pass validates
  correctness and relative per-commit cost, not performance at real
  (AYITISTATS) scale or under concurrent writers.

## v0.1.0 — first dogfoodable release

### Works today

- `dhd` CLI (`create-table`, `insert`, `query`, `list-tables`) against a real
  Arrow Flight server backed by `HybridBackend` (Iceberg as source of truth,
  DuckDB as a read-side reflection) — verified end to end, including as two
  separate processes, not just in-process tests.
- `dhd --engine pyiceberg` — a server-free, `xorq`-free path straight to the
  Iceberg catalog, for the same four commands.
- `xorq` pinned to `0.4.11`; the previously-pinned `0.2.4` had a native
  segfault on `dhd query` that does not reproduce on this version.
- dbt transformations against the DuckDB `dev` target (`dbt run` / `dbt test`
  under `transform/dbt_project`).
- 67 tests passing; `ruff`, `black --check`, `mypy` clean; minimal CI
  (lint + type-check + test) on push/PR.

### Planned, not provided

- **Multi-tenancy** (ADR-0003 to ADR-0005, roadmap Phases 6–9): today, tenants
  are a static list in `config/tenants/tenants.yml`, the orchestrator
  (`orchestrator/core.py`) keeps state in memory only, and the Flight server
  has no authentication. See the README's "Multi-tenancy" section.
- **Trino / BI**: scaffolded in `config/trino/` and `docker-compose.yml`, not
  functional — no shared catalog with the Iceberg tables `dhd` writes yet
  (needs Nessie, ADR-0002).
- **Nessie-backed branching**: `dhd branch` is an explicit stub.

### Known gaps

- `config/trino/etc/catalog/iceberg.properties` hardcodes MinIO credentials.
- The full `docker-compose up` stack was not re-verified in this release pass
  (Docker daemon unavailable in the environment that prepared it); the CLI
  round trip was verified instead against a local filesystem warehouse,
  without Docker.
