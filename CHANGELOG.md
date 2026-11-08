# Changelog

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
