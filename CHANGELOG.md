# Changelog

## Unreleased

### Added

- **Ingestion source registry v2, three connector families** (Vague 1b,
  `feat/vague-1b-registre-dlt`): `ingestion/dlt/sources.yml` is now
  pydantic-validated (`ingestion/dlt/registry.py`) with
  family/connector/secret_ref/schedule/resources/incremental/
  write_disposition/primary_key/schema_contract/target_prefix fields,
  checked with `dhd sources validate` (every error, with its line and
  field) and `dhd sources list`. `pipeline.py` dispatches by family to
  `connectors/{sql,files,rest}.py` instead of hardcoding SQL dialects --
  adding a source needs no code change in any of them, verified live for
  all three families. `dhd ingest-dlt` is now the single entry point:
  `--source NAME`, `--all`, `--dry-run`.
- Two key-free public REST sources (`jsonplaceholder`, `open_brewery_db`,
  the latter exercising real pagination) and two local files sources (a
  pre-existing CSV fixture, a new synthetic `tests/data/sample_products.json`)
  alongside the four existing sql Chinook sources.
- **Exploitation layer** (task 3): `write_disposition` (replace/append/
  merge, merge via pyiceberg's native `Table.upsert`) and `schema_contract`
  (evolve/freeze/discard_value, verified against real pyiceberg 0.11.1
  behavior) are now enforced per source, not hardcoded to "always
  overwrite." A `_ingest_runs` Iceberg table logs one row per table per
  run (falls back to local SQLite if that table can't be written to).
  Every table write stays exactly one Iceberg commit per run regardless of
  write_disposition -- verified by counting snapshots across 3 runs.
- `scripts/new_source.py <family> <name>` generates a TODO-annotated
  registry entry plus a skeleton test; one test module per connector
  family on fixed/local data, a `network` pytest marker (excluded by
  default, like `integration`) for the two REST sources' live tests.
- Experiments against real Docker Postgres/MySQL and the real Flight/MinIO
  stack, written up with exact commands and results in
  [docs/DLT_EVAL.md](docs/DLT_EVAL.md): incremental+merge (including a live
  proof that deletions aren't detected, and a real inclusive-cursor-boundary
  caveat), schema evolution under all three contract modes, crash-then-resume
  (no duplicates under replace/merge; append is **not** resume-safe, by
  design), and volume (1M and 10M synthetic rows: 2.3s/339 MiB and 7.8s/899
  MiB respectively, one commit each). Found and fixed a real bug along the
  way: an incremental timestamp cursor was being re-read as a plain string
  after its first JSON round trip, which `dlt.sources.incremental` rejected.
  See [docs/INGESTION.md](docs/INGESTION.md) for how to add a source.

### `dhd ingest` restructuring (`feat/dhd-ingest`), step A: module layout

- **`ingestion/` is now a real, importable package** -- the pre-existing
  `ingestion/__init__.py` (unrelated multi-tenant scaffold, already
  non-functional: it imported submodules that didn't exist on disk and
  raised `ModuleNotFoundError` the moment anything tried
  `import ingestion.<anything>`) is replaced by a small public API
  (`list_sources`, `validate_registry`, `extract_source`, `run_source`).
  Every module that used to load `ingestion/dlt/pipeline.py`/`registry.py`
  by file path to dodge that trap (`pipeline.py`, 5 test files, 2
  scripts) now uses ordinary `import ingestion...` instead.
- Moved into five layers under `ingestion/` -- `config/` (pydantic models
  + validation rules + the registry loader, moved from `registry.py`),
  `connectors/` (`sql.py`/`files.py`/`rest.py`, each exposing
  `build_resources`; `_dlt_sources.py` isolates every `dlt` import so the
  connectors themselves don't need one), `engine/` (`dlt_engine.DltIngestor`,
  the one orchestration implementation; `state.py`, the incremental
  cursor store, with its directory now configurable via
  `DHD_INGEST_STATE_DIR`), `sinks/` (`flight_sink.py`/`direct_sink.py`/
  `catalog_guard.py`, moved from `datahut_duckhouse/`), `runs/`
  (`store.RunLog`, moved from `datahut_duckhouse/ingest_run_log.py`, now
  also recording a `commits` column; its SQLite fallback path is
  configurable via `DHD_INGEST_RUN_LOG_DB`). See
  [docs/INGESTION_ARCHITECTURE.md](docs/INGESTION_ARCHITECTURE.md) for
  the full layer map, the Sink contract (what each `write_disposition`
  means for crash recovery), and how to add a source/family/engine/sink.
- **No behavior change** -- `dhd ingest-dlt`/`dhd sources ...` still do
  exactly what step 0 left them doing; `datahut_duckhouse/ingest_dlt.py`
  is now a thin adapter translating `ingestion`'s domain exceptions into
  CLI-facing ones with the right exit code.
- Dependency rules (config never imports dlt/connectors/sinks/engine; only
  `engine/dlt_engine.py` and `connectors/_dlt_sources.py` import dlt;
  connectors never know sinks/runs; sinks never know dlt/connectors; the
  CLI adapter only imports `ingestion`'s top-level API; no import cycle)
  are enforced by an AST-based test, not just documented:
  `tests/ingestion/test_dependency_rules.py`.
- Tests mirrored into `tests/ingestion/` alongside the move
  (`test_config.py`, `test_connectors_{sql,files,rest}.py`,
  `test_connectors_registry.py`, `test_engine.py`); 149 tests passing
  (143 before this step, +6 dependency-rule tests, net test count
  otherwise unchanged -- same coverage, relocated and, for the engine
  tests, now exercised directly against `DltIngestor` instead of through
  the CLI).

### `dhd ingest` restructuring (`feat/dhd-ingest`), step 0: visibility

- **Fixed a real bug, reproduced live first**: a table loaded by `dhd
  ingest-dlt` was written straight to pyiceberg from the client and was
  **invisible** to `dhd query`/`dhd list-tables` -- two physically
  different SQLite catalog files (the Flight server's, in a Docker named
  volume; the client's, on the host). Fix: `replace`/`append` now write
  through the Flight server itself (`datahut_duckhouse/ingest_sinks.py`'s
  `FlightSink`, the same action `dhd create-table`/`insert` use), so a
  table lands exactly where reads already look. Regression test:
  `tests/test_cli.py::TestCliEndToEnd::test_ingested_table_is_visible_via_flight_query`.
- `merge` still writes direct via pyiceberg (`DirectSink`) -- no
  Flight-level upsert exists yet (`HybridBackend.upsert()` is inherited,
  unmodified, from xorq, but unreachable over the wire). That direct write
  is now gated by `datahut_duckhouse/catalog_guard.py`: a dynamically
  registered Flight action (xorq's own `add-action`, no `HybridBackend`
  change) proves the client and the server share one catalog before
  allowing it; a mismatch exits **2** with both catalogs named.
  `schema_contract: evolve` under `append` moved server-side the same way
  (`schema_evolve_action.py`).
- This is a real behavior change, stated plainly: `dhd ingest-dlt`/`dhd
  ingest` now always needs a reachable Flight server, for every
  `write_disposition` -- previously this command's whole point was to need
  none. See [docs/INGESTION.md](docs/INGESTION.md)'s rewritten "single
  writer" section, including the estimate for a real Flight-routed
  `merge` (not implemented here, per explicit instruction).

### Follow-up review (same branch)

- `incremental` + `write_disposition: append` is now rejected at
  validation time (no dedup key for `append` to fall back on);
  `incremental` + `replace` warns instead (silent data loss, not
  duplication) -- `dhd sources validate` now prints `WARN:` lines.
- Documented, with concrete verification, **who actually writes to
  Iceberg** for `dhd ingest-dlt`: always the client process via pyiceberg,
  never the Flight server, for all three `write_disposition` values alike.
  In this project's default config that's catalog *fragmentation* (two
  physically separate SQLite catalog files, confirmed), not literal
  concurrent-writer corruption -- but would become real writer concurrency
  if `ICEBERG_CATALOG_URI` is ever shared between the two paths. An
  alternative (routing `dhd ingest-dlt` through Flight) is proposed, not
  implemented -- see [docs/INGESTION.md](docs/INGESTION.md).
- Verified the two REST sources actually load into Iceberg via `dhd
  ingest-dlt` (not just extraction) -- added a dedicated integration test.
- Clarified exactly what the 1M/10M volume numbers measured (a files
  source materializes the whole file in memory twice -- once per
  extract, once per load -- not a streaming extraction), and added a
  chunked-batches measurement for comparison: 10M rows via 20×500k-row
  batches, one commit each, peak RSS 362 MiB vs. 899 MiB for the single
  big load (-60%, at the cost of 20 commits instead of 1).
- Added `dhd drop-table <name>` (confirms unless `--yes`; works against
  both engines, the flight engine via xorq's existing, unmodified
  `drop_table` Flight action) and used it to remove `synthetic_100k_flight`,
  the table the optional experiment 5 had left in the shared dev warehouse.

### Fixed

- **The `dhd create-table` / `dhd query` segfault is now guarded, not just
  documented** (`chore/python-matrix`): every `dhd` command that talks to
  the Flight server now checks the server's Python minor version right
  after connecting (`datahut_duckhouse/flight_version_guard.py`, a tiny
  Flight action registered at runtime via xorq's existing `add-action` — no
  `HybridBackend`/xorq changes) and refuses cleanly, non-zero exit, before
  sending anything that could trigger the bug. Verified live, both
  directions, against real Docker/MinIO containers (3.13 client vs. 3.11
  server, and 3.11 vs. 3.13) — neither segfaults.
- Widened the supported Python-version matrix to **3.11 and 3.13** (dev
  defaults to 3.13 via a new `.python-version`; `requires-python` is now
  `>=3.11,<3.14`): verified first in a temporary `uv venv --python 3.13`
  that every dependency, including `xorq==0.4.11`, `duckdb<1.2` (and its
  `duckdb.functional` submodule, the actual reason for that pin),
  `pyiceberg`, and `pymssql`, installs and imports cleanly — no wheel gaps,
  no module-load-time failures. `.devcontainer/flight-server.Dockerfile`'s
  `ARG PYTHON_VERSION` now defaults to 3.13 and is overridable per build
  (`docker-compose.yml`'s flight-server service gained a matching
  `PYTHON_VERSION` build arg); verified by building and round-tripping
  `create-table`/`insert`/`query`/`list-tables` against both versions with
  a matching client, and via `scripts/smoke_test.py` on both. `ci.yml`
  and `integration.yml` now matrix over `[3.11, 3.13]`.
  `tests/test_python_version_matches_flight_image.py` no longer requires
  the host and the Dockerfile's default to be bit-for-bit equal — it checks
  matrix membership instead, so it doesn't go red just because a host or CI
  runner picked the other supported version.
- One thing tried and reverted: flooring `[tool.mypy]`'s `python_version` to
  `3.11` to match the new minimum. Numpy's own bundled `.pyi` stubs use the
  `type` statement (PEP 695, 3.12+-only syntax), so mypy fails immediately
  on an unrelated syntax error; kept at `3.12`.

### Investigated

- **`dhd create-table` / `dhd query` segfault root cause found and verified**
  (`fix/segfault-diagnosis`): a client/server Python *minor*-version mismatch
  breaks `cloudpickle`'s cross-version reconstruction of xorq's vendored
  `ibis` `Schema`/`Annotable` class on the `get_schema_using_query` Flight
  action — not MinIO, not Docker networking, not an xorq regression. Not
  fixed upstream (the fragile part is in xorq + cloudpickle — draft issue at
  `docs/UPSTREAM_ISSUE.md`); now guarded downstream instead, see "Fixed"
  above. Full writeup in ADR-0001.

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
