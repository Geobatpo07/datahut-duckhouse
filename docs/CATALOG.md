# The Iceberg catalog (ADR-0006)

One shared catalog per deployment, with warehouse versioning. This doc
covers modes, the workstation rule, versioning, and retention; routing
(which sink writes a table) is [docs/INGESTION.md](INGESTION.md)'s job.
See [docs/adr/0006-single-shared-catalog.md](adr/0006-single-shared-catalog.md)
for the decision itself and its evidence, and
[docs/spikes/NESSIE_SPIKES.md](spikes/NESSIE_SPIKES.md) /
[NESSIE_SPIKES_2.md](spikes/NESSIE_SPIKES_2.md) for the spikes behind it.

## One access point

Every component -- the Flight server's `HybridBackend`, `dhd --engine
pyiceberg`, scripts -- opens the catalog through
`iceberg_catalog.get_catalog()`, never `pyiceberg.catalog.load_catalog`
directly (enforced: `tests/test_catalog_factory.py`'s AST check,
`scripts/spikes/` excepted as documented spike-only code). Driven by:

| Env var | Meaning | Default |
|---|---|---|
| `ICEBERG_CATALOG` | Catalog name passed to `load_catalog` | `dhd` (client) / `default` (Flight server) |
| `ICEBERG_CATALOG_TYPE` | `sql` (solo) or `rest` (deployment, Nessie) | `sql` |
| `ICEBERG_CATALOG_URI` | The catalog's connection URI | a local `sqlite:///...` file next to the warehouse |
| `ICEBERG_WAREHOUSE` / `ICEBERG_WAREHOUSE_PATH` | `s3://...` or a local path | a local `data/iceberg_warehouse/` directory |
| `ICEBERG_BRANCH` | The Nessie reference to read/write (deployment mode only) | `main` |

## Two modes

- **Solo**: `ICEBERG_CATALOG_TYPE` unset or `sql` -- a local SQLite
  catalog and a local warehouse folder, one writer, no versioning. The
  zero-infrastructure way to try the product: no Docker, no Nessie.
- **Deployment**: `ICEBERG_CATALOG_TYPE=rest` -- Nessie through the
  Iceberg REST interface, its own Postgres store, warehouse on
  MinIO/S3. Branches, tags, merge, rollback (see "Versioning" below).

`iceberg_catalog.catalog_mode()` / `is_deployment_mode()` detect which
one is active from `ICEBERG_CATALOG_TYPE` alone.
`iceberg_catalog.require_versioning()` is the gate every versioning
command calls first -- exits with `VersioningUnavailableError` (mapped
to CLI exit code 2) in solo mode.

### Workstation rule

**On the Windows development host, work in solo mode.** Deployment mode
runs inside the Docker network (devcontainer, the `flight-server`
container, or a `docker compose run` one-off) -- never bypass this by
pointing a host process at a container's published ports for an actual
*data write*.

Verified (docs/spikes/NESSIE_SPIKES.md, S1; NESSIE_SPIKES_2.md, T1):
Nessie's REST catalog vends a table-level file I/O configuration that
*always* overrides whatever S3 endpoint/credentials a client passes at
`load_catalog` time -- every variant tried (an overridden `s3.endpoint`,
path-style access, forcing `PyArrowFileIO` instead of the vended
`FsspecFileIO`) still resolved to the container network's own address
(`minio:9000`), which the bare Windows host cannot resolve. This is not
a configuration bug to fix; it is how Nessie's REST catalog works.

What *does* work from the host: anything that is a pure Nessie REST API
call with no data write -- `dhd branch list`, `dhd branch create`, `dhd
merge`, `dhd tag`, `dhd rollback` (all plain HTTP against Nessie's own
published port, `docker-compose.nessie.yml`'s `19120:19120`), and
`dhd query`/`dhd list-tables` through a running Flight server (Flight's
own protocol, not a direct S3 connection -- the *data* work happens
server-side, inside the container). Only a direct pyiceberg write (the
`direct` sink, or `dhd --engine pyiceberg`) needs to run inside the
network.

## Versioning (ADR-0006, decision 4)

Nessie's own REST API v2 (`.../api/v2`, not the Iceberg REST endpoint
`ICEBERG_CATALOG_URI` names) -- a plain stdlib HTTP client
(`iceberg_catalog/versioning.py`), no new dependency.

```
dhd branch create <name> [--from <branch>]
dhd branch list
dhd branch delete <name> [--force]
dhd merge <from> [--into <branch>]
dhd tag <name> [--from <branch>]
dhd rollback <to_hash> [--branch <branch>]
dhd --branch <name> <any command>        # ICEBERG_BRANCH for this invocation
```

- `--branch`/`ICEBERG_BRANCH` only affects the `pyiceberg` engine and
  these six commands -- the `flight` engine reads the *running Flight
  server's own* `ICEBERG_BRANCH`, fixed at that server's startup, since
  it is a different, long-lived process a client's per-invocation flag
  cannot reach into.
- `dhd merge` fails on **any** conflict and lists the conflicting
  tables; never applies an automatic resolution. Verified live
  (docs/spikes/NESSIE_SPIKES_2.md, T3): even two branches that each add
  one row to the same table conflict -- Nessie detects conflicts per
  table, not per row -- and Nessie's own `FORCE` resolution overwrites
  one side instead of combining them, so it is never exposed here.
- `dhd branch delete` refuses a branch with work not yet merged into
  `main` unless `--force`, and says so: the files of that unmerged work
  are left in storage (`nessie-gc` never visits a deleted branch's
  commits -- see "Retention" below), not reclaimed by deleting the
  reference.
- All six commands exit **2** in solo mode (no reference model to
  branch).
- `--as-of` (reading a past point in time) is out of scope here --
  noted as a follow-up in the ADR.

## Visibility of direct writes (condition C5)

Verified live (step 6 of the implementation work, against a real
deployment-mode Flight server + a separate direct pyiceberg write):

- **`dhd list-tables` / `dhd query` via Flight: immediate.** A table
  written directly (bypassing Flight entirely) was visible on the very
  first `list_tables()` poll (tens of milliseconds -- normal RPC
  latency, not a refresh delay) and `dhd query` returned the correct
  row. Mechanism: `HybridBackend._reflect_views()` re-lists and
  re-registers every table fresh from the shared catalog on every call
  -- this was already true by construction; routing (direct vs. Flight)
  does not change it.
- **dbt's `dev` target: does not see Iceberg tables at all, from either
  sink.** Two independent, pre-existing facts, neither caused by this
  ADR: (1) `_reflect_views()`'s `register()` calls are scoped to the
  live `HybridBackend` session's in-memory Arrow objects -- never
  persisted into the on-disk `.duckdb` file's own catalog, confirmed by
  opening that exact file cold after stopping the Flight server that
  wrote to it: empty. (2) While a Flight server holds that file open, a
  cold connection cannot even attempt to read it (DuckDB's
  single-writer file lock). The project's dbt models/sources
  (`transform/dbt_project/models/sources.yml`) also don't reference
  Iceberg-backed tables today -- unrelated legacy/demo seed data.

  **Not fixed here** (the only fix available without changing
  `HybridBackend` beyond what ADR-0006 already permits touching --
  catalog-parameter construction -- is a *new* script that
  `IMPORT DATABASE`s the latest Parquet snapshot `_create_snapshot()`
  already writes into a separate file dbt points at; proposed, not
  built, since it's new scope beyond this step's verification).
  Reproduce with `scripts/verify_direct_write_visibility.py` (its own
  docstring has the exact commands).

## Retention (condition C2)

See [docs/RETENTION.md](RETENTION.md) for the full procedure. Short
version: run `nessie-gc` periodically for routine cleanup of superseded
snapshots on tables still reachable from a current branch/tag; run
`scripts/orphan_sweep.py` on a slower schedule to catch what `nessie-gc`
structurally cannot -- files belonging to a branch that was deleted
before being merged. Verified live: a branch created, written to, and
deleted unmerged left files a full `nessie-gc` run's log never once
mentioned; `orphan_sweep.py`'s report found exactly those files.

## Proposed: merging `docker-compose.nessie.yml` into `docker-compose.yml`

Steps 1-7 of this implementation are verified (C5's dbt half is the one
documented, open exception -- "Visibility" above). That clears the bar
this work set for considering the merge, so it is proposed here --
**not applied**:

- Move `nessie`, `nessie-postgres` (and, behind the existing `gc`
  profile, `nessie-gc-postgres`) from `docker-compose.nessie.yml` into
  `docker-compose.yml`'s `services:`, keeping every env var and the
  `19120:19120` port mapping unchanged.
- Point `flight-server`'s `environment:` at deployment-mode defaults
  (`ICEBERG_CATALOG_TYPE=rest`, `ICEBERG_CATALOG_URI=http://nessie:19120/iceberg`,
  `ICEBERG_BRANCH=main`) instead of today's solo-mode defaults, and add
  `depends_on: nessie: condition: service_healthy`.
  **This changes the default developer experience** -- anyone running
  `docker compose up` would now be in deployment mode, not solo --
  worth a deliberate decision, not a side effect of this merge.
- Delete `docker-compose.nessie.yml` once the above is in and verified
  (its own file header already says exactly this: "NOT part of the
  production stack" -- that header becomes false the moment this
  merge happens, so remove the file rather than leave a stale comment).
- Keep `nessie-gc` itself (the cleanup tool, not its Postgres) as a
  `docker run` one-off per `docs/RETENTION.md`, not a long-running
  compose service -- it is a periodic job, not something that should
  restart on every `docker compose up`.

## Conditions C1-C6: final status

| # | Condition | Status | Evidence |
|---|-----------|--------|----------|
| C1 | Host write path, or a documented workstation rule | **Met** | "Workstation rule" above; docs/spikes/NESSIE_SPIKES_2.md, T1 |
| C2 | Retention procedure tested, including orphan files of deleted branches | **Met** | "Retention" above; docs/RETENTION.md; `scripts/orphan_sweep.py` |
| C3 | Merge-conflict behavior documented with a proposed `dhd merge` behavior | **Met** | "Versioning" above; docs/spikes/NESSIE_SPIKES_2.md, T3; `iceberg_catalog/versioning.py` |
| C4 | Commit latency measured against SQLite under the same conditions | **Met** | docs/spikes/NESSIE_SPIKES_2.md, T4 (same order of magnitude) |
| C5 | A table written directly is visible at once to `dhd query`/dbt, or the refresh path is defined | **Partial** -- Flight: met; dbt: refresh path described, not built | "Visibility" above |
| C6 | Routing threshold measured | **Met** | docs/INGESTION.md's routing table; docs/spikes/NESSIE_SPIKES_2.md, T6 (direct dominates, no threshold) |

C2 and C5 were this implementation work's own acceptance criteria
(ADR-0006's status line). C2 is now fully met. C5 is met for the
Flight/`dhd query` path; the dbt half is a real, described, open gap.

### Proposed ADR-0006 update (not applied -- for the next edit of the ADR itself)

- **Status line**: from "Two items remain open... C2 (retention
  procedure) and C5 (visibility of direct writes)" to "C2 is met. C5 is
  partially met: direct writes are visible at once via Flight
  (`dhd query`/`dhd list-tables`); dbt's `dev` target does not see
  Iceberg-backed tables at all today, independent of this ADR -- see
  docs/CATALOG.md's 'Visibility' section."
- **Conditions table**, C2 row: "Met -- `nessie-gc` procedure documented
  (docs/RETENTION.md) and `scripts/orphan_sweep.py` closes the
  deleted-unmerged-branch gap, verified live."
- **Conditions table**, C5 row: "Partial -- Flight/`dhd query` met
  (immediate, verified); dbt not met (two pre-existing, unrelated
  facts: `_reflect_views()` never persists past the live session;
  dbt's own models don't query Iceberg sources yet). Refresh path
  described in docs/CATALOG.md, not built: a new script importing the
  latest Parquet snapshot into a file dbt reads."
- **Decision 8** ("Required code corrections"): mark (a) s3fs and (b)
  HybridBackend's per-type catalog params as done; (c) (the do_put
  status-bounding decorator) is still only proposed upstream, unchanged.
- **New decision** (append after decision 8, or fold into it): "Sink
  routing has no size threshold -- `auto` picks direct whenever this
  process can reach the catalog/storage, confirmed by a real capacity
  check at run time, not a configured guess." (Supersedes the earlier
  draft language in decision 5 about a 'threshold set from
  measurement' -- the measurement found no threshold to set.)
- **Consequences / Negative**: remove "The Windows host cannot write
  data to a Nessie-backed catalog" as an unqualified risk and replace
  with "...for data writes; pure Nessie reference operations (branch/
  merge/tag/rollback) and Flight-mediated reads work from the host
  too, since neither touches storage directly from the client."
