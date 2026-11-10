# ADR-0006: One Shared Iceberg Catalog per Deployment, with Warehouse Versioning

**Status:** Accepted. Two items remain open and are tracked as acceptance criteria of the implementation work: C2 (retention procedure) and C5 (visibility of direct writes). See "Conditions".
**Date:** 2026-11-10
**Deciders:** Geovany Batista Polo Laguerre
**Refines:** ADR-0002 (Nessie as the Iceberg catalog). The choice of Nessie is unchanged; this ADR fixes the access rule, the modes, the branch conventions, the write routing and the order of work.
**Related:** ADR-0001 (canonical hybrid backend), ADR-0003 (Postgres for tenant metadata), ADR-0004 (tenant isolation), ADR-0005 (authentication)

## Context

Several components write Iceberg tables, and they do not share one catalog:

- The Flight server keeps its SQLite catalog in the Docker volume `flight_data`.
- `dhd --engine pyiceberg`, the ingestion module and helper scripts fall back to a SQLite catalog on the host (`ingestion/data/pyiceberg_catalog.db`).
- Both point at the same object storage (MinIO), but a catalog is the pointer to the current state of each table. Two catalogs are two sources of truth. This was reproduced: a table created by the ingestion command was invisible to `dhd query` through Flight.
- `merge` loads are only allowed when a guard proves both sides use the same catalog, which is currently never the case, so incremental sources cannot run.
- Trino does not share any catalog yet.
- ADR-0002 accepted Nessie but nothing implemented it (roadmap phase 3).

Product drivers: each client gets a dedicated deployment, so one catalog per deployment is the natural scope; versioning the warehouse (branches, tags, rollback, time travel) is a feature users can see and that can be sold; the product must stay light, so a person can try it on a laptop with no extra service.

Findings from the measurements and spikes (reports: `docs/spikes/NESSIE_SPIKES.md` and `docs/spikes/NESSIE_SPIKES_2.md`; Nessie 0.108.4, pyiceberg 0.11.1):

- One Iceberg commit costs about 0.34 s locally and 1.0 s on MinIO. Commit latency on Nessie and on SQLite is of the same order (30 commits of 1M rows, same machine). Writes must stay batched.
- Through Flight, each `create_table` or `insert` re-reads the whole table and re-exports the whole DuckDB database. Direct writes to the catalog beat Flight in every cell of the test matrix (target size 0, 1M, 10M rows; batch size 10k, 100k, 1M); at 10M target rows Flight took up to 7.7 times longer and 5.4 times more memory. No crossover point was found. Flight ran in the same process as the test, not in Docker.
- pyiceberg has no "nessie" catalog type; the type is `rest`.
- Two processes committing to the same table at once end with exact data and no corruption, but pyiceberg does not retry a conflicting commit itself.
- Branch creation, isolated writes, merge, tag and rollback work through the Nessie REST API v2. Trino reads tables written by pyiceberg, including a chosen branch. An SQLite table can be registered in Nessie with no data copy. Nessie plus its Postgres store use about 407 MiB.
- Two branches that each append one row to the same table conflict at merge, exactly as a schema conflict does: conflicts are detected per table, not per row. The server-side `FORCE` resolution overwrites instead of combining.
- Nessie imposes table-level file I/O settings that no client-side override beats (storage address, path-style access, I/O implementation). A process on the Windows host cannot write data to MinIO by container name.
- `s3fs` was not constrained in `pyproject.toml` and resolved to a 2020 version incompatible with current `fsspec` and `botocore`; the bug is live in the real `flight-server` image.
- `HybridBackend` passes a `warehouse` parameter that the Nessie REST catalog does not accept.
- The standard pyiceberg snapshot expiry is rejected by Nessie. `nessie-gc` only cleans obsolete files of tables that are still reachable: files left by a deleted branch with unmerged work are never visited.
- A client-side error occurs when Flight returns an oversized error message (the `do_put` handler in xorq lacks the status-bounding decorator that `do_exchange` has); it also hides schema-mismatch errors.

## Decision

1. **One catalog per deployment, one access point.** Every component (Flight server, `dhd`, ingestion, dbt tooling, scripts) obtains the catalog through a single shared factory driven by configuration (`ICEBERG_CATALOG`, `ICEBERG_CATALOG_TYPE`, `ICEBERG_CATALOG_URI`, and a new `ICEBERG_BRANCH`). No component opens its own catalog or hard-codes a path. A test forbids calling `load_catalog` anywhere except in the factory.
2. **Two modes, same code, configuration only.**
   - *Deployment mode:* Nessie through the Iceberg REST interface (`type=rest`), with its own Postgres database (separate from the tenant metadata of ADR-0003), warehouse on MinIO/S3. The Nessie version is pinned and `s3fs>=2024.1.0` becomes a constraint of the project.
   - *Solo mode:* SQLite catalog and local warehouse folder, a single writer, no versioning. It is the zero-infrastructure way to try and demo the product.
   - *Workstation rule (confirmed by the spikes):* on the Windows development host, work in solo mode. Deployment mode runs inside the Docker network (devcontainer, ingestion container or client deployment); ingestion in deployment mode runs as a container job.
3. **Versioning conventions.** `main` is the production state. Every component reads and writes on the reference named by `ICEBERG_BRANCH` (default `main`). Other branches: `dev` for experiments and `tenant/<name>` as in ADR-0004. Because conflicts are detected per table, branches are for short-lived, isolated work (experiments, a transformation run checked before publication, rollback points). Write, audit, publish is available per ingestion source but is not the default.
4. **User-facing commands.** `dhd branch create|list|delete`, `dhd merge`, `dhd tag`, `dhd rollback`, through the Nessie REST API v2 with a plain HTTP client and no extra client library.
   - `dhd merge` fails on any conflict, lists the conflicting tables, and never applies an automatic resolution; the server-side `FORCE` resolution is not exposed.
   - `dhd branch delete` refuses a branch with unmerged work unless `--force` is given, and the message states that files of unmerged work are left in storage (see decision 7).
   - In solo mode all of these commands fail with exit code 2 and a message saying that versioning requires deployment mode.
   - Reads at a past point (`--as-of`) are specified later.
5. **Write routing for ingestion.** Each source has `sink: auto | flight | direct` (default `auto`), overridable with `--sink`.
   - `auto` chooses the direct sink whenever the direct path is possible (catalog and storage reachable from the process), and Flight otherwise. There is no size threshold.
   - An explicit `flight` choice is honored; if the estimated load is large (configurable warning level, default 1M rows to write or in the target table), a warning gives the estimate and says the direct sink would be faster.
   - An explicit `direct` choice that is impossible fails with exit code 2; it never falls back silently.
   - The run journal records the sink used and the reason.
   - `batch_rows` defaults depend on the sink: 500 000 for direct, a single commit for Flight (batching makes Flight slower). `replace` always uses a single commit.
   - Flight remains the query and API endpoint and the write path for remote clients that cannot reach the storage.
6. **Writers retry commit conflicts.** pyiceberg does not replay a conflicting commit. Every writer retries with an increasing delay and a bounded number of attempts (configurable, default 5) and reports a clear error when it gives up. This is part of the sink contract.
7. **Retention.** Branches and old snapshots keep files alive. Cleanup uses Nessie's garbage collector (`nessie-gc`), run periodically, not the standard pyiceberg snapshot expiry. Because it does not visit tables only reachable from deleted branches, a report-only orphan sweep compares the object store with the files referenced by all references; deletion needs an explicit confirmation. A tested procedure is required before the first client deployment (condition C2).
8. **Required code corrections.** (a) constrain `s3fs>=2024.1.0` and rebuild the images; (b) `HybridBackend` builds the catalog parameters per catalog type and no longer passes `warehouse` to a REST catalog — a minimal change, limited to that function and covered by tests, allowed by exception to the ADR-0001 rule that makes the backend canonical; (c) the status-bounding decorator for `do_put` is proposed upstream to xorq, and the client already converts the error.
9. **Follow-ups that depend on this ADR (not decided here):** Trino reads the same catalog; dbt gets a `prod` target on `dbt-trino` that materialises marts as Iceberg tables in the shared catalog so BI tools read them through Trino; a lighter Parquet export for BI stays an option.

## Options Considered

### Option A: Keep two catalogs (status quo)
Data written by one component is invisible to the others and `merge` stays blocked. Rejected.

### Option B: Share one SQLite file between host and containers
Fragile across a Docker bind mount on Windows and keeps the single-writer limit. Rejected as a deployment mode (SQLite is kept for solo mode only).

### Option C: SQL catalog on Postgres as an interim step, Nessie later
Removes the concurrency problem with only configuration changes but offers no versioning, so tables would be migrated twice. Rejected now that versioning is a product requirement. Postgres remains useful as Nessie's store.

### Option D: Nessie now, through the REST interface (chosen)
| Dimension | Assessment |
|-----------|------------|
| Complexity | Medium: one more service and the branch/commit model |
| Cost | One Java service plus a Postgres database, about 407 MiB measured |
| Scalability | Concurrent writers handled by the catalog; writers must retry conflicts |
| Versioning | Catalog-wide branches, tags and rollback; conflicts detected per table |

### Option E: Another REST catalog (for example Lakekeeper, Polaris, Gravitino)
Not evaluated in depth. Because components talk to the Iceberg REST interface, the implementation can be replaced later. Nessie is kept for its catalog-wide branching (ADR-0002).

## Trade-off Analysis

The extra operational weight of Nessie is real and works against the "light" positioning. Solo mode absorbs that cost for people who only want to try the product, and deployment mode is where clients pay for versioning. The one-access-point rule matters most: it keeps the backing technology replaceable. Table-level conflict detection and the cleanup gap limit what versioning can promise today, which is why the commands are conservative (fail on conflict, refuse to delete unmerged branches).

## Consequences

**Positive**
- Data written by any component is visible to all; `merge` and incremental loads become possible.
- Versioning becomes a sellable feature for isolated work and rollback.
- Large loads no longer pay the repeated Flight read-and-export cost; routing is simple (direct when possible).
- Trino and BI tools can read the same state.

**Negative / risks**
- One more JVM service to run, monitor and pin to a version.
- Two branches touching the same table always conflict at merge; long-lived parallel branches are not a good fit.
- Storage grows with branches and snapshots; files of deleted unmerged branches are never cleaned by `nessie-gc`.
- Direct writes bypass the authorization of the Flight server (ADR-0005). Acceptable for a dedicated deployment per client; a shared multi-tenant deployment (phases 6 and 7) needs credentials scoped at the catalog level.
- Direct writes may bypass the DuckDB database that Flight keeps for reads and that dbt reads; visibility must be verified (condition C5).
- Two modes and two sinks enlarge the test matrix.
- A change to `HybridBackend` is needed, against the ADR-0001 default.
- The Windows host cannot write data to a Nessie-backed catalog; development there stays in solo mode.

## Conditions

| # | Condition | Status |
|---|-----------|--------|
| C1 | Host write path, or a documented workstation rule | Met through the workstation rule (decision 2) |
| C2 | Retention procedure tested, including orphan files of deleted branches | Open: partial (`nessie-gc` measured; orphan case not covered) |
| C3 | Merge-conflict behavior documented with a proposed `dhd merge` behavior | Met (decision 4) |
| C4 | Commit latency measured against SQLite under the same conditions | Met: same order of magnitude |
| C5 | A table written directly to the shared catalog is visible at once to `dhd query` and to dbt, or the refresh path is defined | Open: blocked by the corrections of decision 8 |
| C6 | Routing threshold measured | Met: direct dominates, no threshold (decision 5) |

C2 and C5 are acceptance criteria of the implementation work and are verified there.

## Open Questions

- Naming rules for tenant branches and who may merge into `main` (to settle with ADR-0005).
- Whether to offer conflict resolution beyond "fail" (for example a `DROP` of one side) once real usage shows the need.
- Whether the file I/O implementation forced by Nessie affects write throughput compared with the PyArrow one.
- Which time-travel read syntax (`--as-of`) `dhd query` should expose.
