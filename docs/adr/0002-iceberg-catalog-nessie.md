# ADR-0002: Nessie as the Iceberg Catalog

**Status:** Accepted
**Date:** 2026-08-29
**Deciders:** Geovany Batista Polo Laguerre

## Context

`HybridBackend.do_connect` currently opens its Iceberg catalog with `catalog_type="sql"`
— a SQLite file embedded next to the warehouse. This works for a single process with a
single writer, but the platform is multi-tenant and has (or will have) several
concurrent writers: the Flight server, `scripts/create_tenant.py`, and potentially Trino
writing directly to the same tables. An embedded SQLite catalog is a classic source of
lock contention and corruption once more than one process writes concurrently.

A catalog choice also has to be made now, before ADR-0001's rewiring lands, since the
Flight server's `do_connect` call needs a concrete `catalog_type` / connection target.

## Decision

Use **Project Nessie** as the Iceberg catalog, run as its own service in
`docker-compose.yml` alongside `minio` and `trino`.

The deciding factor is Git-like semantics: Nessie exposes branches, tags, and commits
over the table catalog itself. For a multi-tenant platform this maps naturally onto
per-tenant or per-environment branches (e.g. isolate a tenant's schema migration on a
branch before merging), and it gives cheap, catalog-level rollback if a bad write or a
faulty dbt run needs to be undone — a workflow the current SQL catalog cannot offer at
all, and that a plain Hive Metastore only partially approximates.

## Options Considered

### Option A: Hive Metastore
| Dimension | Assessment |
|-----------|------------|
| Complexity | Low-Medium — long-established, largest ecosystem of examples |
| Cost | One extra long-running service (+ its own metadata DB) |
| Scalability | Good — proven at scale, well understood by Trino |
| Team familiarity | Low today, but very well documented |

**Pros:** Simplest, most battle-tested option; first-class Trino support.
**Cons:** No branching/versioning at the catalog level — only Iceberg's per-table
snapshot history, no cross-table or tenant-level rollback story.

### Option B: Nessie
| Dimension | Assessment |
|-----------|------------|
| Complexity | Medium — one more service, plus a mental model shift (branches/commits) |
| Cost | One extra service; can run on an embedded store for small deployments |
| Scalability | Good, though a newer/smaller ecosystem than Hive Metastore |
| Team familiarity | Low today, but the Git-like model shortens the learning curve given the team's existing Git fluency |

**Pros:** Git-like branch/tag/rollback semantics across the whole catalog, not just
per-table snapshots; natural fit for isolating risky tenant operations before merging
them into the tenant's live branch.
**Cons:** Smaller ecosystem than Hive Metastore; one more moving part to operate and
monitor; Trino/Nessie integration, while solid, is less universally battle-tested than
Trino/Hive Metastore.

### Option C: Keep the embedded SQL catalog
| Dimension | Assessment |
|-----------|------------|
| Complexity | Lowest — nothing to add |
| Cost | None |
| Scalability | Poor — single-writer assumption breaks under concurrency |
| Team familiarity | High (already in place) |

**Pros:** Zero migration effort.
**Cons:** Directly incompatible with the multi-tenant, multi-writer direction the
platform is heading; explicitly the risk called out in ADR-0001.

## Trade-off Analysis

Hive Metastore is the safer, more conventional choice and would be the right call if the
priority were minimizing operational risk on a tight timeline. Nessie costs a bit more in
setup and is a newer piece of infrastructure to operate, but it directly serves a
requirement this project has (Git-like branching, isolate-then-merge per tenant, cheap
rollback) that Hive Metastore does not provide. Given that requirement is explicit here,
Nessie is the better fit despite the smaller ecosystem.

## Consequences

- **Easier:** Per-tenant or per-migration isolation via branches; rollback of a bad
  ingestion or dbt run becomes a catalog operation instead of a manual data-restoration
  exercise.
- **Easier:** A natural place to hang future features like "preview a schema change on a
  branch before it's visible to the tenant."
- **Harder:** One more service to run, secure, and monitor (`nessie` container +
  its own backing store); the team has to learn Nessie's branch/commit model on top of
  Iceberg's own snapshot model — the two need to be kept conceptually distinct in docs.
- **To revisit:** Nessie can run against an in-memory or embedded store for dev, but a
  production deployment needs its own persistent backing store (e.g. Postgres — see
  ADR-0003, which could reasonably host Nessie's metadata too if that keeps the infra
  footprint down).

## Action Items

1. [ ] Add a `nessie` service to `docker-compose.yml`, wired to MinIO for warehouse
       storage
2. [ ] Update `HybridBackend.do_connect` (and `xorq_config.py` per ADR-0001) to use
       `catalog_type="nessie"` / the Nessie REST endpoint instead of `"sql"`
3. [ ] Point Trino's Iceberg connector config (`config/trino/etc/`) at the same Nessie
       catalog so Trino and the Flight server always see identical table state
4. [ ] Decide Nessie's backing store (embedded for dev, Postgres for anything durable —
       see ADR-0003) and document the two configurations
5. [ ] Document the branch-per-tenant (or branch-per-migration) convention once a first
       real use case for it appears, so the model doesn't stay theoretical
