# ADR-0003: PostgreSQL for Tenant/Dataset/Billing Metadata

**Status:** Accepted
**Date:** 2026-08-29
**Deciders:** Geovany Batista Polo Laguerre

## Context

`xorq/core.py`'s `XORQOrchestrator` (exposed over HTTP by `xorq/api.py`, run as the
`xorq-service` container) holds all tenant registration, dataset metadata, query
metrics, and billing events **entirely in memory** — `self.tenants` is a plain Python
dict, and `_load_metadata` is a no-op despite an `XORQ_STORAGE` environment variable
suggesting persistence was intended. A restart of the `xorq-service` container silently
discards every tenant, every recorded dataset, and every billing event.

This is tolerable for local development but disqualifying for anything resembling
production SaaS: billing data in particular cannot live only in process memory.

## Decision

Back `XORQOrchestrator` with **PostgreSQL**, added as its own service in
`docker-compose.yml`. Tenants, datasets, catalogs, query metrics, and billing events move
from in-memory Python structures to tables in Postgres, accessed via a lightweight ORM
or query layer (e.g. SQLAlchemy, consistent with the rest of the Python codebase).

Postgres is chosen over a simpler file-backed option (SQLite) because:
- Tenant creation/registration is a concurrent-write path (multiple Flight server
  instances or admin actions could register tenants at the same time); Postgres handles
  concurrent writers correctly, SQLite does not without extra care.
- Billing events are exactly the kind of data that benefits from ACID transactions and
  standard tooling (backups, replication, point-in-time recovery) rather than a bespoke
  file format.
- It is the standard expectation for this class of metadata store, and keeps the
  operational model consistent with what a future team member would expect to find.

## Options Considered

### Option A: Keep in-memory (status quo)
| Dimension | Assessment |
|-----------|------------|
| Complexity | None |
| Cost | None |
| Scalability | Fails immediately on restart or multi-instance deployment |
| Team familiarity | N/A |

**Pros:** Nothing to build.
**Cons:** Data loss on every restart; cannot run more than one `xorq-service` instance;
not viable once billing is real.

### Option B: SQLite file (volume-mounted)
| Dimension | Assessment |
|-----------|------------|
| Complexity | Low |
| Cost | Low — no extra service |
| Scalability | Weak under concurrent writers; acceptable only as a pre-prod stopgap |
| Team familiarity | High |

**Pros:** Minimal infra addition, quick to implement, genuinely persists across
restarts.
**Cons:** Same concurrency ceiling as the Iceberg SQL catalog problem in ADR-0002;
not a credible foundation for real billing data.

### Option C: PostgreSQL (chosen)
| Dimension | Assessment |
|-----------|------------|
| Complexity | Medium — one more service, standard ORM/migration setup |
| Cost | One more long-running container; negligible at this scale |
| Scalability | Good — handles concurrent tenant registration and billing writes correctly |
| Team familiarity | High (stated preference) |

**Pros:** Correct concurrency and durability guarantees; standard backup/migration
tooling; can also serve as Nessie's backing store (ADR-0002) if consolidating infra is
preferred later.
**Cons:** Requires schema design and a migration tool (e.g. Alembic) where today there is
none.

## Trade-off Analysis

SQLite would be defensible as an interim step, but since Postgres is the explicitly
preferred and correct long-term target, taking the SQLite detour would mean doing the
persistence work twice. Given the billing use case already exists in the code
(`track_query_metric`'s billing events), it's worth building the durable version
directly rather than staging through a stopgap.

## Consequences

- **Easier:** `xorq-service` can be restarted, redeployed, or scaled to multiple
  instances without losing tenant/billing state; billing data gets real durability and
  auditability guarantees.
- **Easier:** Standard tooling (pg_dump, migrations, monitoring) applies, rather than
  needing bespoke backup logic for an in-memory structure or a raw SQLite file.
- **Harder:** Schema evolution now needs a migration discipline (Alembic or similar)
  instead of just editing a Python class; local dev requires the Postgres container to be
  running (already true for MinIO/Trino, so consistent with existing dev experience).
- **To revisit:** Whether Postgres also backs Nessie's catalog metadata (ADR-0002) to
  keep the infra footprint smaller, or whether the two stay separate for isolation
  between "platform metadata" and "table catalog" concerns.

## Action Items

1. [ ] Add a `postgres` service to `docker-compose.yml` with a persistent volume
2. [ ] Design the schema: `tenants`, `datasets`, `catalogs`, `query_metrics`,
       `billing_events` tables, mirroring the fields already tracked in
       `TenantMetadata`
3. [ ] Introduce a migration tool (e.g. Alembic) and an initial migration
4. [ ] Rewrite `XORQOrchestrator` in `xorq/core.py` to read/write through
       Postgres instead of in-memory dicts/lists, keeping the same public methods
       (`register_tenant`, `add_dataset`, `track_query_metric`, etc.) so `xorq/api.py`
       does not need to change
5. [ ] Add a health check + startup dependency (`depends_on: postgres`) for
       `xorq-service`, mirroring the existing pattern used for `minio`
6. [ ] Decide whether Nessie (ADR-0002) shares this Postgres instance or gets its own
