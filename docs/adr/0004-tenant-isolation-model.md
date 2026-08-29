# ADR-0004: Tenant Isolation Model

**Status:** Proposed
**Date:** 2026-08-29
**Deciders:** Geovany Batista Polo Laguerre

## Context

The platform is intended to become a SaaS product serving multiple external, paying
tenants — not just an internal multi-dataset analytics stack. Today, `HybridBackend`
and the Iceberg catalog have no tenant concept at all: every table sits in the same
namespace, reachable by any caller that can reach the Flight server. `scripts/create_tenant.py`
registers a tenant in `xorq-service` (ADR-0003), but that registration is purely
metadata bookkeeping — it does not currently constrain where a tenant's data can live or
who can touch it.

With Nessie chosen as the Iceberg catalog (ADR-0002) specifically for its Git-like
branch/commit model, there are now two independent mechanisms available for expressing
"this data belongs to tenant X," and they solve different problems:

- **Iceberg namespaces/schemas** partition tables by name (`tenant_x.orders`,
  `tenant_y.orders`) within a single, shared table history.
- **Nessie branches** partition the *catalog state itself* — a branch can hold a
  divergent view of table definitions, metadata, and commit history from `main`.

Conflating these, or picking neither deliberately, leaves the isolation model
implicit — which is exactly the kind of ambiguity that, left unresolved, becomes a
security incident once real tenants are onboarded. A tenant's blast radius (what a bug,
runaway query, or bad actor within one tenant can affect) is determined by this decision,
not by anything downstream.

## Decision

Use **both mechanisms together, for different purposes**:

- **Iceberg namespace per tenant** (`tenant_<id>`) is the unit of **data isolation**.
  Every table a tenant creates lives under their namespace; no tenant can address
  another tenant's namespace, enforced at the `HybridBackend` layer (see Action Items)
  and, longer-term, at the catalog/authorization layer once ADR-0005 lands.
- **Nessie branch per tenant** (`tenant/<id>`) is the unit of **change isolation and
  recovery**, not of data separation. A tenant's writes land on their branch first;
  promotion to `main` (or a tenant-visible "live" tag) happens as an explicit,
  loggable step. This gives cheap per-tenant rollback and a natural point to insert
  validation (e.g. a dbt test suite) before a tenant's change becomes visible to their
  own reads.

Namespace does the security-relevant work (who can see what); branch does the
operational-safety work (undo a bad write cheaply). Neither alone was sufficient:
namespace-only gives no rollback story beyond Iceberg's per-table snapshot history;
branch-only gives no hard boundary preventing one tenant's query from naming another
tenant's table.

## Options Considered

### Option A: Namespace-per-tenant only
| Dimension | Assessment |
|-----------|------------|
| Complexity | Low — a naming convention plus an authorization check |
| Isolation strength | Good for data addressing; no isolation of catalog *changes* |
| Operational recovery | Falls back to per-table Iceberg snapshots only |
| Cost | Minimal — no extra infra |

**Pros:** Simple, directly enforceable, cheap.
**Cons:** No natural place to stage a risky schema change or bulk load before it's
live; a bad migration is a manual restore, not a branch discard.

### Option B: Branch-per-tenant only
| Dimension | Assessment |
|-----------|------------|
| Complexity | Medium — every read/write path must resolve "which branch for this caller" |
| Isolation strength | Weak on its own — nothing stops a query on `main` from naming a table that should belong to another tenant, since table *names* aren't partitioned |
| Operational recovery | Excellent — branch discard/reset is a first-class Nessie operation |
| Cost | Requires branch-aware routing everywhere |

**Pros:** Best rollback story.
**Cons:** Does not actually create a hard data-addressing boundary between tenants —
branches diverge in history, not in what table names exist or are reachable.

### Option C: Namespace-per-tenant for data isolation + branch-per-tenant for change staging (chosen)
| Dimension | Assessment |
|-----------|------------|
| Complexity | Medium-High — two mechanisms to reason about, but each has a single clear job |
| Isolation strength | Strong — namespace is the hard boundary, branch is the safety net |
| Operational recovery | Excellent — branch discard undoes a bad batch without touching other tenants' namespaces |
| Cost | Namespace convention is free; branch-per-tenant adds a small amount of Nessie branch-management overhead |

**Pros:** Each mechanism does the job it's actually good at; failure mode of one
doesn't compromise the other.
**Cons:** More moving parts to document and test than either option alone; requires
`HybridBackend` and the Flight server to become branch- and namespace-aware, which is
new work (see Action Items).

## Trade-off Analysis

Option A alone is tempting because it's nearly free, but it throws away the exact
capability (branching) that motivated choosing Nessie in ADR-0002 — if isolation stops at
naming, the branch model would only ever be used for `main`, and ADR-0002's stated
rationale would go unused. Option B alone is a non-starter for a SaaS product: it
optimizes for recoverability while leaving the actual tenant-boundary question
unanswered, which is the higher-stakes gap of the two (a data leak matters more than a
slower rollback). Option C costs more to build than either alone, but it's the only
option where the two hard SaaS requirements — "tenant A can never touch tenant B's data"
and "we can cheaply undo a bad write" — are each satisfied by the mechanism actually
suited to it.

## Consequences

- **Easier:** Reasoning about a security incident — "could tenant A have read tenant B's
  data" reduces to "was the namespace check bypassed," a single, auditable question.
- **Easier:** Staging risky changes (bulk loads, schema migrations, dbt model changes)
  per tenant without a full separate environment.
- **Harder:** `HybridBackend.do_connect` and every read/write path need a tenant context
  (namespace + active branch) resolved from the caller, which does not exist yet — this
  is the dependency this ADR has on ADR-0005 (authentication) actually identifying the
  caller in the first place. Isolation without authentication is enforcement with no
  identity to enforce against.
- **To revisit:** Whether "promotion to main" is fully automatic (e.g. after a dbt test
  suite passes) or requires a manual/admin step, once a first real tenant workflow
  exists to design it against.

## Action Items

1. [ ] Add tenant namespace resolution to `HybridBackend` (reject or auto-prefix table
       names outside the caller's namespace)
2. [ ] Extend `scripts/create_tenant.py` to provision both the Iceberg namespace and the
       Nessie branch for a new tenant, recorded in Postgres via `xorq-service` (ADR-0003)
3. [ ] Define the branch-promotion step (manual admin action to start; automate later)
4. [ ] Add a namespace-boundary test: verify a tenant-scoped `HybridBackend` call cannot
       address another tenant's namespace, even by explicit table name
5. [ ] This ADR is not enforceable until caller identity exists — see ADR-0005
       (Authentication & Authorization)
