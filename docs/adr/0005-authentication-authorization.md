# ADR-0005: Authentication & Authorization for Flight and API Access

**Status:** Proposed
**Date:** 2026-08-29
**Deciders:** Geovany Batista Polo Laguerre

## Context

Neither the Arrow Flight server (`app_xorq.py`) nor the `xorq-service` HTTP API
currently authenticate callers. Any client that can reach the Flight port can read or
write any table in any namespace; any client that can reach `xorq-service` can register
tenants, list datasets, or (per ADR-0003) eventually read billing data. This is
acceptable for a single-operator internal tool; it is disqualifying for a SaaS product,
and it's also the missing piece ADR-0004 (tenant isolation) depends on — a namespace
boundary means nothing if there's no verified caller identity to check it against.

Two related but distinct questions need answers: **how does a caller prove who they
are** (authentication), and **what are they allowed to do once identified**
(authorization/scoping).

## Decision

**Authentication:** Issue per-tenant **API tokens** (opaque bearer tokens, not JWTs, to
start — see Options below), validated by both the Flight server and `xorq-service`
against the Postgres-backed tenant store from ADR-0003. Tokens are presented as Arrow
Flight call headers (`authorization: Bearer <token>`) and as standard HTTP
`Authorization` headers for `xorq-service`.

**Authorization:** Every validated token carries exactly one **tenant ID** and a
**scope** (e.g. `read`, `write`, `admin`). The Flight server resolves the tenant ID from
the token *before* any table name is interpreted, and uses it to enforce the namespace
boundary from ADR-0004 — a request is rejected before it reaches `HybridBackend` if the
requested namespace doesn't match the token's tenant ID. There is no cross-tenant scope
in this first version; an operator/admin path (for Anthropic-... for Geovany's own
platform-admin use) is a separate, explicitly-flagged token type, not a "superuser" bit
on a tenant token.

## Options Considered

### Option A: No auth (status quo)
Not viable for any external-facing deployment; included only as the baseline being
replaced.

### Option B: Opaque API tokens per tenant (chosen)
| Dimension | Assessment |
|-----------|------------|
| Complexity | Low-Medium — token generation, hashed storage, a validation lookup per call |
| Revocability | Immediate — delete/invalidate the row in Postgres, next call fails |
| Statelessness | No — every call needs a store lookup (mitigated with a short-TTL cache) |
| Fit for Flight | Good — Flight supports arbitrary call headers/metadata |
| Team familiarity | High — simplest model to reason about and debug |

**Pros:** Simple to implement and revoke; no cryptographic key management beyond
hashing tokens at rest; matches the trust model of a service used by a small number of
tenant backends (not end-user browsers) at this stage.
**Cons:** Requires a store lookup per call unless cached; less standard than JWT/OAuth
for eventual third-party integrations.

### Option C: JWTs (self-contained, signed)
| Dimension | Assessment |
|-----------|------------|
| Complexity | Medium — key rotation, signature verification, claims design |
| Revocability | Hard — a compromised JWT is valid until expiry unless a revocation list is also maintained, which reintroduces the store-lookup cost Option B has anyway |
| Statelessness | Yes, in principle — undermined in practice once revocation is needed |
| Fit for Flight | Good |
| Team familiarity | Medium |

**Pros:** No per-call store lookup if revocation is not needed; standard, well-understood
format if the platform later needs to interoperate with external identity providers.
**Cons:** The revocability gap is a real problem for a billing-relevant system — an
operator needs to be able to cut off a non-paying tenant *immediately*, not "by next
token expiry."

### Option D: mTLS (client certificates per tenant)
| Dimension | Assessment |
|-----------|------------|
| Complexity | High — certificate issuance, rotation, and distribution per tenant |
| Revocability | Requires CRL/OCSP infrastructure |
| Fit for Flight | Good — gRPC/Flight supports TLS client certs natively |
| Team familiarity | Low |

**Pros:** Strong cryptographic identity, no bearer-token leakage risk.
**Cons:** Operationally heavy for the team's current size and the product's current
stage; certificate lifecycle management is a project of its own.

## Trade-off Analysis

JWTs' main advantage — avoiding a store lookup — evaporates once immediate revocation is
required, which it is here because tenant tokens gate billed access. That leaves Option
B and Option D genuinely competing, and mTLS's operational cost (issuance, rotation,
revocation infrastructure) is disproportionate to the platform's current tenant count and
team size. Opaque tokens validated against the already-planned Postgres store (ADR-0003)
reuse infrastructure this project is building anyway, and immediate revocation is a
plain `DELETE` rather than a PKI operation. Option D remains worth revisiting if the
platform later needs machine-to-machine trust at a much larger scale or in a
zero-trust network context.

## Consequences

- **Easier:** Suspending a non-paying or abusive tenant is instantaneous (invalidate
  their token row) rather than waiting on token expiry or certificate revocation
  propagation.
- **Easier:** ADR-0004's namespace boundary becomes actually enforceable, since every
  request now carries a verified tenant ID before any table name is interpreted.
- **Harder:** Every Flight call and every `xorq-service` HTTP call now has a validation
  step with a database round-trip; this needs a short-TTL in-memory cache (e.g. a few
  seconds) to avoid making Postgres a latency bottleneck on the hot path — a plain,
  uncached lookup per call would be a mistake at any real query volume.
- **Harder:** Token issuance, storage (hashed, never plaintext), and rotation need a
  documented process before the first external tenant is onboarded.
- **To revisit:** Whether to move to JWTs or OAuth once/if the platform needs
  browser-facing end-user auth (as opposed to tenant-backend-to-platform auth), since
  that is a genuinely different use case from the one this ADR addresses.

## Action Items

1. [ ] Add a `tenant_tokens` table to the Postgres schema from ADR-0003 (token hash,
       tenant ID, scope, created_at, revoked_at)
2. [ ] Add token validation middleware to `xorq-service` (`xorq/api.py`)
3. [ ] Add token validation to the Flight server, reading the `authorization` call
       header before any request reaches `HybridBackend`
4. [ ] Wire the resolved tenant ID into the namespace-boundary check from ADR-0004
       (Action Item 1)
5. [ ] Add a short-TTL cache (in-process or Redis, if one gets introduced later) for
       token validation to avoid a Postgres round-trip on every call
6. [ ] Document token issuance/rotation as part of the tenant onboarding process
       (`scripts/create_tenant.py`)
7. [ ] Define the platform-admin token type explicitly (separate table or a distinct
       scope value), not as an escalated tenant token
