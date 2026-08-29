# Architecture Decision Records

This directory tracks architectural decisions for DataHut-DuckHouse, starting from the
August 2026 architecture review.

| ADR | Title | Status |
|-----|-------|--------|
| [0001](./0001-canonical-hybrid-backend.md) | Iceberg-as-Source-of-Truth via a Single Canonical Hybrid Backend | Accepted |
| [0002](./0002-iceberg-catalog-nessie.md) | Nessie as the Iceberg Catalog | Accepted |
| [0003](./0003-tenant-metadata-postgres.md) | PostgreSQL for Tenant/Dataset/Billing Metadata | Accepted |

## Reading order

ADR-0001 establishes the core structural decision (one canonical backend, Iceberg-only
writes) that ADR-0002 and ADR-0003 depend on for their own connection details. Read
0001 first.

## Format

Each ADR follows: Context → Decision → Options Considered → Trade-off Analysis →
Consequences → Action Items. New ADRs should follow the same structure and be numbered
sequentially.
