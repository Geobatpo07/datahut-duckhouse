# xorq/: XORQ Orchestration Layer for DataHut SaaS

This folder is the orchestration, registry, and metadata system for the DataHut-DuckHouse SaaS platform. XORQ is responsible for:

* **Multi-tenant registry/metadata** (tenants, datasets, catalogs, query metrics, billing)
* **Service registry/discovery** (Trino, DuckDB, Flight, catalog, dataset, env per tenant)
* **Central admin API** (REST, future Flight/GRPC)
* **Query metric and billing event tracking**
* **Orchestration logic:**
  - Table upload/registration
  - Trino/Iceberg/DuckDB catalog routing
  - All query event tracking
  - Multi-tenant state introspection

**Endpoints:**
- `/tenants/register`, `/datasets/register`, `/catalogs/add`, `/tenants/info/{tenant_id}`
- `/track_query_metric`, `/tenants/{tenant}/metrics`, `/tenants/{tenant}/billing`
- `/catalogs/tenant/{catalog}`

The XORQ registry is the foundation for DbaaS, analytics SaaS, and multi-environment data governance.

