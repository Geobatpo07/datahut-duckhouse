# 🏠 DataHut-DuckHouse

**DataHut-DuckHouse** is a lightweight, hybrid, and open-source analytics platform that combines the simplicity of **DuckDB**, the scalability of **Iceberg**, the speed of **Arrow Flight**, the orchestration power of **Xorq**, and the modularity of **dbt** to create a modern, local or cloud-ready data stack.

## 🧱 Architecture SaaS hybride

```
             +------------------------+
             |  CSV / Local Files     |
             +-----------+------------+
                         |
                         v
             +------------------------+
             |   Arrow Flight Client  |
             |   (ingest_flight.py)   |
             +-----------+------------+
                         |
                         v
          +--------------+---------------+
          |   Arrow Flight Server        |
          |   (Xorq + app_xorq.py)       |
          | - hybrid backend: Iceberg + DuckDB
          | - snapshots, synchronized views
          +--------------+---------------+
                         |
         +---------------+----------------+
         |                                  |
     +--------+                       +-------------+
     | DuckDB |                       |   Iceberg   |
     +--------+                       +-------------+
         |                                  |
         +--------+         +---------------+
                  |         |
               +-------------+        +-------------+
               |     dbt     |        |    Trino    |
               +-------------+        +-------------+
                    |                      |
                    |                      v
                    |              BI Tools (Metabase, Tableau)
                    |
                    v
             SQL models per tenant
```

## ✨ Features

- 🔗 Fast ingestion via Arrow Flight
- 🐤 Hybrid storage: local DuckDB & Iceberg (MinIO)
- 🧠 Orchestration with Xorq (Flight + multi-backend support)
- 🔄 Auto-synchronization with Trino (catalogs)
- 📊 Declarative SQL transformations using dbt
- 📦 Multi-tenancy: dynamic tenant creation/deletion
- ☁️ S3 integration via MinIO

## ⚙️ Requirements

- [Python 3.11 or 3.12](https://www.python.org/downloads/) (uv can install it for you)
- [uv](https://docs.astral.sh/uv/) — dependency & environment manager
- [Docker](https://www.docker.com/) with the Compose plugin

## 🚀 Installation

### 1. Clone the repository

```bash
git clone https://github.com/Geobatpo07/datahut-duckhouse.git
cd datahut-duckhouse
```

### 2. Install Python dependencies

```bash
uv sync          # creates .venv from uv.lock
```

Run any command inside the environment with `uv run <cmd>` (e.g. `uv run pytest`).

### 3. Configure the environment

```bash
make env         # copies .devcontainer/.env.example -> .env
# then edit .env and set MINIO_ROOT_PASSWORD / AWS_SECRET_ACCESS_KEY
```

`.env` is git-ignored and is the only place secrets live — the Dockerfiles and
`docker-compose.yml` contain none.

### 4. Launch the full environment

```bash
docker compose up --build      # or: make docker-up
```

Docker build files live in [`.devcontainer/`](.devcontainer/); `docker-compose.yml`
stays at the repo root. VS Code users can also "Reopen in Container" — the
[`.devcontainer/devcontainer.json`](.devcontainer/devcontainer.json) brings up the
same stack as a dev environment.

### 5. Ingest and query with the `dhd` CLI

`dhd` is the single-user client (roadmap Phase 2). It drives the Flight server —
start one first (`make dev-server`, or `uv run python -m flight_server.app.app_xorq`),
then in another shell:

```bash
uv run dhd create-table sales data/sales.csv      # .csv / .parquet / .json
uv run dhd list-tables
uv run dhd query "SELECT city, sum(amount) FROM sales GROUP BY city" --limit 20
uv run dhd insert sales data/sales_new.csv          # --mode append|overwrite
```

`dhd` reads `FLIGHT_SERVER_HOST` / `FLIGHT_SERVER_PORT` (default `localhost:8815`).
If the server isn't running it fails with a clear message rather than hanging.
`dhd branch …` is a placeholder until Nessie lands (ADR-0002 / Phase 3).

## 📂 Project Structure

```
datahut-duckhouse/
├── datahut_duckhouse/    # `dhd` CLI (the only packaged component)
│   ├── cli.py
│   └── connection.py
├── flight_server/        # Arrow Flight Server + HybridBackend
│   ├── app/
│      ├── app_xorq.py
│      ├── xorq_config.py
│      ├── utils.py
│      └── backends/hybrid_backend.py
├── ingestion/data/       # Source data
├── scripts/              # Ingestion, queries, tenant management
│   ├── ingest_flight.py
│   ├── query_duckdb.py
│   ├── create_tenant.py
│   └── delete_tenant.py
├── transform/dbt_project/ # dbt models
├── config/               # Trino, dbt, tenants, users
│   ├── trino/etc/
│   ├── tenants/
│   └── users/users.yamlx
├── .devcontainer/        # Dockerfiles + devcontainer.json + .env.example
├── .env                  # Secrets & config (git-ignored; from .devcontainer/.env.example)
├── docker-compose.yml    # Stack definition (root; no secrets)
├── uv.lock
└── pyproject.toml
```

## 🧠 Using dbt with DuckDB

```bash
export DBT_PROFILES_DIR=transform/dbt_project/config
cd transform/dbt_project
uv run dbt run
```

## 🔎 Example Local Query

```bash
uv run python scripts/query_duckdb.py
```

## 🔒 Delete a Tenant

```bash
uv run python scripts/delete_tenant.py --id tenant_acme
```

## 📈 BI Interface with Trino

Access Trino at http://localhost:8080
Use `tenant_acme` as the Trino catalog in Superset or Metabase.

## 🛣️ Roadmap

- ✅ Multi-tenant Iceberg + DuckDB
- ✅ Dynamic registration with Xorq + Trino
- 🔜 Flask/React management interface
- 🔜 User authentication + role management
- 🔜 Integration with Metabase or Superset
- 🔜 SaaS deployment on public cloud

## 📄 License

Project under MIT License.

## ✍️ Author

Geovany Batista Polo LAGUERRE – [lgeobatpo98@gmail.com](mailto:lgeobatpo98@gmail.com) | Data Science & Analytics Engineer
