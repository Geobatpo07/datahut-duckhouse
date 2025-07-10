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
          |     (Xorq + app.py)          |
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

- [Python 3.11+](https://www.python.org/downloads/)
- [Poetry](https://python-poetry.org/docs/)
- [Docker](https://www.docker.com/)
- [dbt CLI](https://docs.getdbt.com/dbt-cli/installation)

## 🚀 Installation

### 1. Clone the repository

```bash
git clone https://github.com/Geobatpo07/datahut-duckhouse.git
cd datahut-duckhouse
```

### 2. Install Python dependencies

```bash
poetry install
```

### 3. Launch the full environment

```bash
docker-compose up --build
```

### 4. Create a tenant

```bash
poetry run python scripts/create_tenant.py --id tenant_acme
```

### 5. Ingest data

Place a CSV file in ingestion/data/data.csv then run:

```bash
poetry run python scripts/ingest_flight.py
```

## 📂 Project Structure

```
datahut-duckhouse/
├── flight_server/        # Arrow Flight Server + HybridBackend
│   ├── app/
│      ├── app.py
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
├── .env                  # Environment variables
├── docker-compose.yml
└── pyproject.toml
```

## 🧠 Using dbt with DuckDB

```bash
export DBT_PROFILES_DIR=transform/dbt_project/config
cd transform/dbt_project
poetry run dbt run
```

## 🔎 Example Local Query

```bash
poetry run python scripts/query_duckdb.py
```

## 🔒 Delete a Tenant

```bash
poetry run python scripts/delete_tenant.py --id tenant_acme
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
