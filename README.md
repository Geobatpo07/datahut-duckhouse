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

## 🖥️ CLI (`dhd`)

Phase 2 de `docs/ROADMAP.md` : un CLI qui exerce le chemin Flight complet
contre `HybridBackend` (create/insert écrivent dans Iceberg, ADR-0001).
Pas encore de notion de tenant — volontairement, voir la roadmap.

```bash
poetry install   # installe l'entrypoint `dhd`

dhd create-table my_table ingestion/data/data.csv
dhd insert my_table ingestion/data/more_data.csv
dhd query "SELECT * FROM my_table" --limit 10
dhd list-tables

# Pas encore actif (Nessie, ADR-0002 — Phase 3 de la roadmap) :
dhd branch create my-branch
```

Se connecte via `FLIGHT_SERVER_HOST`/`FLIGHT_SERVER_PORT` (mêmes variables
que le reste du projet). `query`/`list-tables` n'ont pas encore été validés
contre un serveur réel en dehors de leurs tests unitaires (voir
`tests/test_cli.py`) — le chemin d'écriture Iceberg, lui, a été vérifié en
conditions réelles.

## 📂 Project Structure

```
datahut-duckhouse/
├── datahut_duckhouse/    # CLI (dhd) — Phase 2
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
├── docs/adr/             # Architecture Decision Records
├── docs/ROADMAP.md        # Roadmap détaillée et à jour
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

Voir [`docs/ROADMAP.md`](docs/ROADMAP.md) pour la roadmap détaillée et à jour
(séquencement CLI-first, puis multi-tenant derrière un point de bascule
explicite). Statut résumé :

- ✅ Phase 0-1 : backend hybride canonique, Iceberg comme unique source de
  vérité (ADR-0001)
- 🚧 Phase 2 : CLI (`dhd`) — en cours
- 🔜 Phase 3-5 : Nessie, Postgres, dogfooding
- 🔜 Phase 6+ : multi-tenant (isolation, auth, quotas) — conditionné à un
  vrai signal de bascule, pas une date

## 📄 License

Project under MIT License.

## ✍️ Author

Geovany Batista Polo LAGUERRE – [lgeobatpo98@gmail.com](mailto:lgeobatpo98@gmail.com) | Data Science & Analytics Engineer
