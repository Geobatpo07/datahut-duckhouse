# 🏠 DataHut-DuckHouse

**DataHut-DuckHouse** est une plateforme analytique légère, hybride et open source qui combine la simplicité de **DuckDB**, la scalabilité d'**Iceberg**, la rapidité d'**Arrow Flight**, l’orchestration de **Xorq** et la modularité de **dbt** pour créer un data stack moderne, local ou cloud-ready.

## 🧱 Architecture SaaS hybride

```
             +------------------------+
             |  CSV / Fichiers locaux |
             +-----------+------------+
                         |
                         v
             +------------------------+
             |    Client Arrow Flight |
             |  (ingest_flight.py)    |
             +-----------+------------+
                         |
                         v
          +--------------+---------------+
          |     Serveur Arrow Flight     |
          |        (Xorq + app.py)       |
          | - backend hybride Iceberg + DuckDB
          | - snapshots, vues synchronisées
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
                    |              outils BI (Metabase, Tableau)
                    |
                    v
             modèles SQL par tenant
```

## ✨ Fonctionnalités

- 🔗 Ingestion rapide via Arrow Flight
- 🐤 Stockage hybride : **DuckDB** local & **Iceberg** (MinIO)
- 🧠 Orchestration via **Xorq** (Flight + backends multiples)
- 🔄 Synchronisation automatique avec Trino (catalogues)
- 📊 Transformations SQL déclaratives avec **dbt**
- 📦 Multi-tenant : création/suppression dynamique
- ☁️ Intégration S3 via MinIO

## ⚙️ Prérequis

- [Python 3.11+](https://www.python.org/downloads/)
- [Poetry](https://python-poetry.org/docs/)
- [Docker](https://www.docker.com/)
- [dbt CLI](https://docs.getdbt.com/dbt-cli/installation)

## 🚀 Installation

### Prerequisites

- [Python 3.11+](https://www.python.org/downloads/)
- [Poetry](https://python-poetry.org/docs/)
- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)
- [dbt CLI](https://docs.getdbt.com/dbt-cli/installation)

### 1. Clone the Repository

```bash
git clone https://github.com/Geobatpo07/datahut-duckhouse.git
cd datahut-duckhouse
```

### 2. Install Python Dependencies

```bash
# Install dependencies
poetry install

# Install development dependencies
poetry install --with dev

# Set up pre-commit hooks
poetry run pre-commit install
```

### 3. Environment Configuration

Copy and configure the environment file:

```bash
cp .env.example .env
# Edit .env with your configuration
```

### 4. Start the Complete Environment

```bash
# Build and start all services
docker-compose up --build

# Or start in detached mode
docker-compose up -d --build
```

### 5. Verify Installation

```bash
# Check service health
docker-compose ps

# Check MinIO (S3) - http://localhost:9001
# Check Trino - http://localhost:8080
# Check Flight Server - grpc://localhost:8815
```

### 6. Create a Tenant

```bash
poetry run python scripts/create_tenant.py --id tenant_acme
```

### 7. Data Ingestion

Place a CSV file in `ingestion/data/data.csv` and run:

```bash
poetry run python scripts/ingest_flight.py
```

## 📂 Structure du projet

```
datahut-duckhouse/
├── flight_server/        # Serveur Arrow Flight + HybridBackend
|   ├── app/
│      ├── app.py
|      ├── app_xorq.py
│      ├── xorq_config.py
│      ├── utils.py
│      └── backends/hybrid_backend.py
├── ingestion/data/       # Données sources
├── scripts/              # Ingestion, requêtes, gestion tenants
│   ├── ingest_flight.py
│   ├── query_duckdb.py
│   ├── create_tenant.py
│   └── delete_tenant.py
├── transform/dbt_project/ # Modèles dbt
├── config/               # Trino, dbt, tenants, users
│   ├── trino/etc/
│   ├── tenants/
│   └── users/users.yamlx
├── .env                  # Variables d’environnement
├── docker-compose.yml
└── pyproject.toml
```

## 🧠 Utiliser dbt avec DuckDB

```bash
export DBT_PROFILES_DIR=transform/dbt_project/config
cd transform/dbt_project
poetry run dbt run
```

## 🔎 Exemple de requête locale

```bash
poetry run python scripts/query_duckdb.py
```

## 🔒 Suppression d’un tenant

```bash
poetry run python scripts/delete_tenant.py --id tenant_acme
```

## 📊 Development & Testing

### Running Tests

```bash
# Run all tests
poetry run pytest

# Run tests with coverage
poetry run pytest --cov=flight_server --cov=scripts --cov-report=html

# Run specific test file
poetry run pytest tests/test_utils.py -v
```

### Code Quality

```bash
# Format code
poetry run black .
poetry run isort .

# Lint code
poetry run ruff check .
poetry run flake8 .

# Type checking
poetry run mypy flight_server scripts

# Security checks
poetry run bandit -r flight_server scripts
poetry run safety check
```

### Development Workflow

```bash
# 1. Create feature branch
git checkout -b feature/your-feature

# 2. Make changes and test
poetry run pytest
poetry run pre-commit run --all-files

# 3. Commit changes
git add .
git commit -m "feat: your feature description"

# 4. Push and create PR
git push origin feature/your-feature
```

### Monitoring & Observability

```bash
# View application logs
docker-compose logs -f flight-server

# Check health status
curl http://localhost:8815/health

# View metrics
curl http://localhost:8815/metrics
```

## 📊 Data Visualization

### Trino Interface

Access Trino at: [http://localhost:8080](http://localhost:8080)  
Use `tenant_acme` as Trino catalog in Superset or Metabase.

### Query Examples

```sql
-- Query DuckDB tables
SELECT * FROM tenant_acme.default.your_table LIMIT 10;

-- Query Iceberg tables
SELECT * FROM tenant_acme.iceberg.your_iceberg_table LIMIT 10;

-- Join across backends
SELECT d.*, i.additional_data 
FROM tenant_acme.default.duckdb_table d
JOIN tenant_acme.iceberg.iceberg_table i ON d.id = i.id;
```

## 🛣️ Roadmap

- ✅ Multi-tenant Iceberg + DuckDB
- ✅ Enregistrement dynamique Xorq + Trino
- 🔜 Interface Flask/React de gestion
- 🔜 Authentification + rôles utilisateurs
- 🔜 Intégration Metabase ou Superset
- 🔜 Déploiement SaaS sur cloud public

## 📄 Licence

Projet sous licence MIT.

## ✍️ Auteur

Geovany Batista Polo LAGUERRE – [lgeobatpo98@gmail.com](mailto:lgeobatpo98@gmail.com) | Data Science & Analytics Engineer
