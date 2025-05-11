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

### 1. Cloner le dépôt

```bash
git clone https://github.com/Geobatpo07/datahut-duckhouse.git
cd datahut-duckhouse
```

### 2. Installer les dépendances Python

```bash
poetry install
```

### 3. Lancer l’environnement complet

```bash
docker-compose up --build
```

### 4. Créer un tenant

```bash
poetry run python scripts/create_tenant.py --id tenant_acme
```

### 5. Ingestion de données

Placer un CSV dans `ingestion/data/data.csv` puis :

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

## 📈 Interface BI avec Trino

Accéder à Trino : [http://localhost:8080](http://localhost:8080)  
Utiliser `tenant_acme` comme catalogue Trino dans Superset ou Metabase.

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
