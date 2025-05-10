# 🏠 DataHut-DuckHouse

**DataHut-DuckHouse** est une plateforme analytique légère, hybride et open source qui combine la simplicité de **DuckDB**, la scalabilité d'**Iceberg**, la rapidité d'**Arrow Flight** et la modularité de **dbt** pour créer un data stack moderne, local ou cloud-ready.

## Architecture

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
          |        (app.py)              |
          | - stocke dans DuckDB         |
          | - crée vues ou tables        |
          +--------------+---------------+
                         |
            +------------+-----------+
            |        DuckDB          |
            |   (fichier .duckdb)    |
            +------------+-----------+
                         |
                         v
                  +-------------+
                  |     dbt     |
                  | Transformations |
                  +-------------+
```

## Fonctionnalités

- 🔗 Ingestion de données via Arrow Flight
- 🐤 Stockage hybride avec **DuckDB** (et Iceberg à venir)
- 📊 Transformations SQL déclaratives avec **dbt**
- 📦 Environnement reproductible avec **Poetry**
- ☁️ Compatible avec S3/MinIO pour le stockage objet

## ⚙️ Prérequis

- [Python 3.11](https://www.python.org/downloads/)
- [Poetry](https://python-poetry.org/docs/)
- [Docker](https://www.docker.com/)
- [dbt CLI](https://docs.getdbt.com/dbt-cli/installation)

## Installation

### 1. Cloner le dépôt

```bash
git clone https://github.com/ton-utilisateur/datahut-duckhouse.git
cd datahut-duckhouse
```

### 2. Installer les dépendances

```bash
poetry install
```

### 3. Lancer les services (MinIO + Flight Server)

```bash
docker-compose up --build
```

### 4. Ingestion de données

Placer un fichier CSV dans `ingestion/data/data.csv`, puis :

```bash
poetry run python scripts/ingest_flight.py
```

## Structure du projet

```
datahut-duckhouse/
├── flight_server/       # Serveur Arrow Flight avec backend DuckDB
│   ├── app.py
│   └── utils.py
├── ingestion/           # Données sources
│   └── data/
├── scripts/             # Scripts Python (ingestion, requêtes)
│   ├── ingest_flight.py
│   └── query_duckdb.py
├── transform/           # Projet dbt
│   └── dbt_project/
├── config/              # Fichier de configuration dbt
│   └── dbt_profiles.yml
├── .env                 # Variables d’environnement
├── pyproject.toml       # Dépendances Poetry
└── docker-compose.yml   # Conteneurs MinIO + Flight
```

## 🧠 Utilisation de dbt

```bash
export DBT_PROFILES_DIR=transform/dbt_project/config
cd transform/dbt_project
poetry run dbt run
```

## Exemple de requête locale

```bash
poetry run python scripts/query_duckdb.py
```

## Prochaines améliorations

- ❄️ Intégration du backend Iceberg
- 🧪 Ajout de tests dbt (`dbt test`)
- 📈 Dashboards interactifs avec DuckDB + Observable ou Streamlit
- ☁️ Intégration BigQuery / Snowflake via Arrow Flight

## Licence

Ce projet est sous licence MIT.

## Auteur

Geovany Batista Polo LAGUERRE – lgeobatpo98@gmail.com  
Data Science & Analytics Engineer
