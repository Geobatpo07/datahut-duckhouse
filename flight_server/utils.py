import os
import duckdb
from dotenv import load_dotenv

# Charger les variables d'environnement depuis .env
load_dotenv()

# Obtenir le chemin de la base DuckDB (défaut dans ingestion/data/)
def get_duckdb_path():
    return os.getenv(
        "DUCKDB_PATH",
        os.path.join(os.getcwd(), "ingestion", "data", "duckhouse.duckdb")
    )

# Ouvre ou crée une connexion DuckDB
def get_duckdb_connection():
    db_path = get_duckdb_path()

    # S'assurer que le dossier contenant le fichier existe
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    print(f"Connexion à DuckDB : {db_path}")
    return duckdb.connect(db_path)
