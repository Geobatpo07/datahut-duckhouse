import os
import sys
import pyarrow as pa
import pyarrow.csv as csv
import duckdb
import subprocess
from dotenv import load_dotenv

# Réutilise la connexion Flight déjà vérifiée du CLI plutôt que de construire
# un FlightDescriptor à la main : le do_put réel du serveur attend un
# descripteur de type "command" (voir FlightServerDelegate.do_put dans le
# package xorq), pas for_path(...) -- ce script ne pouvait pas fonctionner
# contre le vrai serveur avant cette correction, indépendamment du paramètre
# `target` retiré par ADR-0001.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from datahut_duckhouse.connection import get_connection

# Charger les variables d'environnement depuis .env
load_dotenv()

# Paramètres de configuration
TABLE_NAME = os.getenv("FLIGHT_TABLE_NAME", "diseases")
CSV_PATH = os.getenv("CSV_PATH", "ingestion/data/data.csv")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "ingestion/data/duckhouse.duckdb")
DBT_PROJECT_PATH = "transform/dbt_project"
DBT_PROFILES_DIR = f"{DBT_PROJECT_PATH}/config"

def ingest_data():
    print("Étape 1 : Envoi des données au serveur Arrow Flight...")

    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"Fichier CSV introuvable : {CSV_PATH}")

    with open(CSV_PATH, "rb") as f:
        table = csv.read_csv(f)

    con = get_connection()
    # create_table et insert envoient le même appel côté client ; c'est le
    # serveur qui décide création vs ajout selon que la table existe déjà.
    con.create_table(TABLE_NAME, table)
    print(f"Données envoyées vers '{TABLE_NAME}' ({table.num_rows} lignes) dans Iceberg")

def run_dbt():
    print("Étape 2 : Exécution des transformations dbt...")
    result = subprocess.run(
        [
            "poetry", "run", "dbt", "run",
            "--project-dir", DBT_PROJECT_PATH,
            "--profiles-dir", DBT_PROFILES_DIR
        ],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print("Erreur dans dbt run :")
        print(result.stdout)
        print(result.stderr)
        raise RuntimeError("dbt run failed")
    print("dbt run exécuté avec succès")

def query_results():
    print("Étape 3 : Requête de validation dans DuckDB")
    if not os.path.exists(DUCKDB_PATH):
        print(f"Fichier DuckDB introuvable : {DUCKDB_PATH}")
        return
    conn = duckdb.connect(DUCKDB_PATH)
    try:
        df = conn.execute("SELECT * FROM mart_rev_metrics LIMIT 5").fetchdf()
        print("Résultat :")
        print(df)
    except Exception as e:
        print(f"Impossible de lire 'mart_rev_metrics' : {e}")

if __name__ == "__main__":
    # Iceberg est l'unique cible de persistance (ADR-0001) : plus de branche
    # "si target == duckdb" -- dbt tourne systématiquement sur les vues
    # DuckDB qui reflètent Iceberg.
    ingest_data()
    run_dbt()
    query_results()
