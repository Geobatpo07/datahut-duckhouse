import os
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.flight as flight
import duckdb
import subprocess
from dotenv import load_dotenv

# Charger les variables d'environnement depuis .env
load_dotenv()

# Paramètres de configuration
FLIGHT_SERVER_HOST = os.getenv("FLIGHT_SERVER_HOST", "localhost")
FLIGHT_SERVER_PORT = os.getenv("FLIGHT_SERVER_PORT", "8815")
TABLE_NAME = os.getenv("FLIGHT_TABLE_NAME", "diseases")
CSV_PATH = os.getenv("CSV_PATH", "ingestion/data/data.csv")
FLIGHT_TARGET = os.getenv("FLIGHT_TARGET", "duckdb")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "ingestion/data/duckhouse.duckdb")
DBT_PROJECT_PATH = "transform/dbt_project"
DBT_PROFILES_DIR = f"{DBT_PROJECT_PATH}/config"

def ingest_data():
    print("Étape 1 : Envoi des données au serveur Arrow Flight...")
    client = flight.FlightClient(f"grpc://{FLIGHT_SERVER_HOST}:{FLIGHT_SERVER_PORT}")

    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"Fichier CSV introuvable : {CSV_PATH}")

    with open(CSV_PATH, "rb") as f:
        table = csv.read_csv(f)

    descriptor = flight.FlightDescriptor.for_path(TABLE_NAME)
    options = flight.FlightCallOptions(headers=[("target", FLIGHT_TARGET)])

    writer, _ = client.do_put(descriptor, table.schema, options=options)
    writer.write_table(table)
    writer.done_writing()
    print(f"Données envoyées vers '{TABLE_NAME}' ({table.num_rows} lignes) dans {FLIGHT_TARGET.upper()}")

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
    ingest_data()
    if FLIGHT_TARGET.lower() == "duckdb":
        run_dbt()
        query_results()
    else:
        print("Données envoyées dans Iceberg : dbt/queries ignorées.")
