import os
from dotenv import load_dotenv
import pyarrow.csv as csv
from xorq.flight.client import FlightClient

# Charger les variables d'environnement
load_dotenv()

# Configuration
FLIGHT_SERVER_HOST = os.getenv("FLIGHT_SERVER_HOST", "localhost")
FLIGHT_SERVER_PORT = os.getenv("FLIGHT_SERVER_PORT", "8815")
CSV_PATH = os.getenv("CSV_PATH", "ingestion/data/data.csv")
TABLE_NAME = os.getenv("FLIGHT_TABLE_NAME", "diseases")
FLIGHT_TARGET = os.getenv("FLIGHT_TARGET", "duckdb")

def main():
    endpoint = f"grpc://{FLIGHT_SERVER_HOST}:{FLIGHT_SERVER_PORT}"
    print(f"Connexion au serveur Xorq Arrow Flight : {endpoint}")
    client = FlightClient(endpoint)

    # Lecture du fichier CSV
    if not os.path.exists(CSV_PATH):
        print(f"Fichier CSV introuvable : {CSV_PATH}")
        return

    print(f"Chargement du fichier CSV : {CSV_PATH}")
    with open(CSV_PATH, "rb") as f:
        table = csv.read_csv(f)

    print(f"{table.num_rows} lignes lues. Envoi vers backend : {FLIGHT_TARGET.upper()}")

    # Envoi via client Xorq
    client.upload_data(
        key=TABLE_NAME,
        data=table,
        target=FLIGHT_TARGET
    )

    print(f"Données envoyées avec succès à la table : {TABLE_NAME}")

if __name__ == "__main__":
    main()
