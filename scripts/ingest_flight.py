import os
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.flight as flight
from dotenv import load_dotenv

# Charger les variables depuis .env
load_dotenv()

# Configuration depuis l'environnement
FLIGHT_SERVER_HOST = os.getenv("FLIGHT_SERVER_HOST", "localhost")
FLIGHT_SERVER_PORT = os.getenv("FLIGHT_SERVER_PORT", "8815")
TABLE_NAME = os.getenv("FLIGHT_TABLE_NAME", "diseases")
CSV_PATH = os.getenv("CSV_PATH", "ingestion/data/data.csv")

def main():
    endpoint = f"grpc://{FLIGHT_SERVER_HOST}:{FLIGHT_SERVER_PORT}"
    print(f"Connexion au serveur Arrow Flight : {endpoint}")
    client = flight.FlightClient(endpoint)

    # Lecture du fichier CSV
    if not os.path.exists(CSV_PATH):
        print(f"Fichier CSV non trouvé : {CSV_PATH}")
        return

    print(f"Chargement du fichier CSV : {CSV_PATH}")
    with open(CSV_PATH, "rb") as f:
        table = csv.read_csv(f)

    print(f"Données prêtes : {table.num_rows} lignes, {table.num_columns} colonnes")

    # Descripteur pour la table cible
    descriptor = flight.FlightDescriptor.for_path(TABLE_NAME)

    # Envoi des données
    writer, _ = client.do_put(descriptor, table.schema)
    writer.write_table(table)
    writer.done_writing()

    print(f"Données envoyées avec succès vers la table '{TABLE_NAME}'")

if __name__ == "__main__":
    main()
