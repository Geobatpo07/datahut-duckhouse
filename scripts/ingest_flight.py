import os
import sys
from dotenv import load_dotenv
import pyarrow.csv as csv

# Réutilise la connexion Flight déjà vérifiée du CLI (datahut_duckhouse/connection.py)
# plutôt qu'une construction manuelle : xorq.flight.client.FlightClient attend
# host=/port=, pas une URL "grpc://..." unique, et son API réelle n'a pas de
# méthode upload_data(target=...) -- voir ADR-0001, note post-implémentation.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from datahut_duckhouse.connection import get_connection

# Charger les variables d'environnement
load_dotenv()

# Configuration
CSV_PATH = os.getenv("CSV_PATH", "ingestion/data/data.csv")
TABLE_NAME = os.getenv("FLIGHT_TABLE_NAME", "diseases")

def main():
    # Lecture du fichier CSV
    if not os.path.exists(CSV_PATH):
        print(f"Fichier CSV introuvable : {CSV_PATH}")
        return

    print(f"Chargement du fichier CSV : {CSV_PATH}")
    with open(CSV_PATH, "rb") as f:
        table = csv.read_csv(f)

    print(f"{table.num_rows} lignes lues. Connexion au serveur Flight...")
    con = get_connection()

    # create_table et insert envoient le même appel côté client (upload_table) ;
    # c'est le serveur qui décide création vs ajout selon que la table existe
    # déjà (voir FlightServerDelegate.do_put dans le package xorq, et
    # datahut_duckhouse/cli.py qui suit exactement le même principe).
    con.create_table(TABLE_NAME, table)

    print(f"Données envoyées avec succès à la table : {TABLE_NAME}")

if __name__ == "__main__":
    main()
