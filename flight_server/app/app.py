import os
import pyarrow.flight as flight
from dotenv import load_dotenv

# Import des utilitaires
from .utils import get_duckdb_connection
from flight_server.app.backends.iceberg_backend import write_arrow_to_iceberg

load_dotenv()

class DuckHouseFlightServer(flight.FlightServerBase):
    def __init__(self, location="grpc://0.0.0.0:8815"):
        super().__init__(location)
        self.db = get_duckdb_connection()

    def do_put(self, context, descriptor, reader, writer):
        table_name = descriptor.path[0].decode("utf-8")
        print(f"🟡 Réception des données pour la table : {table_name}")

        arrow_table = reader.read_all()

        # Récupérer les métadonnées (ex. : target=iceberg ou duckdb)
        metadata = dict(context.metadata()) if context.metadata() else {}
        target = metadata.get("target", "duckdb").lower()

        if target == "iceberg":
            print("Écriture vers Iceberg")
            write_arrow_to_iceberg(table_name, arrow_table)
        else:
            print("Écriture vers DuckDB")
            df = arrow_table.to_pandas()
            self.db.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")

        print(f"Table '{table_name}' écrite avec succès dans {target.upper()}")

    def list_flights(self, context, criteria):
        return []

if __name__ == "__main__":
    location = f"grpc://0.0.0.0:{os.getenv('FLIGHT_SERVER_PORT', '8815')}"
    server = DuckHouseFlightServer(location=location)
    print(f"Serveur Arrow Flight en cours d'exécution sur {location}")
    server.serve()
