import os
import pyarrow as pa
import pyarrow.flight as flight

from utils import get_duckdb_connection

class DuckDBFlightServer(flight.FlightServerBase):
    def __init__(self, location="grpc://0.0.0.0:8815"):
        super().__init__(location)
        self.db = get_duckdb_connection()

    def do_put(self, context, descriptor, reader, writer):
        table_name = descriptor.path[0].decode("utf-8")
        print(f"Réception des données pour la table : {table_name}")

        # Lire les données envoyées
        table = reader.read_all()
        df = table.to_pandas()

        # Insérer dans DuckDB (remplace si existe)
        self.db.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        print(f"Table '{table_name}' créée avec {len(df)} lignes.")

        writer.done_writing()

    def list_flights(self, context, criteria):
        # Exemple basique de réponse (à enrichir plus tard)
        return [
            flight.FlightInfo(
                schema=pa.schema([("example", pa.int64())]),
                descriptor=flight.FlightDescriptor.for_path("example"),
                endpoints=[],
                total_records=0,
                total_bytes=0
            )
        ]

if __name__ == "__main__":
    location = f"grpc://0.0.0.0:{os.getenv('FLIGHT_SERVER_PORT', '8815')}"
    server = DuckDBFlightServer(location=location)
    print(f"Serveur Arrow Flight en cours d'exécution sur {location}")
    server.serve()
