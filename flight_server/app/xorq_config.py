from xorq.registry import registry
from xorq.flight.server import FlightServer
from xorq.flight.client import FlightClient
from flight_server.backends.hybrid_backend import HybridBackend

from utils import get_flight_client, get_duckdb_path, get_iceberg_warehouse_path

def register_backends():
    """
    Enregistre le backend hybride combinant Iceberg et DuckDB.
    """
    duckdb_path = get_duckdb_path()
    warehouse_path = get_iceberg_warehouse_path()

    backend = HybridBackend(warehouse_path=warehouse_path)
    backend.do_connect(
        warehouse_path=warehouse_path,
        duckdb_path=duckdb_path,
        snapshot_dir=f"{warehouse_path}/snapshots",
        catalog_name="default",
        namespace="default"
    )

    # Tu peux enregistrer sous "hybrid" ou directement "default"
    registry.register("default", backend)
    print("Backend hybride 'default' enregistré")

def get_flight_server() -> FlightServer:
    """
    Instancie et retourne un FlightServer Xorq prêt à être démarré.
    """
    register_backends()
    client: FlightClient = get_flight_client()
    return FlightServer(client=client)
