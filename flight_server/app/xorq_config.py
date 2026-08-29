import os
import logging

from xorq.flight import FlightServer, FlightUrl

from .backends.hybrid_backend import HybridBackend
from .utils import get_duckdb_path

logger = logging.getLogger(__name__)


def make_hybrid_connection() -> HybridBackend:
    """
    Construit et connecte l'unique backend du serveur Flight (ADR-0001) :
    HybridBackend, où Iceberg est la seule cible d'écriture et DuckDB ne fait
    que refléter les tables Iceberg via des vues.

    Note : `warehouse_path` est ici un chemin de fichier local, pas une URI S3
    (`s3://...`) — c'est ce qu'attend `PyIcebergBackend.do_connect` dans la
    version de xorq réellement installée (elle fait `Path(warehouse_path)`).
    Le branchement effectif sur MinIO/S3 (`ICEBERG_WAREHOUSE`,
    `get_s3_filesystem` dans utils.py) n'est pas encore câblé à ce backend —
    à traiter comme un point ouvert distinct, pas résolu par ce changement.
    """
    warehouse_path = os.getenv(
        "ICEBERG_WAREHOUSE_PATH",
        os.path.join(os.getcwd(), "data", "iceberg_warehouse"),
    )
    duckdb_path = get_duckdb_path()
    logger.info(f"Connexion HybridBackend — warehouse={warehouse_path}, duckdb={duckdb_path}")

    backend = HybridBackend()
    backend.do_connect(
        warehouse_path=warehouse_path,
        duckdb_path=duckdb_path,
        namespace=os.getenv("ICEBERG_NAMESPACE", "default"),
        catalog_name=os.getenv("ICEBERG_CATALOG", "default"),
        catalog_type=os.getenv("ICEBERG_CATALOG_TYPE", "sql"),
    )
    return backend


def get_flight_server() -> FlightServer:
    """
    Crée le serveur Flight avec HybridBackend comme unique backend (ADR-0001).
    do_get/do_put sont fournis génériquement par xorq.flight.FlightServerDelegate,
    qui délègue à la connexion retournée par `make_connection` — pas besoin
    d'implémenter do_get/list_flights/get_flight_info à la main.
    """
    host = os.getenv("FLIGHT_SERVER_HOST", "0.0.0.0")
    port_env = os.getenv("FLIGHT_SERVER_PORT")
    flight_url = FlightUrl(host=host, port=int(port_env) if port_env else None)

    return FlightServer(flight_url=flight_url, make_connection=make_hybrid_connection)
