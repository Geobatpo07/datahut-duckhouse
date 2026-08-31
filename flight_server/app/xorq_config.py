import os
import logging

from xorq.flight import FlightServer, FlightUrl

from .backends.hybrid_backend import HybridBackend
from .utils import get_duckdb_path, get_iceberg_warehouse_path, get_iceberg_namespace

logger = logging.getLogger(__name__)


def make_hybrid_connection() -> HybridBackend:
    """
    Construit et connecte l'unique backend du serveur Flight (ADR-0001) :
    HybridBackend, où Iceberg est la seule cible d'écriture et DuckDB ne fait
    que refléter les tables Iceberg via des vues.

    `ICEBERG_WAREHOUSE` (voir `.env.example`) peut être une URI MinIO/S3
    (`s3://...`, le défaut du projet) ou un chemin local — `HybridBackend`
    détecte le cas S3 et bascule sur son propre chemin de connexion au
    catalogue plutôt que sur celui, local uniquement, de la version de xorq
    réellement installée (voir la note post-implémentation de l'ADR-0001).
    """
    warehouse_path = get_iceberg_warehouse_path()
    duckdb_path = get_duckdb_path()
    logger.info(f"Connexion HybridBackend — warehouse={warehouse_path}, duckdb={duckdb_path}")

    backend = HybridBackend()
    backend.do_connect(
        warehouse_path=warehouse_path,
        duckdb_path=duckdb_path,
        namespace=get_iceberg_namespace(),
        catalog_name=os.getenv("ICEBERG_CATALOG", "default"),
        catalog_type=os.getenv("ICEBERG_CATALOG_TYPE", "sql"),
        s3_endpoint=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        s3_access_key=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        s3_secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
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
