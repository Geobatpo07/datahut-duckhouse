import os
import logging

from xorq.flight import FlightServer, FlightUrl

from .backends.hybrid_backend import HybridBackend
from .utils import get_duckdb_path

logger = logging.getLogger(__name__)


def _warehouse_location() -> str:
    """
    Emplacement du warehouse Iceberg.

    Priorité : ``ICEBERG_WAREHOUSE`` (typiquement une URI ``s3://…`` — MinIO en
    dev, S3 en prod) puis ``ICEBERG_WAREHOUSE_PATH`` (chemin local), puis un
    répertoire local par défaut. `HybridBackend.do_connect` détecte le schéma
    ``s3://`` et configure l'accès objet à partir des variables
    ``S3_ENDPOINT`` / ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY``.
    """
    return (
        os.getenv("ICEBERG_WAREHOUSE")
        or os.getenv("ICEBERG_WAREHOUSE_PATH")
        or os.path.join(os.getcwd(), "data", "iceberg_warehouse")
    )


def make_hybrid_connection() -> HybridBackend:
    """
    Construit et connecte l'unique backend du serveur Flight (ADR-0001) :
    HybridBackend, où Iceberg est la seule cible d'écriture et DuckDB ne fait
    que refléter les tables Iceberg.

    Le warehouse peut être local ou objet (``s3://`` / MinIO) : le câblage S3
    est porté par `HybridBackend`, qui lit l'endpoint et les identifiants dans
    l'environnement. Le catalogue reste un catalogue SQL — SQLite local tant
    qu'il n'y a qu'un writer, ``ICEBERG_CATALOG_URI`` pour un catalogue partagé
    en attendant Nessie (ADR-0002).
    """
    warehouse = _warehouse_location()
    duckdb_path = get_duckdb_path()
    logger.info(
        f"Connexion HybridBackend — warehouse={warehouse}, duckdb={duckdb_path}"
    )

    backend = HybridBackend()
    backend.do_connect(
        warehouse_path=warehouse,
        duckdb_path=duckdb_path,
        namespace=os.getenv("ICEBERG_NAMESPACE", "default"),
        catalog_name=os.getenv("ICEBERG_CATALOG", "default"),
        catalog_type=os.getenv("ICEBERG_CATALOG_TYPE", "sql"),
        catalog_uri=os.getenv("ICEBERG_CATALOG_URI"),
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
