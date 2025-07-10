import os
import logging
from xorq.registry import registry
from xorq.flight.server import FlightServer
from xorq.flight.client import FlightClient
from xorq.vendor.ibis.backends.duckdb import Backend as DuckDBBackend
from xorq.vendor.ibis.backends.pyiceberg import Backend as PyIcebergBackend

from .utils import get_flight_client, get_duckdb_path, get_iceberg_warehouse_path
from .trino_client import get_trino_client

logger = logging.getLogger(__name__)

def register_backends():
    """
    Register separate backends for DuckDB (local) and Iceberg (distributed via Trino).
    Xorq will orchestrate between them based on data size and query complexity.
    """
    # Register DuckDB backend for local, lightweight queries
    duckdb_path = get_duckdb_path()
    logger.info(f"Registering DuckDB backend with path: {duckdb_path}")
    
    duckdb_backend = DuckDBBackend(duckdb_path=duckdb_path)
    registry.register("duckdb", duckdb_backend)
    logger.info("DuckDB backend registered for local queries")
    
    # Register Iceberg backend for distributed, large dataset queries
    warehouse_path = get_iceberg_warehouse_path()
    logger.info(f"Registering Iceberg backend with warehouse: {warehouse_path}")
    
    iceberg_backend = PyIcebergBackend(
        warehouse_path=warehouse_path,
        catalog_name="iceberg",
        namespace="default"
    )
    registry.register("iceberg", iceberg_backend)
    logger.info("Iceberg backend registered for distributed queries")
    
    # Set DuckDB as default for lightweight operations
    registry.register("default", duckdb_backend)
    logger.info("Backend registration completed")

def get_flight_server() -> FlightServer:
    """
    Create and return a FlightServer with properly configured backends.
    Xorq will route queries to appropriate backends based on data characteristics.
    """
    register_backends()
    client: FlightClient = get_flight_client()
    return FlightServer(client=client)
