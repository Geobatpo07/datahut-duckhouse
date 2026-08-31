import os
import logging
from typing import Optional

import duckdb
import s3fs
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog

from xorq.flight.client import FlightClient
from xorq.vendor.ibis.backends.duckdb import Backend as DuckDBBackend
from xorq.backends.pyiceberg import Backend as IcebergBackend

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Charger les variables d'environnement
load_dotenv()

# --------------- DuckDB

def get_duckdb_path() -> str:
    """Get DuckDB database path from environment or default location."""
    return os.getenv(
        "DUCKDB_PATH",
        os.path.join(os.getcwd(), "ingestion", "data", "duckhouse.duckdb")
    )

def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """Create and return a DuckDB connection."""
    try:
        db_path = get_duckdb_path()
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        logger.info(f"Connecting to DuckDB: {db_path}")
        return duckdb.connect(db_path)
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB: {e}")
        raise

# --------------- S3 / MinIO

def get_s3_filesystem() -> s3fs.S3FileSystem:
    """Create and return an S3 filesystem instance."""
    try:
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        if not aws_access_key or not aws_secret_key:
            raise ValueError("AWS credentials not found in environment variables")
        
        return s3fs.S3FileSystem(
            key=aws_access_key,
            secret=aws_secret_key,
            client_kwargs={"endpoint_url": os.getenv("S3_ENDPOINT", "http://localhost:9000")},
        )
    except Exception as e:
        logger.error(f"Failed to create S3 filesystem: {e}")
        raise

# --------------- Iceberg

def get_iceberg_catalog():
    """Create and return an Iceberg catalog instance (MinIO/S3-backed).

    Utilise les clés de propriété réelles de pyiceberg (`s3.endpoint`,
    `s3.access-key-id`, `s3.secret-access-key`) plutôt qu'un objet `s3fs`
    imbriqué, qui n'est pas un format reconnu par `load_catalog`. Le
    catalogue lui-même (métadonnées) reste dans un fichier SQLite local ;
    seules les données des tables vont sur MinIO — même approche que
    `HybridBackend._connect_s3_iceberg` (voir `backends/hybrid_backend.py`,
    ADR-0001).
    """
    try:
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")
        metadata_dir = os.path.join(os.getcwd(), "data", "iceberg_catalog")
        os.makedirs(metadata_dir, exist_ok=True)
        return load_catalog(
            os.getenv("ICEBERG_CATALOG", "minio_catalog"),
            **{
                "type": "sql",
                "uri": f"sqlite:///{os.path.join(metadata_dir, 'pyiceberg_catalog.db')}",
                "warehouse": get_iceberg_warehouse_path(),
                "s3.endpoint": os.getenv("S3_ENDPOINT", "http://localhost:9000"),
                "s3.access-key-id": aws_access_key,
                "s3.secret-access-key": aws_secret_key,
                "s3.force-virtual-addressing": "false",
            }
        )
    except Exception as e:
        logger.error(f"Failed to create Iceberg catalog: {e}")
        raise

def get_iceberg_namespace() -> str:
    """Get Iceberg namespace from environment or default."""
    return os.getenv("ICEBERG_NAMESPACE", "default")

def get_iceberg_warehouse_path() -> str:
    """Get Iceberg warehouse path from environment or default."""
    return os.getenv("ICEBERG_WAREHOUSE", "s3://duckhouse-warehouse/")

# --------------- Xorq-specific: Flight + Backends

def get_flight_client() -> FlightClient:
    """Create and return a Flight client instance."""
    try:
        host = os.getenv("FLIGHT_SERVER_HOST", "localhost")
        port = int(os.getenv("FLIGHT_SERVER_PORT", "8815"))
        logger.info(f"Creating Flight client for {host}:{port}")
        return FlightClient(host=host, port=port)
    except Exception as e:
        logger.error(f"Failed to create Flight client: {e}")
        raise

def get_duckdb_backend() -> DuckDBBackend:
    """Create and return a connected DuckDB backend instance."""
    try:
        backend = DuckDBBackend()
        backend.do_connect(database=get_duckdb_path())
        return backend
    except Exception as e:
        logger.error(f"Failed to create DuckDB backend: {e}")
        raise

def get_iceberg_backend() -> IcebergBackend:
    """
    Create and return a connected Iceberg backend instance (Iceberg seul, sans
    la couche DuckDB — pour un accès en lecture directe si besoin).

    Note : contrairement à `HybridBackend` (voir `backends/hybrid_backend.py`,
    qui gère désormais un entrepôt MinIO/S3 via `_connect_s3_iceberg`), ce
    `IcebergBackend` brut appelle directement `do_connect` de xorq, qui ne
    supporte qu'un chemin de fichier local pour `warehouse_path` (voir la
    note post-implémentation de l'ADR-0001). Utiliser `get_iceberg_catalog()`
    ci-dessus, ou `HybridBackend`, si un accès MinIO/S3 est nécessaire.
    """
    try:
        backend = IcebergBackend()
        backend.do_connect(
            warehouse_path=os.getenv(
                "ICEBERG_LOCAL_WAREHOUSE_PATH",
                os.path.join(os.getcwd(), "data", "iceberg_warehouse"),
            ),
            namespace=get_iceberg_namespace(),
            catalog_name=os.getenv("ICEBERG_CATALOG", "default"),
        )
        return backend
    except Exception as e:
        logger.error(f"Failed to create Iceberg backend: {e}")
        raise

# --------------- Configuration Validation

def validate_environment() -> None:
    """Validate that all required environment variables are set."""
    required_vars = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "S3_ENDPOINT",
        "ICEBERG_WAREHOUSE",
        "FLIGHT_SERVER_HOST",
        "FLIGHT_SERVER_PORT"
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )
    
    logger.info("Environment validation passed")
