import os
import duckdb
import s3fs
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog

import xorq as xo
from xorq.flight.client import FlightClient
from xorq.vendor.ibis.backends.duckdb import Backend as DuckDBBackend
from xorq.vendor.ibis.backends.pyiceberg import Backend as IcebergBackend

# Charger les variables d'environnement
load_dotenv()

# --------------- DuckDB

def get_duckdb_path():
    return os.getenv(
        "DUCKDB_PATH",
        os.path.join(os.getcwd(), "ingestion", "data", "duckhouse.duckdb")
    )

def get_duckdb_connection():
    db_path = get_duckdb_path()
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    print(f"Connexion à DuckDB : {db_path}")
    return duckdb.connect(db_path)

# --------------- S3 / MinIO

def get_s3_filesystem():
    return s3fs.S3FileSystem(
        key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
        client_kwargs={"endpoint_url": os.getenv("S3_ENDPOINT", "http://localhost:9000")},
    )

# --------------- Iceberg

def get_iceberg_catalog():
    return load_catalog(
        name=os.getenv("ICEBERG_CATALOG", "minio_catalog"),
        uri=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        warehouse=os.getenv("ICEBERG_WAREHOUSE", "s3://duckhouse-warehouse/"),
        s3={"s3fs": get_s3_filesystem()}
    )

def get_iceberg_namespace():
    return os.getenv("ICEBERG_NAMESPACE", "default")

def get_iceberg_warehouse_path():
    return os.getenv("ICEBERG_WAREHOUSE", "s3://duckhouse-warehouse/")

# --------------- Xorq-specific: Flight + Backends

def get_flight_client() -> FlightClient:
    host = os.getenv("FLIGHT_SERVER_HOST", "localhost")
    port = os.getenv("FLIGHT_SERVER_PORT", "8815")
    return FlightClient(f"grpc://{host}:{port}")

def get_duckdb_backend() -> DuckDBBackend:
    return DuckDBBackend(duckdb_path=get_duckdb_path())

def get_iceberg_backend() -> IcebergBackend:
    return IcebergBackend(
        catalog_name=os.getenv("ICEBERG_CATALOG", "minio_catalog"),
        warehouse=get_iceberg_warehouse_path(),
        endpoint=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        access_key=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        secret_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
    )
