import os
import argparse
from pathlib import Path
from dotenv import load_dotenv
import boto3
from pyiceberg.catalog import load_catalog
import xorq.registry as registry
from flight_server.app.backends.hybrid_backend import HybridBackend

# Charger les variables d'environnement
load_dotenv()

TRINO_CATALOG_DIR = Path("config/trino/etc/catalog")
DEFAULT_S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
DEFAULT_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
DEFAULT_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")

TRINO_CATALOG_TEMPLATE = """connector.name=iceberg
iceberg.catalog.type=hadoop
iceberg.catalog.warehouse=s3a://{warehouse}
iceberg.file-format=parquet
hive.metastore=catalog
hive.s3.aws-access-key={access_key}
hive.s3.aws-secret-key={secret_key}
hive.s3.endpoint={s3_endpoint}
hive.s3.path-style-access=true
"""

def create_catalog_file(tenant_id: str, warehouse: str):
    TRINO_CATALOG_DIR.mkdir(parents=True, exist_ok=True)
    path = TRINO_CATALOG_DIR / f"tenant_{tenant_id}.properties"

    content = TRINO_CATALOG_TEMPLATE.format(
        warehouse=warehouse,
        access_key=DEFAULT_ACCESS_KEY,
        secret_key=DEFAULT_SECRET_KEY,
        s3_endpoint=DEFAULT_S3_ENDPOINT
    )

    with open(path, "w") as f:
        f.write(content)

    print(f"Fichier Trino catalog généré : {path}")

def create_namespace_in_iceberg(tenant_id: str, warehouse: str):
    catalog = load_catalog(
        name="default",
        uri=DEFAULT_S3_ENDPOINT,
        warehouse=f"s3://{warehouse}",
        s3={"s3fs": boto3.resource("s3")}
    )
    if tenant_id not in catalog.list_namespaces():
        catalog.create_namespace(tenant_id)
        print(f"Namespace Iceberg '{tenant_id}' créé.")
    else:
        print(f"Namespace Iceberg '{tenant_id}' existe déjà.")

def register_backend(tenant_id: str, warehouse: str):
    duckdb_path = os.path.join("ingestion", "data", f"{tenant_id}.duckdb")
    snapshot_dir = os.path.join("snapshots", tenant_id)

    backend = HybridBackend(warehouse_path=f"s3://{warehouse}")
    backend.do_connect(
        warehouse_path=f"s3://{warehouse}",
        duckdb_path=duckdb_path,
        snapshot_dir=snapshot_dir,
        namespace=tenant_id,
        catalog_name=tenant_id,
        catalog_type="sql"
    )

    registry.register(tenant_id, backend)
    print(f"Backend Xorq enregistré sous le nom '{tenant_id}'")

def create_minio_bucket_path(warehouse: str):
    bucket_name = warehouse.split("/")[0]
    s3 = boto3.client(
        "s3",
        aws_access_key_id=DEFAULT_ACCESS_KEY,
        aws_secret_access_key=DEFAULT_SECRET_KEY,
        endpoint_url=DEFAULT_S3_ENDPOINT,
    )
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' déjà existant.")
    except s3.exceptions.ClientError:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket MinIO '{bucket_name}' créé.")

def main():
    parser = argparse.ArgumentParser(description="Créer un nouveau tenant analytique.")
    parser.add_argument("--id", required=True, help="Identifiant du tenant (ex: tenant_acme)")
    parser.add_argument("--warehouse", help="Chemin S3 du warehouse (ex: tenant-acme)")
    args = parser.parse_args()

    tenant_id = args.id.strip().lower()
    warehouse = args.warehouse or f"tenant-{tenant_id}"

    print(f"\nCréation du tenant : {tenant_id}\n")

    create_catalog_file(tenant_id, warehouse)
    create_minio_bucket_path(warehouse)
    create_namespace_in_iceberg(tenant_id, warehouse)
    register_backend(tenant_id, warehouse)

    print(f"\nTenant '{tenant_id}' initialisé avec succès.")

if __name__ == "__main__":
    main()
