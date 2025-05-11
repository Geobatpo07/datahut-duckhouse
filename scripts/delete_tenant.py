import os
import argparse
from pathlib import Path
import boto3
from dotenv import load_dotenv
import xorq.registry as registry

# Charger les variables d’environnement
load_dotenv()

# Répertoires
TRINO_CATALOG_DIR = Path("config/trino/etc/catalog")
INGESTION_DIR = Path("ingestion/data")
SNAPSHOT_ROOT = Path("snapshots")

# Accès S3 / MinIO
DEFAULT_S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
DEFAULT_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
DEFAULT_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")


def delete_catalog_file(tenant_id: str):
    path = TRINO_CATALOG_DIR / f"tenant_{tenant_id}.properties"
    if path.exists():
        path.unlink()
        print(f"Fichier Trino supprimé : {path}")
    else:
        print(f"Aucun fichier Trino trouvé pour le tenant : {tenant_id}")

def unregister_backend(tenant_id: str):
    if tenant_id in registry.backends:
        del registry.backends[tenant_id]
        print(f"Backend Xorq supprimé : {tenant_id}")
    else:
        print(f"Aucun backend Xorq enregistré sous : {tenant_id}")

def delete_duckdb_files(tenant_id: str):
    duckdb_file = INGESTION_DIR / f"{tenant_id}.duckdb"
    if duckdb_file.exists():
        duckdb_file.unlink()
        print(f"Fichier DuckDB supprimé : {duckdb_file}")

    snapshot_path = SNAPSHOT_ROOT / tenant_id
    if snapshot_path.exists():
        for file in snapshot_path.glob("*.duckdb"):
            file.unlink()
        snapshot_path.rmdir()
        print(f"Snapshots supprimés : {snapshot_path}")

def delete_minio_bucket(warehouse: str):
    bucket = warehouse.split("/")[0]
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=DEFAULT_ACCESS_KEY,
        aws_secret_access_key=DEFAULT_SECRET_KEY,
        endpoint_url=DEFAULT_S3_ENDPOINT,
    )

    try:
        bucket_obj = s3.Bucket(bucket)
        for obj in bucket_obj.objects.all():
            obj.delete()
        bucket_obj.delete()
        print(f"Bucket supprimé sur MinIO : {bucket}")
    except Exception as e:
        print(f"Échec suppression bucket ou déjà supprimé : {bucket} ({e})")

def main():
    parser = argparse.ArgumentParser(description="Supprimer un tenant et ses composants.")
    parser.add_argument("--id", required=True, help="Identifiant du tenant")
    parser.add_argument("--warehouse", help="Nom du bucket S3 (ex: tenant-acme)")

    args = parser.parse_args()
    tenant_id = args.id.strip().lower()
    warehouse = args.warehouse or f"tenant-{tenant_id}"

    print(f"\nSuppression du tenant : {tenant_id}\n")

    delete_catalog_file(tenant_id)
    unregister_backend(tenant_id)
    delete_duckdb_files(tenant_id)
    delete_minio_bucket(warehouse)

    print(f"\nTenant '{tenant_id}' supprimé avec succès.")

if __name__ == "__main__":
    main()
