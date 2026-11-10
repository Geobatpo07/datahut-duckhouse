"""ADR-0006, decision 7 / condition C2: a report of storage objects that
are not referenced by ANY current Nessie reference (branch or tag).

Catches what ``nessie-gc`` structurally cannot (confirmed live --
docs/spikes/NESSIE_SPIKES_2.md, T2): ``nessie-gc`` builds its live/dead
bookkeeping by walking commit history across *currently named*
references; once a branch is deleted, its commits (and the files they
point to) simply fall out of that bookkeeping entirely -- they are never
considered, so never swept, no matter how old. This script instead lists
objects that physically exist in storage (via boto3, independent of
Nessie's own bookkeeping) and compares them against every file reachable
from the full snapshot history of every table on every CURRENT branch/
tag -- a deleted branch's files still show up in storage, and since
nothing current references them any more, they show up here as orphans.

Report-only by default -- prints what it found and exits. ``--delete``
(plus a confirmation prompt, or ``--yes`` to skip it for scripted use)
actually removes the found objects from storage.

    uv run python scripts/orphan_sweep.py              # report only
    uv run python scripts/orphan_sweep.py --delete      # asks, then deletes
    uv run python scripts/orphan_sweep.py --delete --yes

Needs deployment mode (ICEBERG_CATALOG_TYPE=rest) and the usual
S3_ENDPOINT/AWS_*/ICEBERG_WAREHOUSE env vars -- run it the same way any
other deployment-mode script in this project runs (inside the Docker
network; the workstation rule applies here too, since this reads actual
data files).
"""

from __future__ import annotations

import argparse
import os
from urllib.parse import urlparse

from iceberg_catalog import get_catalog
from iceberg_catalog.versioning import list_references


def _s3_client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def _warehouse_bucket_and_prefix() -> tuple[str, str]:
    warehouse = os.environ["ICEBERG_WAREHOUSE"]
    parsed = urlparse(warehouse)
    return parsed.netloc, parsed.path.lstrip("/")


def _normalize(path: str) -> str:
    """pyiceberg's ``file_path`` values may use ``s3a://`` -- normalize
    to ``s3://`` so they compare equal to boto3's own listing."""
    return path.replace("s3a://", "s3://", 1)


def list_storage_objects() -> dict[str, int]:
    """``{s3://bucket/key: size_bytes}`` for every object under the
    warehouse prefix."""
    bucket, prefix = _warehouse_bucket_and_prefix()
    client = _s3_client()
    objects: dict[str, int] = {}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects[f"s3://{bucket}/{obj['Key']}"] = obj["Size"]
    return objects


def referenced_files() -> set[str]:
    """Every file (data, manifest, manifest list, metadata.json, and
    prior metadata.json's from the metadata log) reachable from the full
    snapshot history of every table on every CURRENT branch/tag."""
    referenced: set[str] = set()
    for ref in list_references():
        catalog = get_catalog(catalog_branch=ref.name)
        for namespace in catalog.list_namespaces():
            for _, table_name in catalog.list_tables(namespace[0]):
                table = catalog.load_table(f"{namespace[0]}.{table_name}")
                referenced.add(_normalize(table.metadata_location))
                for entry in table.metadata.metadata_log:
                    referenced.add(_normalize(entry.metadata_file))
                for snapshot in table.snapshots():
                    if snapshot.manifest_list:
                        referenced.add(_normalize(snapshot.manifest_list))
                    for manifest in snapshot.manifests(table.io):
                        referenced.add(_normalize(manifest.manifest_path))
                        for manifest_entry in manifest.fetch_manifest_entry(table.io):
                            referenced.add(
                                _normalize(manifest_entry.data_file.file_path)
                            )
    return referenced


def find_orphans() -> dict[str, int]:
    storage = list_storage_objects()
    referenced = referenced_files()
    return {path: size for path, size in storage.items() if path not in referenced}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--delete", action="store_true", help="Delete the found orphans."
    )
    parser.add_argument(
        "--yes", action="store_true", help="Skip the --delete confirmation prompt."
    )
    args = parser.parse_args()

    orphans = find_orphans()
    total_bytes = sum(orphans.values())
    print(f"{len(orphans)} orphan object(s), {total_bytes} byte(s) total.")
    for path, size in sorted(orphans.items()):
        print(f"  {path}  ({size} bytes)")

    if not orphans:
        return
    if not args.delete:
        print("\nReport only -- pass --delete to remove these.")
        return

    if not args.yes:
        reply = input(
            f"\nDelete {len(orphans)} object(s) ({total_bytes} bytes)? [y/N] "
        )
        if reply.strip().lower() != "y":
            print("Aborted.")
            return

    bucket, _ = _warehouse_bucket_and_prefix()
    client = _s3_client()
    prefix_len = len(f"s3://{bucket}/")
    keys = [{"Key": path[prefix_len:]} for path in orphans]
    for i in range(0, len(keys), 1000):  # S3 DeleteObjects caps at 1000/call
        client.delete_objects(Bucket=bucket, Delete={"Objects": keys[i : i + 1000]})
    print(f"Deleted {len(orphans)} object(s).")


if __name__ == "__main__":
    main()
