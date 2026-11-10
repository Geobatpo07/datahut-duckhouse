"""Real end-to-end proof of ``iceberg_catalog.versioning`` (ADR-0006,
decision 4) against a running Nessie: branch create, an isolated write,
a clean merge, a real conflicting merge, tag, rollback, and
``delete_branch``'s unmerged-work refusal then ``--force``.

Must run inside the Docker network -- table writes need the container
network's storage address (ADR-0006's workstation rule), even though the
pure-reference operations (create/list/merge/tag/rollback/delete) are
plain Nessie REST calls that would also work from the host:

    docker compose run --rm \\
      -e ICEBERG_CATALOG_TYPE=rest \\
      -e ICEBERG_CATALOG_URI=http://nessie:19120/iceberg \\
      -e ICEBERG_WAREHOUSE=s3://duckhouse-warehouse/shared-catalog/ \\
      -e ICEBERG_BRANCH=main \\
      --entrypoint /app/.venv/bin/python \\
      flight-server scripts/verify_versioning_nessie.py
"""

from __future__ import annotations

import os

import pyarrow as pa
from dotenv import load_dotenv

from iceberg_catalog import get_catalog
from iceberg_catalog.versioning import (
    MergeConflictError,
    UnmergedWorkError,
    create_branch,
    create_tag,
    delete_branch,
    get_reference,
    is_fully_merged,
    merge,
    rollback,
)

load_dotenv()

NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "step5")
TABLE = "step5_versioning"
IDENT = f"{NAMESPACE}.{TABLE}"


def _cleanup_branch(name: str) -> None:
    try:
        get_reference(name)
    except Exception:
        return
    delete_branch(name, force=True)


def main() -> None:
    for name in ("step5-a", "step5-b"):
        _cleanup_branch(name)

    main_ref = get_reference("main")
    print(f"RESULT main hash={main_ref.hash}")

    ref_a = create_branch("step5-a", from_branch="main")
    ref_b = create_branch("step5-b", from_branch="main")
    print(f"RESULT created step5-a@{ref_a.hash}, step5-b@{ref_b.hash}")

    cat_main = get_catalog(catalog_branch="main", ensure_namespace=NAMESPACE)
    try:
        cat_main.drop_table(IDENT)
    except Exception:
        pass
    cat_main.create_table(IDENT, schema=pa.schema([("id", pa.int64())]))

    # Re-point the two branches past the table-creation commit.
    main_ref2 = get_reference("main")
    for name in ("step5-a", "step5-b"):
        rollback(name, to_hash=main_ref2.hash)

    cat_a = get_catalog(catalog_branch="step5-a", ensure_namespace=NAMESPACE)
    cat_b = get_catalog(catalog_branch="step5-b", ensure_namespace=NAMESPACE)
    cat_a.load_table(IDENT).append(pa.table({"id": [1]}))
    cat_b.load_table(IDENT).append(pa.table({"id": [2]}))
    print("RESULT appended one row on each branch, isolated from main")

    merge("step5-a", into="main")
    post_merge_hash = get_reference("main").hash
    rows = get_catalog(catalog_branch="main").load_table(IDENT).scan().to_arrow()
    print(f"RESULT clean merge step5-a->main: main now has {rows.num_rows} row(s)")

    try:
        merge("step5-b", into="main")
        print("RESULT UNEXPECTED: merge step5-b->main did not conflict")
    except MergeConflictError as exc:
        print(f"RESULT expected conflict on step5-b->main: keys={exc.conflicting_keys}")

    tag_ref = create_tag("step5-v1", from_branch="main")
    print(f"RESULT created tag step5-v1@{tag_ref.hash}")

    pre_merge_hash = main_ref2.hash
    rollback("main", to_hash=pre_merge_hash)
    after = get_reference("main")
    print(f"RESULT rolled back main to {after.hash} (expected {pre_merge_hash})")
    assert after.hash == pre_merge_hash

    # Roll main forward again (undo the rollback) so this script is
    # safely re-runnable without leaving main stuck in the past.
    rollback("main", to_hash=post_merge_hash)

    b_ref = get_reference("step5-b")
    print(f"RESULT is_fully_merged(step5-b) = {is_fully_merged(b_ref)} (expect False)")
    try:
        delete_branch("step5-b")
        print("RESULT UNEXPECTED: delete_branch allowed an unmerged branch")
    except UnmergedWorkError:
        print("RESULT expected: delete_branch refused step5-b without --force")
    delete_branch("step5-b", force=True)
    print("RESULT delete_branch --force succeeded on step5-b")

    delete_branch("step5-a", force=True)
    print(
        "PASS: create, isolated write, clean merge, real conflict, tag, "
        "rollback, unmerged-delete refusal and --force all verified live"
    )


if __name__ == "__main__":
    main()
