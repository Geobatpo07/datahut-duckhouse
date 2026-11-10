"""``scripts.orphan_sweep`` (ADR-0006, decision 7 / C2): the comparison
logic itself (storage objects minus every file reachable from current
references). ``list_storage_objects``/``referenced_files`` each need a
real MinIO/Nessie, so they're mocked here -- the real detection (a
deleted-unmerged-branch's files showing up as orphans, and never once
appearing in a real nessie-gc run) was verified live; see
``docs/RETENTION.md``."""

from __future__ import annotations

import importlib.util
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent


def _load_script_module(name: str, relative_path: str):
    """Import a scripts/*.py file as a module without running its
    ``if __name__ == "__main__"`` block -- same convention as
    ``tests/test_scripts.py``."""
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


orphan_sweep = _load_script_module("orphan_sweep", "scripts/orphan_sweep.py")


def test_normalize_s3a_to_s3():
    assert orphan_sweep._normalize("s3a://bucket/x") == "s3://bucket/x"
    assert orphan_sweep._normalize("s3://bucket/x") == "s3://bucket/x"


def test_find_orphans_is_storage_minus_referenced(monkeypatch):
    monkeypatch.setattr(
        orphan_sweep,
        "list_storage_objects",
        lambda: {
            "s3://b/live/data.parquet": 100,
            "s3://b/orphan/data.parquet": 50,
        },
    )
    monkeypatch.setattr(
        orphan_sweep, "referenced_files", lambda: {"s3://b/live/data.parquet"}
    )
    assert orphan_sweep.find_orphans() == {"s3://b/orphan/data.parquet": 50}


def test_find_orphans_empty_when_everything_referenced(monkeypatch):
    monkeypatch.setattr(orphan_sweep, "list_storage_objects", lambda: {"s3://b/x": 1})
    monkeypatch.setattr(orphan_sweep, "referenced_files", lambda: {"s3://b/x"})
    assert orphan_sweep.find_orphans() == {}


def test_warehouse_bucket_and_prefix(monkeypatch):
    monkeypatch.setenv("ICEBERG_WAREHOUSE", "s3://my-bucket/some/prefix/")
    bucket, prefix = orphan_sweep._warehouse_bucket_and_prefix()
    assert bucket == "my-bucket"
    assert prefix == "some/prefix/"
