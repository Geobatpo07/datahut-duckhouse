"""Enforces ADR-0006, decision 1: every component opens the Iceberg catalog
through ``iceberg_catalog.factory.get_catalog`` -- no module calls
``pyiceberg.catalog.load_catalog`` directly except that one factory.

``scripts/spikes/`` is excluded: those are documented spike-only, throwaway
scripts (``docs/spikes/NESSIE_SPIKES*.md``), explicitly never wired against
production code.
"""

from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
SCAN_DIRS = ("flight_server", "datahut_duckhouse", "ingestion", "scripts")
ALLOWED_MODULE = "iceberg_catalog.factory"


def _module_files() -> list[Path]:
    files: list[Path] = []
    for top in SCAN_DIRS:
        base = REPO_ROOT / top
        if not base.is_dir():
            continue
        for path in base.rglob("*.py"):
            if "__pycache__" in path.parts:
                continue
            if top == "scripts" and "spikes" in path.parts:
                continue
            files.append(path)
    return files


def _calls_load_catalog(path: Path) -> bool:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "pyiceberg.catalog":
            if any(alias.name == "load_catalog" for alias in node.names):
                return True
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "pyiceberg.catalog":
                    return True
    return False


def test_load_catalog_only_called_in_the_factory():
    offenders = []
    for path in _module_files():
        rel = path.relative_to(REPO_ROOT).as_posix()
        if rel == "iceberg_catalog/factory.py":
            continue
        if _calls_load_catalog(path):
            offenders.append(rel)
    assert not offenders, (
        f"load_catalog() is imported outside {ALLOWED_MODULE}: {offenders} -- "
        f"every component must open the catalog through iceberg_catalog.get_catalog() "
        f"(ADR-0006, decision 1)."
    )
