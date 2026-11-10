"""Enforces ``docs/INGESTION_ARCHITECTURE.md``'s dependency rules by
parsing every module's imports with ``ast`` (no import-linter dependency
needed for five straightforward prefix checks). A violation here means the
architecture doc is already wrong, not just untested.

Rules:

1. ``ingestion/config/*`` imports none of: ``dlt``, ``ingestion.connectors``,
   ``ingestion.sinks``, ``ingestion.engine``.
2. Only ``ingestion/engine/dlt_engine.py`` and
   ``ingestion/connectors/_dlt_sources.py`` import ``dlt``.
3. ``ingestion/connectors/*`` imports neither ``ingestion.sinks`` nor
   ``ingestion.runs``.
4. ``ingestion/sinks/*`` imports neither ``dlt`` nor ``ingestion.connectors``.
5. ``datahut_duckhouse/cli.py`` and ``datahut_duckhouse/ingest_dlt.py``
   (the CLI's ingest-facing modules) import only ``ingestion``'s top-level
   public API -- never ``ingestion.<anything>``.
6. No import cycle among ``ingestion``'s own submodules.
"""

from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent.parent
INGESTION_DIR = REPO_ROOT / "ingestion"


def _module_files(package_dir: Path) -> list[Path]:
    return sorted(p for p in package_dir.rglob("*.py") if "__pycache__" not in p.parts)


def _imported_modules(path: Path) -> set[str]:
    """Every dotted module name this file imports (``import a.b`` ->
    ``"a.b"``; ``from a.b import c`` -> ``"a.b"``), including names
    imported inside function bodies (e.g. a lazy ``import dlt``)."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.add(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            modules.add(node.module)
    return modules


def _module_name(path: Path) -> str:
    rel = path.relative_to(REPO_ROOT).with_suffix("")
    parts = rel.parts
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _starts_with_any(modules: set[str], prefixes: tuple[str, ...]) -> set[str]:
    return {
        m for m in modules if any(m == p or m.startswith(p + ".") for p in prefixes)
    }


def test_config_imports_nothing_executable():
    offenders = {}
    for path in _module_files(INGESTION_DIR / "config"):
        hits = _starts_with_any(
            _imported_modules(path),
            ("dlt", "ingestion.connectors", "ingestion.sinks", "ingestion.engine"),
        )
        if hits:
            offenders[_module_name(path)] = hits
    assert not offenders, f"config/ imported forbidden modules: {offenders}"


def test_only_two_modules_import_dlt():
    allowed = {
        "ingestion.engine.dlt_engine",
        "ingestion.connectors._dlt_sources",
    }
    offenders = []
    for path in _module_files(INGESTION_DIR):
        name = _module_name(path)
        if name in allowed:
            continue
        hits = _starts_with_any(_imported_modules(path), ("dlt",))
        if hits:
            offenders.append(name)
    assert not offenders, f"modules other than {allowed} imported dlt: {offenders}"


def test_connectors_do_not_know_sinks_or_runs():
    offenders = {}
    for path in _module_files(INGESTION_DIR / "connectors"):
        hits = _starts_with_any(
            _imported_modules(path), ("ingestion.sinks", "ingestion.runs")
        )
        if hits:
            offenders[_module_name(path)] = hits
    assert not offenders, f"connectors/ imported forbidden modules: {offenders}"


def test_sinks_do_not_know_dlt_or_connectors():
    offenders = {}
    for path in _module_files(INGESTION_DIR / "sinks"):
        hits = _starts_with_any(
            _imported_modules(path), ("dlt", "ingestion.connectors")
        )
        if hits:
            offenders[_module_name(path)] = hits
    assert not offenders, f"sinks/ imported forbidden modules: {offenders}"


def test_ingestion_cli_module_imports_only_the_public_api():
    """``ingestion/cli.py`` itself -- the real "cli.py n'importe que l'API
    publique" rule. ``datahut_duckhouse/cli.py`` is allowed one deliberate
    deep import (``ingestion.cli``, to mount the group); that one file
    aside, ``datahut_duckhouse/ingest_dlt.py`` (the deprecated-alias
    adapter) is held to the same rule as ``ingestion/cli.py``."""
    cli_modules = [
        INGESTION_DIR / "cli.py",
        REPO_ROOT / "datahut_duckhouse" / "ingest_dlt.py",
    ]
    offenders = {}
    for path in cli_modules:
        hits = {m for m in _imported_modules(path) if m.startswith("ingestion.")}
        if hits:
            offenders[_module_name(path)] = hits
    assert (
        not offenders
    ), f"CLI-facing modules reached past ingestion's public API: {offenders}"


def test_no_import_cycle_within_ingestion():
    """A simple DFS over the module -> module import graph restricted to
    ``ingestion``'s own submodules."""
    graph: dict[str, set[str]] = {}
    for path in _module_files(INGESTION_DIR):
        name = _module_name(path)
        graph[name] = {m for m in _imported_modules(path) if m.startswith("ingestion.")}

    visiting: set[str] = set()
    visited: set[str] = set()
    stack: list[str] = []

    def visit(node: str) -> None:
        if node in visited or node not in graph:
            return
        if node in visiting:
            cycle = " -> ".join(stack[stack.index(node) :] + [node])
            raise AssertionError(f"import cycle detected: {cycle}")
        visiting.add(node)
        stack.append(node)
        for dep in graph[node]:
            visit(dep)
        stack.pop()
        visiting.discard(node)
        visited.add(node)

    for module in list(graph):
        visit(module)
