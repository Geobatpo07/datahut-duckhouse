"""Generic multi-family extraction pipeline (Vague 1b, task 2 -- supersedes
Vague 1 step 3's sql-only version).

Reads ``sources.yml`` (schema: ``registry.py``) and, for each configured
source, dispatches to the connector matching its ``family``
(``connectors/sql.py``, ``connectors/files.py``, ``connectors/rest.py``) --
one Parquet file per table/resource, written in one call per connector, so
a run produces exactly one file per table regardless of how many chunks the
connector pulled it in. Loading that Parquet into DataHut (one Iceberg
commit per file) is ``datahut_duckhouse/ingest_dlt.py``.

Note: this file is loaded by path (see ``datahut_duckhouse/ingest_dlt.py`` /
``scripts/bench_writes.py``), not via ``import ingestion.dlt.pipeline`` --
``ingestion/__init__.py`` is unrelated pre-existing code that imports
submodules which do not exist on disk (``ingestion.connectors`` etc.) and
raises ``ModuleNotFoundError`` on import; going through ``ingestion/dlt/``
directly avoids triggering that. ``connectors/`` and ``registry.py`` *are*
imported normally (``from ingestion.dlt.connectors import sql`` etc.) since
they're reached from here, after this module is already on ``sys.path`` by
the same path-loading trick -- see ``_ensure_importable`` below.
"""

from __future__ import annotations

import sys
from pathlib import Path

_DLT_DIR = Path(__file__).resolve().parent


def _ensure_importable() -> None:
    """Make ``registry``/``connectors.*`` importable as bare (non-``ingestion.``-
    prefixed) modules, regardless of how *this* file itself was loaded.

    This file, ``registry.py`` and ``connectors/`` all live under
    ``ingestion/dlt/`` and are always loaded by path (not via ``import
    ingestion.dlt...``) -- ``ingestion/__init__.py`` is unrelated
    pre-existing code that imports submodules which don't exist on disk and
    raises ``ModuleNotFoundError`` the moment Python resolves the
    ``ingestion`` package to import any of its dotted children. Putting
    ``ingestion/dlt/`` itself on ``sys.path`` and importing ``registry``/
    ``connectors`` as top-level names sidesteps ``ingestion/__init__.py``
    entirely, for every module in this tree, not just this one."""
    dlt_dir = str(_DLT_DIR)
    if dlt_dir not in sys.path:
        sys.path.insert(0, dlt_dir)


_ensure_importable()

from connectors import files, rest, sql  # noqa: E402
from connectors.sql import MissingDriverError  # noqa: E402
from registry import (  # noqa: E402
    SOURCES_YML,
    SourceConfig,
    SourcesConfigError,
    get_source,
    load_sources,
    prefixed_table_name,
)

DEFAULT_WORK_DIR = _DLT_DIR / "_pipeline_data"

_EXTRACTORS = {
    "sql": sql.extract_source,
    "files": files.extract_source,
    "rest": rest.extract_source,
}

__all__ = [
    "SOURCES_YML",
    "DEFAULT_WORK_DIR",
    "SourceConfig",
    "SourcesConfigError",
    "MissingDriverError",
    "load_sources",
    "get_source",
    "prefixed_table_name",
    "extract_source",
    "extract_all",
]


class ConnectorError(RuntimeError):
    """Wraps any connector-level failure with the source name attached."""


def extract_source(
    source: SourceConfig, work_dir: Path = DEFAULT_WORK_DIR
) -> dict[str, Path]:
    """Extract one source via the connector matching its ``family``."""
    try:
        return _EXTRACTORS[source.family](source, work_dir)
    except (MissingDriverError, SourcesConfigError):
        raise
    except Exception as exc:  # noqa: BLE001 - attach the source name, re-raise
        raise ConnectorError(f"'{source.name}' ({source.family}): {exc}") from exc


def extract_all(
    sources: list[SourceConfig] | None = None, work_dir: Path = DEFAULT_WORK_DIR
) -> dict[str, Path]:
    """Extract every source in ``sources.yml`` (or the given list)."""
    result: dict[str, Path] = {}
    for source in sources or load_sources():
        result.update(extract_source(source, work_dir))
    return result
