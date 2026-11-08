"""Generic multi-engine extraction pipeline (Vague 1, step 3).

Reads ``sources.yml`` and, for each configured source, pulls every table via
dlt's ``sql_database`` source (SQLAlchemy under the hood -- one driver per
dialect: ``sqlite3`` stdlib, ``psycopg2``, ``pymysql``, ``pymssql``) and
writes it to a local Parquet file in a working directory, named
``<source>_<table>.parquet`` (e.g. source ``pg_chinook`` + table ``album`` ->
``pg_chinook_album.parquet``).

This module only *extracts* -- it uses dlt purely as the per-dialect SQL
reader (``backend="pyarrow"`` yields ``pyarrow.Table`` chunks directly) and
writes Parquet itself with ``pyarrow``, rather than dlt's own ``filesystem``
destination/pipeline-state machinery. That destination's local-path state
bookkeeping (``_dlt_pipeline_state/*.jsonl``) does not work reliably on
Windows (observed: ``FileNotFoundError`` on load even though the directory
already exists) -- this sidesteps it entirely, and extraction is all this
step needs. Loading the Parquet into DataHut is step 4 (``dhd ingest-dlt``).

Note: this file is loaded by path (see ``scripts/ingest_dlt.py`` /
``datahut_duckhouse/cli.py``), not via ``import ingestion.dlt.pipeline`` --
``ingestion/__init__.py`` is unrelated pre-existing code that imports
submodules which do not exist on disk (``ingestion.connectors`` etc.) and
raises ``ModuleNotFoundError`` on import; going through ``ingestion/dlt/``
directly avoids triggering that.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from importlib.util import find_spec
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as papq
import yaml

SOURCES_YML = Path(__file__).parent / "sources.yml"
DEFAULT_WORK_DIR = Path(__file__).parent / "_pipeline_data"

# dialect -> the pip package providing its SQLAlchemy driver (None = stdlib).
_DRIVER_PACKAGE: dict[str, str | None] = {
    "sqlite": None,
    "postgresql": "psycopg2",
    "mysql": "pymysql",
    "sqlserver": "pymssql",
}
VALID_DIALECTS = tuple(_DRIVER_PACKAGE)


class SourcesConfigError(RuntimeError):
    """``sources.yml`` is missing, malformed, or missing an env var it names."""


class MissingDriverError(RuntimeError):
    """The SQL driver package for a source's dialect is not installed."""


@dataclass(frozen=True)
class Source:
    name: str
    dialect: str
    connection_env: str
    schema: str | None
    tables: tuple[str, ...]  # empty = every table in the schema

    @property
    def connection_url(self) -> str:
        url = os.getenv(self.connection_env)
        if not url:
            raise SourcesConfigError(
                f"Source '{self.name}': environment variable "
                f"'{self.connection_env}' is not set. See "
                f".devcontainer/.env.example."
            )
        return url


def load_sources(path: Path = SOURCES_YML) -> list[Source]:
    """Parse ``sources.yml`` into a list of :class:`Source`."""
    try:
        raw = yaml.safe_load(path.read_text())
    except OSError as exc:
        raise SourcesConfigError(f"Could not read {path}: {exc}") from exc

    entries = (raw or {}).get("sources") or []
    sources: list[Source] = []
    for entry in entries:
        name = entry.get("name")
        dialect = entry.get("dialect")
        connection_env = entry.get("connection_env")
        if not name or not dialect or not connection_env:
            raise SourcesConfigError(
                f"Malformed source entry (needs name/dialect/connection_env): {entry}"
            )
        if dialect not in VALID_DIALECTS:
            raise SourcesConfigError(
                f"Source '{name}': unknown dialect '{dialect}'. "
                f"Choose from: {', '.join(VALID_DIALECTS)}."
            )
        sources.append(
            Source(
                name=name,
                dialect=dialect,
                connection_env=connection_env,
                schema=entry.get("schema") or None,
                tables=tuple(entry.get("tables") or ()),
            )
        )
    return sources


def get_source(name: str, sources: list[Source] | None = None) -> Source:
    for source in sources or load_sources():
        if source.name == name:
            return source
    raise SourcesConfigError(f"No source named '{name}' in sources.yml.")


def prefixed_table_name(source_name: str, table_name: str) -> str:
    return f"{source_name}_{table_name}"


def _check_driver(dialect: str) -> None:
    package = _DRIVER_PACKAGE.get(dialect)
    if package is not None and find_spec(package) is None:
        raise MissingDriverError(
            f"Dialect '{dialect}' needs the '{package}' driver, which is not "
            f"installed in this environment. Install it with the optional "
            f"'ingest' extra: uv sync --extra ingest"
        )


def extract_source(
    source: Source, work_dir: Path = DEFAULT_WORK_DIR
) -> dict[str, Path]:
    """Extract every configured table of ``source`` to Parquet under ``work_dir``.

    Returns ``{prefixed_table_name: parquet_path}``.
    """
    _check_driver(source.dialect)

    from dlt.sources.sql_database import sql_database

    db_source = sql_database(
        credentials=source.connection_url,
        schema=source.schema,
        table_names=list(source.tables) or None,
        backend="pyarrow",
    )

    work_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}
    for table_name, resource in db_source.resources.items():
        chunks = list(resource)
        if not chunks:
            continue  # empty table: nothing to write, no schema to invent
        arrow_table = pa.concat_tables(chunks)
        out_name = prefixed_table_name(source.name, table_name)
        out_path = work_dir / f"{out_name}.parquet"
        papq.write_table(arrow_table, out_path)
        written[out_name] = out_path
    return written


def extract_all(
    sources: list[Source] | None = None, work_dir: Path = DEFAULT_WORK_DIR
) -> dict[str, Path]:
    """Extract every source in ``sources.yml`` (or the given list)."""
    result: dict[str, Path] = {}
    for source in sources or load_sources():
        result.update(extract_source(source, work_dir))
    return result
