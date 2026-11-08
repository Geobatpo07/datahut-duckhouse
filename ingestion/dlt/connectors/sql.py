"""The ``sql`` connector family (Vague 1b, task 2): dlt's ``sql_database``
source (SQLAlchemy under the hood -- one driver per dialect: ``sqlite3``
stdlib, ``psycopg2``, ``pymysql``, ``pymssql``). Carried over from Vague 1's
``pipeline.py`` almost unchanged, plus incremental-cursor support (task 3).
"""

from __future__ import annotations

from importlib.util import find_spec
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as papq
from registry import SourceConfig, prefixed_table_name

from connectors import _state

# dialect -> the pip package providing its SQLAlchemy driver (None = stdlib).
_DRIVER_PACKAGE: dict[str, str | None] = {
    "sqlite": None,
    "postgresql": "psycopg2",
    "mysql": "pymysql",
    "sqlserver": "pymssql",
}


class MissingDriverError(RuntimeError):
    """The SQL driver package for a source's dialect is not installed."""


def check_driver(dialect: str) -> None:
    package = _DRIVER_PACKAGE.get(dialect)
    if package is not None and find_spec(package) is None:
        raise MissingDriverError(
            f"Dialect '{dialect}' needs the '{package}' driver, which is not "
            f"installed in this environment. Install it with the optional "
            f"'ingest' extra: uv sync --extra ingest"
        )


def extract_source(source: SourceConfig, work_dir: Path) -> dict[str, Path]:
    """Extract every configured table of ``source`` to Parquet under ``work_dir``.

    One Parquet file per table, written in one call (see ``pa.concat_tables``
    below) -- this is what gives task 3's "one Iceberg commit per table per
    run" its foundation: the loader (``datahut_duckhouse/ingest_dlt.py``)
    does exactly one create/append/overwrite/upsert per file it finds here.
    """
    check_driver(source.dialect or "")

    from dlt.sources.sql_database import sql_database

    db_source = sql_database(
        credentials=source.connection_url,
        schema=source.schema_name,
        table_names=list(source.resources) or None,
        backend="pyarrow",
    )

    inc = source.incremental
    if inc is not None:
        import dlt

        for resource in db_source.resources.values():
            last_value = _state.read_last_value(source.name, resource.name)
            resource.apply_hints(
                incremental=dlt.sources.incremental(
                    inc.column, initial_value=last_value
                )
            )

    work_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}
    for table_name, resource in db_source.resources.items():
        chunks = list(resource)
        if not chunks:
            continue  # empty table/no new rows: nothing to write
        arrow_table = pa.concat_tables(chunks)

        if inc is not None and inc.column in arrow_table.column_names:
            seen_max = pc.max(arrow_table.column(inc.column)).as_py()
            previous = _state.read_last_value(source.name, table_name)
            new_max = seen_max if previous is None else max(previous, seen_max)
            _state.write_last_value(source.name, table_name, new_max)

        out_name = prefixed_table_name(source.prefix, table_name)
        out_path = work_dir / f"{out_name}.parquet"
        papq.write_table(arrow_table, out_path)
        written[out_name] = out_path
    return written
