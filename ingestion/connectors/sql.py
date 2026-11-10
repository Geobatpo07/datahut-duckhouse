"""The ``sql`` connector family (Vague 1b, task 2): dlt's ``sql_database``
source (SQLAlchemy under the hood -- one driver per dialect: ``sqlite3``
stdlib, ``psycopg2``, ``pymysql``, ``pymssql``), via ``_dlt_sources.py`` so
this module itself never imports ``dlt`` directly. Carried over from
Vague 1's ``pipeline.py`` almost unchanged, plus incremental-cursor
support (task 3).
"""

from __future__ import annotations

import datetime
from importlib.util import find_spec
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as papq

from ingestion.config.models import SourceConfig
from ingestion.config.registry import prefixed_table_name
from ingestion.connectors import _dlt_sources
from ingestion.engine import state as _state

# dialect -> the pip package providing its SQLAlchemy driver (None = stdlib).
_DRIVER_PACKAGE: dict[str, str | None] = {
    "sqlite": None,
    "postgresql": "psycopg2",
    "mysql": "pymysql",
    "sqlserver": "pymssql",
}


class MissingDriverError(RuntimeError):
    """The SQL driver package for a source's dialect is not installed."""


def _decode_cursor(value, cursor_type: str):
    """Undo JSON's stringification of a stored cursor value (``_state``
    round-trips everything through ``json.dumps(..., default=str)``) so it
    matches the pyarrow column's actual Python type -- otherwise dlt's
    ``incremental`` rejects a str ``initial_value`` against a timestamp
    column. Verified against the live Postgres Chinook container (task 5,
    experiment 1): a plain ``str`` initial_value against a ``timestamp[us,
    tz=UTC]`` column fails with ``Could not coerce start_value/initial_value``.
    """
    if value is not None and cursor_type == "timestamp" and isinstance(value, str):
        return datetime.datetime.fromisoformat(value)
    return value


def check_driver(dialect: str) -> None:
    package = _DRIVER_PACKAGE.get(dialect)
    if package is not None and find_spec(package) is None:
        raise MissingDriverError(
            f"Dialect '{dialect}' needs the '{package}' driver, which is not "
            f"installed in this environment. Install it with the optional "
            f"'ingest' extra: uv sync --extra ingest"
        )


def build_resources(source: SourceConfig, work_dir: Path) -> dict[str, Path]:
    """Extract every configured table of ``source`` to Parquet under ``work_dir``.

    One Parquet file per table, written in one call (see ``pa.concat_tables``
    below) -- this is what gives task 3's "one Iceberg commit per table per
    run" its foundation: the engine does exactly one create/append/
    overwrite/upsert per file it finds here.
    """
    check_driver(source.dialect or "")

    db_source = _dlt_sources.sql_database_source(
        credentials=source.connection_url,
        schema=source.schema_name,
        table_names=list(source.resources) or None,
    )

    inc = source.incremental
    if inc is not None:
        for resource in db_source.resources.values():
            last_value = _decode_cursor(
                _state.read_last_value(source.name, resource.name), inc.type
            )
            resource.apply_hints(
                incremental=_dlt_sources.incremental(inc.column, last_value)
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
            previous = _decode_cursor(
                _state.read_last_value(source.name, table_name), inc.type
            )
            new_max = seen_max if previous is None else max(previous, seen_max)
            _state.write_last_value(source.name, table_name, new_max)

        out_name = prefixed_table_name(source.prefix, table_name)
        out_path = work_dir / f"{out_name}.parquet"
        papq.write_table(arrow_table, out_path)
        written[out_name] = out_path
    return written
