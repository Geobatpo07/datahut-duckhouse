"""The only module besides ``engine/dlt_engine.py`` that imports ``dlt``
(enforced by ``tests/ingestion/test_dependency_rules.py``) -- every dlt
source constructor the ``sql``/``rest`` connector families need, isolated
here so ``sql.py``/``rest.py`` themselves stay dlt-free and only talk to
this thin wrapper.
"""

from __future__ import annotations

from typing import Any, Literal


def sql_database_source(
    credentials: str,
    schema: str | None,
    table_names: list[str] | None,
    backend: Literal["sqlalchemy", "pyarrow", "pandas", "connectorx"] = "pyarrow",
):
    from dlt.sources.sql_database import sql_database

    return sql_database(
        credentials=credentials,
        schema=schema,
        table_names=table_names,
        backend=backend,
    )


def rest_api_source(config: dict[str, Any]):
    from dlt.sources.rest_api import rest_api_source as _rest_api_source

    return _rest_api_source(config)  # type: ignore[arg-type]


def incremental(column: str, initial_value: Any):
    import dlt

    return dlt.sources.incremental(column, initial_value=initial_value)
