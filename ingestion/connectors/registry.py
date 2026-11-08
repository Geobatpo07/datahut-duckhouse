"""The single place a new connector family gets registered.

Each built-in family registers its own connector *module* here at import
time -- a module satisfies :class:`~ingestion.connectors.base.Connector`
structurally (it has a ``build_resources`` function), so no wrapper class
is needed. Adding a fourth family means adding one more ``register(...)``
call here and nothing else in :mod:`ingestion.engine.dlt_engine`.
"""

from __future__ import annotations

from ingestion.connectors.base import Connector

_CONNECTORS: dict[str, Connector] = {}


def register(family: str, connector: Connector) -> None:
    _CONNECTORS[family] = connector


def get(family: str) -> Connector:
    try:
        return _CONNECTORS[family]
    except KeyError:
        raise ValueError(f"No connector registered for family '{family}'.") from None


def _register_builtins() -> None:
    from ingestion.connectors import files, rest, sql

    register("sql", sql)
    register("files", files)
    register("rest", rest)


_register_builtins()
