"""The single access point to the Iceberg catalog (ADR-0006, decision 1).

A new top-level package, sibling to ``flight_server/``, ``datahut_duckhouse/``
and ``ingestion/`` -- not nested under any of them. Those three packages
never import each other today (glued only by shared environment variables
and the Flight wire protocol), and each is reachable from the others only
because every image ``COPY``s the whole repo and sets ``PYTHONPATH=/app``
(and, on the host, pytest's rootdir does the same). A new sibling package is
the only location this factory can live in without making one of the three
existing packages depend on another.
"""

from iceberg_catalog.factory import (
    CatalogUnavailableError,
    VersioningUnavailableError,
    get_catalog,
    is_deployment_mode,
    require_versioning,
)

__all__ = [
    "CatalogUnavailableError",
    "VersioningUnavailableError",
    "get_catalog",
    "is_deployment_mode",
    "require_versioning",
]
