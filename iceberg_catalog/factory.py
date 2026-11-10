"""``get_catalog()``: the one place that calls ``pyiceberg.catalog.load_catalog``
(ADR-0006, decision 1). Every component -- the Flight server's
``HybridBackend``, ``dhd --engine pyiceberg``, scripts -- builds its own
identity (``warehouse``/``uri``/``is_remote``, kept where each already
computed it, for ``ingestion/sinks/catalog_guard.py``'s cross-process
comparison) and then calls this module to actually open the catalog, so the
sql/rest parameter-building logic exists exactly once.

Two modes, detected from ``ICEBERG_CATALOG_TYPE`` (ADR-0006, decision 2):

- *solo*: ``catalog_type`` resolves to anything other than ``"rest"`` (the
  default, ``"sql"``, or unset) -- a local SQLite catalog, single writer, no
  versioning.
- *deployment*: ``catalog_type == "rest"`` -- Nessie through the Iceberg REST
  interface, with branches/tags/merge/rollback.

``require_versioning()`` is the gate every versioning command (``dhd
branch``, ``dhd merge``, ``dhd tag``, ``dhd rollback`` -- ADR-0006, decision
4) calls first.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

_S3_SCHEMES = ("s3://", "s3a://")


class CatalogUnavailableError(RuntimeError):
    """The Iceberg catalog could not be reached or opened."""


class VersioningUnavailableError(RuntimeError):
    """A versioning command was used while in solo mode."""


def _env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def is_remote_warehouse(path: str) -> bool:
    return str(path).startswith(_S3_SCHEMES)


def default_warehouse_location() -> str:
    """``ICEBERG_WAREHOUSE`` (typically ``s3://...`` -- MinIO/S3), then
    ``ICEBERG_WAREHOUSE_PATH`` (a local path), then a local default."""
    return (
        os.getenv("ICEBERG_WAREHOUSE")
        or os.getenv("ICEBERG_WAREHOUSE_PATH")
        or os.path.join(os.getcwd(), "data", "iceberg_warehouse")
    )


def catalog_mode() -> str:
    """``"deployment"`` when ``ICEBERG_CATALOG_TYPE=rest``, else ``"solo"``."""
    return (
        "deployment" if os.getenv("ICEBERG_CATALOG_TYPE", "sql") == "rest" else "solo"
    )


def is_deployment_mode() -> bool:
    return catalog_mode() == "deployment"


def require_versioning() -> None:
    """Raise :class:`VersioningUnavailableError` unless in deployment mode.

    Every versioning-facing ``dhd`` command (``branch``, ``merge``, ``tag``,
    ``rollback``) calls this first and turns the error into exit code 2
    (ADR-0006, decision 4) -- solo mode has no Nessie reference model to
    branch.
    """
    if not is_deployment_mode():
        raise VersioningUnavailableError(
            "Versioning (branch/merge/tag/rollback) requires deployment mode "
            "(ICEBERG_CATALOG_TYPE=rest, a Nessie catalog). This process is "
            "in solo mode (SQLite catalog, single writer, no versioning) -- "
            "see docs/CATALOG.md."
        )


def build_catalog_params(
    *,
    catalog_type: str,
    catalog_uri: str,
    catalog_branch: str,
    is_remote: bool,
    warehouse: str,
    s3_endpoint: str | None = None,
    s3_access_key: str | None = None,
    s3_secret_key: str | None = None,
    s3_region: str | None = None,
    s3_path_style: bool | None = None,
) -> dict[str, Any]:
    """``load_catalog`` kwargs, dispatched by ``catalog_type``.

    ``rest`` (Nessie) is a distinct shape, not a variant of ``sql``: a REST
    catalog already knows its warehouse location server-side (see
    ``NESSIE_CATALOG_DEFAULT_WAREHOUSE``) and rejects a client-supplied
    ``warehouse`` property outright ("Warehouse ... is not known" --
    docs/spikes/NESSIE_SPIKES_2.md, T5). Branch/reference selection is a
    ``prefix`` property instead, which the ``sql`` catalog type has no
    equivalent of.
    """
    if catalog_type == "rest":
        params: dict[str, Any] = {
            "type": "rest",
            "uri": catalog_uri,
            "prefix": catalog_branch,
        }
        endpoint = s3_endpoint or _env("S3_ENDPOINT", "AWS_ENDPOINT_URL_S3")
        access_key = s3_access_key or _env("AWS_ACCESS_KEY_ID")
        secret_key = s3_secret_key or _env("AWS_SECRET_ACCESS_KEY")
        region = s3_region or _env("AWS_REGION", "AWS_DEFAULT_REGION") or "us-east-1"
        params["s3.region"] = region
        if endpoint:
            params["s3.endpoint"] = endpoint
            params["s3.path-style-access"] = "true"
        if access_key and secret_key:
            params["s3.access-key-id"] = access_key
            params["s3.secret-access-key"] = secret_key
        return params

    params = {
        "type": catalog_type,
        "uri": catalog_uri,
        "warehouse": warehouse if is_remote else f"file://{warehouse}",
    }
    if not is_remote:
        return params

    endpoint = s3_endpoint or _env("S3_ENDPOINT", "AWS_ENDPOINT_URL_S3")
    access_key = s3_access_key or _env("AWS_ACCESS_KEY_ID")
    secret_key = s3_secret_key or _env("AWS_SECRET_ACCESS_KEY")
    region = s3_region or _env("AWS_REGION", "AWS_DEFAULT_REGION") or "us-east-1"

    if s3_path_style is None:
        s3_path_style = endpoint is not None

    params["s3.region"] = region
    if endpoint:
        params["s3.endpoint"] = endpoint
    if access_key and secret_key:
        params["s3.access-key-id"] = access_key
        params["s3.secret-access-key"] = secret_key
    if s3_path_style:
        params["s3.path-style-access"] = "true"

    return params


def get_catalog(
    *,
    catalog_name: str | None = None,
    warehouse: str | None = None,
    is_remote: bool | None = None,
    catalog_type: str | None = None,
    catalog_uri: str | None = None,
    catalog_branch: str | None = None,
    local_meta_dir: str | Path | None = None,
    s3_endpoint: str | None = None,
    s3_access_key: str | None = None,
    s3_secret_key: str | None = None,
    s3_region: str | None = None,
    s3_path_style: bool | None = None,
    ensure_namespace: str | None = None,
):
    """Open (and return) the Iceberg catalog. The only call to
    ``pyiceberg.catalog.load_catalog`` outside this module is a rule
    violation (see ``tests/test_catalog_factory.py``'s AST check).

    Every parameter falls back to the environment variables ADR-0006 names
    (``ICEBERG_CATALOG``, ``ICEBERG_CATALOG_TYPE``, ``ICEBERG_CATALOG_URI``,
    ``ICEBERG_WAREHOUSE``/``ICEBERG_WAREHOUSE_PATH``, ``ICEBERG_BRANCH``)
    when a caller already resolved its own identity (``HybridBackend``,
    ``PyIcebergEngine`` -- for ``ingestion/sinks/catalog_guard.py``'s
    cross-process comparison), it passes that resolved value through
    explicitly instead of letting this function re-derive it.
    """
    from pyiceberg.catalog import load_catalog

    name = catalog_name or os.getenv("ICEBERG_CATALOG", "dhd")
    resolved_warehouse = warehouse or default_warehouse_location()
    remote = (
        is_remote if is_remote is not None else is_remote_warehouse(resolved_warehouse)
    )
    resolved_type: str = catalog_type or os.getenv("ICEBERG_CATALOG_TYPE") or "sql"
    branch = catalog_branch or os.getenv("ICEBERG_BRANCH") or "main"

    if remote:
        warehouse_param = str(resolved_warehouse).replace("s3a://", "s3://", 1)
        meta_dir = Path(local_meta_dir) if local_meta_dir else Path.cwd()
    else:
        warehouse_param = str(Path(resolved_warehouse).absolute())
        Path(warehouse_param).mkdir(parents=True, exist_ok=True)
        meta_dir = Path(warehouse_param)
    meta_dir.mkdir(parents=True, exist_ok=True)

    uri = (
        catalog_uri
        or os.getenv("ICEBERG_CATALOG_URI")
        or f"sqlite:///{meta_dir.absolute().as_posix()}/pyiceberg_catalog.db"
    )

    params = build_catalog_params(
        catalog_type=resolved_type,
        catalog_uri=uri,
        catalog_branch=branch,
        is_remote=remote,
        warehouse=warehouse_param,
        s3_endpoint=s3_endpoint,
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key,
        s3_region=s3_region,
        s3_path_style=s3_path_style,
    )

    try:
        catalog = load_catalog(name, **params)
    except Exception as exc:  # noqa: BLE001 - surface a clean, actionable error
        raise CatalogUnavailableError(
            f"Could not open the Iceberg catalog ({type(exc).__name__}: {exc}). "
            f"Check ICEBERG_CATALOG_TYPE/ICEBERG_CATALOG_URI/ICEBERG_WAREHOUSE "
            f"/ S3_ENDPOINT / AWS_* and that the catalog (Nessie, in "
            f"deployment mode) and storage (MinIO) are reachable."
        ) from exc

    if ensure_namespace:
        existing = {n[0] for n in catalog.list_namespaces()}
        if ensure_namespace not in existing:
            catalog.create_namespace(ensure_namespace)

    return catalog
