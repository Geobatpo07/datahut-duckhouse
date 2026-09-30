"""Serverless ingestion engine for the ``dhd`` CLI — ``--engine pyiceberg``.

Where the default ``flight`` engine drives the Arrow Flight server (which owns the
xorq ``HybridBackend``), this engine talks straight to the Iceberg catalog with
``pyiceberg`` + ``pyarrow`` and never imports ``xorq``. Same warehouse, same
environment variables (``ICEBERG_*`` / ``S3_ENDPOINT`` / ``AWS_*``) as the
server, so both write the same ``s3://`` warehouse on MinIO.

Trade-off (single-user, ADR-0001): this engine keeps its *own* SQL catalog file
(SQLite, next to ``DUCKDB_PATH`` or in the cwd) — it does not share the running
server's catalog. Reads go through a throwaway in-process DuckDB that scans the
Iceberg tables via pyiceberg, mirroring ``HybridBackend._reflect_views`` without
the xorq dependency.
"""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa

_S3_SCHEMES = ("s3://", "s3a://")


class IngestUnavailableError(RuntimeError):
    """The Iceberg catalog / object store could not be reached."""


def _env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def _warehouse_location() -> str:
    return (
        os.getenv("ICEBERG_WAREHOUSE")
        or os.getenv("ICEBERG_WAREHOUSE_PATH")
        or os.path.join(os.getcwd(), "data", "iceberg_warehouse")
    )


def _local_meta_dir() -> Path:
    """Where to keep this engine's SQL catalog file for a remote (s3://) warehouse.

    ``DUCKDB_PATH`` in ``.env`` is a *container* path (``/app/...``); only trust
    it when its parent actually exists on this host. Otherwise sit next to the
    repo's ``ingestion/data`` if present, else the cwd.
    """
    duckdb_path = os.getenv("DUCKDB_PATH")
    if duckdb_path:
        parent = Path(duckdb_path).parent
        if parent.is_dir():
            return parent
    repo_data = Path("ingestion") / "data"
    return repo_data if repo_data.is_dir() else Path.cwd()


def _to_arrow(data) -> pa.Table:
    if isinstance(data, pa.Table):
        return data
    if isinstance(data, pa.RecordBatch):
        return pa.Table.from_batches([data])
    try:
        import pandas as pd

        if isinstance(data, pd.DataFrame):
            return pa.Table.from_pandas(data)
    except ImportError:
        pass
    return pa.table(data)


class _Query:
    """Lazy query handle mirroring the slice of the ibis API the CLI uses."""

    def __init__(self, con, sql: str) -> None:
        self._con = con
        self._sql = sql
        self._limit: int | None = None

    def limit(self, n: int) -> _Query:
        self._limit = n
        return self

    def to_pyarrow(self) -> pa.Table:
        query = self._sql
        if self._limit is not None:
            query = (
                f"SELECT * FROM (\n{self._sql}\n) AS _dhd_q LIMIT {int(self._limit)}"
            )
        return self._con.execute(query).arrow()


class PyIcebergEngine:
    """Drop-in replacement for the Flight-backed backend, xorq-free."""

    name = "pyiceberg"

    def __init__(self) -> None:
        try:
            from pyiceberg.catalog import load_catalog
        except ImportError as exc:  # pragma: no cover - pyiceberg is a hard dep
            raise IngestUnavailableError(f"pyiceberg not installed: {exc}") from exc

        import duckdb

        self.namespace = os.getenv("ICEBERG_NAMESPACE", "default")
        params = self._catalog_params()

        try:
            self.catalog = load_catalog(os.getenv("ICEBERG_CATALOG", "dhd"), **params)
            existing = {n[0] for n in self.catalog.list_namespaces()}
            if self.namespace not in existing:
                self.catalog.create_namespace(self.namespace)
        except IngestUnavailableError:
            raise
        except Exception as exc:  # noqa: BLE001 - surface a clean CLI message
            raise IngestUnavailableError(
                f"Could not open the Iceberg catalog ({type(exc).__name__}: {exc}). "
                f"Check ICEBERG_WAREHOUSE / S3_ENDPOINT / AWS_* and that MinIO is up."
            ) from exc

        self._duck = duckdb.connect()
        self.con = self  # `dhd insert` calls backend.con.upload_table(...)

    def _catalog_params(self) -> dict[str, str]:
        """Build ``load_catalog`` kwargs from the same env vars the server reads."""
        warehouse = _warehouse_location()
        self.is_remote = str(warehouse).startswith(_S3_SCHEMES)

        if self.is_remote:
            warehouse = str(warehouse).replace("s3a://", "s3://", 1)
            meta_dir = _local_meta_dir()
        else:
            warehouse = str(Path(warehouse).absolute())
            Path(warehouse).mkdir(parents=True, exist_ok=True)
            meta_dir = Path(warehouse)
        meta_dir.mkdir(parents=True, exist_ok=True)

        params: dict[str, str] = {
            "type": os.getenv("ICEBERG_CATALOG_TYPE", "sql"),
            "uri": _env("ICEBERG_CATALOG_URI")
            or f"sqlite:///{meta_dir.absolute().as_posix()}/pyiceberg_catalog.db",
            "warehouse": warehouse if self.is_remote else f"file://{warehouse}",
        }
        if not self.is_remote:
            return params

        endpoint = _env("S3_ENDPOINT", "AWS_ENDPOINT_URL_S3")
        access_key = _env("AWS_ACCESS_KEY_ID")
        secret_key = _env("AWS_SECRET_ACCESS_KEY")
        params["s3.region"] = _env("AWS_REGION", "AWS_DEFAULT_REGION") or "us-east-1"
        params["s3.path-style-access"] = "true" if endpoint else "false"
        if endpoint:
            params["s3.endpoint"] = endpoint
        if access_key and secret_key:
            params["s3.access-key-id"] = access_key
            params["s3.secret-access-key"] = secret_key
        return params

    # -- table ops -----------------------------------------------------------

    def _ident(self, name: str) -> str:
        return f"{self.namespace}.{name}"

    def list_tables(self) -> list[str]:
        return [tbl[-1] for tbl in self.catalog.list_tables(self.namespace)]

    def create_table(self, table_name: str, data, **_: object) -> bool:
        table = _to_arrow(data)
        iceberg_table = self.catalog.create_table(
            self._ident(table_name), schema=table.schema
        )
        iceberg_table.append(table)
        return True

    def upload_table(self, table_name: str, data, mode: str = "append", **_: object):
        table = _to_arrow(data)
        iceberg_table = self.catalog.load_table(self._ident(table_name))
        if mode == "overwrite":
            iceberg_table.overwrite(table)
        else:
            iceberg_table.append(table)
        return True

    # -- read path ---------------------------------------------------------

    def _reflect_views(self) -> None:
        for table_name in self.list_tables():
            iceberg_table = self.catalog.load_table(self._ident(table_name))
            arrow_table = iceberg_table.scan().to_arrow()
            self._duck.register(table_name, arrow_table)

    def sql(self, query: str) -> _Query:
        self._reflect_views()
        return _Query(self._duck, query)


def connect() -> PyIcebergEngine:
    """Return a connected serverless ingestion engine."""
    return PyIcebergEngine()
