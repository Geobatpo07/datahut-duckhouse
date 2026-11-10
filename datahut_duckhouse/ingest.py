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


class SchemaContractViolationError(RuntimeError):
    """A ``schema_contract: freeze`` source tried to write a new column."""


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
        import duckdb

        from iceberg_catalog import CatalogUnavailableError, get_catalog

        self.namespace = os.getenv("ICEBERG_NAMESPACE", "default")
        warehouse, is_remote, meta_dir = self._resolve_identity()
        self.is_remote = is_remote

        catalog_type = os.getenv("ICEBERG_CATALOG_TYPE", "sql")
        catalog_uri = _env("ICEBERG_CATALOG_URI") or (
            f"sqlite:///{meta_dir.absolute().as_posix()}/pyiceberg_catalog.db"
        )
        # Exposed for catalog_guard.check_same_catalog: the identity that
        # must match the Flight server's own (HybridBackend.uri /
        # .warehouse_path) before a direct pyiceberg write is allowed.
        self.uri = catalog_uri
        self.warehouse_path = warehouse if self.is_remote else f"file://{warehouse}"

        try:
            self.catalog = get_catalog(
                catalog_name=os.getenv("ICEBERG_CATALOG", "dhd"),
                warehouse=warehouse,
                is_remote=self.is_remote,
                catalog_type=catalog_type,
                catalog_uri=catalog_uri,
                catalog_branch=_env("ICEBERG_BRANCH") or "main",
                ensure_namespace=self.namespace,
            )
        except CatalogUnavailableError as exc:
            raise IngestUnavailableError(str(exc)) from exc

        self._duck = duckdb.connect()
        self.con = self  # `dhd insert` calls backend.con.upload_table(...)

    def _resolve_identity(self) -> tuple[str, bool, Path]:
        """This engine's own warehouse/meta-dir identity, computed once so
        ``self.uri``/``self.warehouse_path`` (read by ``catalog_guard``) stay
        exactly as before -- ``get_catalog`` is told the result rather than
        re-deriving it."""
        warehouse = _warehouse_location()
        is_remote = str(warehouse).startswith(_S3_SCHEMES)

        if is_remote:
            warehouse = str(warehouse).replace("s3a://", "s3://", 1)
            meta_dir = _local_meta_dir()
        else:
            warehouse = str(Path(warehouse).absolute())
            Path(warehouse).mkdir(parents=True, exist_ok=True)
            meta_dir = Path(warehouse)
        meta_dir.mkdir(parents=True, exist_ok=True)
        return warehouse, is_remote, meta_dir

    # -- table ops -----------------------------------------------------------

    def _ident(self, name: str) -> str:
        return f"{self.namespace}.{name}"

    def list_tables(self) -> list[str]:
        return [tbl[-1] for tbl in self.catalog.list_tables(self.namespace)]

    def drop_table(self, table_name: str) -> None:
        self.catalog.drop_table(self._ident(table_name))

    def create_table(self, table_name: str, data, **_: object) -> bool:
        table = _to_arrow(data)
        iceberg_table = self.catalog.create_table(
            self._ident(table_name), schema=table.schema
        )
        iceberg_table.append(table)
        return True

    def upload_table(
        self,
        table_name: str,
        data,
        mode: str = "append",
        primary_key: list[str] | None = None,
        **_: object,
    ):
        table = _to_arrow(data)
        iceberg_table = self.catalog.load_table(self._ident(table_name))
        if mode == "overwrite":
            iceberg_table.overwrite(table)
        elif mode == "merge":
            if not primary_key:
                raise ValueError("upload_table(mode='merge') needs primary_key.")
            iceberg_table.upsert(table, join_cols=list(primary_key))
        else:
            iceberg_table.append(table)
        return True

    def apply_schema_contract(
        self, table_name: str, data: pa.Table, contract: str
    ) -> pa.Table:
        """Enforce ``schema_contract`` (task 3) before writing into an
        *existing* table. Verified against pyiceberg 0.11.1: appending or
        overwriting with an unseen column raises ``ValueError`` naming it and
        suggesting ``union_by_name`` -- which is exactly what 'evolve' does
        here, proactively, so the caller never hits that error. A column
        *missing* from the incoming data is not a contract concern: pyiceberg
        already fills it with nulls with no special handling needed.
        """
        iceberg_table = self.catalog.load_table(self._ident(table_name))
        existing_cols = set(iceberg_table.schema().as_arrow().names)
        new_cols = [c for c in data.column_names if c not in existing_cols]
        if not new_cols:
            return data
        if contract == "freeze":
            raise SchemaContractViolationError(
                f"'{table_name}': incoming data has column(s) not in the "
                f"table's schema ({', '.join(new_cols)}) and this source's "
                f"schema_contract is 'freeze'."
            )
        if contract == "discard_value":
            return data.drop_columns(new_cols)
        # "evolve" (default): widen the Iceberg table's schema in place.
        iceberg_table.update_schema().union_by_name(data.schema).commit()
        return data

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
