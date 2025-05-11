import os
import shutil
import datetime
from pathlib import Path
from typing import Optional, Mapping, Any, Union

import pyarrow as pa
import xorq as xo
from xorq.backends.pyiceberg import Backend as PyIcebergBackend
from xorq.common.utils.logging_utils import get_print_logger
from xorq.vendor.ibis.expr import types as ir
from xorq.vendor.ibis.expr import schema as sch

logger = get_print_logger()


class HybridBackend(PyIcebergBackend):
    """
    Backend hybride combinant Iceberg (via MinIO) pour la persistance
    et DuckDB pour l'exécution rapide + vues synchronisées.
    """

    def __init__(self, warehouse_path=None, **kwargs):
        super().__init__(warehouse_path=warehouse_path, **kwargs)
        self.duckdb_path = None
        self.snapshot_dir = None

        if warehouse_path is not None:
            self.do_connect(warehouse_path=warehouse_path, **kwargs)

    def do_connect(
        self,
        warehouse_path: str,
        duckdb_path: Optional[str] = None,
        snapshot_dir: Optional[str] = None,
        namespace: str = "default",
        catalog_name: str = "default",
        catalog_type: str = "sql",
        **kwargs
    ) -> None:
        super().do_connect(
            warehouse_path=warehouse_path,
            namespace=namespace,
            catalog_name=catalog_name,
            catalog_type=catalog_type,
            **kwargs
        )

        self.duckdb_path = duckdb_path or os.path.join(warehouse_path, "duckhouse.duckdb")
        self.snapshot_dir = Path(snapshot_dir or os.path.join(warehouse_path, "snapshots")).absolute()
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Connexion DuckDB → {self.duckdb_path}")
        logger.info(f"Répertoire des snapshots : {self.snapshot_dir}")

        self.duckdb_con = xo.duckdb.connect(self.duckdb_path)
        self._setup_duckdb_connection()
        self._reflect_views()
        self._create_snapshot()

    def create_table(self, table_name: str, data, target: str = "", **kwargs) -> bool:
        logger.info(f"Création de la table '{table_name}' dans {target.upper()}")
        result = None

        if target == "iceberg":
            result = super().create_table(table_name, data, **kwargs)
        elif target == "duckdb":
            try:
                result = self.duckdb_con.create_table(table_name, data)
            except:
                result = self.duckdb_con.insert(table_name, data)
        else:
            raise ValueError("Le paramètre 'target' doit être 'duckdb' ou 'iceberg'")

        self._reflect_views()
        self._create_snapshot()
        return result

    def insert(self, table_name: str, data, target: str = "", mode: str = "append", **kwargs) -> bool:
        logger.info(f"Insertion dans '{table_name}' [{target.upper()}]")

        if target == "iceberg":
            result = super().insert(table_name, data, mode=mode)
        elif target == "duckdb":
            result = self.duckdb_con.insert(table_name, data)
        else:
            raise ValueError("Le paramètre 'target' doit être 'duckdb' ou 'iceberg'")

        self._reflect_views()
        self._create_snapshot()
        return result

    def _reflect_views(self):
        """
        Crée/actualise dans DuckDB les vues pointant vers les tables Iceberg.
        Cela permet d’interroger Iceberg depuis DuckDB.
        """
        tables = self.catalog.list_tables(self.namespace)

        for (_, table_name) in tables:
            path = f"{self.warehouse_path}/{self.namespace}.db/{table_name}"
            escaped = path.replace("'", "''")
            safe_name = f'"{table_name}"' if "-" in table_name else table_name

            self.duckdb_con.raw_sql(f"""
                CREATE OR REPLACE VIEW {safe_name} AS
                SELECT * FROM iceberg_scan(
                    '{escaped}',
                    version='?',
                    allow_moved_paths=true
                );
            """)

    def _setup_duckdb_connection(self):
        """Initialise les extensions DuckDB nécessaires à Iceberg."""
        commands = [
            "INSTALL iceberg;",
            "LOAD iceberg;",
            "SET unsafe_enable_version_guessing=true;",
        ]
        for cmd in commands:
            self.duckdb_con.raw_sql(cmd)

    def _create_snapshot(self):
        """Crée un snapshot DuckDB avec CHECKPOINT + sauvegarde."""
        ts = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d_%H%M%S")
        snap_path = os.path.join(self.snapshot_dir, f"{ts}.duckdb")

        for t in self.duckdb_con.tables:
            self.duckdb_con.raw_sql(f"CREATE OR REPLACE TABLE {t}_snapshot AS SELECT * FROM {t};")

        self.duckdb_con.raw_sql(f"CHECKPOINT '{self.duckdb_path}';")
        shutil.copy(self.duckdb_path, snap_path)

        logger.info(f"Snapshot DuckDB écrit : {snap_path}")

    def _get_schema_using_query(self, query: str) -> sch.Schema:
        """Retourne le schéma Arrow d’une requête."""
        limit_query = f"SELECT * FROM ({query}) AS t LIMIT 0"
        result = self.duckdb_con.sql(limit_query)
        return sch.Schema.from_pyarrow(result.to_pyarrow())

    def to_pyarrow_batches(
        self,
        expr: ir.Expr,
        *,
        params: Optional[Mapping[ir.Scalar, Any]] = None,
        limit: Optional[Union[int, str]] = None,
        chunk_size: int = 10_000,
        **_: Any,
    ) -> pa.ipc.RecordBatchReader:
        self._reflect_views()
        return self.duckdb_con.to_pyarrow_batches(expr, params=params, limit=limit, chunk_size=chunk_size)
