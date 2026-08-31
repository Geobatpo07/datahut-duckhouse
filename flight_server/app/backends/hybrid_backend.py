import os
import shutil
import datetime
from pathlib import Path
from typing import Optional, Mapping, Any, Union

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from xorq.backends.pyiceberg import Backend as PyIcebergBackend
from xorq.common.utils.logging_utils import get_print_logger
from xorq.vendor.ibis.backends.duckdb import Backend as DuckDBBackend
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
        self._s3_config = None

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
        catalog_uri: Optional[str] = None,
        s3_endpoint: Optional[str] = None,
        s3_access_key: Optional[str] = None,
        s3_secret_key: Optional[str] = None,
        s3_force_virtual_addressing: bool = False,
        **kwargs
    ) -> None:
        self._s3_config = None

        if self._is_s3_path(warehouse_path):
            self._connect_s3_iceberg(
                warehouse_path=warehouse_path,
                namespace=namespace,
                catalog_name=catalog_name,
                catalog_type=catalog_type,
                catalog_uri=catalog_uri,
                s3_endpoint=s3_endpoint or os.getenv("S3_ENDPOINT", "http://localhost:9000"),
                s3_access_key=s3_access_key or os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
                s3_secret_key=s3_secret_key or os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
                s3_force_virtual_addressing=s3_force_virtual_addressing,
            )
            # snapshot_dir et duckdb_path restent locaux même en mode S3 : le
            # cache DuckDB n'est jamais la source de vérité (ADR-0001), pas
            # besoin qu'il vive sur MinIO.
            local_base = os.getenv(
                "ICEBERG_LOCAL_STATE_DIR",
                os.path.join(os.getcwd(), "data", "local_state"),
            )
        else:
            super().do_connect(
                warehouse_path=warehouse_path,
                namespace=namespace,
                catalog_name=catalog_name,
                catalog_type=catalog_type,
                **kwargs
            )
            local_base = warehouse_path

        self.duckdb_path = duckdb_path or os.path.join(local_base, "duckhouse.duckdb")
        self.snapshot_dir = Path(snapshot_dir or os.path.join(local_base, "snapshots")).absolute()
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Connexion DuckDB → {self.duckdb_path}")
        logger.info(f"Répertoire des snapshots : {self.snapshot_dir}")

        self.duckdb_con = DuckDBBackend()
        self.duckdb_con.do_connect(database=self.duckdb_path)
        self._setup_duckdb_connection()
        self._reflect_views()
        self._create_snapshot()

    @staticmethod
    def _is_s3_path(path: str) -> bool:
        return str(path).startswith("s3://") or str(path).startswith("s3a://")

    def _connect_s3_iceberg(
        self,
        warehouse_path: str,
        namespace: str,
        catalog_name: str,
        catalog_type: str,
        catalog_uri: Optional[str],
        s3_endpoint: str,
        s3_access_key: str,
        s3_secret_key: str,
        s3_force_virtual_addressing: bool,
    ) -> None:
        """
        Réplique ce que fait `PyIcebergBackend.do_connect` (attributs
        `self.warehouse_path`/`self.namespace`/`self.uri`/`self.catalog_params`/
        `self.catalog`), mais avec un entrepôt de données sur MinIO/S3 plutôt
        qu'un chemin de fichier local.

        Nécessaire car le `do_connect()` réel de xorq (voir
        `xorq.backends.pyiceberg.Backend.do_connect`) force toujours
        `warehouse` à un chemin local (`file://{Path(warehouse_path).absolute()}`),
        quel que soit `warehouse_path` fourni — il ne peut pas être réutilisé
        tel quel pour un entrepôt S3 (voir la note post-implémentation de
        l'ADR-0001).

        Le catalogue (métadonnées : quelles tables existent, quels snapshots)
        reste dans un fichier SQLite local — seules les données des tables
        (fichiers Parquet Iceberg) vont sur MinIO via la propriété `warehouse`.
        """
        local_metadata_dir = Path(
            os.getenv("ICEBERG_CATALOG_METADATA_DIR", os.path.join(os.getcwd(), "data", "iceberg_catalog"))
        ).absolute()
        local_metadata_dir.mkdir(parents=True, exist_ok=True)

        self.warehouse_path = warehouse_path.rstrip("/")
        self.namespace = namespace
        self.uri = catalog_uri or f"sqlite:///{local_metadata_dir}/pyiceberg_catalog.db"

        self._s3_config = {
            "endpoint": s3_endpoint,
            "access_key": s3_access_key,
            "secret_key": s3_secret_key,
            "force_virtual_addressing": s3_force_virtual_addressing,
        }

        self.catalog_params = {
            "type": catalog_type,
            "uri": self.uri,
            "warehouse": self.warehouse_path,
            "s3.endpoint": s3_endpoint,
            "s3.access-key-id": s3_access_key,
            "s3.secret-access-key": s3_secret_key,
            "s3.force-virtual-addressing": str(s3_force_virtual_addressing).lower(),
        }

        logger.info(f"Connexion Iceberg (MinIO/S3) → warehouse={self.warehouse_path}, endpoint={s3_endpoint}")
        self.catalog = load_catalog(catalog_name, **self.catalog_params)

        if self.namespace not in [n[0] for n in self.catalog.list_namespaces()]:
            self.catalog.create_namespace(self.namespace)

    def create_table(self, table_name: str, data, **kwargs) -> bool:
        """
        Crée une table. Iceberg est l'unique cible de persistance (voir ADR-0001) :
        DuckDB ne fait jamais que refléter les tables Iceberg via des vues
        (`_reflect_views`), il ne stocke jamais de données de façon durable.
        """
        logger.info(f"Création de la table '{table_name}' dans ICEBERG")
        result = super().create_table(table_name, data, **kwargs)

        self._reflect_views()
        self._create_snapshot()
        return result

    def insert(self, table_name: str, data, mode: str = "append", **kwargs) -> bool:
        """
        Insère des données. Iceberg est l'unique cible de persistance (voir ADR-0001).
        Un besoin réel de stockage éphémère doit passer par un connecteur séparé,
        explicitement nommé comme tel — jamais par une branche silencieuse ici.
        """
        logger.info(f"Insertion dans '{table_name}' [ICEBERG]")
        result = super().insert(table_name, data, mode=mode)

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
        """Initialise les extensions DuckDB nécessaires à Iceberg (et à S3/MinIO
        si l'entrepôt Iceberg y vit — voir `_connect_s3_iceberg`)."""
        commands = ["INSTALL iceberg;", "LOAD iceberg;", "SET unsafe_enable_version_guessing=true;"]

        if self._s3_config is not None:
            endpoint = self._s3_config["endpoint"].replace("http://", "").replace("https://", "")
            use_ssl = self._s3_config["endpoint"].startswith("https://")
            commands = [
                "INSTALL httpfs;",
                "LOAD httpfs;",
                f"SET s3_endpoint='{endpoint}';",
                f"SET s3_access_key_id='{self._s3_config['access_key']}';",
                f"SET s3_secret_access_key='{self._s3_config['secret_key']}';",
                # MinIO exige l'accès path-style (bucket dans le chemin, pas
                # dans un sous-domaine) — équivalent DuckDB de
                # s3.force-virtual-addressing=false côté pyiceberg.
                "SET s3_url_style='path';",
                f"SET s3_use_ssl={'true' if use_ssl else 'false'};",
            ] + commands

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
