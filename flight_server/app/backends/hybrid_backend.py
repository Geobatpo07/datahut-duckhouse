import datetime
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from xorq.backends.pyiceberg import Backend as PyIcebergBackend
from xorq.common.utils.logging_utils import get_print_logger
from xorq.vendor.ibis.backends.duckdb import Backend as DuckDBBackend
from xorq.vendor.ibis.expr import schema as sch
from xorq.vendor.ibis.expr import types as ir

logger = get_print_logger()

_S3_SCHEMES = ("s3://", "s3a://")


def _is_remote_warehouse(warehouse_path: str) -> bool:
    return str(warehouse_path).startswith(_S3_SCHEMES)


def _env(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


class HybridBackend(PyIcebergBackend):
    """
    Backend hybride : Iceberg est l'unique cible de persistance (ADR-0001), DuckDB
    ne fait que refléter les tables Iceberg pour l'exécution des requêtes.

    Contrairement au ``PyIcebergBackend`` de xorq — qui code en dur un warehouse
    ``file://`` local et un catalogue SQLite embarqué —, ce backend construit
    lui-même les paramètres du catalogue et sait pointer un warehouse objet
    (``s3://`` / MinIO). Le catalogue lui-même reste un catalogue SQL : local
    (SQLite) tant qu'on a un seul writer, ou distant (``catalog_uri``) en
    attendant Nessie (ADR-0002).

    La lecture depuis DuckDB passe par pyiceberg (scan Iceberg → Arrow →
    ``register`` dans DuckDB), pas par l'extension ``iceberg`` de DuckDB : un
    seul chemin de lecture, identique en local et sur S3, sans dépendre de la
    version de l'extension ni d'un httpfs configuré à part.
    """

    def __init__(self, warehouse_path=None, **kwargs):
        super().__init__(warehouse_path=warehouse_path)
        self.duckdb_path = None
        self.snapshot_dir = None
        self.is_remote = False

        if warehouse_path is not None:
            self.do_connect(warehouse_path=warehouse_path, **kwargs)

    def do_connect(
        self,
        warehouse_path: str,
        duckdb_path: str | None = None,
        snapshot_dir: str | None = None,
        namespace: str = "default",
        catalog_name: str = "default",
        catalog_type: str = "sql",
        catalog_uri: str | None = None,
        s3_endpoint: str | None = None,
        s3_access_key: str | None = None,
        s3_secret_key: str | None = None,
        s3_region: str | None = None,
        s3_path_style: bool | None = None,
        **kwargs,
    ) -> None:
        self.is_remote = _is_remote_warehouse(warehouse_path)
        self.namespace = namespace

        if self.is_remote:
            self.warehouse_path = str(warehouse_path).replace("s3a://", "s3://", 1)
            # Le catalogue SQL a besoin d'un fichier local : à côté du DuckDB si un
            # chemin est fourni, sinon le cwd. (Remplacé par Nessie en Phase 3.)
            local_meta_dir = Path(
                os.path.dirname(duckdb_path) if duckdb_path else os.getcwd()
            )
        else:
            self.warehouse_path = str(Path(warehouse_path).absolute())
            Path(self.warehouse_path).mkdir(parents=True, exist_ok=True)
            local_meta_dir = Path(self.warehouse_path)

        local_meta_dir.mkdir(parents=True, exist_ok=True)
        self.uri = (
            catalog_uri
            or _env("ICEBERG_CATALOG_URI")
            or (
                f"sqlite:///{local_meta_dir.absolute().as_posix()}/pyiceberg_catalog.db"
            )
        )

        self.catalog_params = self._build_catalog_params(
            catalog_type=catalog_type,
            s3_endpoint=s3_endpoint,
            s3_access_key=s3_access_key,
            s3_secret_key=s3_secret_key,
            s3_region=s3_region,
            s3_path_style=s3_path_style,
        )

        logger.info(
            f"Connexion catalogue Iceberg '{catalog_name}' "
            f"(warehouse={self.warehouse_path}, remote={self.is_remote})"
        )
        self.catalog = load_catalog(catalog_name, **self.catalog_params)

        if self.namespace not in [n[0] for n in self.catalog.list_namespaces()]:
            self.catalog.create_namespace(self.namespace)

        duckdb_dir = os.getcwd() if self.is_remote else self.warehouse_path
        self.duckdb_path = duckdb_path or os.path.join(duckdb_dir, "duckhouse.duckdb")
        self.snapshot_dir = Path(
            snapshot_dir or os.path.join(os.path.dirname(self.duckdb_path), "snapshots")
        ).absolute()
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Connexion DuckDB -> {self.duckdb_path}")
        logger.info(f"Repertoire des snapshots : {self.snapshot_dir}")

        self.duckdb_con = DuckDBBackend()
        self.duckdb_con.do_connect(database=self.duckdb_path)
        self._reflect_views()
        self._create_snapshot()

    def _build_catalog_params(
        self,
        catalog_type: str,
        s3_endpoint: str | None,
        s3_access_key: str | None,
        s3_secret_key: str | None,
        s3_region: str | None,
        s3_path_style: bool | None,
    ) -> dict:
        params = {
            "type": catalog_type,
            "uri": self.uri,
            "warehouse": (
                self.warehouse_path
                if self.is_remote
                else f"file://{self.warehouse_path}"
            ),
        }

        if not self.is_remote:
            return params

        endpoint = s3_endpoint or _env("S3_ENDPOINT", "AWS_ENDPOINT_URL_S3")
        access_key = s3_access_key or _env("AWS_ACCESS_KEY_ID")
        secret_key = s3_secret_key or _env("AWS_SECRET_ACCESS_KEY")
        region = s3_region or _env("AWS_REGION", "AWS_DEFAULT_REGION") or "us-east-1"

        # Un endpoint personnalisé => MinIO / S3-compatible => path-style par défaut.
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

    def create_table(self, table_name: str, data, **kwargs) -> bool:
        """
        Crée une table. Iceberg est l'unique cible de persistance (voir ADR-0001) :
        DuckDB ne fait que refléter les tables Iceberg via des vues
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

        Le chemin ``mode="overwrite"`` est réimplémenté ici : celui du
        ``PyIcebergBackend`` de xorq 0.2.4 appelle ``iceberg_table.writer()``,
        une API absente des pyiceberg récents (>= 0.9). On passe par
        ``Table.overwrite`` à la place.
        """
        logger.info(f"Insertion dans '{table_name}' [ICEBERG] (mode={mode})")

        if mode == "overwrite":
            data = self._to_arrow(data)
            iceberg_table = self.catalog.load_table(f"{self.namespace}.{table_name}")
            iceberg_table.overwrite(data)
            result = True
        else:
            result = super().insert(table_name, data, mode=mode)

        self._reflect_views()
        self._create_snapshot()
        return result

    @staticmethod
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

    def _reflect_views(self):
        """
        Rafraîchit dans DuckDB une vue par table Iceberg du namespace courant.

        La donnée est lue via pyiceberg (qui gère lui-même l'accès S3/MinIO) puis
        enregistrée dans DuckDB avec `register`. C'est un chargement complet en
        mémoire : suffisant à l'échelle mono-utilisateur visée aujourd'hui, à
        remplacer par un scan poussé (predicate/projection pushdown) quand le
        volume l'imposera — même famille de dette que le snapshot par copie
        complète, cf. ADR-0001.
        """
        for _, table_name in self.catalog.list_tables(self.namespace):
            iceberg_table = self.catalog.load_table(f"{self.namespace}.{table_name}")
            arrow_table = iceberg_table.scan().to_arrow()
            self.duckdb_con.con.register(table_name, arrow_table)

    def _create_snapshot(self):
        """
        Écrit un snapshot du cache DuckDB via ``EXPORT DATABASE`` (portable, et
        possible pendant que la connexion est ouverte — contrairement à une copie
        du fichier ``.duckdb``, verrouillé sous Windows).

        C'est du best-effort : la source de vérité est Iceberg, qui a son propre
        historique de snapshots. Un échec ici ne doit jamais faire échouer un
        commit Iceberg déjà abouti — on loggue et on continue. Désactivable via
        ``HYBRID_DUCKDB_SNAPSHOTS=0``. Stratégie à revoir (cf. ADR-0001).
        """
        if _env("HYBRID_DUCKDB_SNAPSHOTS") == "0":
            return

        ts = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d_%H%M%S_%f")
        snap_path = self.snapshot_dir / ts
        escaped = str(snap_path).replace("'", "''")
        try:
            self.duckdb_con.raw_sql("CHECKPOINT;")
            self.duckdb_con.raw_sql(f"EXPORT DATABASE '{escaped}' (FORMAT PARQUET);")
            logger.info(f"Snapshot DuckDB écrit : {snap_path}")
        except Exception as exc:  # noqa: BLE001 - best-effort, ne doit pas propager
            logger.warning(f"Snapshot DuckDB ignoré ({type(exc).__name__}: {exc})")

    def _get_schema_using_query(self, query: str) -> sch.Schema:
        """Retourne le schéma Arrow d’une requête."""
        limit_query = f"SELECT * FROM ({query}) AS t LIMIT 0"
        result = self.duckdb_con.sql(limit_query)
        return sch.Schema.from_pyarrow(result.to_pyarrow())

    def to_pyarrow_batches(
        self,
        expr: ir.Expr,
        *,
        params: Mapping[ir.Scalar, Any] | None = None,
        limit: int | str | None = None,
        chunk_size: int = 10_000,
        **_: Any,
    ) -> pa.ipc.RecordBatchReader:
        self._reflect_views()
        return self.duckdb_con.to_pyarrow_batches(
            expr, params=params, limit=limit, chunk_size=chunk_size
        )
