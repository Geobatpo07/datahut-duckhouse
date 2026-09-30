"""
Tests for flight_server.app.backends.hybrid_backend.

These exercise the *real* backend (real xorq PyIcebergBackend, real pyiceberg
catalog, real DuckDB) rather than mocking it — the whole point of ADR-0001 is
that HybridBackend is the one code path that matters, so the tests have to
actually run it. The S3/MinIO wiring is covered against a local moto server.
"""

import pyarrow as pa
import pytest

from flight_server.app.backends.hybrid_backend import (
    HybridBackend,
    _is_remote_warehouse,
)


@pytest.fixture
def local_backend(tmp_path):
    be = HybridBackend()
    be.do_connect(
        warehouse_path=str(tmp_path / "warehouse"),
        duckdb_path=str(tmp_path / "duckhouse.duckdb"),
        snapshot_dir=str(tmp_path / "snapshots"),
        namespace="default",
    )
    return be


class TestLocalRoundTrip:
    """Iceberg is the only write target; DuckDB reflects it (ADR-0001)."""

    def _duckdb(self, be, sql):
        return be.duckdb_con.raw_sql(sql).fetchall()

    def _iceberg_rows(self, be, name="default.t"):
        return be.catalog.load_table(name).scan().to_arrow().num_rows

    def test_create_then_read_back_via_duckdb_view(self, local_backend):
        local_backend.create_table(
            "t", pa.table({"id": [1, 2, 3], "v": ["a", "b", "c"]})
        )
        assert self._duckdb(
            local_backend, "SELECT count(*) AS n, sum(id) AS s FROM t"
        ) == [(3, 6)]

    def test_insert_append_is_visible_in_both_engines(self, local_backend):
        local_backend.create_table("t", pa.table({"id": [1, 2]}))
        local_backend.insert("t", pa.table({"id": [3, 4]}), mode="append")

        n_duckdb = self._duckdb(local_backend, "SELECT count(*) FROM t")[0][0]
        assert n_duckdb == self._iceberg_rows(local_backend) == 4

    def test_insert_overwrite_replaces_data(self, local_backend):
        local_backend.create_table("t", pa.table({"id": [1, 2, 3]}))
        local_backend.insert("t", pa.table({"id": [9]}), mode="overwrite")

        assert self._duckdb(local_backend, "SELECT * FROM t ORDER BY id") == [(9,)]

    def test_snapshot_written_on_write(self, local_backend):
        local_backend.create_table("t", pa.table({"id": [1]}))
        snaps = list(local_backend.snapshot_dir.iterdir())
        assert snaps, "expected at least one DuckDB snapshot after a write"

    def test_snapshot_can_be_disabled(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HYBRID_DUCKDB_SNAPSHOTS", "0")
        be = HybridBackend()
        be.do_connect(
            warehouse_path=str(tmp_path / "wh"),
            duckdb_path=str(tmp_path / "h.duckdb"),
            snapshot_dir=str(tmp_path / "snaps"),
        )
        be.create_table("t", pa.table({"id": [1]}))
        assert not list(be.snapshot_dir.iterdir())


class TestWarehouseDetection:
    @pytest.mark.parametrize(
        "path,expected",
        [
            ("s3://bucket/wh", True),
            ("s3a://bucket/wh", True),
            ("gs://bucket/wh", False),  # only S3/MinIO is wired today
            ("/var/lib/warehouse", False),
            ("C:\\data\\warehouse", False),
            ("./data/wh", False),
        ],
    )
    def test_is_remote_warehouse(self, path, expected):
        assert _is_remote_warehouse(path) is expected


class TestS3CatalogParams:
    """The S3 wiring the vanilla xorq backend does not do."""

    def _connect(self, tmp_path, **kw):
        be = HybridBackend.__new__(HybridBackend)
        be.is_remote = True
        be.namespace = "default"
        be.warehouse_path = "s3://bucket/wh"
        be.uri = f"sqlite:///{tmp_path}/cat.db"
        return be._build_catalog_params(catalog_type="sql", **kw)

    def test_explicit_s3_params_are_forwarded(self, tmp_path):
        params = self._connect(
            tmp_path,
            s3_endpoint="http://minio:9000",
            s3_access_key="ak",
            s3_secret_key="sk",
            s3_region="eu-west-3",
            s3_path_style=None,
        )
        assert params["warehouse"] == "s3://bucket/wh"
        assert params["s3.endpoint"] == "http://minio:9000"
        assert params["s3.access-key-id"] == "ak"
        assert params["s3.secret-access-key"] == "sk"
        assert params["s3.region"] == "eu-west-3"
        # custom endpoint -> path-style on by default (MinIO)
        assert params["s3.path-style-access"] == "true"

    def test_s3_params_fall_back_to_environment(self, tmp_path, monkeypatch):
        monkeypatch.setenv("S3_ENDPOINT", "http://localhost:9000")
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "envkey")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "envsecret")
        monkeypatch.delenv("AWS_REGION", raising=False)
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)

        params = self._connect(
            tmp_path,
            s3_endpoint=None,
            s3_access_key=None,
            s3_secret_key=None,
            s3_region=None,
            s3_path_style=None,
        )
        assert params["s3.endpoint"] == "http://localhost:9000"
        assert params["s3.access-key-id"] == "envkey"
        assert params["s3.secret-access-key"] == "envsecret"
        assert params["s3.region"] == "us-east-1"  # documented default


moto_server = pytest.importorskip("moto.server", reason="moto not installed")


class TestS3RoundTrip:
    """End-to-end write+read against an in-process S3 (moto), no Docker needed."""

    @pytest.fixture
    def s3_env(self, monkeypatch):
        import socket

        import boto3
        from moto.server import ThreadedMotoServer

        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]

        server = ThreadedMotoServer(ip_address="127.0.0.1", port=port)
        server.start()
        endpoint = f"http://127.0.0.1:{port}"

        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
        monkeypatch.setenv("S3_ENDPOINT", endpoint)

        boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        ).create_bucket(Bucket="warehouse")

        yield endpoint
        server.stop()

    def test_write_and_read_back_on_s3(self, s3_env, tmp_path):
        be = HybridBackend()
        be.do_connect(
            warehouse_path="s3://warehouse/wh",
            duckdb_path=str(tmp_path / "h.duckdb"),
            snapshot_dir=str(tmp_path / "snaps"),
            namespace="default",
        )
        assert be.is_remote is True

        be.create_table("sales", pa.table({"id": [1, 2, 3], "amount": [10, 20, 30]}))
        be.insert("sales", pa.table({"id": [4], "amount": [40]}), mode="append")

        rows = be.duckdb_con.raw_sql(
            "SELECT count(*) AS n, sum(amount) AS total FROM sales"
        ).fetchall()
        assert rows == [(4, 100)]

        table = be.catalog.load_table("default.sales")
        assert table.metadata_location.startswith("s3://warehouse/wh/")
        assert table.scan().to_arrow().num_rows == 4
