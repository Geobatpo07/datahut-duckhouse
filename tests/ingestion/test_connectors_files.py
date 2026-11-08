"""Connector test (task 4): the ``files`` family against fixed, local,
no-network data -- both registry sources, plus the HTTP-fetch path (served
by a local ``http.server`` in this test, never a real external URL, so this
stays deterministic and network-free)."""

from __future__ import annotations

import http.server
import threading

import pytest

from ingestion.config.models import SourceConfig
from ingestion.config.registry import get_source
from ingestion.connectors import files


class TestFilesConnectorAgainstFixedData:
    def test_csv_source(self, tmp_path):
        import pyarrow.parquet as papq

        source = get_source("disease_symptoms_csv")
        written = files.build_resources(source, work_dir=tmp_path)
        (path,) = written.values()
        table = papq.read_table(path)
        assert table.num_rows == 21369
        assert "Disease" in table.column_names

    def test_json_source(self, tmp_path):
        import pyarrow.parquet as papq

        source = get_source("sample_products_json")
        written = files.build_resources(source, work_dir=tmp_path)
        (path,) = written.values()
        table = papq.read_table(path)
        assert table.num_rows == 200
        assert set(table.column_names) >= {
            "product_id",
            "name",
            "category",
            "price",
            "in_stock",
        }

    def test_nested_json_is_flattened(self, tmp_path):
        import json

        import pyarrow.parquet as papq

        records = [
            {"id": 1, "address": {"city": "Paris", "zip": "75001"}},
            {"id": 2, "address": {"city": "Lyon", "zip": "69001"}},
        ]
        source_path = tmp_path / "nested.json"
        source_path.write_text(json.dumps(records))

        source = SourceConfig(
            name="nested_test",
            family="files",
            connector="local_file",
            path=str(source_path),
            format="json",
        )
        written = files.build_resources(source, work_dir=tmp_path / "out")
        (out_path,) = written.values()
        table = papq.read_table(out_path)
        assert set(table.column_names) == {"id", "address_city", "address_zip"}
        assert table.column("address_city").to_pylist() == ["Paris", "Lyon"]


class TestFilesConnectorHttpFetch:
    """The ``http_url`` path (download-then-read), exercised against a local
    server -- not a real external URL, so this test stays deterministic."""

    @pytest.fixture
    def local_csv_server(self, tmp_path):
        served_dir = tmp_path / "served"
        served_dir.mkdir()
        (served_dir / "data.csv").write_text("id,name\n1,Alice\n2,Bob\n")

        handler = lambda *a, **kw: http.server.SimpleHTTPRequestHandler(  # noqa: E731
            *a, directory=str(served_dir), **kw
        )
        server = http.server.HTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}/data.csv"
        finally:
            server.shutdown()
            thread.join(timeout=5)

    def test_http_url_is_downloaded_then_read(self, local_csv_server, tmp_path):
        import pyarrow.parquet as papq

        source = SourceConfig(
            name="http_test",
            family="files",
            connector="local_file",
            http_url=local_csv_server,
            format="csv",
        )
        written = files.build_resources(source, work_dir=tmp_path)
        (path,) = written.values()
        table = papq.read_table(path)
        assert table.to_pylist() == [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
