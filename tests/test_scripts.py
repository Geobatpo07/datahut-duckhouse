"""
Tests for scripts/ingest_flight.py and scripts/pipeline.py.

Both scripts were fixed as a follow-up to ADR-0001: they referenced a
`target=` parameter removed from HybridBackend.create_table/insert, and
pipeline.py additionally built a raw pyarrow FlightDescriptor.for_path(...)
that the real server's do_put (FlightServerDelegate, which expects a
"command"-style descriptor) can't parse. Both now go through
datahut_duckhouse.connection.get_connection(), the same verified path the
CLI uses. Only that network boundary is mocked here.
"""
import importlib.util
import os
import sys
from pathlib import Path
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.csv as pa_csv
import pytest

REPO_ROOT = Path(__file__).parent.parent


def _load_script_module(name: str, relative_path: str):
    """Import a scripts/*.py file as a module without running its
    `if __name__ == "__main__"` block."""
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def sample_csv(tmp_path):
    csv_path = tmp_path / "sample.csv"
    table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    pa_csv.write_csv(table, str(csv_path))
    return str(csv_path)


class TestIngestFlightScript:
    def test_main_creates_table_via_verified_connection(self, sample_csv, monkeypatch, capsys):
        monkeypatch.setenv("CSV_PATH", sample_csv)
        monkeypatch.setenv("FLIGHT_TABLE_NAME", "diseases")

        module = _load_script_module("ingest_flight_test", "scripts/ingest_flight.py")

        mock_con = Mock()
        with patch.object(module, "get_connection", return_value=mock_con):
            module.main()

        mock_con.create_table.assert_called_once()
        called_name, called_data = mock_con.create_table.call_args[0]
        assert called_name == "diseases"
        assert called_data.num_rows == 3

    def test_main_handles_missing_csv_without_connecting(self, monkeypatch, tmp_path):
        monkeypatch.setenv("CSV_PATH", str(tmp_path / "does_not_exist.csv"))

        module = _load_script_module("ingest_flight_test_missing", "scripts/ingest_flight.py")

        with patch.object(module, "get_connection") as mock_get_connection:
            module.main()

        mock_get_connection.assert_not_called()


class TestPipelineScript:
    def test_ingest_data_creates_table_via_verified_connection(self, sample_csv, monkeypatch):
        monkeypatch.setenv("CSV_PATH", sample_csv)
        monkeypatch.setenv("FLIGHT_TABLE_NAME", "diseases")

        module = _load_script_module("pipeline_test", "scripts/pipeline.py")

        mock_con = Mock()
        with patch.object(module, "get_connection", return_value=mock_con):
            module.ingest_data()

        mock_con.create_table.assert_called_once()
        called_name, called_data = mock_con.create_table.call_args[0]
        assert called_name == "diseases"
        assert called_data.num_rows == 3

    def test_ingest_data_raises_on_missing_csv(self, monkeypatch, tmp_path):
        monkeypatch.setenv("CSV_PATH", str(tmp_path / "does_not_exist.csv"))

        module = _load_script_module("pipeline_test_missing", "scripts/pipeline.py")

        with patch.object(module, "get_connection") as mock_get_connection:
            with pytest.raises(FileNotFoundError):
                module.ingest_data()

        mock_get_connection.assert_not_called()

    def test_no_flight_target_branching_remains(self):
        """Guard against regressing to the removed target=/FLIGHT_TARGET
        pattern (ADR-0001): every write now targets Iceberg unconditionally."""
        source = (REPO_ROOT / "scripts" / "pipeline.py").read_text()
        assert "FLIGHT_TARGET" not in source
        assert "target=" not in source
