"""
Tests for datahut_duckhouse.cli (the dhd CLI, Phase 2 — see docs/ROADMAP.md).

These mock only `get_connection` (the network boundary to the Flight
server) — everything else (argument parsing, file reading, command
dispatch) runs for real through click's CliRunner, against the real click
package rather than a mocked one.
"""
import os
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.csv as pa_csv
from click.testing import CliRunner

from datahut_duckhouse.cli import cli


class TestCreateTableAndInsert:
    """create-table and insert both funnel into the same upload call."""

    def _write_sample_csv(self, path):
        table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        pa_csv.write_csv(table, path)

    @patch("datahut_duckhouse.cli.get_connection")
    def test_create_table(self, mock_get_connection, tmp_path):
        mock_con = Mock()
        mock_get_connection.return_value = mock_con

        csv_path = tmp_path / "sample.csv"
        self._write_sample_csv(str(csv_path))

        runner = CliRunner()
        result = runner.invoke(cli, ["create-table", "my_table", str(csv_path)])

        assert result.exit_code == 0
        assert "my_table" in result.output
        assert "3 lignes" in result.output
        mock_con.create_table.assert_called_once()
        called_name, called_data = mock_con.create_table.call_args[0]
        assert called_name == "my_table"
        assert called_data.num_rows == 3

    @patch("datahut_duckhouse.cli.get_connection")
    def test_insert_uses_same_upload_path_as_create_table(self, mock_get_connection, tmp_path):
        """Per the CLI's own docstring: insert calls create_table client-side too —
        the server (not the client) decides create vs insert based on existence."""
        mock_con = Mock()
        mock_get_connection.return_value = mock_con

        csv_path = tmp_path / "sample.csv"
        self._write_sample_csv(str(csv_path))

        runner = CliRunner()
        result = runner.invoke(cli, ["insert", "my_table", str(csv_path)])

        assert result.exit_code == 0
        mock_con.create_table.assert_called_once()

    @patch("datahut_duckhouse.cli.get_connection")
    def test_create_table_rejects_unsupported_format(self, mock_get_connection, tmp_path):
        txt_path = tmp_path / "sample.txt"
        txt_path.write_text("not a real source")

        runner = CliRunner()
        result = runner.invoke(cli, ["create-table", "my_table", str(txt_path)])

        assert result.exit_code != 0
        assert "non supporté" in result.output
        mock_get_connection.assert_not_called()


class TestQuery:
    @patch("datahut_duckhouse.cli.get_connection")
    def test_query_prints_dataframe(self, mock_get_connection):
        import pandas as pd

        mock_con = Mock()
        mock_con.to_pandas.return_value = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        mock_get_connection.return_value = mock_con

        runner = CliRunner()
        result = runner.invoke(cli, ["query", "SELECT * FROM my_table"])

        assert result.exit_code == 0
        mock_con.sql.assert_called_once_with("SELECT * FROM my_table")
        mock_con.to_pandas.assert_called_once()
        assert "id" in result.output


class TestListTables:
    @patch("datahut_duckhouse.cli.get_connection")
    def test_list_tables_with_results(self, mock_get_connection):
        mock_con = Mock()
        mock_con.list_tables.return_value = ["table_a", "table_b"]
        mock_get_connection.return_value = mock_con

        runner = CliRunner()
        result = runner.invoke(cli, ["list-tables"])

        assert result.exit_code == 0
        assert "table_a" in result.output
        assert "table_b" in result.output

    @patch("datahut_duckhouse.cli.get_connection")
    def test_list_tables_empty(self, mock_get_connection):
        mock_con = Mock()
        mock_con.list_tables.return_value = []
        mock_get_connection.return_value = mock_con

        runner = CliRunner()
        result = runner.invoke(cli, ["list-tables"])

        assert result.exit_code == 0
        assert "Aucune table" in result.output


class TestBranchStub:
    """Branches aren't wired yet (Nessie is Phase 3) — these must fail clearly,
    not silently no-op, and must never touch the network."""

    @patch("datahut_duckhouse.cli.get_connection")
    def test_branch_create_not_ready(self, mock_get_connection):
        runner = CliRunner()
        result = runner.invoke(cli, ["branch", "create", "my-branch"])

        assert result.exit_code == 1
        assert "pas encore" in result.output
        mock_get_connection.assert_not_called()

    @patch("datahut_duckhouse.cli.get_connection")
    def test_branch_list_not_ready(self, mock_get_connection):
        runner = CliRunner()
        result = runner.invoke(cli, ["branch", "list"])

        assert result.exit_code == 1
        mock_get_connection.assert_not_called()


class TestConnectionEnvVars:
    def test_get_connection_reads_env_vars(self):
        from datahut_duckhouse.connection import get_connection

        with patch("datahut_duckhouse.connection.FlightBackend") as mock_backend_cls:
            mock_backend = Mock()
            mock_backend_cls.return_value = mock_backend

            with patch.dict(
                os.environ,
                {"FLIGHT_SERVER_HOST": "custom-host", "FLIGHT_SERVER_PORT": "9999"},
            ):
                result = get_connection()

            mock_backend.do_connect.assert_called_once_with(host="custom-host", port=9999)
            assert result == mock_backend
