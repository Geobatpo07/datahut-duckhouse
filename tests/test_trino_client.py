"""
Tests for Trino client integration.
"""

from unittest.mock import Mock, patch

import pandas as pd

from flight_server.app.trino_client import TrinoClient, TrinoConfig, get_trino_client


class TestTrinoConfig:
    """Test TrinoConfig dataclass."""

    def test_default_config(self):
        """Test default configuration values."""
        config = TrinoConfig()

        assert config.host == "localhost"
        assert config.port == 8080
        assert config.user == "datahut"
        assert config.catalog == "iceberg"
        assert config.schema == "default"
        assert config.source == "datahut-duckhouse"
        assert "datahut" in config.client_tags
        assert "iceberg" in config.client_tags

    def test_custom_config(self):
        """Test custom configuration values."""
        config = TrinoConfig(
            host="custom-host",
            port=9999,
            user="custom-user",
            catalog="custom-catalog",
            schema="custom-schema",
        )

        assert config.host == "custom-host"
        assert config.port == 9999
        assert config.user == "custom-user"
        assert config.catalog == "custom-catalog"
        assert config.schema == "custom-schema"


class TestTrinoClient:
    """Test TrinoClient functionality."""

    @patch("flight_server.app.trino_client.trino")
    def test_connection_creation(self, mock_trino):
        """Test connection creation."""
        mock_connection = Mock()
        mock_trino.dbapi.connect.return_value = mock_connection

        config = TrinoConfig()
        client = TrinoClient(config)

        # Access connection property
        connection = client.connection

        mock_trino.dbapi.connect.assert_called_once_with(
            host=config.host,
            port=config.port,
            user=config.user,
            catalog=config.catalog,
            schema=config.schema,
            auth=config.auth,
            source=config.source,
            client_tags=config.client_tags,
            request_timeout=60,
        )

        assert connection == mock_connection

    @patch("flight_server.app.trino_client.trino")
    def test_execute_query_success(self, mock_trino):
        """Test successful query execution."""
        # Mock connection and cursor
        mock_cursor = Mock()
        mock_cursor.description = [["col1"], ["col2"]]
        mock_cursor.fetchall.return_value = [["val1", "val2"], ["val3", "val4"]]

        mock_connection = Mock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        mock_trino.dbapi.connect.return_value = mock_connection

        client = TrinoClient()

        # Mock the monitoring decorator
        with patch("flight_server.app.trino_client.trace_operation"):
            result = client.execute_query("SELECT * FROM test_table")

        # Verify cursor calls
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test_table")
        mock_cursor.fetchall.assert_called_once()

        # Verify result
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == ["col1", "col2"]

    @patch("flight_server.app.trino_client.trino")
    def test_execute_query_with_parameters(self, mock_trino):
        """Test query execution with parameters."""
        mock_cursor = Mock()
        mock_cursor.description = [["count"]]
        mock_cursor.fetchall.return_value = [[5]]

        mock_connection = Mock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        mock_trino.dbapi.connect.return_value = mock_connection

        client = TrinoClient()

        with patch("flight_server.app.trino_client.trace_operation"):
            client.execute_query(
                "SELECT COUNT(*) FROM test_table WHERE id = ?", parameters={"id": 123}
            )

        mock_cursor.execute.assert_called_once_with(
            "SELECT COUNT(*) FROM test_table WHERE id = ?", {"id": 123}
        )

    @patch("flight_server.app.trino_client.trino")
    def test_list_catalogs(self, mock_trino):
        """Test listing catalogs."""
        mock_cursor = Mock()
        mock_cursor.description = [["Catalog"]]
        mock_cursor.fetchall.return_value = [["iceberg"], ["memory"], ["system"]]

        mock_connection = Mock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        mock_trino.dbapi.connect.return_value = mock_connection

        client = TrinoClient()

        with patch("flight_server.app.trino_client.trace_operation"):
            catalogs = client.list_catalogs()

        mock_cursor.execute.assert_called_once_with("SHOW CATALOGS")
        assert catalogs == ["iceberg", "memory", "system"]

    @patch("flight_server.app.trino_client.trino")
    def test_list_schemas(self, mock_trino):
        """Test listing schemas."""
        mock_cursor = Mock()
        mock_cursor.description = [["Schema"]]
        mock_cursor.fetchall.return_value = [["default"], ["test"]]

        mock_connection = Mock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        mock_trino.dbapi.connect.return_value = mock_connection

        client = TrinoClient()

        with patch("flight_server.app.trino_client.trace_operation"):
            schemas = client.list_schemas("iceberg")

        mock_cursor.execute.assert_called_once_with("SHOW SCHEMAS FROM iceberg")
        assert schemas == ["default", "test"]

    @patch("flight_server.app.trino_client.trino")
    def test_list_tables(self, mock_trino):
        """Test listing tables."""
        mock_cursor = Mock()
        mock_cursor.description = [["Table"]]
        mock_cursor.fetchall.return_value = [["patient_data"], ["disease_trends"]]

        mock_connection = Mock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        mock_trino.dbapi.connect.return_value = mock_connection

        client = TrinoClient()

        with patch("flight_server.app.trino_client.trace_operation"):
            tables = client.list_tables("iceberg", "default")

        mock_cursor.execute.assert_called_once_with("SHOW TABLES FROM iceberg.default")
        assert tables == ["patient_data", "disease_trends"]

    @patch("flight_server.app.trino_client.trino")
    def test_create_table_as_select(self, mock_trino):
        """Test creating table as select."""
        mock_cursor = Mock()
        mock_cursor.description = None
        mock_cursor.fetchall.return_value = []

        mock_connection = Mock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        mock_trino.dbapi.connect.return_value = mock_connection

        client = TrinoClient()

        with patch("flight_server.app.trino_client.trace_operation"):
            result = client.create_table_as_select(
                target_table="new_table",
                source_query="SELECT * FROM existing_table",
                table_properties={"format": "PARQUET"},
            )

        mock_cursor.execute.assert_called_once()
        assert result is True

    @patch("flight_server.app.trino_client.trino")
    def test_optimize_table(self, mock_trino):
        """Test table optimization."""
        mock_cursor = Mock()
        mock_cursor.description = None
        mock_cursor.fetchall.return_value = []

        mock_connection = Mock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        mock_trino.dbapi.connect.return_value = mock_connection

        client = TrinoClient()

        with patch("flight_server.app.trino_client.trace_operation"):
            result = client.optimize_table("test_table")

        mock_cursor.execute.assert_called_once_with(
            "CALL iceberg.system.rewrite_data_files('default', 'test_table')"
        )
        assert result is True

    def test_close_connection(self):
        """Test closing connection."""
        mock_connection = Mock()
        client = TrinoClient()
        client._connection = mock_connection

        client.close()

        mock_connection.close.assert_called_once()
        assert client._connection is None


class TestUtilityFunctions:
    """Test utility functions."""

    @patch("flight_server.app.trino_client.TrinoClient")
    def test_get_trino_client_default(self, mock_client_class):
        """Test getting Trino client with default configuration."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        with patch.dict("os.environ", {}, clear=True):
            get_trino_client()

        mock_client_class.assert_called_once()
        config = mock_client_class.call_args[0][0]
        assert config.host == "localhost"
        assert config.port == 8080
        assert config.user == "datahut"
        assert config.catalog == "iceberg"
        assert config.schema == "default"

    @patch("flight_server.app.trino_client.TrinoClient")
    def test_get_trino_client_custom_env(self, mock_client_class):
        """Test getting Trino client with custom environment variables."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        with patch.dict(
            "os.environ",
            {
                "TRINO_HOST": "custom-host",
                "TRINO_PORT": "9999",
                "TRINO_USER": "custom-user",
                "TRINO_CATALOG": "custom-catalog",
                "TRINO_SCHEMA": "custom-schema",
            },
        ):
            get_trino_client()

        config = mock_client_class.call_args[0][0]
        assert config.host == "custom-host"
        assert config.port == 9999
        assert config.user == "custom-user"
        assert config.catalog == "custom-catalog"
        assert config.schema == "custom-schema"

    @patch("flight_server.app.trino_client.get_trino_client")
    def test_query_iceberg_table(self, mock_get_client):
        """Test querying Iceberg table utility function."""
        mock_client = Mock()
        mock_client.config.catalog = "iceberg"
        mock_client.config.schema = "default"
        mock_client.execute_query.return_value = pd.DataFrame({"col1": [1, 2, 3]})

        mock_get_client.return_value = mock_client

        from flight_server.app.trino_client import query_iceberg_table

        result = query_iceberg_table("test_table", limit=10)

        mock_client.execute_query.assert_called_once_with(
            "SELECT * FROM iceberg.default.test_table LIMIT 10"
        )
        mock_client.close.assert_called_once()
        assert len(result) == 3
