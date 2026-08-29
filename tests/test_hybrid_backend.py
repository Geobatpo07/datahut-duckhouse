"""
Tests for flight_server.app.backends.hybrid_backend module.
"""
import os
import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from flight_server.app.backends.hybrid_backend import HybridBackend


class TestHybridBackend:
    """Test HybridBackend class."""

    @patch('flight_server.app.backends.hybrid_backend.xo')
    @patch('flight_server.app.backends.hybrid_backend.PyIcebergBackend')
    def test_init_with_warehouse_path(self, mock_parent, mock_xo):
        """Test HybridBackend initialization with warehouse path."""
        warehouse_path = "s3://test-warehouse/"
        
        # Mock parent class
        mock_parent.__init__ = Mock(return_value=None)
        
        # Create instance
        backend = HybridBackend(warehouse_path=warehouse_path)
        
        # Verify parent initialization
        mock_parent.__init__.assert_called_once()
        assert backend.duckdb_path is None
        assert backend.snapshot_dir is None

    @patch('flight_server.app.backends.hybrid_backend.xo')
    @patch('flight_server.app.backends.hybrid_backend.PyIcebergBackend')
    def test_init_without_warehouse_path(self, mock_parent, mock_xo):
        """Test HybridBackend initialization without warehouse path."""
        mock_parent.__init__ = Mock(return_value=None)
        
        backend = HybridBackend()
        
        mock_parent.__init__.assert_called_once()
        assert backend.duckdb_path is None
        assert backend.snapshot_dir is None

    @patch('flight_server.app.backends.hybrid_backend.shutil')
    @patch('flight_server.app.backends.hybrid_backend.Path')
    @patch('flight_server.app.backends.hybrid_backend.xo')
    def test_do_connect(self, mock_xo, mock_path, mock_shutil):
        """Test do_connect method."""
        # Setup mocks
        mock_path_instance = Mock()
        mock_path_instance.absolute.return_value = mock_path_instance
        mock_path_instance.mkdir = Mock()
        mock_path.return_value = mock_path_instance
        
        mock_connection = Mock()
        mock_xo.duckdb.connect.return_value = mock_connection
        
        # Create backend instance
        backend = HybridBackend()
        backend.do_connect = Mock(wraps=backend.do_connect)
        
        # Mock parent methods
        with patch.object(backend, '_setup_duckdb_connection'), \
             patch.object(backend, '_reflect_views'), \
             patch.object(backend, '_create_snapshot'), \
             patch('flight_server.app.backends.hybrid_backend.PyIcebergBackend.do_connect'):
            
            # Call do_connect
            backend.do_connect(
                warehouse_path="s3://test-warehouse/",
                duckdb_path="/custom/path.duckdb",
                snapshot_dir="/custom/snapshots",
                namespace="test",
                catalog_name="test_catalog"
            )
            
            # Verify setup
            assert backend.duckdb_path == "/custom/path.duckdb"
            mock_path_instance.mkdir.assert_called_once_with(parents=True, exist_ok=True)
            mock_xo.duckdb.connect.assert_called_once_with("/custom/path.duckdb")

    def test_create_table_always_writes_iceberg(self):
        """Per ADR-0001, create_table has no target parameter: every write goes to
        Iceberg unconditionally, DuckDB never receives a durable table directly."""
        backend = HybridBackend()
        backend._reflect_views = Mock()
        backend._create_snapshot = Mock()

        with patch('flight_server.app.backends.hybrid_backend.PyIcebergBackend.create_table') as mock_parent:
            mock_parent.return_value = True

            result = backend.create_table("test_table", "test_data")

            mock_parent.assert_called_once_with("test_table", "test_data")
            backend._reflect_views.assert_called_once()
            backend._create_snapshot.assert_called_once()
            assert result is True

    def test_insert_always_writes_iceberg(self):
        """Per ADR-0001, insert has no target parameter: every write goes to Iceberg
        unconditionally."""
        backend = HybridBackend()
        backend._reflect_views = Mock()
        backend._create_snapshot = Mock()

        with patch('flight_server.app.backends.hybrid_backend.PyIcebergBackend.insert') as mock_parent:
            mock_parent.return_value = True

            result = backend.insert("test_table", "test_data", mode="append")

            mock_parent.assert_called_once_with("test_table", "test_data", mode="append")
            backend._reflect_views.assert_called_once()
            backend._create_snapshot.assert_called_once()
            assert result is True

    def test_reflect_views(self):
        """Test _reflect_views method."""
        backend = HybridBackend()
        backend.catalog = Mock()
        backend.namespace = "test_namespace"
        backend.warehouse_path = "s3://test-warehouse/"
        backend.duckdb_con = Mock()
        
        # Mock catalog tables
        backend.catalog.list_tables.return_value = [
            ("test_namespace", "table1"),
            ("test_namespace", "table-with-dash")
        ]
        
        backend._reflect_views()
        
        # Verify SQL commands were executed
        expected_calls = [
            f"""
                CREATE OR REPLACE VIEW table1 AS
                SELECT * FROM iceberg_scan(
                    's3://test-warehouse/test_namespace.db/table1',
                    version='?',
                    allow_moved_paths=true
                );
            """,
            f"""
                CREATE OR REPLACE VIEW "table-with-dash" AS
                SELECT * FROM iceberg_scan(
                    's3://test-warehouse/test_namespace.db/table-with-dash',
                    version='?',
                    allow_moved_paths=true
                );
            """
        ]
        
        assert backend.duckdb_con.raw_sql.call_count == 2

    def test_setup_duckdb_connection(self):
        """Test _setup_duckdb_connection method."""
        backend = HybridBackend()
        backend.duckdb_con = Mock()
        
        backend._setup_duckdb_connection()
        
        expected_commands = [
            "INSTALL iceberg;",
            "LOAD iceberg;",
            "SET unsafe_enable_version_guessing=true;",
        ]
        
        for cmd in expected_commands:
            backend.duckdb_con.raw_sql.assert_any_call(cmd)

    @patch('flight_server.app.backends.hybrid_backend.shutil')
    @patch('flight_server.app.backends.hybrid_backend.datetime')
    def test_create_snapshot(self, mock_datetime, mock_shutil):
        """Test _create_snapshot method."""
        backend = HybridBackend()
        backend.duckdb_con = Mock()
        backend.duckdb_path = "/test/path.duckdb"
        backend.snapshot_dir = Path("/test/snapshots")
        backend.duckdb_con.tables = ["table1", "table2"]
        
        # Mock datetime
        mock_datetime.datetime.now.return_value.strftime.return_value = "20231225_120000"
        mock_datetime.UTC = Mock()
        
        backend._create_snapshot()
        
        # Verify snapshot table creation
        expected_calls = [
            "CREATE OR REPLACE TABLE table1_snapshot AS SELECT * FROM table1;",
            "CREATE OR REPLACE TABLE table2_snapshot AS SELECT * FROM table2;",
            "CHECKPOINT '/test/path.duckdb';",
        ]
        
        for cmd in expected_calls:
            backend.duckdb_con.raw_sql.assert_any_call(cmd)
        
        # Verify file copy
        mock_shutil.copy.assert_called_once_with(
            "/test/path.duckdb",
            "/test/snapshots/20231225_120000.duckdb"
        )
