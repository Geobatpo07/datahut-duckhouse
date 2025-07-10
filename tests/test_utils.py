"""
Tests for flight_server.app.utils module.
"""
import os
import pytest
from unittest.mock import patch, Mock

from flight_server.app.utils import (
    get_duckdb_path,
    get_duckdb_connection,
    get_s3_filesystem,
    get_iceberg_warehouse_path,
    get_flight_client,
)


class TestDuckDBUtils:
    """Test DuckDB utility functions."""

    def test_get_duckdb_path_default(self):
        """Test get_duckdb_path with default path."""
        with patch.dict(os.environ, {}, clear=True):
            path = get_duckdb_path()
            assert path.endswith("ingestion/data/duckhouse.duckdb")

    def test_get_duckdb_path_custom(self):
        """Test get_duckdb_path with custom path."""
        custom_path = "/custom/path/test.duckdb"
        with patch.dict(os.environ, {'DUCKDB_PATH': custom_path}):
            path = get_duckdb_path()
            assert path == custom_path

    @patch('duckdb.connect')
    @patch('os.makedirs')
    def test_get_duckdb_connection(self, mock_makedirs, mock_connect):
        """Test get_duckdb_connection creates connection."""
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        with patch.dict(os.environ, {'DUCKDB_PATH': '/test/path.duckdb'}):
            result = get_duckdb_connection()
            
        mock_makedirs.assert_called_once()
        mock_connect.assert_called_once_with('/test/path.duckdb')
        assert result == mock_connection


class TestS3Utils:
    """Test S3/MinIO utility functions."""

    @patch('s3fs.S3FileSystem')
    def test_get_s3_filesystem(self, mock_s3fs):
        """Test get_s3_filesystem creates filesystem."""
        mock_fs = Mock()
        mock_s3fs.return_value = mock_fs
        
        with patch.dict(os.environ, {
            'AWS_ACCESS_KEY_ID': 'test_key',
            'AWS_SECRET_ACCESS_KEY': 'test_secret',
            'S3_ENDPOINT': 'http://localhost:9000'
        }):
            result = get_s3_filesystem()
            
        mock_s3fs.assert_called_once_with(
            key='test_key',
            secret='test_secret',
            client_kwargs={'endpoint_url': 'http://localhost:9000'}
        )
        assert result == mock_fs

    def test_get_iceberg_warehouse_path_default(self):
        """Test get_iceberg_warehouse_path with default value."""
        with patch.dict(os.environ, {}, clear=True):
            path = get_iceberg_warehouse_path()
            assert path == "s3://duckhouse-warehouse/"

    def test_get_iceberg_warehouse_path_custom(self):
        """Test get_iceberg_warehouse_path with custom value."""
        custom_path = "s3://my-warehouse/"
        with patch.dict(os.environ, {'ICEBERG_WAREHOUSE': custom_path}):
            path = get_iceberg_warehouse_path()
            assert path == custom_path


class TestFlightUtils:
    """Test Arrow Flight utility functions."""

    @patch('xorq.flight.client.FlightClient')
    def test_get_flight_client_default(self, mock_client_class):
        """Test get_flight_client with default host and port."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        with patch.dict(os.environ, {}, clear=True):
            result = get_flight_client()
            
        mock_client_class.assert_called_once_with("grpc://localhost:8815")
        assert result == mock_client

    @patch('xorq.flight.client.FlightClient')
    def test_get_flight_client_custom(self, mock_client_class):
        """Test get_flight_client with custom host and port."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        with patch.dict(os.environ, {
            'FLIGHT_SERVER_HOST': 'custom-host',
            'FLIGHT_SERVER_PORT': '9999'
        }):
            result = get_flight_client()
            
        mock_client_class.assert_called_once_with("grpc://custom-host:9999")
        assert result == mock_client
