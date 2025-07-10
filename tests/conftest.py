"""
Pytest configuration and fixtures for DataHut-DuckHouse tests.
"""
import os
import tempfile
from pathlib import Path
from typing import Generator

import pytest
import duckdb
import pandas as pd
from unittest.mock import Mock, patch


@pytest.fixture(scope="session")
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture
def test_duckdb_path(temp_dir: Path) -> str:
    """Create a temporary DuckDB path for testing."""
    return str(temp_dir / "test.duckdb")


@pytest.fixture
def sample_data() -> pd.DataFrame:
    """Create sample data for testing."""
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 28, 32],
        'city': ['New York', 'London', 'Paris', 'Tokyo', 'Sydney']
    })


@pytest.fixture
def sample_csv_path(temp_dir: Path, sample_data: pd.DataFrame) -> str:
    """Create a sample CSV file for testing."""
    csv_path = temp_dir / "sample.csv"
    sample_data.to_csv(csv_path, index=False)
    return str(csv_path)


@pytest.fixture
def mock_env_vars():
    """Mock environment variables for testing."""
    with patch.dict(os.environ, {
        'DUCKDB_PATH': 'test.duckdb',
        'FLIGHT_SERVER_HOST': 'localhost',
        'FLIGHT_SERVER_PORT': '8815',
        'AWS_ACCESS_KEY_ID': 'test_key',
        'AWS_SECRET_ACCESS_KEY': 'test_secret',
        'S3_ENDPOINT': 'http://localhost:9000',
        'ICEBERG_WAREHOUSE': 's3://test-warehouse/',
        'ICEBERG_NAMESPACE': 'test',
        'ICEBERG_CATALOG': 'test_catalog'
    }):
        yield


@pytest.fixture
def mock_s3_client():
    """Mock S3 client for testing."""
    with patch('boto3.client') as mock_client:
        mock_s3 = Mock()
        mock_client.return_value = mock_s3
        yield mock_s3


@pytest.fixture
def mock_flight_client():
    """Mock Arrow Flight client for testing."""
    with patch('xorq.flight.client.FlightClient') as mock_client:
        yield mock_client


@pytest.fixture
def duckdb_connection(test_duckdb_path: str) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Create a DuckDB connection for testing."""
    conn = duckdb.connect(test_duckdb_path)
    yield conn
    conn.close()
