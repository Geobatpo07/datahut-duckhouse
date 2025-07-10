"""
Tests for the query orchestrator.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

from flight_server.app.query_orchestrator import (
    QueryOrchestrator, 
    QueryMetrics, 
    QueryType, 
    DataSource,
    get_query_orchestrator
)


class TestQueryMetrics:
    """Test QueryMetrics dataclass."""
    
    def test_default_values(self):
        """Test default values."""
        metrics = QueryMetrics()
        assert metrics.estimated_rows == 0
        assert metrics.table_count == 0
        assert metrics.has_joins is False
        assert metrics.has_aggregations is False
        assert metrics.has_subqueries is False
        assert metrics.complexity_score == 0.0
        assert metrics.query_type == QueryType.SIMPLE_SELECT


class TestQueryOrchestrator:
    """Test QueryOrchestrator functionality."""
    
    def test_init(self):
        """Test orchestrator initialization."""
        orchestrator = QueryOrchestrator()
        
        assert orchestrator.row_threshold == 1_000_000
        assert orchestrator.complexity_threshold == 5.0
        assert orchestrator.join_threshold == 3
        assert isinstance(orchestrator.table_registry, dict)
        
        # Check table registry
        assert "rev" in orchestrator.table_registry
        assert orchestrator.table_registry["rev"] == DataSource.DUCKDB
        assert "patient_data" in orchestrator.table_registry
        assert orchestrator.table_registry["patient_data"] == DataSource.ICEBERG
    
    def test_analyze_query_simple(self):
        """Test query analysis for simple queries."""
        orchestrator = QueryOrchestrator()
        
        query = "SELECT * FROM local_patients"
        metrics = orchestrator._analyze_query(query)
        
        assert metrics.table_count == 1
        assert metrics.has_joins is False
        assert metrics.has_aggregations is False
        assert metrics.has_subqueries is False
        assert metrics.query_type == QueryType.SIMPLE_SELECT
        assert metrics.complexity_score == 1.0  # 1 table * 1.0
    
    def test_analyze_query_with_joins(self):
        """Test query analysis with joins."""
        orchestrator = QueryOrchestrator()
        
        query = "SELECT * FROM patients p JOIN diseases d ON p.disease_id = d.id"
        metrics = orchestrator._analyze_query(query)
        
        assert metrics.table_count == 2
        assert metrics.has_joins is True
        assert metrics.has_aggregations is False
        assert metrics.query_type == QueryType.JOIN
        assert metrics.complexity_score == 4.0  # 2 tables + 2 for joins
    
    def test_analyze_query_with_aggregations(self):
        """Test query analysis with aggregations."""
        orchestrator = QueryOrchestrator()
        
        query = "SELECT disease, COUNT(*) FROM patients GROUP BY disease"
        metrics = orchestrator._analyze_query(query)
        
        assert metrics.table_count == 1
        assert metrics.has_joins is False
        assert metrics.has_aggregations is True
        assert metrics.query_type == QueryType.AGGREGATION
        assert metrics.complexity_score == 2.5  # 1 table + 1.5 for aggregations
    
    def test_analyze_query_analytical(self):
        """Test query analysis for analytical queries."""
        orchestrator = QueryOrchestrator()
        
        query = "SELECT disease, COUNT(*), AVG(age) FROM patients p JOIN outcomes o ON p.id = o.patient_id GROUP BY disease"
        metrics = orchestrator._analyze_query(query)
        
        assert metrics.table_count == 2
        assert metrics.has_joins is True
        assert metrics.has_aggregations is True
        assert metrics.query_type == QueryType.ANALYTICAL
        assert metrics.complexity_score == 5.5  # 2 tables + 2 joins + 1.5 aggregations
    
    def test_analyze_query_complex(self):
        """Test query analysis for complex queries."""
        orchestrator = QueryOrchestrator()
        
        query = """
        SELECT disease, 
               COUNT(*) OVER (PARTITION BY disease) as disease_count,
               AVG(age) OVER (PARTITION BY gender) as avg_age_by_gender
        FROM patients 
        WHERE id IN (SELECT patient_id FROM outcomes WHERE result = 'positive')
        """
        metrics = orchestrator._analyze_query(query)
        
        assert metrics.has_subqueries is True
        assert metrics.complexity_score > 3.0  # Should be classified as complex
    
    @patch('flight_server.app.query_orchestrator.get_duckdb_connection')
    def test_estimate_data_size_duckdb(self, mock_get_conn):
        """Test data size estimation for DuckDB tables."""
        mock_conn = Mock()
        mock_conn.execute.return_value.fetchone.return_value = (5000,)
        mock_get_conn.return_value = mock_conn
        
        orchestrator = QueryOrchestrator()
        size = orchestrator._estimate_data_size("SELECT * FROM rev", ["rev"])
        
        assert size == 5000
        mock_conn.execute.assert_called_with("SELECT COUNT(*) FROM rev")
    
    @patch('flight_server.app.query_orchestrator.get_trino_client')
    def test_estimate_data_size_iceberg(self, mock_get_client):
        """Test data size estimation for Iceberg tables."""
        mock_client = Mock()
        mock_stats = pd.DataFrame({
            'Column Name': [''],
            'Row Count': [1000000]
        })
        mock_client.get_table_stats.return_value = mock_stats
        mock_get_client.return_value = mock_client
        
        orchestrator = QueryOrchestrator()
        size = orchestrator._estimate_data_size("SELECT * FROM patient_data", ["patient_data"])
        
        assert size == 1000000
        mock_client.get_table_stats.assert_called_with("patient_data")
    
    def test_should_use_trino_iceberg_table(self):
        """Test that Iceberg tables always use Trino."""
        orchestrator = QueryOrchestrator()
        metrics = QueryMetrics()
        
        result = orchestrator._should_use_trino("SELECT * FROM patient_data", metrics)
        assert result is True
    
    def test_should_use_trino_large_dataset(self):
        """Test that large datasets use Trino."""
        orchestrator = QueryOrchestrator()
        metrics = QueryMetrics(estimated_rows=2_000_000)
        
        with patch.object(orchestrator, '_estimate_data_size', return_value=2_000_000):
            result = orchestrator._should_use_trino("SELECT * FROM unknown_table", metrics)
        
        assert result is True
    
    def test_should_use_trino_complex_query(self):
        """Test that complex queries use Trino."""
        orchestrator = QueryOrchestrator()
        metrics = QueryMetrics(complexity_score=6.0)
        
        with patch.object(orchestrator, '_estimate_data_size', return_value=50_000):
            result = orchestrator._should_use_trino("SELECT * FROM unknown_table", metrics)
        
        assert result is True
    
    def test_should_use_duckdb_simple_query(self):
        """Test that simple queries use DuckDB."""
        orchestrator = QueryOrchestrator()
        metrics = QueryMetrics(
            estimated_rows=1000,
            complexity_score=1.0,
            table_count=1
        )
        
        with patch.object(orchestrator, '_estimate_data_size', return_value=1000):
            result = orchestrator._should_use_trino("SELECT * FROM local_table", metrics)
        
        assert result is False
    
    @patch('flight_server.app.query_orchestrator.get_trino_client')
    @patch('flight_server.app.query_orchestrator.get_duckdb_connection')
    def test_execute_query_trino(self, mock_get_duckdb, mock_get_trino):
        """Test query execution via Trino."""
        mock_trino_client = Mock()
        mock_trino_client.execute_query.return_value = pd.DataFrame({'count': [1000]})
        mock_get_trino.return_value = mock_trino_client
        
        orchestrator = QueryOrchestrator()
        
        with patch.object(orchestrator, '_should_use_trino', return_value=True):
            result = orchestrator.execute_query("SELECT COUNT(*) FROM patient_data")
        
        mock_trino_client.execute_query.assert_called_once_with("SELECT COUNT(*) FROM patient_data")
        assert len(result) == 1
        assert result.iloc[0]['count'] == 1000
    
    @patch('flight_server.app.query_orchestrator.get_trino_client')
    @patch('flight_server.app.query_orchestrator.get_duckdb_connection')
    def test_execute_query_duckdb(self, mock_get_duckdb, mock_get_trino):
        """Test query execution via DuckDB."""
        mock_duckdb_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.description = [('count',)]
        mock_cursor.fetchall.return_value = [(500,)]
        mock_duckdb_conn.execute.return_value = mock_cursor
        mock_get_duckdb.return_value = mock_duckdb_conn
        
        orchestrator = QueryOrchestrator()
        
        with patch.object(orchestrator, '_should_use_trino', return_value=False):
            result = orchestrator.execute_query("SELECT COUNT(*) FROM local_patients")
        
        mock_duckdb_conn.execute.assert_called_once_with("SELECT COUNT(*) FROM local_patients")
        assert len(result) == 1
        assert result.iloc[0]['count'] == 500
    
    @patch('flight_server.app.query_orchestrator.get_trino_client')
    @patch('flight_server.app.query_orchestrator.get_duckdb_connection')
    def test_execute_query_forced_backend(self, mock_get_duckdb, mock_get_trino):
        """Test forcing a specific backend."""
        mock_trino_client = Mock()
        mock_trino_client.execute_query.return_value = pd.DataFrame({'result': ['forced']})
        mock_get_trino.return_value = mock_trino_client
        
        orchestrator = QueryOrchestrator()
        
        # Force Trino even for a simple query
        result = orchestrator.execute_query("SELECT 1", target_backend="trino")
        
        mock_trino_client.execute_query.assert_called_once_with("SELECT 1")
        assert result.iloc[0]['result'] == 'forced'
    
    @patch('flight_server.app.query_orchestrator.get_trino_client')
    def test_get_table_info_iceberg(self, mock_get_trino):
        """Test getting table info for Iceberg tables."""
        mock_trino_client = Mock()
        mock_description = pd.DataFrame({'column': ['id', 'name']})
        mock_stats = pd.DataFrame({'stat': ['rows', 'size']})
        mock_history = pd.DataFrame({'timestamp': ['2023-01-01']})
        
        mock_trino_client.describe_table.return_value = mock_description
        mock_trino_client.get_table_stats.return_value = mock_stats
        mock_trino_client.get_table_history.return_value = mock_history
        mock_get_trino.return_value = mock_trino_client
        
        orchestrator = QueryOrchestrator()
        info = orchestrator.get_table_info("patient_data")
        
        assert info["source"] == "iceberg"
        assert info["description"].equals(mock_description)
        assert info["stats"].equals(mock_stats)
        assert info["history"].equals(mock_history)
    
    @patch('flight_server.app.query_orchestrator.get_duckdb_connection')
    def test_get_table_info_duckdb(self, mock_get_duckdb):
        """Test getting table info for DuckDB tables."""
        mock_conn = Mock()
        mock_conn.execute.return_value.fetchall.side_effect = [
            [('id', 'INTEGER', 'NO', '', None, ''), ('name', 'VARCHAR', 'YES', '', None, '')],  # DESCRIBE
            (1000,)  # COUNT
        ]
        mock_get_duckdb.return_value = mock_conn
        
        orchestrator = QueryOrchestrator()
        info = orchestrator.get_table_info("rev")
        
        assert info["source"] == "duckdb"
        assert info["row_count"] == 1000
        assert len(info["description"]) == 2
    
    @patch('flight_server.app.query_orchestrator.get_trino_client')
    @patch('flight_server.app.query_orchestrator.get_duckdb_connection')
    def test_list_tables(self, mock_get_duckdb, mock_get_trino):
        """Test listing tables from both backends."""
        mock_duckdb_conn = Mock()
        mock_duckdb_conn.execute.return_value.fetchall.return_value = [('rev',), ('local_patients',)]
        mock_get_duckdb.return_value = mock_duckdb_conn
        
        mock_trino_client = Mock()
        mock_trino_client.list_tables.return_value = ['patient_data', 'disease_trends']
        mock_get_trino.return_value = mock_trino_client
        
        orchestrator = QueryOrchestrator()
        tables = orchestrator.list_tables()
        
        assert tables["duckdb"] == ['rev', 'local_patients']
        assert tables["iceberg"] == ['patient_data', 'disease_trends']
    
    def test_close(self):
        """Test closing connections."""
        orchestrator = QueryOrchestrator()
        orchestrator.trino_client = Mock()
        orchestrator.duckdb_connection = Mock()
        
        orchestrator.close()
        
        orchestrator.trino_client.close.assert_called_once()
        orchestrator.duckdb_connection.close.assert_called_once()


class TestUtilityFunctions:
    """Test utility functions."""
    
    def test_get_query_orchestrator_singleton(self):
        """Test that get_query_orchestrator returns singleton."""
        orchestrator1 = get_query_orchestrator()
        orchestrator2 = get_query_orchestrator()
        
        assert orchestrator1 is orchestrator2
    
    def test_get_query_orchestrator_type(self):
        """Test that get_query_orchestrator returns correct type."""
        orchestrator = get_query_orchestrator()
        assert isinstance(orchestrator, QueryOrchestrator)
