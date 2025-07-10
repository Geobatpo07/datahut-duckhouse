"""
Query orchestrator that decides between Trino (Iceberg) and DuckDB based on data characteristics.
"""
import logging
import re
from typing import Dict, List, Optional, Union, Any
from dataclasses import dataclass
from enum import Enum

import pandas as pd
import pyarrow as pa

from .trino_client import get_trino_client, TrinoClient
from .utils import get_duckdb_connection
from .monitoring import trace_operation, monitor_performance

logger = logging.getLogger(__name__)


class QueryType(Enum):
    """Types of queries for routing decisions."""
    ANALYTICAL = "analytical"
    OPERATIONAL = "operational"
    AGGREGATION = "aggregation"
    SIMPLE_SELECT = "simple_select"
    JOIN = "join"
    COMPLEX = "complex"


class DataSource(Enum):
    """Data source types."""
    DUCKDB = "duckdb"
    ICEBERG = "iceberg"
    HYBRID = "hybrid"


@dataclass
class QueryMetrics:
    """Metrics for query routing decisions."""
    estimated_rows: int = 0
    table_count: int = 0
    has_joins: bool = False
    has_aggregations: bool = False
    has_subqueries: bool = False
    complexity_score: float = 0.0
    query_type: QueryType = QueryType.SIMPLE_SELECT


class QueryOrchestrator:
    """
    Orchestrates queries between Trino (Iceberg) and DuckDB based on:
    - Data size and complexity
    - Query type and patterns
    - Performance characteristics
    """
    
    def __init__(self):
        self.trino_client = None
        self.duckdb_connection = None
        
        # Configuration thresholds
        self.row_threshold = 1_000_000  # Use Trino for > 1M rows
        self.complexity_threshold = 5.0  # Use Trino for complex queries
        self.join_threshold = 3  # Use Trino for > 3 table joins
        
        # Table registry (which tables are in which backend)
        self.table_registry = self._build_table_registry()
    
    def _build_table_registry(self) -> Dict[str, DataSource]:
        """Build registry of which tables are in which backend."""
        registry = {}
        
        # DuckDB tables (local, lightweight data)
        duckdb_tables = [
            "rev", "diseases", "patients_local", "staging_data",
            "temp_results", "lookup_tables", "reference_data"
        ]
        
        for table in duckdb_tables:
            registry[table] = DataSource.DUCKDB
        
        # Iceberg tables (distributed, large datasets)
        iceberg_tables = [
            "patient_data", "disease_trends", "historical_data",
            "patient_analytics", "large_datasets", "time_series_data"
        ]
        
        for table in iceberg_tables:
            registry[table] = DataSource.ICEBERG
        
        return registry
    
    def _get_trino_client(self) -> TrinoClient:
        """Get or create Trino client."""
        if self.trino_client is None:
            self.trino_client = get_trino_client()
        return self.trino_client
    
    def _get_duckdb_connection(self):
        """Get or create DuckDB connection."""
        if self.duckdb_connection is None:
            self.duckdb_connection = get_duckdb_connection()
        return self.duckdb_connection
    
    def _analyze_query(self, query: str) -> QueryMetrics:
        """Analyze query to determine routing strategy."""
        query_lower = query.lower()
        metrics = QueryMetrics()
        
        # Extract table names
        table_pattern = r'from\s+([a-zA-Z_][a-zA-Z0-9_]*)|join\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        tables = re.findall(table_pattern, query_lower)
        table_names = [t[0] or t[1] for t in tables]
        metrics.table_count = len(set(table_names))
        
        # Check for joins
        metrics.has_joins = bool(re.search(r'\bjoin\b', query_lower))
        
        # Check for aggregations
        aggregation_keywords = ['group by', 'having', 'count(', 'sum(', 'avg(', 'max(', 'min(']
        metrics.has_aggregations = any(keyword in query_lower for keyword in aggregation_keywords)
        
        # Check for subqueries
        metrics.has_subqueries = '(' in query and 'select' in query_lower
        
        # Calculate complexity score
        complexity_score = 0.0
        complexity_score += metrics.table_count * 1.0
        complexity_score += 2.0 if metrics.has_joins else 0.0
        complexity_score += 1.5 if metrics.has_aggregations else 0.0
        complexity_score += 2.0 if metrics.has_subqueries else 0.0
        
        # Additional complexity factors
        if 'window' in query_lower or 'over(' in query_lower:
            complexity_score += 2.0
        if 'recursive' in query_lower or 'with' in query_lower:
            complexity_score += 1.5
        
        metrics.complexity_score = complexity_score
        
        # Determine query type
        if metrics.has_aggregations and metrics.table_count > 1:
            metrics.query_type = QueryType.ANALYTICAL
        elif metrics.has_joins:
            metrics.query_type = QueryType.JOIN
        elif metrics.has_aggregations:
            metrics.query_type = QueryType.AGGREGATION
        elif metrics.complexity_score > 3.0:
            metrics.query_type = QueryType.COMPLEX
        else:
            metrics.query_type = QueryType.SIMPLE_SELECT
        
        return metrics
    
    def _estimate_data_size(self, query: str, table_names: List[str]) -> int:
        """Estimate data size for the query."""
        total_rows = 0
        
        for table_name in table_names:
            try:
                # Check table registry first
                if table_name in self.table_registry:
                    source = self.table_registry[table_name]
                    
                    if source == DataSource.DUCKDB:
                        # Get row count from DuckDB
                        conn = self._get_duckdb_connection()
                        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                        if result:
                            total_rows += result[0]
                    
                    elif source == DataSource.ICEBERG:
                        # Get row count from Trino/Iceberg
                        trino = self._get_trino_client()
                        try:
                            stats = trino.get_table_stats(table_name)
                            if not stats.empty:
                                # Extract row count from stats
                                row_count_row = stats[stats['Column Name'] == '']
                                if not row_count_row.empty:
                                    total_rows += int(row_count_row['Row Count'].iloc[0])
                        except Exception as e:
                            logger.warning(f"Could not get stats for {table_name}: {e}")
                            # Assume large dataset for Iceberg tables
                            total_rows += 10_000_000
                
                else:
                    # Unknown table, assume medium size
                    total_rows += 100_000
                    
            except Exception as e:
                logger.warning(f"Could not estimate size for {table_name}: {e}")
                total_rows += 100_000
        
        return total_rows
    
    def _should_use_trino(self, query: str, metrics: QueryMetrics) -> bool:
        """Determine if query should use Trino instead of DuckDB."""
        
        # Extract table names for analysis
        table_pattern = r'from\s+([a-zA-Z_][a-zA-Z0-9_]*)|join\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        tables = re.findall(table_pattern, query.lower())
        table_names = [t[0] or t[1] for t in tables]
        
        # Check if any table is explicitly Iceberg-only
        iceberg_tables = [name for name in table_names if self.table_registry.get(name) == DataSource.ICEBERG]
        if iceberg_tables:
            logger.info(f"Using Trino because query involves Iceberg tables: {iceberg_tables}")
            return True
        
        # Estimate data size
        estimated_rows = self._estimate_data_size(query, table_names)
        metrics.estimated_rows = estimated_rows
        
        # Decision criteria
        use_trino = False
        reasons = []
        
        # Large dataset threshold
        if estimated_rows > self.row_threshold:
            use_trino = True
            reasons.append(f"Large dataset ({estimated_rows:,} rows)")
        
        # Complex query threshold
        if metrics.complexity_score > self.complexity_threshold:
            use_trino = True
            reasons.append(f"Complex query (score: {metrics.complexity_score})")
        
        # Multiple joins
        if metrics.table_count > self.join_threshold:
            use_trino = True
            reasons.append(f"Multiple joins ({metrics.table_count} tables)")
        
        # Analytical queries with large datasets
        if metrics.query_type == QueryType.ANALYTICAL and estimated_rows > 100_000:
            use_trino = True
            reasons.append("Analytical query with medium+ dataset")
        
        if use_trino:
            logger.info(f"Using Trino for query. Reasons: {', '.join(reasons)}")
        else:
            logger.info(f"Using DuckDB for query. Estimated rows: {estimated_rows:,}, Complexity: {metrics.complexity_score}")
        
        return use_trino
    
    @monitor_performance
    def execute_query(self, query: str, target_backend: Optional[str] = None) -> pd.DataFrame:
        """
        Execute query using the appropriate backend.
        
        Args:
            query: SQL query to execute
            target_backend: Force specific backend ('trino' or 'duckdb')
            
        Returns:
            DataFrame with query results
        """
        with trace_operation("orchestrated_query", {"query": query[:100]}):
            try:
                # Analyze query
                metrics = self._analyze_query(query)
                
                # Determine backend
                if target_backend:
                    use_trino = target_backend.lower() == 'trino'
                    logger.info(f"Forced to use {target_backend} backend")
                else:
                    use_trino = self._should_use_trino(query, metrics)
                
                # Execute query
                if use_trino:
                    trino = self._get_trino_client()
                    result = trino.execute_query(query)
                    logger.info(f"Query executed via Trino: {len(result)} rows returned")
                else:
                    conn = self._get_duckdb_connection()
                    result_cursor = conn.execute(query)
                    
                    # Convert to DataFrame
                    columns = [desc[0] for desc in result_cursor.description]
                    rows = result_cursor.fetchall()
                    result = pd.DataFrame(rows, columns=columns)
                    logger.info(f"Query executed via DuckDB: {len(result)} rows returned")
                
                return result
                
            except Exception as e:
                logger.error(f"Query execution failed: {e}")
                raise
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about a table from the appropriate backend."""
        source = self.table_registry.get(table_name)
        
        if source == DataSource.ICEBERG:
            trino = self._get_trino_client()
            return {
                "source": "iceberg",
                "description": trino.describe_table(table_name),
                "stats": trino.get_table_stats(table_name),
                "history": trino.get_table_history(table_name)
            }
        else:
            # DuckDB or unknown (assume DuckDB)
            conn = self._get_duckdb_connection()
            try:
                # Get table schema
                schema_result = conn.execute(f"DESCRIBE {table_name}").fetchall()
                description = pd.DataFrame(schema_result, columns=["column_name", "column_type", "null", "key", "default", "extra"])
                
                # Get row count
                count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                row_count = count_result[0] if count_result else 0
                
                return {
                    "source": "duckdb",
                    "description": description,
                    "row_count": row_count,
                    "stats": pd.DataFrame()  # DuckDB stats would need separate implementation
                }
            except Exception as e:
                logger.error(f"Could not get table info for {table_name}: {e}")
                return {"source": "unknown", "error": str(e)}
    
    def list_tables(self) -> Dict[str, List[str]]:
        """List tables from both backends."""
        tables = {"duckdb": [], "iceberg": []}
        
        try:
            # Get DuckDB tables
            conn = self._get_duckdb_connection()
            duckdb_tables = conn.execute("SHOW TABLES").fetchall()
            tables["duckdb"] = [row[0] for row in duckdb_tables]
            
            # Get Iceberg tables
            trino = self._get_trino_client()
            iceberg_tables = trino.list_tables()
            tables["iceberg"] = iceberg_tables
            
        except Exception as e:
            logger.error(f"Error listing tables: {e}")
        
        return tables
    
    def close(self):
        """Close all connections."""
        if self.trino_client:
            self.trino_client.close()
        if self.duckdb_connection:
            self.duckdb_connection.close()


# Global orchestrator instance
_orchestrator = None

def get_query_orchestrator() -> QueryOrchestrator:
    """Get the global query orchestrator instance."""
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = QueryOrchestrator()
    return _orchestrator
