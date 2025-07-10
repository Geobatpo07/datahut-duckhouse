"""
Unified query engine providing dialect-independent querying across DuckDB and Trino.

This module abstracts the differences between DuckDB and Trino SQL dialects,
allowing users to write queries in a unified syntax that gets translated
to the appropriate target engine dialect.
"""

import logging
import re
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import duckdb
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Suppress warnings
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class QueryEngine(Enum):
    """Supported query engines."""
    DUCKDB = "duckdb"
    TRINO = "trino"


class QueryType(Enum):
    """Types of SQL queries."""
    SELECT = "select"
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    CREATE = "create"
    DROP = "drop"
    ALTER = "alter"


@dataclass
class QueryConfig:
    """Configuration for query execution."""
    engine: QueryEngine
    tenant_id: str
    timeout_seconds: int = 300
    max_memory_mb: int = 2048
    enable_caching: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QueryResult:
    """Result of query execution."""
    data: Optional[pd.DataFrame] = None
    rows_affected: int = 0
    execution_time_seconds: float = 0.0
    query_plan: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    success: bool = True
    error_message: Optional[str] = None


class SQLDialectTranslator:
    """Translates unified SQL dialect to engine-specific dialects."""
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.SQLDialectTranslator")
        
        # Define translation rules
        self.duckdb_translations = {
            # Date/time functions
            r'\bCURRENT_TIMESTAMP\b': 'CURRENT_TIMESTAMP',
            r'\bDATE_TRUNC\s*\(\s*[\'"](\w+)[\'"]\s*,\s*([^)]+)\)': r"DATE_TRUNC('\1', \2)",
            r'\bEXTRACT\s*\(\s*(\w+)\s+FROM\s+([^)]+)\)': r'EXTRACT(\1 FROM \2)',
            
            # String functions
            r'\bCONCAT\s*\(([^)]+)\)': r'CONCAT(\1)',
            r'\bLENGTH\s*\(([^)]+)\)': r'LENGTH(\1)',
            r'\bSUBSTRING\s*\(([^)]+)\)': r'SUBSTRING(\1)',
            
            # Mathematical functions
            r'\bROUND\s*\(([^)]+)\)': r'ROUND(\1)',
            r'\bCEIL\s*\(([^)]+)\)': r'CEIL(\1)',
            r'\bFLOOR\s*\(([^)]+)\)': r'FLOOR(\1)',
            
            # Window functions
            r'\bROW_NUMBER\s*\(\s*\)\s+OVER\s*\(([^)]+)\)': r'ROW_NUMBER() OVER(\1)',
            r'\bRANK\s*\(\s*\)\s+OVER\s*\(([^)]+)\)': r'RANK() OVER(\1)',
            
            # Aggregation functions
            r'\bCOUNT\s*\(\s*DISTINCT\s+([^)]+)\)': r'COUNT(DISTINCT \1)',
            r'\bSUM\s*\(\s*DISTINCT\s+([^)]+)\)': r'SUM(DISTINCT \1)',
        }
        
        self.trino_translations = {
            # Date/time functions
            r'\bCURRENT_TIMESTAMP\b': 'CURRENT_TIMESTAMP',
            r'\bDATE_TRUNC\s*\(\s*[\'"](\w+)[\'"]\s*,\s*([^)]+)\)': r"DATE_TRUNC('\1', \2)",
            r'\bEXTRACT\s*\(\s*(\w+)\s+FROM\s+([^)]+)\)': r'EXTRACT(\1 FROM \2)',
            
            # String functions
            r'\bCONCAT\s*\(([^)]+)\)': r'CONCAT(\1)',
            r'\bLENGTH\s*\(([^)]+)\)': r'LENGTH(\1)',
            r'\bSUBSTRING\s*\(([^)]+)\)': r'SUBSTR(\1)',  # Trino uses SUBSTR
            
            # Mathematical functions
            r'\bROUND\s*\(([^)]+)\)': r'ROUND(\1)',
            r'\bCEIL\s*\(([^)]+)\)': r'CEILING(\1)',  # Trino uses CEILING
            r'\bFLOOR\s*\(([^)]+)\)': r'FLOOR(\1)',
            
            # Array functions (Trino specific)
            r'\bARRAY_AGG\s*\(([^)]+)\)': r'ARRAY_AGG(\1)',
            r'\bCARDINALITY\s*\(([^)]+)\)': r'CARDINALITY(\1)',
            
            # JSON functions (Trino specific)
            r'\bJSON_EXTRACT\s*\(([^)]+)\)': r'JSON_EXTRACT(\1)',
            r'\bJSON_EXTRACT_SCALAR\s*\(([^)]+)\)': r'JSON_EXTRACT_SCALAR(\1)',
        }
    
    def translate_to_duckdb(self, sql: str) -> str:
        """Translate unified SQL to DuckDB dialect."""
        translated = sql
        for pattern, replacement in self.duckdb_translations.items():
            translated = re.sub(pattern, replacement, translated, flags=re.IGNORECASE)
        
        # DuckDB specific optimizations
        translated = self._add_duckdb_optimizations(translated)
        
        self.logger.debug(f"Translated to DuckDB: {translated}")
        return translated
    
    def translate_to_trino(self, sql: str) -> str:
        """Translate unified SQL to Trino dialect."""
        translated = sql
        for pattern, replacement in self.trino_translations.items():
            translated = re.sub(pattern, replacement, translated, flags=re.IGNORECASE)
        
        # Trino specific optimizations
        translated = self._add_trino_optimizations(translated)
        
        self.logger.debug(f"Translated to Trino: {translated}")
        return translated
    
    def _add_duckdb_optimizations(self, sql: str) -> str:
        """Add DuckDB-specific optimizations."""
        # Add PRAGMA optimizations for DuckDB
        optimizations = []
        
        # Check if it's a large join query
        if re.search(r'\bJOIN\b.*\bJOIN\b', sql, re.IGNORECASE):
            optimizations.append("PRAGMA enable_optimizer = true;")
        
        # Check if it's an aggregation query
        if re.search(r'\bGROUP BY\b', sql, re.IGNORECASE):
            optimizations.append("PRAGMA enable_object_cache = true;")
        
        if optimizations:
            return '\n'.join(optimizations) + '\n' + sql
        return sql
    
    def _add_trino_optimizations(self, sql: str) -> str:
        """Add Trino-specific optimizations."""
        # Add session properties for Trino
        optimizations = []
        
        # Memory optimization for large queries
        if len(sql) > 1000:  # Heuristic for complex queries
            optimizations.append("SET SESSION query_max_memory = '4GB';")
        
        # Join optimization
        if re.search(r'\bJOIN\b', sql, re.IGNORECASE):
            optimizations.append("SET SESSION join_reordering_strategy = 'AUTOMATIC';")
        
        if optimizations:
            return '\n'.join(optimizations) + '\n' + sql
        return sql
    
    def detect_query_type(self, sql: str) -> QueryType:
        """Detect the type of SQL query."""
        sql_upper = sql.strip().upper()
        
        if sql_upper.startswith('SELECT'):
            return QueryType.SELECT
        elif sql_upper.startswith('INSERT'):
            return QueryType.INSERT
        elif sql_upper.startswith('UPDATE'):
            return QueryType.UPDATE
        elif sql_upper.startswith('DELETE'):
            return QueryType.DELETE
        elif sql_upper.startswith('CREATE'):
            return QueryType.CREATE
        elif sql_upper.startswith('DROP'):
            return QueryType.DROP
        elif sql_upper.startswith('ALTER'):
            return QueryType.ALTER
        else:
            return QueryType.SELECT  # Default


class BaseQueryExecutor(ABC):
    """Abstract base class for query executors."""
    
    def __init__(self, config: QueryConfig):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.translator = SQLDialectTranslator()
    
    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the query engine."""
        pass
    
    @abstractmethod
    def execute_query(self, sql: str, params: Optional[Dict] = None) -> QueryResult:
        """Execute a SQL query."""
        pass
    
    @abstractmethod
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about a table."""
        pass
    
    def disconnect(self) -> None:
        """Clean up resources and disconnect."""
        pass


class DuckDBExecutor(BaseQueryExecutor):
    """Query executor for DuckDB."""
    
    def __init__(self, config: QueryConfig):
        super().__init__(config)
        self.connection: Optional[duckdb.DuckDBPyConnection] = None
        self.db_path = f"data/tenant_{config.tenant_id}.duckdb"
    
    def connect(self) -> bool:
        """Establish DuckDB connection."""
        try:
            # Ensure data directory exists
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Connect to DuckDB
            self.connection = duckdb.connect(self.db_path)
            
            # Configure DuckDB settings
            self.connection.execute("PRAGMA enable_progress_bar = false;")
            self.connection.execute(f"PRAGMA memory_limit = '{self.config.max_memory_mb}MB';")
            self.connection.execute("PRAGMA threads = 4;")
            
            self.logger.info(f"Connected to DuckDB: {self.db_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"DuckDB connection failed: {e}")
            return False
    
    def execute_query(self, sql: str, params: Optional[Dict] = None) -> QueryResult:
        """Execute query in DuckDB."""
        if not self.connection:
            raise RuntimeError("DuckDB connection not established")
        
        start_time = datetime.utcnow()
        result = QueryResult()
        
        try:
            # Translate SQL to DuckDB dialect
            translated_sql = self.translator.translate_to_duckdb(sql)
            query_type = self.translator.detect_query_type(sql)
            
            self.logger.debug(f"Executing DuckDB query: {translated_sql}")
            
            if query_type == QueryType.SELECT:
                # For SELECT queries, return DataFrame
                df = self.connection.execute(translated_sql).df()
                result.data = df
                result.rows_affected = len(df)
            else:
                # For other queries, execute and get affected rows
                cursor = self.connection.execute(translated_sql)
                result.rows_affected = cursor.rowcount if hasattr(cursor, 'rowcount') else 0
            
            result.success = True
            
        except Exception as e:
            result.success = False
            result.error_message = str(e)
            self.logger.error(f"DuckDB query execution failed: {e}")
        
        finally:
            end_time = datetime.utcnow()
            result.execution_time_seconds = (end_time - start_time).total_seconds()
        
        return result
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get DuckDB table information."""
        if not self.connection:
            raise RuntimeError("DuckDB connection not established")
        
        try:
            # Get table schema
            schema_df = self.connection.execute(f"PRAGMA table_info('{table_name}')").df()
            
            # Get table statistics
            stats_df = self.connection.execute(f"SELECT COUNT(*) as row_count FROM {table_name}").df()
            
            return {
                'schema': schema_df.to_dict('records'),
                'row_count': stats_df.iloc[0]['row_count'],
                'engine': 'duckdb'
            }
            
        except Exception as e:
            self.logger.error(f"Error getting DuckDB table info: {e}")
            raise
    
    def disconnect(self) -> None:
        """Close DuckDB connection."""
        if self.connection:
            self.connection.close()
            self.logger.info("DuckDB connection closed")


class TrinoExecutor(BaseQueryExecutor):
    """Query executor for Trino."""
    
    def __init__(self, config: QueryConfig):
        super().__init__(config)
        self.engine: Optional[Engine] = None
        self.connection_string = self._build_connection_string()
    
    def _build_connection_string(self) -> str:
        """Build Trino connection string."""
        # This would typically be configured from environment or config file
        host = "localhost"  # Configure as needed
        port = 8080
        catalog = "iceberg"
        schema = f"tenant_{self.config.tenant_id}"
        
        return f"trino://{host}:{port}/{catalog}/{schema}"
    
    def connect(self) -> bool:
        """Establish Trino connection."""
        try:
            from sqlalchemy import create_engine
            
            self.engine = create_engine(
                self.connection_string,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False
            )
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self.logger.info(f"Connected to Trino: {self.connection_string}")
            return True
            
        except Exception as e:
            self.logger.error(f"Trino connection failed: {e}")
            return False
    
    def execute_query(self, sql: str, params: Optional[Dict] = None) -> QueryResult:
        """Execute query in Trino."""
        if not self.engine:
            raise RuntimeError("Trino connection not established")
        
        start_time = datetime.utcnow()
        result = QueryResult()
        
        try:
            # Translate SQL to Trino dialect
            translated_sql = self.translator.translate_to_trino(sql)
            query_type = self.translator.detect_query_type(sql)
            
            self.logger.debug(f"Executing Trino query: {translated_sql}")
            
            with self.engine.connect() as conn:
                if query_type == QueryType.SELECT:
                    # For SELECT queries, return DataFrame
                    df = pd.read_sql(text(translated_sql), conn, params=params)
                    result.data = df
                    result.rows_affected = len(df)
                else:
                    # For other queries, execute and get affected rows
                    cursor = conn.execute(text(translated_sql), params or {})
                    result.rows_affected = cursor.rowcount
            
            result.success = True
            
        except Exception as e:
            result.success = False
            result.error_message = str(e)
            self.logger.error(f"Trino query execution failed: {e}")
        
        finally:
            end_time = datetime.utcnow()
            result.execution_time_seconds = (end_time - start_time).total_seconds()
        
        return result
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get Trino table information."""
        if not self.engine:
            raise RuntimeError("Trino connection not established")
        
        try:
            with self.engine.connect() as conn:
                # Get table schema
                schema_df = pd.read_sql(
                    text(f"DESCRIBE {table_name}"), 
                    conn
                )
                
                # Get table statistics
                stats_df = pd.read_sql(
                    text(f"SELECT COUNT(*) as row_count FROM {table_name}"), 
                    conn
                )
                
                return {
                    'schema': schema_df.to_dict('records'),
                    'row_count': stats_df.iloc[0]['row_count'],
                    'engine': 'trino'
                }
                
        except Exception as e:
            self.logger.error(f"Error getting Trino table info: {e}")
            raise
    
    def disconnect(self) -> None:
        """Close Trino connection."""
        if self.engine:
            self.engine.dispose()
            self.logger.info("Trino connection closed")


class UnifiedQueryEngine:
    """Unified query engine that routes queries to appropriate backend."""
    
    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.logger = logging.getLogger(f"{__name__}.UnifiedQueryEngine")
        self.executors: Dict[QueryEngine, BaseQueryExecutor] = {}
        self.translator = SQLDialectTranslator()
    
    def add_executor(self, engine: QueryEngine, config: QueryConfig) -> bool:
        """Add a query executor for a specific engine."""
        try:
            if engine == QueryEngine.DUCKDB:
                executor = DuckDBExecutor(config)
            elif engine == QueryEngine.TRINO:
                executor = TrinoExecutor(config)
            else:
                raise ValueError(f"Unsupported query engine: {engine}")
            
            if executor.connect():
                self.executors[engine] = executor
                self.logger.info(f"Added {engine.value} executor")
                return True
            else:
                self.logger.error(f"Failed to connect {engine.value} executor")
                return False
                
        except Exception as e:
            self.logger.error(f"Error adding {engine.value} executor: {e}")
            return False
    
    def _determine_target_engine(self, sql: str, table_hint: Optional[str] = None) -> QueryEngine:
        """Determine which engine should execute the query."""
        try:
            # If table hint is provided, use it to determine engine
            if table_hint:
                # Check if table exists in DuckDB first
                if QueryEngine.DUCKDB in self.executors:
                    try:
                        duckdb_executor = self.executors[QueryEngine.DUCKDB]
                        duckdb_executor.get_table_info(table_hint)
                        return QueryEngine.DUCKDB
                    except Exception:
                        pass  # Table not in DuckDB
                
                # Fall back to Trino
                if QueryEngine.TRINO in self.executors:
                    return QueryEngine.TRINO
            
            # Analyze query complexity for routing decision
            complexity_score = self._calculate_query_complexity(sql)
            
            # Route based on complexity
            if complexity_score > 5 and QueryEngine.TRINO in self.executors:
                return QueryEngine.TRINO
            elif QueryEngine.DUCKDB in self.executors:
                return QueryEngine.DUCKDB
            else:
                # Return first available engine
                return next(iter(self.executors.keys()))
                
        except Exception as e:
            self.logger.error(f"Error determining target engine: {e}")
            # Default to DuckDB if available
            return QueryEngine.DUCKDB if QueryEngine.DUCKDB in self.executors else QueryEngine.TRINO
    
    def _calculate_query_complexity(self, sql: str) -> int:
        """Calculate query complexity score for routing decisions."""
        complexity = 0
        sql_upper = sql.upper()
        
        # Join complexity
        complexity += len(re.findall(r'\bJOIN\b', sql_upper)) * 2
        
        # Subquery complexity
        complexity += len(re.findall(r'\bSELECT\b', sql_upper)) - 1
        
        # Window function complexity
        complexity += len(re.findall(r'\bOVER\s*\(', sql_upper)) * 2
        
        # Aggregation complexity
        complexity += len(re.findall(r'\bGROUP BY\b', sql_upper))
        
        # CTE complexity
        complexity += len(re.findall(r'\bWITH\b', sql_upper))
        
        return complexity
    
    def execute(self, sql: str, engine: Optional[QueryEngine] = None, 
               params: Optional[Dict] = None, table_hint: Optional[str] = None) -> QueryResult:
        """Execute a query using the unified interface."""
        try:
            # Determine target engine
            target_engine = engine or self._determine_target_engine(sql, table_hint)
            
            if target_engine not in self.executors:
                raise RuntimeError(f"No executor available for {target_engine.value}")
            
            executor = self.executors[target_engine]
            self.logger.info(f"Executing query on {target_engine.value}")
            
            # Execute query
            result = executor.execute_query(sql, params)
            result.metadata['engine'] = target_engine.value
            result.metadata['tenant_id'] = self.tenant_id
            
            return result
            
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            return QueryResult(
                success=False,
                error_message=str(e),
                metadata={'tenant_id': self.tenant_id}
            )
    
    def get_table_info(self, table_name: str, engine: Optional[QueryEngine] = None) -> Dict[str, Any]:
        """Get table information from appropriate engine."""
        target_engine = engine or self._determine_target_engine("", table_name)
        
        if target_engine not in self.executors:
            raise RuntimeError(f"No executor available for {target_engine.value}")
        
        return self.executors[target_engine].get_table_info(table_name)
    
    def list_tables(self, engine: Optional[QueryEngine] = None) -> List[str]:
        """List all tables across engines or specific engine."""
        tables = []
        
        engines_to_check = [engine] if engine else list(self.executors.keys())
        
        for eng in engines_to_check:
            if eng in self.executors:
                try:
                    if eng == QueryEngine.DUCKDB:
                        result = self.executors[eng].execute_query(
                            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
                        )
                    else:  # Trino
                        result = self.executors[eng].execute_query(
                            "SHOW TABLES"
                        )
                    
                    if result.success and result.data is not None:
                        engine_tables = result.data.iloc[:, 0].tolist()
                        tables.extend([f"{eng.value}.{table}" for table in engine_tables])
                        
                except Exception as e:
                    self.logger.warning(f"Error listing tables from {eng.value}: {e}")
        
        return tables
    
    def close_all_connections(self) -> None:
        """Close all executor connections."""
        for executor in self.executors.values():
            executor.disconnect()
        self.executors.clear()
        self.logger.info("All connections closed")


# Convenience functions
def create_unified_query_engine(tenant_id: str, enable_duckdb: bool = True, 
                              enable_trino: bool = False) -> UnifiedQueryEngine:
    """Create a unified query engine with specified backends."""
    engine = UnifiedQueryEngine(tenant_id)
    
    if enable_duckdb:
        duckdb_config = QueryConfig(
            engine=QueryEngine.DUCKDB,
            tenant_id=tenant_id,
            timeout_seconds=300,
            max_memory_mb=2048
        )
        engine.add_executor(QueryEngine.DUCKDB, duckdb_config)
    
    if enable_trino:
        trino_config = QueryConfig(
            engine=QueryEngine.TRINO,
            tenant_id=tenant_id,
            timeout_seconds=600,
            max_memory_mb=4096
        )
        engine.add_executor(QueryEngine.TRINO, trino_config)
    
    return engine
