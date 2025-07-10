"""
Core data ingestion module for multi-source, multi-tenant, partitioned architecture.

This module provides a unified data ingestion framework that:
- Supports multiple data sources (databases, files)
- Routes data to DuckDB for small datasets and Trino/Iceberg for large datasets
- Maintains tenant-based partitioning for optimization
- Provides unified query dialect internally
"""

import logging
import warnings
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SourceType(Enum):
    """Enumeration of supported data source types."""
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"
    SQLSERVER = "sqlserver"
    SQLITE = "sqlite"
    CSV = "csv"
    PARQUET = "parquet"


class TargetEngine(Enum):
    """Enumeration of target engines based on dataset size."""
    DUCKDB = "duckdb"
    TRINO = "trino"


@dataclass
class IngestionConfig:
    """Configuration for data ingestion operations."""
    tenant_id: str
    source_type: SourceType
    target_engine: TargetEngine
    batch_size: int = 10000
    max_memory_mb: int = 1024
    enable_validation: bool = True
    enable_partitioning: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class IngestionMetrics:
    """Metrics collected during ingestion process."""
    records_processed: int = 0
    batches_processed: int = 0
    processing_time_seconds: float = 0.0
    data_size_mb: float = 0.0
    errors_encountered: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None


class BaseConnector(ABC):
    """Abstract base class for data source connectors."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the data source."""
        pass
    
    @abstractmethod
    def read_data(self, query: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """Read data from the source."""
        pass
    
    @abstractmethod
    def get_schema(self) -> Dict[str, str]:
        """Get schema information from the source."""
        pass
    
    def disconnect(self) -> None:
        """Clean up resources and disconnect."""
        pass


class DatabaseConnector(BaseConnector):
    """Connector for SQL databases using SQLAlchemy."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.engine: Optional[Engine] = None
        self.metadata: Optional[MetaData] = None
    
    def connect(self) -> bool:
        """Establish database connection."""
        try:
            connection_string = self._build_connection_string()
            self.engine = create_engine(
                connection_string,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False
            )
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self.metadata = MetaData()
            self.logger.info(f"Connected to {self.config.get('host', 'database')}")
            return True
            
        except SQLAlchemyError as e:
            self.logger.error(f"Database connection failed: {e}")
            return False
    
    def _build_connection_string(self) -> str:
        """Build SQLAlchemy connection string."""
        db_type = self.config['type']
        user = self.config.get('user', '')
        password = self.config.get('password', '')
        host = self.config.get('host', 'localhost')
        port = self.config.get('port')
        database = self.config.get('database', '')
        
        if db_type == SourceType.MYSQL.value:
            port = port or 3306
            return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        elif db_type == SourceType.POSTGRESQL.value:
            port = port or 5432
            return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
        elif db_type == SourceType.SQLSERVER.value:
            port = port or 1433
            return f"mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
        elif db_type == SourceType.SQLITE.value:
            return f"sqlite:///{database}"
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    def read_data(self, query: Optional[str] = None, table_name: Optional[str] = None, 
                  chunk_size: Optional[int] = None) -> pd.DataFrame:
        """Read data from database."""
        if not self.engine:
            raise RuntimeError("Database connection not established")
        
        try:
            if query:
                sql_query = query
            elif table_name:
                sql_query = f"SELECT * FROM {table_name}"
            else:
                raise ValueError("Either query or table_name must be provided")
            
            if chunk_size:
                return pd.read_sql(sql_query, self.engine, chunksize=chunk_size)
            else:
                return pd.read_sql(sql_query, self.engine)
                
        except Exception as e:
            self.logger.error(f"Error reading data: {e}")
            raise
    
    def get_schema(self, table_name: str) -> Dict[str, str]:
        """Get table schema information."""
        if not self.engine or not self.metadata:
            raise RuntimeError("Database connection not established")
        
        try:
            table = Table(table_name, self.metadata, autoload_with=self.engine)
            schema = {}
            for column in table.columns:
                schema[column.name] = str(column.type)
            return schema
            
        except Exception as e:
            self.logger.error(f"Error getting schema: {e}")
            raise
    
    def disconnect(self) -> None:
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            self.logger.info("Database connection closed")


class CSVConnector(BaseConnector):
    """Connector for CSV files."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.file_path = Path(config['file_path'])
    
    def connect(self) -> bool:
        """Verify CSV file exists and is readable."""
        try:
            if not self.file_path.exists():
                self.logger.error(f"CSV file not found: {self.file_path}")
                return False
            
            if not self.file_path.is_file():
                self.logger.error(f"Path is not a file: {self.file_path}")
                return False
            
            self.logger.info(f"CSV file validated: {self.file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating CSV file: {e}")
            return False
    
    def read_data(self, chunk_size: Optional[int] = None, **kwargs) -> pd.DataFrame:
        """Read data from CSV file."""
        try:
            csv_params = {
                'sep': self.config.get('separator', ','),
                'encoding': self.config.get('encoding', 'utf-8'),
                'header': self.config.get('header', 0),
                'na_values': self.config.get('na_values', ['', 'NULL', 'null', 'NA']),
                **kwargs
            }
            
            if chunk_size:
                csv_params['chunksize'] = chunk_size
            
            return pd.read_csv(self.file_path, **csv_params)
            
        except Exception as e:
            self.logger.error(f"Error reading CSV file: {e}")
            raise
    
    def get_schema(self) -> Dict[str, str]:
        """Get CSV schema by reading a sample."""
        try:
            sample_df = pd.read_csv(self.file_path, nrows=100)
            schema = {}
            for column, dtype in sample_df.dtypes.items():
                schema[column] = str(dtype)
            return schema
            
        except Exception as e:
            self.logger.error(f"Error getting CSV schema: {e}")
            raise


class ParquetConnector(BaseConnector):
    """Connector for Parquet files."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.file_path = Path(config['file_path'])
    
    def connect(self) -> bool:
        """Verify Parquet file exists and is readable."""
        try:
            if not self.file_path.exists():
                self.logger.error(f"Parquet file not found: {self.file_path}")
                return False
            
            # Try to read metadata to validate file
            pq.read_metadata(self.file_path)
            self.logger.info(f"Parquet file validated: {self.file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating Parquet file: {e}")
            return False
    
    def read_data(self, columns: Optional[List[str]] = None, **kwargs) -> pd.DataFrame:
        """Read data from Parquet file."""
        try:
            return pd.read_parquet(self.file_path, columns=columns, **kwargs)
            
        except Exception as e:
            self.logger.error(f"Error reading Parquet file: {e}")
            raise
    
    def get_schema(self) -> Dict[str, str]:
        """Get Parquet schema information."""
        try:
            parquet_file = pq.ParquetFile(self.file_path)
            schema = parquet_file.schema.to_arrow_schema()
            
            schema_dict = {}
            for field in schema:
                schema_dict[field.name] = str(field.type)
            return schema_dict
            
        except Exception as e:
            self.logger.error(f"Error getting Parquet schema: {e}")
            raise


class DataValidator:
    """Data validation and quality checks."""
    
    def __init__(self, config: IngestionConfig):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.DataValidator")
    
    def validate_dataframe(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Validate DataFrame quality and consistency."""
        issues = []
        
        try:
            # Check for empty DataFrame
            if df.empty:
                issues.append("DataFrame is empty")
                return False, issues
            
            # Check for all null columns
            null_columns = df.columns[df.isnull().all()].tolist()
            if null_columns:
                issues.append(f"Columns with all null values: {null_columns}")
            
            # Check for duplicate rows
            duplicate_count = df.duplicated().sum()
            if duplicate_count > 0:
                issues.append(f"Found {duplicate_count} duplicate rows")
            
            # Check data types consistency
            for column in df.columns:
                if df[column].dtype == 'object':
                    # Check for mixed types in object columns
                    sample_types = df[column].dropna().apply(type).unique()
                    if len(sample_types) > 1:
                        issues.append(f"Column '{column}' has mixed data types")
            
            # Memory usage check
            memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
            if memory_mb > self.config.max_memory_mb:
                issues.append(f"DataFrame size ({memory_mb:.2f}MB) exceeds limit ({self.config.max_memory_mb}MB)")
            
            success = len(issues) == 0
            if success:
                self.logger.info("Data validation passed")
            else:
                self.logger.warning(f"Data validation issues: {issues}")
            
            return success, issues
            
        except Exception as e:
            self.logger.error(f"Error during validation: {e}")
            return False, [f"Validation error: {e}"]


class TenantDataManager:
    """Manager for tenant-specific data operations and partitioning."""
    
    def __init__(self, config: IngestionConfig):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.TenantDataManager")
    
    def add_tenant_metadata(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add tenant-specific metadata columns to DataFrame."""
        try:
            df_with_tenant = df.copy()
            df_with_tenant['tenant_id'] = self.config.tenant_id
            df_with_tenant['ingestion_timestamp'] = datetime.utcnow()
            df_with_tenant['source_type'] = self.config.source_type.value
            
            self.logger.info(f"Added tenant metadata for tenant: {self.config.tenant_id}")
            return df_with_tenant
            
        except Exception as e:
            self.logger.error(f"Error adding tenant metadata: {e}")
            raise
    
    def determine_partitioning_strategy(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Determine optimal partitioning strategy based on data characteristics."""
        try:
            strategy = {
                'partition_by': ['tenant_id'],
                'sort_by': ['ingestion_timestamp'],
                'enable_clustering': True
            }
            
            # Add date-based partitioning if timestamp columns exist
            timestamp_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
            if timestamp_cols and self.config.enable_partitioning:
                strategy['partition_by'].extend([f"date({timestamp_cols[0]})"])
            
            # Determine clustering based on data size
            if len(df) > 100000:  # Large dataset
                strategy['enable_clustering'] = True
                strategy['cluster_by'] = ['tenant_id']
            
            self.logger.info(f"Partitioning strategy: {strategy}")
            return strategy
            
        except Exception as e:
            self.logger.error(f"Error determining partitioning strategy: {e}")
            raise


class DataIngestionOrchestrator:
    """Main orchestrator for multi-source data ingestion with tenant partitioning."""
    
    def __init__(self, config: IngestionConfig):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.DataIngestionOrchestrator")
        self.validator = DataValidator(config) if config.enable_validation else None
        self.tenant_manager = TenantDataManager(config)
        self.metrics = IngestionMetrics()
        self.connector: Optional[BaseConnector] = None
    
    def _create_connector(self, source_config: Dict[str, Any]) -> BaseConnector:
        """Factory method to create appropriate connector based on source type."""
        source_type = self.config.source_type
        
        if source_type in [SourceType.MYSQL, SourceType.POSTGRESQL, 
                          SourceType.SQLSERVER, SourceType.SQLITE]:
            return DatabaseConnector(source_config)
        elif source_type == SourceType.CSV:
            return CSVConnector(source_config)
        elif source_type == SourceType.PARQUET:
            return ParquetConnector(source_config)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
    
    def _determine_target_engine(self, df: pd.DataFrame) -> TargetEngine:
        """Determine target engine based on dataset size and characteristics."""
        try:
            # Calculate data size in MB
            data_size_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
            record_count = len(df)
            
            # Decision logic based on size thresholds
            if data_size_mb > 500 or record_count > 1000000:  # Large dataset
                target = TargetEngine.TRINO
                self.logger.info(f"Large dataset detected ({data_size_mb:.2f}MB, {record_count} records) - routing to Trino")
            else:  # Small dataset
                target = TargetEngine.DUCKDB
                self.logger.info(f"Small dataset detected ({data_size_mb:.2f}MB, {record_count} records) - routing to DuckDB")
            
            self.metrics.data_size_mb = data_size_mb
            return target
            
        except Exception as e:
            self.logger.error(f"Error determining target engine: {e}")
            # Default to DuckDB for safety
            return TargetEngine.DUCKDB
    
    def _write_to_duckdb(self, df: pd.DataFrame, table_name: str) -> bool:
        """Write data to DuckDB for small datasets."""
        try:
            import duckdb
            
            # Create connection
            db_path = f"data/tenant_{self.config.tenant_id}.duckdb"
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            
            conn = duckdb.connect(db_path)
            
            # Create table with tenant partitioning
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df LIMIT 0")
            conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
            
            conn.close()
            self.logger.info(f"Data written to DuckDB: {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error writing to DuckDB: {e}")
            return False
    
    def _write_to_trino(self, df: pd.DataFrame, table_name: str) -> bool:
        """Write data to Iceberg via Trino for large datasets."""
        try:
            # This would typically use Trino SQL connection
            # For now, we'll write to Parquet as a placeholder
            output_path = f"data/iceberg/tenant_{self.config.tenant_id}/{table_name}"
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Write as partitioned Parquet
            df.to_parquet(
                f"{output_path}/data.parquet",
                partition_cols=['tenant_id'] if 'tenant_id' in df.columns else None,
                compression='snappy'
            )
            
            self.logger.info(f"Data written to Iceberg staging: {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error writing to Trino/Iceberg: {e}")
            return False
    
    def ingest_data(self, source_config: Dict[str, Any], 
                   destination_table: str, **kwargs) -> IngestionMetrics:
        """Main ingestion method orchestrating the entire process."""
        self.metrics.start_time = datetime.utcnow()
        
        try:
            # Create and connect to source
            self.connector = self._create_connector(source_config)
            if not self.connector.connect():
                raise RuntimeError("Failed to connect to data source")
            
            # Read data
            self.logger.info("Reading data from source")
            df = self.connector.read_data(**kwargs)
            
            if isinstance(df, pd.io.common.TextFileReader):  # Handle chunked reading
                # Process chunks for large datasets
                all_chunks = []
                for chunk in df:
                    processed_chunk = self._process_chunk(chunk)
                    all_chunks.append(processed_chunk)
                    self.metrics.batches_processed += 1
                
                df = pd.concat(all_chunks, ignore_index=True)
            else:
                df = self._process_chunk(df)
                self.metrics.batches_processed = 1
            
            # Determine target engine
            target_engine = self._determine_target_engine(df)
            
            # Write to appropriate target
            if target_engine == TargetEngine.DUCKDB:
                success = self._write_to_duckdb(df, destination_table)
            else:
                success = self._write_to_trino(df, destination_table)
            
            if not success:
                raise RuntimeError("Failed to write data to target")
            
            # Update metrics
            self.metrics.records_processed = len(df)
            self.metrics.end_time = datetime.utcnow()
            self.metrics.processing_time_seconds = (
                self.metrics.end_time - self.metrics.start_time
            ).total_seconds()
            
            self.logger.info(f"Ingestion completed successfully. Records: {self.metrics.records_processed}")
            return self.metrics
            
        except Exception as e:
            self.metrics.errors_encountered += 1
            self.logger.error(f"Ingestion failed: {e}")
            raise
            
        finally:
            if self.connector:
                self.connector.disconnect()
    
    def _process_chunk(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process a data chunk with validation and tenant metadata."""
        try:
            # Validate data if enabled
            if self.validator:
                is_valid, issues = self.validator.validate_dataframe(df)
                if not is_valid:
                    self.logger.warning(f"Data validation issues: {issues}")
            
            # Add tenant metadata
            df_with_metadata = self.tenant_manager.add_tenant_metadata(df)
            
            return df_with_metadata
            
        except Exception as e:
            self.logger.error(f"Error processing chunk: {e}")
            raise


# Example usage and convenience functions
def create_mysql_config(host: str, database: str, user: str, password: str, 
                       port: int = 3306) -> Dict[str, Any]:
    """Create MySQL connection configuration."""
    return {
        'type': SourceType.MYSQL.value,
        'host': host,
        'port': port,
        'database': database,
        'user': user,
        'password': password
    }


def create_csv_config(file_path: str, separator: str = ',', 
                     encoding: str = 'utf-8', header: int = 0) -> Dict[str, Any]:
    """Create CSV file configuration."""
    return {
        'type': SourceType.CSV.value,
        'file_path': file_path,
        'separator': separator,
        'encoding': encoding,
        'header': header
    }


def quick_ingest(tenant_id: str, source_config: Dict[str, Any], 
                destination_table: str, source_type: SourceType, 
                **kwargs) -> IngestionMetrics:
    """Quick ingestion function for simple use cases."""
    # Determine target engine based on source type and config
    target_engine = TargetEngine.DUCKDB  # Default to DuckDB
    
    config = IngestionConfig(
        tenant_id=tenant_id,
        source_type=source_type,
        target_engine=target_engine,
        enable_validation=True,
        enable_partitioning=True
    )
    
    orchestrator = DataIngestionOrchestrator(config)
    return orchestrator.ingest_data(source_config, destination_table, **kwargs)
