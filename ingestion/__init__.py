"""Multi-source data ingestion module for multi-tenant architecture.

This module provides a unified interface for ingesting data from various sources
into either DuckDB (for small datasets) or Iceberg tables via Trino (for large datasets).
"""

from .multi_source_ingestion import DataIngestionOrchestrator
from .connectors import BaseConnector, CSVConnector, PostgreSQLConnector, MySQLConnector
from .data_validator import DataValidator
from .tenant_manager import TenantDataManager

__all__ = [
    'DataIngestionOrchestrator',
    'BaseConnector',
    'CSVConnector',
    'PostgreSQLConnector',
    'MySQLConnector',
    'DataValidator',
    'TenantDataManager'
]
