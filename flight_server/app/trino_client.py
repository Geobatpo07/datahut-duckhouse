"""
Trino client for querying Iceberg tables.
"""

import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import pandas as pd
import trino
from trino.auth import BasicAuthentication
from trino.exceptions import TrinoQueryError

from .monitoring import monitor_performance, trace_operation

logger = logging.getLogger(__name__)


@dataclass
class TrinoConfig:
    """Configuration for Trino connection."""

    host: str = "localhost"
    port: int = 8080
    user: str = "datahut"
    catalog: str = "iceberg"
    schema: str = "default"
    auth: BasicAuthentication | None = None
    source: str = "datahut-duckhouse"
    client_tags: list[str] = None

    def __post_init__(self):
        if self.client_tags is None:
            self.client_tags = ["datahut", "iceberg"]


class TrinoClient:
    """Enhanced Trino client for Iceberg operations."""

    def __init__(self, config: TrinoConfig | None = None):
        """Initialize Trino client with configuration."""
        self.config = config or TrinoConfig()
        self._connection = None
        self._cursor = None

    @property
    def connection(self) -> trino.dbapi.Connection:
        """Get or create Trino connection."""
        if self._connection is None:
            self._connection = trino.dbapi.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                catalog=self.config.catalog,
                schema=self.config.schema,
                auth=self.config.auth,
                source=self.config.source,
                client_tags=self.config.client_tags,
                request_timeout=60,
            )
        return self._connection

    @contextmanager
    def cursor(self):
        """Context manager for database cursor."""
        cursor = self.connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    @monitor_performance
    def execute_query(
        self, query: str, parameters: dict[str, Any] | None = None
    ) -> pd.DataFrame:
        """Execute a query and return results as DataFrame."""
        with trace_operation("trino_query", {"query": query[:100]}):
            try:
                with self.cursor() as cursor:
                    logger.info(f"Executing query: {query[:200]}...")

                    if parameters:
                        cursor.execute(query, parameters)
                    else:
                        cursor.execute(query)

                    # Get column names
                    columns = [desc[0] for desc in cursor.description or []]

                    # Fetch all results
                    results = cursor.fetchall()

                    # Convert to DataFrame
                    df = pd.DataFrame(results, columns=columns)
                    logger.info(f"Query executed successfully. Returned {len(df)} rows")

                    return df

            except TrinoQueryError as e:
                logger.error(f"Trino query failed: {e}")
                raise
            except Exception as e:
                logger.error(f"Unexpected error executing query: {e}")
                raise

    @monitor_performance
    def list_catalogs(self) -> list[str]:
        """List all available catalogs."""
        with trace_operation("list_catalogs"):
            df = self.execute_query("SHOW CATALOGS")
            return df["Catalog"].tolist()

    @monitor_performance
    def list_schemas(self, catalog: str | None = None) -> list[str]:
        """List all schemas in a catalog."""
        catalog = catalog or self.config.catalog
        with trace_operation("list_schemas", {"catalog": catalog}):
            df = self.execute_query(f"SHOW SCHEMAS FROM {catalog}")
            return df["Schema"].tolist()

    @monitor_performance
    def list_tables(
        self, catalog: str | None = None, schema: str | None = None
    ) -> list[str]:
        """List all tables in a schema."""
        catalog = catalog or self.config.catalog
        schema = schema or self.config.schema
        with trace_operation("list_tables", {"catalog": catalog, "schema": schema}):
            df = self.execute_query(f"SHOW TABLES FROM {catalog}.{schema}")
            return df["Table"].tolist()

    @monitor_performance
    def describe_table(
        self, table_name: str, catalog: str | None = None, schema: str | None = None
    ) -> pd.DataFrame:
        """Describe table structure."""
        catalog = catalog or self.config.catalog
        schema = schema or self.config.schema
        with trace_operation(
            "describe_table", {"table": f"{catalog}.{schema}.{table_name}"}
        ):
            return self.execute_query(f"DESCRIBE {catalog}.{schema}.{table_name}")

    @monitor_performance
    def get_table_stats(
        self, table_name: str, catalog: str | None = None, schema: str | None = None
    ) -> pd.DataFrame:
        """Get table statistics."""
        catalog = catalog or self.config.catalog
        schema = schema or self.config.schema
        with trace_operation(
            "get_table_stats", {"table": f"{catalog}.{schema}.{table_name}"}
        ):
            return self.execute_query(f"SHOW STATS FOR {catalog}.{schema}.{table_name}")

    @monitor_performance
    def get_table_partitions(
        self, table_name: str, catalog: str | None = None, schema: str | None = None
    ) -> pd.DataFrame:
        """Get table partitions."""
        catalog = catalog or self.config.catalog
        schema = schema or self.config.schema
        with trace_operation(
            "get_table_partitions", {"table": f"{catalog}.{schema}.{table_name}"}
        ):
            return self.execute_query(
                f'SELECT * FROM {catalog}.{schema}."{table_name}$partitions"'
            )

    @monitor_performance
    def create_table_as_select(
        self,
        target_table: str,
        source_query: str,
        catalog: str | None = None,
        schema: str | None = None,
        table_properties: dict[str, str] | None = None,
    ) -> bool:
        """Create table as select (CTAS)."""
        catalog = catalog or self.config.catalog
        schema = schema or self.config.schema

        with trace_operation(
            "create_table_as_select",
            {
                "target_table": f"{catalog}.{schema}.{target_table}",
                "source_query": source_query[:100],
            },
        ):
            # Build table properties
            properties_str = ""
            if table_properties:
                props = [f"'{k}' = '{v}'" for k, v in table_properties.items()]
                properties_str = f"WITH ({', '.join(props)})"

            query = f"""
                CREATE TABLE {catalog}.{schema}.{target_table} {properties_str}
                AS {source_query}
            """

            try:
                self.execute_query(query)
                logger.info(f"Table {target_table} created successfully")
                return True
            except Exception as e:
                logger.error(f"Failed to create table {target_table}: {e}")
                return False

    @monitor_performance
    def insert_into_table(
        self,
        table_name: str,
        source_query: str,
        catalog: str | None = None,
        schema: str | None = None,
    ) -> bool:
        """Insert data into table."""
        catalog = catalog or self.config.catalog
        schema = schema or self.config.schema

        with trace_operation(
            "insert_into_table",
            {
                "table": f"{catalog}.{schema}.{table_name}",
                "source_query": source_query[:100],
            },
        ):
            query = f"""
                INSERT INTO {catalog}.{schema}.{table_name}
                {source_query}
            """

            try:
                self.execute_query(query)
                logger.info(f"Data inserted into {table_name} successfully")
                return True
            except Exception as e:
                logger.error(f"Failed to insert data into {table_name}: {e}")
                return False

    @monitor_performance
    def optimize_table(
        self, table_name: str, catalog: str | None = None, schema: str | None = None
    ) -> bool:
        """Optimize Iceberg table (compaction)."""
        catalog = catalog or self.config.catalog
        schema = schema or self.config.schema

        with trace_operation(
            "optimize_table", {"table": f"{catalog}.{schema}.{table_name}"}
        ):
            query = (
                f"CALL {catalog}.system.rewrite_data_files('{schema}', '{table_name}')"
            )

            try:
                self.execute_query(query)
                logger.info(f"Table {table_name} optimized successfully")
                return True
            except Exception as e:
                logger.error(f"Failed to optimize table {table_name}: {e}")
                return False

    @monitor_performance
    def expire_snapshots(
        self,
        table_name: str,
        older_than: str = "7d",
        catalog: str | None = None,
        schema: str | None = None,
    ) -> bool:
        """Expire old snapshots."""
        catalog = catalog or self.config.catalog
        schema = schema or self.config.schema

        with trace_operation(
            "expire_snapshots",
            {"table": f"{catalog}.{schema}.{table_name}", "older_than": older_than},
        ):
            query = f"CALL {catalog}.system.expire_snapshots('{schema}', '{table_name}', INTERVAL '{older_than}')"

            try:
                self.execute_query(query)
                logger.info(f"Snapshots expired for table {table_name}")
                return True
            except Exception as e:
                logger.error(f"Failed to expire snapshots for table {table_name}: {e}")
                return False

    @monitor_performance
    def get_table_history(
        self, table_name: str, catalog: str | None = None, schema: str | None = None
    ) -> pd.DataFrame:
        """Get table history/snapshots."""
        catalog = catalog or self.config.catalog
        schema = schema or self.config.schema

        with trace_operation(
            "get_table_history", {"table": f"{catalog}.{schema}.{table_name}"}
        ):
            return self.execute_query(
                f'SELECT * FROM {catalog}.{schema}."{table_name}$history"'
            )

    @monitor_performance
    def create_materialized_view(
        self,
        view_name: str,
        query: str,
        catalog: str | None = None,
        schema: str | None = None,
        refresh_interval: str = "1h",
    ) -> bool:
        """Create materialized view."""
        catalog = catalog or self.config.catalog
        schema = schema or self.config.schema

        with trace_operation(
            "create_materialized_view",
            {"view": f"{catalog}.{schema}.{view_name}", "query": query[:100]},
        ):
            create_query = f"""
                CREATE MATERIALIZED VIEW {catalog}.{schema}.{view_name}
                WITH (refresh_interval = '{refresh_interval}')
                AS {query}
            """

            try:
                self.execute_query(create_query)
                logger.info(f"Materialized view {view_name} created successfully")
                return True
            except Exception as e:
                logger.error(f"Failed to create materialized view {view_name}: {e}")
                return False

    def close(self):
        """Close the connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Trino connection closed")


def get_trino_client(config: TrinoConfig | None = None) -> TrinoClient:
    """Factory function to create Trino client."""
    if config is None:
        config = TrinoConfig(
            host=os.getenv("TRINO_HOST", "localhost"),
            port=int(os.getenv("TRINO_PORT", "8080")),
            user=os.getenv("TRINO_USER", "datahut"),
            catalog=os.getenv("TRINO_CATALOG", "iceberg"),
            schema=os.getenv("TRINO_SCHEMA", "default"),
        )

    return TrinoClient(config)


# Helper functions for common operations
def query_iceberg_table(
    table_name: str, query: str | None = None, limit: int | None = None
) -> pd.DataFrame:
    """Quick function to query an Iceberg table."""
    client = get_trino_client()

    if query is None:
        query = (
            f"SELECT * FROM {client.config.catalog}.{client.config.schema}.{table_name}"
        )
        if limit:
            query += f" LIMIT {limit}"

    try:
        return client.execute_query(query)
    finally:
        client.close()


def get_iceberg_table_info(table_name: str) -> dict[str, Any]:
    """Get comprehensive information about an Iceberg table."""
    client = get_trino_client()

    try:
        info = {}
        info["description"] = client.describe_table(table_name)
        info["stats"] = client.get_table_stats(table_name)
        info["history"] = client.get_table_history(table_name)

        try:
            info["partitions"] = client.get_table_partitions(table_name)
        except Exception:
            info["partitions"] = pd.DataFrame()  # Not all tables have partitions

        return info
    finally:
        client.close()
