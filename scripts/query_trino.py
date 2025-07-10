#!/usr/bin/env python3
"""
Query Trino for Iceberg tables with comprehensive examples.
"""
import os
import sys
import argparse
import logging
from pathlib import Path

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from flight_server.app.trino_client import get_trino_client, TrinoConfig, query_iceberg_table, get_iceberg_table_info
from flight_server.app.monitoring import setup_monitoring

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Main function to demonstrate Trino/Iceberg querying."""
    parser = argparse.ArgumentParser(description="Query Trino for Iceberg tables")
    parser.add_argument("--host", default="localhost", help="Trino host")
    parser.add_argument("--port", type=int, default=8080, help="Trino port")
    parser.add_argument("--catalog", default="iceberg", help="Trino catalog")
    parser.add_argument("--schema", default="default", help="Trino schema")
    parser.add_argument("--table", help="Specific table to query")
    parser.add_argument("--query", help="Custom SQL query")
    parser.add_argument("--limit", type=int, default=10, help="Limit results")
    parser.add_argument("--info", action="store_true", help="Show table info")
    parser.add_argument("--list", action="store_true", help="List tables")
    
    args = parser.parse_args()
    
    # Setup monitoring
    setup_monitoring()
    
    # Create Trino configuration
    config = TrinoConfig(
        host=args.host,
        port=args.port,
        catalog=args.catalog,
        schema=args.schema
    )
    
    try:
        client = get_trino_client(config)
        
        if args.list:
            # List catalogs, schemas, and tables
            print("\\n=== Available Catalogs ===")
            catalogs = client.list_catalogs()
            for catalog in catalogs:
                print(f"  - {catalog}")
            
            print(f"\\n=== Schemas in {args.catalog} ===")
            schemas = client.list_schemas(args.catalog)
            for schema in schemas:
                print(f"  - {schema}")
            
            print(f"\\n=== Tables in {args.catalog}.{args.schema} ===")
            tables = client.list_tables(args.catalog, args.schema)
            for table in tables:
                print(f"  - {table}")
            
        elif args.info and args.table:
            # Show table information
            print(f"\\n=== Table Info: {args.table} ===")
            info = get_iceberg_table_info(args.table)
            
            print("\\n--- Table Structure ---")
            print(info["description"])
            
            print("\\n--- Table Statistics ---")
            print(info["stats"])
            
            print("\\n--- Table History ---")
            history = info["history"]
            if not history.empty:
                print(history.head())
            else:
                print("No history available")
            
            print("\\n--- Table Partitions ---")
            partitions = info["partitions"]
            if not partitions.empty:
                print(partitions.head())
            else:
                print("No partitions or table is not partitioned")
        
        elif args.query:
            # Execute custom query
            print(f"\\n=== Executing Query ===")
            print(f"Query: {args.query}")
            
            df = client.execute_query(args.query)
            print(f"\\nResults ({len(df)} rows):")
            print(df.head(args.limit))
            
        elif args.table:
            # Query specific table
            print(f"\\n=== Querying Table: {args.table} ===")
            
            # Get basic table info
            description = client.describe_table(args.table)
            print("\\nTable Structure:")
            print(description)
            
            # Query data
            df = query_iceberg_table(args.table, limit=args.limit)
            print(f"\\nData Sample ({len(df)} rows):")
            print(df.head(args.limit))
            
        else:
            # Show system information
            print("\\n=== Trino System Information ===")
            
            # System tables query
            system_query = """
                SELECT 
                    node_id, 
                    http_uri, 
                    node_version, 
                    coordinator
                FROM system.runtime.nodes
            """
            
            try:
                nodes_df = client.execute_query(system_query)
                print("\\nCluster Nodes:")
                print(nodes_df)
            except Exception as e:
                print(f"Could not get cluster info: {e}")
            
            # Query running queries
            queries_query = """
                SELECT 
                    query_id, 
                    query_type, 
                    state, 
                    created, 
                    elapsed_time,
                    source
                FROM system.runtime.queries 
                WHERE state != 'FINISHED'
                ORDER BY created DESC
                LIMIT 5
            """
            
            try:
                queries_df = client.execute_query(queries_query)
                print("\\nRunning Queries:")
                print(queries_df)
            except Exception as e:
                print(f"Could not get queries info: {e}")
    
    except Exception as e:
        logger.error(f"Error executing Trino operation: {e}")
        sys.exit(1)
    
    finally:
        client.close()


if __name__ == "__main__":
    main()
