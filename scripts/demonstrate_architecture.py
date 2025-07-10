#!/usr/bin/env python3
"""
Demonstrate the corrected DataHut-DuckHouse architecture:
- DuckDB for local, lightweight queries
- Trino for distributed queries on Iceberg tables
- Xorq orchestrating the process via Flight server
"""
import os
import sys
import time
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from flight_server.app.query_orchestrator import get_query_orchestrator
from flight_server.app.trino_client import get_trino_client
from flight_server.app.utils import get_duckdb_connection

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def demonstrate_duckdb_queries():
    """Demonstrate DuckDB for local, lightweight queries."""
    print("\\n" + "="*60)
    print("🦆 DuckDB: Local, Lightweight Queries")
    print("="*60)
    
    try:
        conn = get_duckdb_connection()
        
        # Create sample local data
        sample_data = """
        CREATE OR REPLACE TABLE local_patients AS
        SELECT 
            'P' || generate_series AS patient_id,
            (ARRAY['Flu', 'Cold', 'Headache', 'Allergy'])[1 + (generate_series % 4)] AS disease,
            20 + (generate_series % 50) AS age,
            (ARRAY['Male', 'Female'])[1 + (generate_series % 2)] AS gender,
            (ARRAY['Positive', 'Negative'])[1 + (generate_series % 2)] AS outcome
        FROM generate_series(1, 1000)
        """
        
        conn.execute(sample_data)
        print("✅ Created local sample data (1,000 rows)")
        
        # Example lightweight queries
        queries = [
            ("Count by disease", "SELECT disease, COUNT(*) as count FROM local_patients GROUP BY disease"),
            ("Age distribution", "SELECT AVG(age) as avg_age, MIN(age) as min_age, MAX(age) as max_age FROM local_patients"),
            ("Outcome summary", "SELECT outcome, COUNT(*) as count FROM local_patients GROUP BY outcome"),
        ]
        
        for description, query in queries:
            start_time = time.time()
            result = conn.execute(query).fetchall()
            end_time = time.time()
            
            print(f"\\n📊 {description}:")
            for row in result:
                print(f"  {row}")
            print(f"⏱️  Query time: {(end_time - start_time)*1000:.2f}ms")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"DuckDB demonstration failed: {e}")


def demonstrate_trino_queries():
    """Demonstrate Trino for distributed queries on Iceberg."""
    print("\\n" + "="*60)
    print("🚀 Trino: Distributed Queries on Iceberg")
    print("="*60)
    
    try:
        client = get_trino_client()
        
        # List available Iceberg tables
        print("📋 Available Iceberg tables:")
        tables = client.list_tables()
        for table in tables:
            print(f"  - {table}")
        
        if not tables:
            print("  No Iceberg tables found. Run 'make setup-iceberg' first.")
            return
        
        # Example distributed queries
        queries = [
            ("Total patients", "SELECT COUNT(*) as total_patients FROM iceberg.default.patient_data"),
            ("Disease distribution", "SELECT disease, COUNT(*) as count FROM iceberg.default.patient_data GROUP BY disease ORDER BY count DESC"),
            ("Recent data", "SELECT disease, COUNT(*) as count FROM iceberg.default.patient_data WHERE recorded_at >= CURRENT_DATE - INTERVAL '7' DAY GROUP BY disease"),
        ]
        
        for description, query in queries:
            try:
                start_time = time.time()
                result = client.execute_query(query)
                end_time = time.time()
                
                print(f"\\n📊 {description}:")
                print(result.head(10))  # Show first 10 rows
                print(f"⏱️  Query time: {(end_time - start_time)*1000:.2f}ms")
                print(f"📈 Rows returned: {len(result)}")
                
            except Exception as e:
                print(f"  ❌ Query failed: {e}")
        
        client.close()
        
    except Exception as e:
        logger.error(f"Trino demonstration failed: {e}")


def demonstrate_orchestration():
    """Demonstrate query orchestration between DuckDB and Trino."""
    print("\\n" + "="*60)
    print("🎯 Query Orchestration: Smart Routing")
    print("="*60)
    
    try:
        orchestrator = get_query_orchestrator()
        
        # Test queries that should go to different backends
        test_queries = [
            {
                "query": "SELECT COUNT(*) FROM local_patients",
                "expected_backend": "DuckDB",
                "reason": "Local table"
            },
            {
                "query": "SELECT COUNT(*) FROM patient_data", 
                "expected_backend": "Trino",
                "reason": "Iceberg table"
            },
            {
                "query": "SELECT disease, COUNT(*) FROM local_patients GROUP BY disease",
                "expected_backend": "DuckDB", 
                "reason": "Simple aggregation on local data"
            },
            {
                "query": "SELECT disease, AVG(CAST(JSON_EXTRACT_SCALAR(demographics, '$.age') AS INTEGER)) FROM patient_data GROUP BY disease",
                "expected_backend": "Trino",
                "reason": "Complex query on large dataset"
            }
        ]
        
        for test in test_queries:
            print(f"\\n🔍 Testing query: {test['query'][:50]}...")
            print(f"📝 Expected: {test['expected_backend']} ({test['reason']})")
            
            try:
                # This will log which backend was chosen
                result = orchestrator.execute_query(test['query'])
                print(f"✅ Query executed successfully, returned {len(result)} rows")
                
            except Exception as e:
                print(f"❌ Query failed: {e}")
        
        # List tables from both backends
        print("\\n📋 Available tables:")
        tables = orchestrator.list_tables()
        print(f"  DuckDB: {tables['duckdb']}")
        print(f"  Iceberg: {tables['iceberg']}")
        
        orchestrator.close()
        
    except Exception as e:
        logger.error(f"Orchestration demonstration failed: {e}")


def demonstrate_data_routing():
    """Demonstrate data routing based on size."""
    print("\\n" + "="*60)
    print("📊 Data Routing: Size-Based Backend Selection")
    print("="*60)
    
    print("🔄 Data Routing Logic:")
    print("  • Small datasets (< 100k rows) → DuckDB (local, fast)")
    print("  • Large datasets (> 100k rows) → Iceberg (distributed, scalable)")
    print("  • Complex queries → Trino (distributed processing)")
    print("  • Simple queries → DuckDB (local processing)")
    
    print("\\n📋 Table Registry:")
    orchestrator = get_query_orchestrator()
    
    duckdb_tables = [k for k, v in orchestrator.table_registry.items() if v.value == 'duckdb']
    iceberg_tables = [k for k, v in orchestrator.table_registry.items() if v.value == 'iceberg']
    
    print(f"  DuckDB Tables: {duckdb_tables}")
    print(f"  Iceberg Tables: {iceberg_tables}")
    
    print("\\n🎯 Query Routing Thresholds:")
    print(f"  • Row threshold: {orchestrator.row_threshold:,} rows")
    print(f"  • Complexity threshold: {orchestrator.complexity_threshold}")
    print(f"  • Join threshold: {orchestrator.join_threshold} tables")


def main():
    """Main demonstration function."""
    print("🏠 DataHut-DuckHouse Architecture Demonstration")
    print("=" * 60)
    print("Architecture Overview:")
    print("  🦆 DuckDB: Local, lightweight queries")
    print("  🚀 Trino: Distributed queries on Iceberg")
    print("  🎯 Xorq: Orchestrates via Flight server")
    print("  📊 dbt: Transforms data in both environments")
    
    # Demonstrate each component
    demonstrate_duckdb_queries()
    demonstrate_trino_queries()
    demonstrate_orchestration()
    demonstrate_data_routing()
    
    print("\\n" + "="*60)
    print("✅ Architecture demonstration completed!")
    print("="*60)
    
    print("\\n🔧 To test the full stack:")
    print("  1. Start services: make docker-up")
    print("  2. Setup Iceberg: make setup-iceberg")
    print("  3. Ingest data: make ingest-data")
    print("  4. Run dbt: make dbt-run-dev")
    print("  5. Query data: make query-trino-list")


if __name__ == "__main__":
    main()
