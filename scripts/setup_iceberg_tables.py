#!/usr/bin/env python3
"""
Setup Iceberg tables through Trino for the analytics stack.
"""
import os
import sys
import logging
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from flight_server.app.trino_client import get_trino_client, TrinoConfig
from flight_server.app.monitoring import setup_monitoring

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_sample_data():
    """Create sample data for Iceberg tables."""
    np.random.seed(42)
    
    # Sample patient data
    diseases = ['COVID-19', 'Influenza', 'Pneumonia', 'Bronchitis', 'Asthma']
    patients = []
    
    for i in range(100):
        patient = {
            'patient_id': f'P{i:04d}',
            'disease': np.random.choice(diseases),
            'symptoms': {
                'fever': np.random.choice([True, False]),
                'cough': np.random.choice([True, False]),
                'fatigue': np.random.choice([True, False]),
                'breathing_difficulty': np.random.choice([True, False])
            },
            'demographics': {
                'age': np.random.randint(18, 80),
                'gender': np.random.choice(['Male', 'Female']),
                'location': np.random.choice(['Urban', 'Rural'])
            },
            'outcome': np.random.choice(['Positive', 'Negative']),
            'recorded_at': datetime.now() - timedelta(days=np.random.randint(0, 30))
        }
        patients.append(patient)
    
    return patients


def setup_iceberg_tables():
    """Set up Iceberg tables in Trino."""
    config = TrinoConfig(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        catalog="iceberg",
        schema="default"
    )
    
    client = get_trino_client(config)
    
    try:
        # Create patient_data table
        logger.info("Creating patient_data table...")
        
        create_table_query = """
            CREATE TABLE IF NOT EXISTS iceberg.default.patient_data (
                patient_id VARCHAR,
                disease VARCHAR,
                symptoms JSON,
                demographics JSON,
                outcome VARCHAR,
                recorded_at TIMESTAMP
            )
            WITH (
                format = 'PARQUET',
                partitioning = ARRAY['bucket(patient_id, 16)'],
                sorted_by = ARRAY['recorded_at']
            )
        """
        
        try:
            client.execute_query(create_table_query)
            logger.info("✅ patient_data table created successfully")
        except Exception as e:
            logger.warning(f"Table creation failed (might already exist): {e}")
        
        # Create historical trends table
        logger.info("Creating disease_trends table...")
        
        create_trends_query = """
            CREATE SCHEMA IF NOT EXISTS iceberg.historical;
            
            CREATE TABLE IF NOT EXISTS iceberg.historical.disease_trends (
                date DATE,
                disease_type VARCHAR,
                case_count INTEGER,
                region VARCHAR,
                severity_avg DOUBLE
            )
            WITH (
                format = 'PARQUET',
                partitioning = ARRAY['month(date)', 'region']
            )
        """
        
        try:
            client.execute_query(create_trends_query)
            logger.info("✅ disease_trends table created successfully")
        except Exception as e:
            logger.warning(f"Trends table creation failed: {e}")
        
        # Insert sample data
        logger.info("Inserting sample data...")
        
        sample_data = create_sample_data()
        
        # Insert data in batches
        for i in range(0, len(sample_data), 10):
            batch = sample_data[i:i+10]
            
            values = []
            for patient in batch:
                values.append(f"""
                    ('{patient['patient_id']}', 
                     '{patient['disease']}', 
                     JSON '{patient['symptoms']}', 
                     JSON '{patient['demographics']}', 
                     '{patient['outcome']}', 
                     TIMESTAMP '{patient['recorded_at'].strftime('%Y-%m-%d %H:%M:%S')}')
                """)
            
            insert_query = f"""
                INSERT INTO iceberg.default.patient_data 
                (patient_id, disease, symptoms, demographics, outcome, recorded_at)
                VALUES {', '.join(values)}
            """
            
            try:
                client.execute_query(insert_query)
                logger.info(f"✅ Inserted batch {i//10 + 1}")
            except Exception as e:
                logger.error(f"Failed to insert batch {i//10 + 1}: {e}")
        
        # Create materialized view for quick analytics
        logger.info("Creating analytics materialized view...")
        
        mv_query = """
            CREATE OR REPLACE MATERIALIZED VIEW iceberg.default.patient_analytics
            WITH (refresh_interval = '1h')
            AS
            SELECT 
                disease,
                COUNT(*) as total_cases,
                AVG(CASE WHEN outcome = 'Positive' THEN 1.0 ELSE 0.0 END) as positivity_rate,
                AVG(CAST(JSON_EXTRACT_SCALAR(demographics, '$.age') AS INTEGER)) as avg_age,
                DATE_TRUNC('day', recorded_at) as recorded_date
            FROM iceberg.default.patient_data
            WHERE recorded_at >= CURRENT_DATE - INTERVAL '30' DAY
            GROUP BY disease, DATE_TRUNC('day', recorded_at)
        """
        
        try:
            client.execute_query(mv_query)
            logger.info("✅ Analytics materialized view created successfully")
        except Exception as e:
            logger.warning(f"Materialized view creation failed: {e}")
        
        # Test queries
        logger.info("Running test queries...")
        
        # Test 1: Count records
        count_query = "SELECT COUNT(*) as total_records FROM iceberg.default.patient_data"
        result = client.execute_query(count_query)
        logger.info(f"Total records in patient_data: {result.iloc[0]['total_records']}")
        
        # Test 2: Disease distribution
        disease_query = """
            SELECT disease, COUNT(*) as count 
            FROM iceberg.default.patient_data 
            GROUP BY disease 
            ORDER BY count DESC
        """
        result = client.execute_query(disease_query)
        logger.info("Disease distribution:")
        print(result)
        
        # Test 3: Table statistics
        stats_query = "SHOW STATS FOR iceberg.default.patient_data"
        try:
            result = client.execute_query(stats_query)
            logger.info("Table statistics:")
            print(result)
        except Exception as e:
            logger.warning(f"Could not get table statistics: {e}")
        
        logger.info("🎉 Iceberg tables setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error setting up Iceberg tables: {e}")
        sys.exit(1)
    
    finally:
        client.close()


def main():
    """Main function."""
    setup_monitoring()
    setup_iceberg_tables()


if __name__ == "__main__":
    main()
