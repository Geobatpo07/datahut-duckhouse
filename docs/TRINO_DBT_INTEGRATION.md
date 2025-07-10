# Trino and dbt Integration Guide

This document provides comprehensive guidance on using Trino to query Iceberg tables and dbt for data transformations in the DataHut-DuckHouse project.

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   DuckDB        │    │     Trino       │    │   Iceberg       │
│   (Local)       │    │   (Query Engine)│    │   (Storage)     │
│                 │    │                 │    │                 │
│ • Fast queries  │◄──►│ • SQL interface │◄──►│ • Versioned     │
│ • Development   │    │ • Federation    │    │ • Scalable      │
│ • Staging       │    │ • Performance   │    │ • ACID          │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         ▲                        ▲                        ▲
         │                        │                        │
         └────────────────────────┼────────────────────────┘
                                  │
                       ┌─────────────────┐
                       │      dbt        │
                       │ (Transformation)│
                       │                 │
                       │ • Dev: DuckDB   │
                       │ • Prod: Trino   │
                       │ • Testing       │
                       └─────────────────┘
```

## Trino Configuration

### 1. Core Configuration

The Trino configuration is located in `config/trino/etc/`:

- **`trino.properties`**: Main Trino server configuration
- **`resource-groups.json`**: Resource management for query execution
- **`catalog/iceberg.properties`**: Iceberg connector configuration
- **`hadoop-config.xml`**: Hadoop/S3 configuration for Iceberg

### 2. Iceberg Connector Features

Our Iceberg connector is configured with:

- **Format**: Parquet with Snappy compression
- **Caching**: Metadata caching enabled (10min TTL)
- **Performance**: Projection pushdown, dynamic filtering
- **Optimization**: Table statistics, extended statistics
- **Procedures**: Optimize, register table, materialized views

### 3. Resource Management

Resource groups are configured to manage query execution:

- **Data Engineers**: 50% memory, 50 concurrent queries
- **Analysts**: 30% memory, 30 concurrent queries  
- **Ad-hoc**: 20% memory, 20 concurrent queries

## dbt Configuration

### 1. Multi-Environment Setup

dbt is configured to work with both DuckDB (development) and Trino (production):

```yaml
# config/dbt_profiles.yml
dbt_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ingestion/data/duckhouse.duckdb
      threads: 4
    prod:
      type: trino
      schema: analytics
      database: iceberg
      host: localhost
      port: 8080
      user: datahut
      threads: 10
```

### 2. Model Structure

Models are organized in a standard structure:

```
transform/dbt_project/models/
├── sources.yml              # Source definitions
├── staging/
│   ├── stg_healthcare_data.sql
│   └── schema.yml
├── marts/
│   ├── mart_disease_analytics.sql
│   └── mart_iceberg_integration.sql
├── tests/
│   └── test_data_quality.sql
└── macros/
    ├── get_column_values.sql
    └── data_quality_checks.sql
```

### 3. Environment-Specific Logic

Models use Jinja templating for environment-specific logic:

```sql
{% if target.name == 'prod' %}
-- Production: Use Trino to query Iceberg tables
SELECT * FROM {{ source('iceberg_raw', 'patient_data') }}
{% else %}
-- Development: Use DuckDB data
SELECT * FROM {{ ref('stg_healthcare_data') }}
{% endif %}
```

## Usage Examples

### 1. Querying Trino

#### Basic Query
```bash
# Query a specific table
make query-trino-table TABLE=patient_data

# Execute custom query
make query-trino QUERY="SELECT disease, COUNT(*) FROM iceberg.default.patient_data GROUP BY disease"

# Get table information
make query-trino-info TABLE=patient_data
```

#### Python Client
```python
from flight_server.app.trino_client import get_trino_client

client = get_trino_client()

# List available tables
tables = client.list_tables()
print(tables)

# Execute query
df = client.execute_query("SELECT * FROM iceberg.default.patient_data LIMIT 10")
print(df)

# Get table statistics
stats = client.get_table_stats("patient_data")
print(stats)

client.close()
```

### 2. Running dbt

#### Development Mode (DuckDB)
```bash
# Run models in development
make dbt-run-dev

# Test models in development
make dbt-test-dev
```

#### Production Mode (Trino)
```bash
# Run models in production
make dbt-run-prod

# Test models in production
make dbt-test-prod
```

#### Advanced dbt Commands
```bash
# Run specific model
cd transform/dbt_project
dbt run --profiles-dir config --select stg_healthcare_data

# Run with full refresh
dbt run --profiles-dir config --full-refresh

# Generate documentation
dbt docs generate --profiles-dir config
dbt docs serve --profiles-dir config
```

### 3. Setting Up Iceberg Tables

```bash
# Setup Iceberg tables through Trino
make setup-iceberg

# This will create:
# - iceberg.default.patient_data
# - iceberg.historical.disease_trends
# - Sample data for testing
```

## Advanced Features

### 1. Materialized Views

Create materialized views for better query performance:

```sql
CREATE MATERIALIZED VIEW iceberg.default.patient_analytics
WITH (refresh_interval = '1h')
AS
SELECT 
    disease,
    COUNT(*) as total_cases,
    AVG(CASE WHEN outcome = 'Positive' THEN 1.0 ELSE 0.0 END) as positivity_rate
FROM iceberg.default.patient_data
GROUP BY disease
```

### 2. Table Optimization

Optimize Iceberg tables for better performance:

```python
from flight_server.app.trino_client import get_trino_client

client = get_trino_client()

# Optimize table (compaction)
client.optimize_table("patient_data")

# Expire old snapshots
client.expire_snapshots("patient_data", older_than="7d")

# Create table with partitioning
client.create_table_as_select(
    target_table="partitioned_data",
    source_query="SELECT * FROM patient_data",
    table_properties={
        "format": "PARQUET",
        "partitioning": "ARRAY['bucket(disease, 16)']"
    }
)
```

### 3. Data Quality Testing

dbt includes comprehensive data quality tests:

```sql
-- Custom data quality test
{{ config(severity='warn') }}

SELECT
    model_name,
    total_rows,
    null_count,
    invalid_count
FROM data_quality_summary
WHERE null_count > 0 OR invalid_count > 0
```

## Performance Optimization

### 1. Query Optimization

- Use **projection pushdown** for column selection
- Leverage **partition pruning** for time-based queries
- Use **bucketed tables** for join optimization
- Enable **dynamic filtering** for complex queries

### 2. Storage Optimization

- **Parquet format** with Snappy compression
- **Optimal file sizes** (128MB target)
- **Partition strategy** based on query patterns
- **Regular optimization** via compaction

### 3. Resource Management

- **Resource groups** for workload isolation
- **Memory allocation** based on query complexity
- **Concurrent query limits** to prevent resource exhaustion
- **Spill to disk** for large queries

## Monitoring and Observability

### 1. Query Monitoring

```sql
-- Monitor running queries
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
```

### 2. Table Statistics

```sql
-- Get table statistics
SHOW STATS FOR iceberg.default.patient_data;

-- Get table history
SELECT * FROM iceberg.default."patient_data$history";

-- Get snapshots
SELECT * FROM iceberg.default."patient_data$snapshots";
```

### 3. Performance Metrics

The system includes comprehensive monitoring:

- **Query execution times**
- **Resource utilization**
- **Table access patterns**
- **Error rates and types**

## Troubleshooting

### Common Issues

1. **Connection Errors**
   - Check Trino server is running: `docker-compose ps`
   - Verify network connectivity: `curl http://localhost:8080/ui`

2. **Query Failures**
   - Check resource limits in resource groups
   - Verify table permissions and catalog access
   - Review query complexity and optimize

3. **dbt Errors**
   - Ensure correct profile configuration
   - Check model dependencies
   - Verify source table availability

### Debugging Commands

```bash
# Check Trino cluster status
make query-trino

# List available catalogs and schemas
make query-trino-list

# Test basic connectivity
curl http://localhost:8080/v1/info

# Check dbt configuration
cd transform/dbt_project
dbt debug --profiles-dir config
```

## Best Practices

### 1. Development Workflow

1. **Develop locally** with DuckDB for fast iteration
2. **Test with sample data** before production deployment
3. **Use environment-specific models** for different data sources
4. **Implement comprehensive testing** with dbt tests

### 2. Production Deployment

1. **Optimize queries** for Trino execution
2. **Use appropriate partitioning** strategies
3. **Monitor query performance** and resource usage
4. **Schedule regular maintenance** tasks

### 3. Data Management

1. **Implement data quality checks** at ingestion
2. **Use versioning** for schema evolution
3. **Regular cleanup** of old snapshots
4. **Monitor storage usage** and costs

## Integration Testing

Run the full stack integration test:

```bash
make full-stack-test
```

This will:
1. Start all services (MinIO, Trino, Flight Server)
2. Setup Iceberg tables
3. Ingest sample data
4. Run dbt transformations
5. Test Trino connectivity
6. Validate data quality

## Conclusion

The Trino and dbt integration provides a powerful, scalable analytics platform that combines:

- **Fast development** with DuckDB
- **Scalable production** with Trino and Iceberg
- **Comprehensive testing** with dbt
- **Data quality assurance** with automated tests
- **Performance optimization** through proper configuration

This architecture supports both ad-hoc analysis and production data pipelines while maintaining data quality and performance standards.
