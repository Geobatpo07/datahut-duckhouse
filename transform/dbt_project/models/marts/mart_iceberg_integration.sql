{{
  config(
    materialized='table',
    tags=['mart', 'iceberg', 'integration'],
    description='Integrated view combining DuckDB and Iceberg data sources'
  )
}}

{% if target.name == 'prod' %}
-- Production: Use Trino to query Iceberg tables (large datasets)
with iceberg_patients as (
  select
    patient_id,
    disease,
    symptoms,
    demographics,
    outcome,
    recorded_at
  from {{ source('iceberg_distributed', 'patient_data') }}
  where recorded_at >= current_date - interval '30' day
),

duckdb_summary as (
  select
    disease_name,
    total_cases,
    positivity_rate,
    avg_symptoms_per_case,
    calculated_at
  from {{ ref('mart_disease_analytics') }}
),

integrated_data as (
  select
    i.patient_id,
    i.disease,
    i.symptoms,
    i.demographics,
    i.outcome,
    i.recorded_at,
    d.total_cases,
    d.positivity_rate,
    d.avg_symptoms_per_case,
    d.calculated_at as analytics_updated_at
  from iceberg_patients i
  left join duckdb_summary d on upper(i.disease) = d.disease_name
)

select * from integrated_data

{% else %}
-- Development: Use DuckDB data only (local, lightweight queries)
select
  patient_id,
  disease_name as disease,
  json_object(
    'fever', has_fever,
    'cough', has_cough,
    'fatigue', has_fatigue,
    'breathing_difficulty', has_difficulty_breathing
  ) as symptoms,
  json_object(
    'age', age,
    'gender', gender,
    'age_group', age_group
  ) as demographics,
  outcome_label as outcome,
  processed_at as recorded_at,
  null as total_cases,
  null as positivity_rate,
  null as avg_symptoms_per_case,
  null as analytics_updated_at
from {{ ref('stg_healthcare_data') }}
where processed_at >= current_date - interval '30' day

{% endif %}
