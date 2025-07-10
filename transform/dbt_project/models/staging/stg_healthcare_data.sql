{{
  config(
    materialized='view',
    tags=['staging', 'healthcare', 'duckdb'],
    description='Cleaned and standardized healthcare data from DuckDB'
  )
}}

with source_data as (
  select * from {{ source('duckdb_local', 'rev') }}
),

cleaned_data as (
  select
    -- Patient identifiers
    row_number() over (order by "Disease", "Age", "Gender") as patient_id,
    
    -- Disease information
    trim(upper("Disease")) as disease_name,
    
    -- Symptoms (binary encoding)
    case when trim(upper("Fever")) = 'YES' then 1 else 0 end as has_fever,
    case when trim(upper("Cough")) = 'YES' then 1 else 0 end as has_cough,
    case when trim(upper("Fatigue")) = 'YES' then 1 else 0 end as has_fatigue,
    case when trim(upper("Difficulty Breathing")) = 'YES' then 1 else 0 end as has_difficulty_breathing,
    
    -- Demographics
    cast("Age" as integer) as age,
    trim(upper("Gender")) as gender,
    
    -- Health indicators
    trim(upper("Blood Pressure")) as blood_pressure_level,
    trim(upper("Cholesterol Level")) as cholesterol_level,
    
    -- Outcome
    case 
      when trim(upper("Outcome Variable")) = 'POSITIVE' then 1
      when trim(upper("Outcome Variable")) = 'NEGATIVE' then 0
      else null
    end as outcome_binary,
    
    trim(upper("Outcome Variable")) as outcome_label,
    
    -- Calculated fields
    (
      case when trim(upper("Fever")) = 'YES' then 1 else 0 end +
      case when trim(upper("Cough")) = 'YES' then 1 else 0 end +
      case when trim(upper("Fatigue")) = 'YES' then 1 else 0 end +
      case when trim(upper("Difficulty Breathing")) = 'YES' then 1 else 0 end
    ) as total_symptoms,
    
    -- Risk categorization
    case 
      when cast("Age" as integer) < 18 then 'Child'
      when cast("Age" as integer) < 65 then 'Adult'
      else 'Senior'
    end as age_group,
    
    -- Metadata
    current_timestamp as processed_at,
    'duckdb' as source_system
    
  from source_data
  where "Disease" is not null
    and "Age" is not null
    and "Gender" is not null
    and "Outcome Variable" is not null
)

select * from cleaned_data
