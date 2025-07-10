-- Test data quality across all models
{{ config(severity='warn') }}

with data_quality_summary as (
  select
    'stg_healthcare_data' as model_name,
    count(*) as total_rows,
    count(case when patient_id is null then 1 end) as null_patient_ids,
    count(case when disease_name is null then 1 end) as null_diseases,
    count(case when outcome_binary not in (0, 1) then 1 end) as invalid_outcomes,
    count(case when age < 0 or age > 120 then 1 end) as invalid_ages
  from {{ ref('stg_healthcare_data') }}
  
  union all
  
  select
    'mart_disease_analytics' as model_name,
    count(*) as total_rows,
    count(case when disease_name is null then 1 end) as null_diseases,
    count(case when total_cases <= 0 then 1 end) as invalid_case_counts,
    count(case when positivity_rate < 0 or positivity_rate > 100 then 1 end) as invalid_rates,
    0 as additional_check
  from {{ ref('mart_disease_analytics') }}
)

select
  model_name,
  total_rows,
  null_patient_ids,
  null_diseases,
  invalid_outcomes,
  invalid_ages
from data_quality_summary
where null_patient_ids > 0
   or null_diseases > 0
   or invalid_outcomes > 0
   or invalid_ages > 0
