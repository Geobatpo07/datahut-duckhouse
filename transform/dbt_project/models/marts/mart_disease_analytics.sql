{{
  config(
    materialized='table',
    tags=['mart', 'analytics', 'disease'],
    description='Comprehensive disease analytics with metrics and insights'
  )
}}

with healthcare_data as (
  select * from {{ ref('stg_healthcare_data') }}
),

disease_metrics as (
  select
    disease_name,
    count(*) as total_cases,
    count(case when outcome_binary = 1 then 1 end) as positive_cases,
    count(case when outcome_binary = 0 then 1 end) as negative_cases,
    
    -- Calculate rates
    round(
      cast(count(case when outcome_binary = 1 then 1 end) as decimal) / 
      cast(count(*) as decimal) * 100, 2
    ) as positivity_rate,
    
    -- Symptom analysis
    round(avg(cast(has_fever as decimal)) * 100, 2) as fever_prevalence,
    round(avg(cast(has_cough as decimal)) * 100, 2) as cough_prevalence,
    round(avg(cast(has_fatigue as decimal)) * 100, 2) as fatigue_prevalence,
    round(avg(cast(has_difficulty_breathing as decimal)) * 100, 2) as breathing_difficulty_prevalence,
    round(avg(cast(total_symptoms as decimal)), 2) as avg_symptoms_per_case,
    
    -- Demographics
    round(avg(cast(age as decimal)), 1) as avg_age,
    min(age) as min_age,
    max(age) as max_age,
    
    -- Gender distribution
    round(
      cast(count(case when gender = 'FEMALE' then 1 end) as decimal) / 
      cast(count(*) as decimal) * 100, 2
    ) as female_percentage,
    
    -- Age group distribution
    round(
      cast(count(case when age_group = 'Child' then 1 end) as decimal) / 
      cast(count(*) as decimal) * 100, 2
    ) as child_percentage,
    round(
      cast(count(case when age_group = 'Adult' then 1 end) as decimal) / 
      cast(count(*) as decimal) * 100, 2
    ) as adult_percentage,
    round(
      cast(count(case when age_group = 'Senior' then 1 end) as decimal) / 
      cast(count(*) as decimal) * 100, 2
    ) as senior_percentage,
    
    -- Health indicators
    round(
      cast(count(case when blood_pressure_level = 'HIGH' then 1 end) as decimal) / 
      cast(count(*) as decimal) * 100, 2
    ) as high_bp_percentage,
    round(
      cast(count(case when cholesterol_level = 'HIGH' then 1 end) as decimal) / 
      cast(count(*) as decimal) * 100, 2
    ) as high_cholesterol_percentage,
    
    -- Risk scoring
    case 
      when round(
        cast(count(case when outcome_binary = 1 then 1 end) as decimal) / 
        cast(count(*) as decimal) * 100, 2
      ) > 75 then 'High Risk'
      when round(
        cast(count(case when outcome_binary = 1 then 1 end) as decimal) / 
        cast(count(*) as decimal) * 100, 2
      ) > 50 then 'Medium Risk'
      else 'Low Risk'
    end as risk_category,
    
    -- Metadata
    current_timestamp as calculated_at
    
  from healthcare_data
  group by disease_name
),

disease_ranking as (
  select
    *,
    row_number() over (order by total_cases desc) as case_volume_rank,
    row_number() over (order by positivity_rate desc) as positivity_rank,
    row_number() over (order by avg_symptoms_per_case desc) as symptom_severity_rank
  from disease_metrics
)

select * from disease_ranking
order by total_cases desc
