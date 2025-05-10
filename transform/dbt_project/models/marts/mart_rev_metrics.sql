SELECT
    "Gender",
    COUNT(*) AS total_patients,
    AVG(outcome_binary) AS positive_rate,
    AVG(fever_flag) AS avg_fever,
    AVG(cough_flag) AS avg_cough,
    AVG(breath_flag) AS avg_breath_diff,
    AVG(age) AS avg_age
FROM {{ ref('stg_rev') }}
GROUP BY "Gender"
