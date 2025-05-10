SELECT
    "Disease",
    "Blood Pressure",
    "Cholesterol Level",
    COUNT(*) AS total_cases,
    AVG(outcome_binary) AS positivity_rate,
    AVG(age) AS avg_age
FROM {{ ref('stg_rev') }}
GROUP BY
    "Disease",
    "Blood Pressure",
    "Cholesterol Level"
