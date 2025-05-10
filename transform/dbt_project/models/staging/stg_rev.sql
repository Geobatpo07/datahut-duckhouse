SELECT
    "Disease",
    CASE WHEN "Fever" = 'Yes' THEN 1 ELSE 0 END AS fever_flag,
    CASE WHEN "Cough" = 'Yes' THEN 1 ELSE 0 END AS cough_flag,
    CASE WHEN "Fatigue" = 'Yes' THEN 1 ELSE 0 END AS fatigue_flag,
    CASE WHEN "Difficulty Breathing" = 'Yes' THEN 1 ELSE 0 END AS breath_flag,
    "Age",
    "Gender",
    "Blood Pressure",
    "Cholesterol Level",
    CASE
        WHEN "Outcome Variable" = 'Positive' THEN 1
        WHEN "Outcome Variable" = 'Negative' THEN 0
        ELSE NULL
    END AS outcome_binary
FROM {{ source('main', 'rev') }}
