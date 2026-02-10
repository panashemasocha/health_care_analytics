-- Analytics-Ready Encounter Summary

-- Produces one row per patient × facility × month with:
--   - Only the latest version of each encounter (by updated_at)
--   - Aggregated metrics: total encounters, total cost, distinct diagnoses
--

-- Step 1: Deduplicate encounters — keep only the latest update per encounter_id
WITH latest_encounters AS (
    SELECT
        encounter_id,
        patient_id,
        facility_id,
        encounter_date,
        diagnosis_code,
        cost,
        updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY encounter_id
            ORDER BY updated_at DESC
        ) AS row_num
    FROM raw_encounters
),

-- Step 2: Filter to only the latest version of each encounter
deduplicated AS (
    SELECT
        encounter_id,
        patient_id,
        facility_id,
        encounter_date,
        diagnosis_code,
        cost,
        updated_at
    FROM latest_encounters
    WHERE row_num = 1
)

-- Step 3: Aggregate per patient × facility × month
SELECT
    patient_id,
    facility_id,
    TO_CHAR(encounter_date, 'YYYY-MM')        AS year_month,
    COUNT(DISTINCT encounter_id)               AS total_encounters,
    SUM(cost)                                  AS total_cost,
    COUNT(DISTINCT diagnosis_code)             AS distinct_diagnosis_count
FROM deduplicated
GROUP BY
    patient_id,
    facility_id,
    TO_CHAR(encounter_date, 'YYYY-MM')
ORDER BY
    patient_id,
    facility_id,
    year_month;
