-- Load sample encounter data
-- Generates 1000 rows with intentional data quality issues:
--   - Duplicate encounter_ids (late updates with different updated_at)
--   - Negative costs (~2% of records)
--   - Skewed patient distribution

INSERT INTO raw_encounters (
    encounter_id,
    patient_id,
    facility_id,
    encounter_date,
    diagnosis_code,
    cost,
    updated_at
)
SELECT
    CONCAT('E', LPAD((g.id % 850 + 1)::TEXT, 4, '0')) AS encounter_id,
    CONCAT('P', LPAD((g.id % 50 + 1)::TEXT, 3, '0')) AS patient_id,
    CONCAT('F', LPAD((g.id % 5 + 1)::TEXT, 3, '0')) AS facility_id,
    DATE '2025-01-01' + (g.id % 90) AS encounter_date,
    CASE g.id % 7
        WHEN 0 THEN 'A01'
        WHEN 1 THEN 'B02'
        WHEN 2 THEN 'C03'
        WHEN 3 THEN 'D04'
        WHEN 4 THEN 'E05'
        WHEN 5 THEN 'F06'
        ELSE 'G07'
    END AS diagnosis_code,
    CASE
        WHEN g.id % 50 = 0 THEN -ROUND((20 + (g.id % 80))::NUMERIC, 2)
        ELSE ROUND((30 + (g.id % 450))::NUMERIC, 2)
    END AS cost,
    TIMESTAMP '2025-01-01 08:00:00'
        + (g.id % 90) * INTERVAL '1 day'
        + (g.id % 48) * INTERVAL '1 hour'
        + (g.id % 20) * INTERVAL '1 minute'
        + CASE
            WHEN g.id % 6 = 0 THEN INTERVAL '5 days'
            ELSE INTERVAL '0 days'
          END AS updated_at
FROM generate_series(1, 1000) AS g(id);
