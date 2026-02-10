-- Healthcare Analytics Platform — Schema Definition
-- Context: Ministry of Health central data hub
-- This schema supports patient encounter data ingested from multiple sources
-- facility-level systems (e.g. DHIS2, OpenMRS, facility EMRs).

CREATE TABLE IF NOT EXISTS raw_encounters (
    encounter_id   VARCHAR(20)    NOT NULL,
    patient_id     VARCHAR(20)    NOT NULL,
    facility_id    VARCHAR(20)    NOT NULL,
    encounter_date DATE           NOT NULL,
    diagnosis_code VARCHAR(10)    NOT NULL,
    cost           NUMERIC(12,2)  NOT NULL,
    updated_at     TIMESTAMP      NOT NULL
);

-- Index to accelerate deduplication (latest version per encounter)
CREATE INDEX IF NOT EXISTS idx_encounter_updated
    ON raw_encounters (encounter_id, updated_at DESC);

-- Index to accelerate the monthly aggregation query
CREATE INDEX IF NOT EXISTS idx_patient_facility_date
    ON raw_encounters (patient_id, facility_id, encounter_date);
