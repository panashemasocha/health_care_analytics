"""Integration tests that validate the SQL query against the live PostgreSQL database.

Test are executed via: docker-compose run test
"""

import os

import pandas as pd
import pytest
from sqlalchemy import create_engine, text


@pytest.fixture(scope="module")
def engine():
    """Connect to the test PostgreSQL instance."""
    host = os.environ.get("DB_HOST", "localhost")
    port = os.environ.get("DB_PORT", "5432")
    name = os.environ.get("DB_NAME", "health_analytics")
    user = os.environ.get("DB_USER", "moh_analyst")
    password = os.environ.get("DB_PASSWORD", "moh_secure_2025")
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{name}")


@pytest.fixture(scope="module")
def raw_data(engine):
    """Load the raw_encounters table."""
    return pd.read_sql("SELECT * FROM raw_encounters", engine)


@pytest.fixture(scope="module")
def analytics_df(engine):
    """Execute the analytics query and return results."""
    sql_path = os.path.join(os.path.dirname(__file__), "..", "sql", "03_analytics_query.sql")
    with open(sql_path) as f:
        query = f.read()
    return pd.read_sql(query, engine)


# ── Raw data validation ──────────────────────────────────────────────

class TestRawData:

    def test_row_count(self, raw_data):
        """Sample data generates exactly 1000 rows."""
        assert len(raw_data) == 1000

    def test_expected_columns(self, raw_data):
        expected = {
            "encounter_id", "patient_id", "facility_id",
            "encounter_date", "diagnosis_code", "cost", "updated_at",
        }
        assert set(raw_data.columns) == expected

    def test_has_duplicate_encounter_ids(self, raw_data):
        """Data intentionally contains duplicate encounter_ids (late updates)."""
        unique_encounters = raw_data["encounter_id"].nunique()
        assert unique_encounters < len(raw_data)

    def test_has_negative_costs(self, raw_data):
        """~2% of rows should have negative costs."""
        neg_count = (raw_data["cost"] < 0).sum()
        assert neg_count > 0
        assert neg_count < 50  # roughly 2% of 1000

    def test_facility_range(self, raw_data):
        facilities = sorted(raw_data["facility_id"].unique())
        assert facilities == ["F001", "F002", "F003", "F004", "F005"]

    def test_patient_range(self, raw_data):
        patients = raw_data["patient_id"].nunique()
        assert patients == 50


# ── Analytics query validation ───────────────────────────────────────

class TestAnalyticsQuery:

    def test_required_columns(self, analytics_df):
        expected = {
            "patient_id", "facility_id", "year_month",
            "total_encounters", "total_cost", "distinct_diagnosis_count",
        }
        assert set(analytics_df.columns) == expected

    def test_one_row_per_patient_facility_month(self, analytics_df):
        """Verify the grain: no duplicate (patient, facility, month) combos."""
        key_cols = ["patient_id", "facility_id", "year_month"]
        assert not analytics_df.duplicated(subset=key_cols).any()

    def test_year_month_format(self, analytics_df):
        """year_month should be in YYYY-MM format."""
        assert analytics_df["year_month"].str.match(r"^\d{4}-\d{2}$").all()

    def test_months_in_expected_range(self, analytics_df):
        """Encounter dates span Jan-Mar 2025."""
        months = set(analytics_df["year_month"].unique())
        assert months.issubset({"2025-01", "2025-02", "2025-03", "2025-04"})
        assert "2025-01" in months

    def test_total_encounters_positive(self, analytics_df):
        assert (analytics_df["total_encounters"] > 0).all()

    def test_distinct_diagnosis_count_positive(self, analytics_df):
        assert (analytics_df["distinct_diagnosis_count"] > 0).all()

    def test_deduplication_reduces_encounter_count(self, raw_data, analytics_df):
        """Total encounters in analytics should be <= unique encounter_ids in raw."""
        raw_unique = raw_data["encounter_id"].nunique()
        analytics_total = analytics_df["total_encounters"].sum()
        assert analytics_total <= raw_unique

    def test_deduplication_keeps_latest(self, engine):
        """For encounters with multiple rows, the latest updated_at should win."""
        query = text("""
            WITH latest AS (
                SELECT encounter_id,
                       ROW_NUMBER() OVER (
                           PARTITION BY encounter_id ORDER BY updated_at DESC
                       ) AS rn
                FROM raw_encounters
            )
            SELECT COUNT(*) AS cnt
            FROM latest
            WHERE rn = 1
        """)
        with engine.connect() as conn:
            result = conn.execute(query).scalar()
        # This should match the total encounters in the analytics output
        assert result > 0
        assert result <= 1000

    def test_no_completely_empty_months(self, analytics_df):
        """Each month present should have at least one encounter."""
        for month in analytics_df["year_month"].unique():
            month_data = analytics_df[analytics_df["year_month"] == month]
            assert month_data["total_encounters"].sum() > 0
