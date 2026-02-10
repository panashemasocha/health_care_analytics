"""Unit tests for data quality checks."""

import pandas as pd
import pytest

from src.data_quality import (
    flag_negative_costs,
    flag_high_encounter_counts,
    run_quality_checks,
)


@pytest.fixture
def sample_analytics_df():
    """Realistic analytics DataFrame mimicking the SQL output."""
    return pd.DataFrame({
        "patient_id":             ["P001", "P001", "P002", "P002", "P003", "P004", "P005",
                                   "P006", "P007", "P008", "P009", "P010"],
        "facility_id":            ["F001", "F001", "F002", "F002", "F001", "F003", "F001",
                                   "F002", "F003", "F001", "F002", "F003"],
        "year_month":             ["2025-01", "2025-02", "2025-01", "2025-02", "2025-01",
                                   "2025-01", "2025-01", "2025-01", "2025-01", "2025-01",
                                   "2025-01", "2025-01"],
        "total_encounters":       [3, 2, 5, 1, 4, 2, 3, 1, 2, 2, 1, 50],
        "total_cost":             [150.0, 200.0, -30.0, 80.0, 300.0, 120.0, 250.0,
                                   90.0, 110.0, 70.0, 60.0, 5000.0],
        "distinct_diagnosis_count": [2, 1, 3, 1, 2, 1, 2, 1, 1, 1, 1, 5],
    })


#  flag negative costs ──────────────────────────────────────────────

class TestFlagNegativeCosts:

    def test_detects_negative_cost(self, sample_analytics_df):
        result = flag_negative_costs(sample_analytics_df)
        assert len(result) == 1
        assert result.iloc[0]["patient_id"] == "P002"
        assert result.iloc[0]["total_cost"] == -30.0

    def test_flag_reason_column(self, sample_analytics_df):
        result = flag_negative_costs(sample_analytics_df)
        assert "flag_reason" in result.columns
        assert result.iloc[0]["flag_reason"] == "negative_cost"

    def test_no_negatives_returns_empty(self):
        df = pd.DataFrame({
            "patient_id": ["P001"],
            "facility_id": ["F001"],
            "year_month": ["2025-01"],
            "total_encounters": [3],
            "total_cost": [150.0],
            "distinct_diagnosis_count": [2],
        })
        result = flag_negative_costs(df)
        assert len(result) == 0

    def test_all_negative(self):
        df = pd.DataFrame({
            "patient_id": ["P001", "P002"],
            "facility_id": ["F001", "F002"],
            "year_month": ["2025-01", "2025-01"],
            "total_encounters": [1, 2],
            "total_cost": [-10.0, -20.0],
            "distinct_diagnosis_count": [1, 1],
        })
        result = flag_negative_costs(df)
        assert len(result) == 2

    def test_zero_cost_not_flagged(self):
        df = pd.DataFrame({
            "patient_id": ["P001"],
            "facility_id": ["F001"],
            "year_month": ["2025-01"],
            "total_encounters": [1],
            "total_cost": [0.0],
            "distinct_diagnosis_count": [1],
        })
        result = flag_negative_costs(df)
        assert len(result) == 0

    def test_does_not_modify_original(self, sample_analytics_df):
        original_len = len(sample_analytics_df)
        flag_negative_costs(sample_analytics_df)
        assert len(sample_analytics_df) == original_len
        assert "flag_reason" not in sample_analytics_df.columns


# ── flag high encounter counts ───────────────────────────────────────

class TestFlagHighEncounterCounts:

    def test_detects_outlier(self, sample_analytics_df):
        result = flag_high_encounter_counts(sample_analytics_df)
        assert len(result) >= 1
        # P010 has 50 encounters — clearly above 99th percentile
        assert "P010" in result["patient_id"].values

    def test_flag_reason_contains_threshold(self, sample_analytics_df):
        result = flag_high_encounter_counts(sample_analytics_df)
        assert all("high_encounter_count" in r for r in result["flag_reason"])

    def test_custom_percentile(self, sample_analytics_df):
        # Lower threshold should flag more records
        result_99 = flag_high_encounter_counts(sample_analytics_df, percentile=0.99)
        result_50 = flag_high_encounter_counts(sample_analytics_df, percentile=0.50)
        assert len(result_50) >= len(result_99)

    def test_uniform_data_no_flags(self):
        """When all encounter counts are the same, none exceed the percentile."""
        df = pd.DataFrame({
            "patient_id": [f"P{i:03d}" for i in range(1, 11)],
            "facility_id": ["F001"] * 10,
            "year_month": ["2025-01"] * 10,
            "total_encounters": [5] * 10,
            "total_cost": [100.0] * 10,
            "distinct_diagnosis_count": [2] * 10,
        })
        result = flag_high_encounter_counts(df)
        assert len(result) == 0

    def test_does_not_modify_original(self, sample_analytics_df):
        original_len = len(sample_analytics_df)
        flag_high_encounter_counts(sample_analytics_df)
        assert len(sample_analytics_df) == original_len
        assert "flag_reason" not in sample_analytics_df.columns


# ── run quality checks ───────────────────────────────────────────────

class TestRunQualityChecks:

    def test_returns_two_dataframes(self, sample_analytics_df):
        cleaned, flagged = run_quality_checks(sample_analytics_df)
        assert isinstance(cleaned, pd.DataFrame)
        assert isinstance(flagged, pd.DataFrame)

    def test_cleaned_plus_flagged_covers_all_rows(self, sample_analytics_df):
        cleaned, flagged = run_quality_checks(sample_analytics_df)
        assert len(cleaned) + len(flagged) == len(sample_analytics_df)

    def test_cleaned_has_no_negative_costs(self, sample_analytics_df):
        cleaned, _ = run_quality_checks(sample_analytics_df)
        assert (cleaned["total_cost"] >= 0).all()

    def test_flagged_has_reason_column(self, sample_analytics_df):
        _, flagged = run_quality_checks(sample_analytics_df)
        if not flagged.empty:
            assert "flag_reason" in flagged.columns

    def test_empty_dataframe(self):
        df = pd.DataFrame(columns=[
            "patient_id", "facility_id", "year_month",
            "total_encounters", "total_cost", "distinct_diagnosis_count",
        ])
        cleaned, flagged = run_quality_checks(df)
        assert len(cleaned) == 0
        assert len(flagged) == 0

    def test_all_clean_data(self):
        df = pd.DataFrame({
            "patient_id": ["P001", "P002"],
            "facility_id": ["F001", "F002"],
            "year_month": ["2025-01", "2025-01"],
            "total_encounters": [3, 3],
            "total_cost": [100.0, 200.0],
            "distinct_diagnosis_count": [1, 2],
        })
        cleaned, flagged = run_quality_checks(df)
        assert len(cleaned) == 2
        assert len(flagged) == 0

    def test_no_duplicate_rows_in_flagged(self, sample_analytics_df):
        _, flagged = run_quality_checks(sample_analytics_df)
        if not flagged.empty:
            key_cols = ["patient_id", "facility_id", "year_month"]
            assert not flagged.duplicated(subset=key_cols).any()
