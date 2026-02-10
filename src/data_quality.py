"""
Data quality checks for the healthcare encounter analytics pipeline.

Flags:
  1. Negative costs — encounter cost should never be below zero.

  2. Outlier encounter counts — patients with monthly encounter counts
     above the 99th percentile. High volumes may indicate duplicate
     registrations, data entry errors, or (legitimately) chronic-care
     patients requiring investigation.
"""

import pandas as pd


def flag_negative_costs(df: pd.DataFrame) -> pd.DataFrame:
    """Flag rows where total_cost is negative.

    Args:
        df: Analytics DataFrame with a 'total_cost' column.

    Returns:
        DataFrame containing only the rows with negative total_cost,
        with an added 'flag_reason' column.
    """
    flagged = df[df["total_cost"] < 0].copy()
    flagged["flag_reason"] = "negative_cost"
    return flagged


def flag_high_encounter_counts(df: pd.DataFrame, percentile: float = 0.99) -> pd.DataFrame:
    """Flag patients with encounter counts above the given percentile.

    Args:
        df: Analytics DataFrame with a 'total_encounters' column.
        percentile: Threshold percentile (default 99th).

    Returns:
        DataFrame containing only the outlier rows,
        with an added 'flag_reason' column.
    """
    threshold = df["total_encounters"].quantile(percentile)
    flagged = df[df["total_encounters"] > threshold].copy()
    flagged["flag_reason"] = f"high_encounter_count (>{threshold:.0f}, p{percentile*100:.0f})"
    return flagged


def run_quality_checks(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run all data quality checks and return cleaned + flagged DataFrames.

    Args:
        df: Analytics-ready DataFrame from the SQL aggregation.

    Returns:
        Tuple of (cleaned_df, flagged_df):
          - cleaned_df: rows with no quality flags
          - flagged_df: rows that failed one or more checks, with flag reasons
    """
    neg_cost = flag_negative_costs(df)
    high_enc = flag_high_encounter_counts(df)

    # Combine all flagged records, keeping distinct rows
    flagged = pd.concat([neg_cost, high_enc], ignore_index=True)

    # A row may be flagged for multiple reasons — group flag reasons
    if not flagged.empty:
        key_cols = ["patient_id", "facility_id", "year_month"]
        flag_summary = (
            flagged.groupby(key_cols)["flag_reason"]
            .apply(lambda x: "; ".join(sorted(x.unique())))
            .reset_index()
        )
        flagged = flagged.drop(columns=["flag_reason"]).drop_duplicates(subset=key_cols)
        flagged = flagged.merge(flag_summary, on=key_cols, how="left")

    # Cleaned = all rows not present in the flagged set
    if flagged.empty:
        cleaned = df.copy()
    else:
        key_cols = ["patient_id", "facility_id", "year_month"]
        flagged_keys = flagged[key_cols]
        merged = df.merge(flagged_keys, on=key_cols, how="left", indicator=True)
        cleaned = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

    return cleaned, flagged
