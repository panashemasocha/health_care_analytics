"""
Healthcare Encounter Analytics Pipeline — Main Entry Point

Loads the analytics-ready SQL output from the Ministry of Health
PostgreSQL database, runs data quality checks, and produces:
  1. cleaned_encounters.csv   — records passing all quality checks
  2. flagged_encounters.csv   — records failing one or more checks
  3. analytics_summary.csv    — full analytics query output (before cleaning)
  4. pipeline_report.txt      — summary of the run
"""

import os
import sys
from pathlib import Path

import pandas as pd

from src.db import get_engine
from src.data_quality import run_quality_checks


OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "/app/output"))
SQL_DIR = Path(os.environ.get("SQL_DIR", "/app/sql"))


def load_analytics_data(engine) -> pd.DataFrame:
    """Execute the analytics query and return results as a DataFrame."""
    sql_path = SQL_DIR / "03_analytics_query.sql"
    query = sql_path.read_text()
    return pd.read_sql(query, engine)


def generate_report(
    analytics_df: pd.DataFrame,
    cleaned_df: pd.DataFrame,
    flagged_df: pd.DataFrame,
) -> str:
    """Generate a human-readable pipeline report."""
    lines = [
        "=" * 65,
        "  HEALTHCARE ENCOUNTER ANALYTICS — PIPELINE REPORT",
        "  Ministry of Health — Central Data Hub",
        "=" * 65,
        "",
        "DATA OVERVIEW",
        f"  Total aggregated rows (patient × facility × month): {len(analytics_df):,}",
        f"  Unique patients:    {analytics_df['patient_id'].nunique()}",
        f"  Unique facilities:  {analytics_df['facility_id'].nunique()}",
        f"  Reporting months:   {sorted(analytics_df['year_month'].unique())}",
        "",
        "COST SUMMARY",
        f"  Total cost (all records):     {analytics_df['total_cost'].sum():,.2f}",
        f"  Mean cost per row:            {analytics_df['total_cost'].mean():,.2f}",
        f"  Min cost:                     {analytics_df['total_cost'].min():,.2f}",
        f"  Max cost:                     {analytics_df['total_cost'].max():,.2f}",
        "",
        "DATA QUALITY RESULTS",
        f"  Records passing all checks:   {len(cleaned_df):,}",
        f"  Records flagged:              {len(flagged_df):,}",
    ]

    if not flagged_df.empty:
        lines.append("")
        lines.append("  FLAG BREAKDOWN:")
        for reason in flagged_df["flag_reason"].unique():
            count = len(flagged_df[flagged_df["flag_reason"].str.contains(reason.split(";")[0].strip())])
            lines.append(f"    - {reason}: {count} record(s)")

    lines.extend([
        "",
        "OUTPUT FILES",
        f"  analytics_summary.csv   — Full analytics output ({len(analytics_df)} rows)",
        f"  cleaned_encounters.csv  — Quality-checked records ({len(cleaned_df)} rows)",
        f"  flagged_encounters.csv  — Flagged records ({len(flagged_df)} rows)",
        "",
        "=" * 65,
    ])
    return "\n".join(lines)


def main():
    print("Connecting to PostgreSQL...")
    engine = get_engine()

    print("Running analytics query...")
    analytics_df = load_analytics_data(engine)
    print(f"  Loaded {len(analytics_df)} aggregated rows.")

    print("Running data quality checks...")
    cleaned_df, flagged_df = run_quality_checks(analytics_df)
    print(f"  Cleaned: {len(cleaned_df)} rows | Flagged: {len(flagged_df)} rows")

    # Write outputs
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    analytics_df.to_csv(OUTPUT_DIR / "analytics_summary.csv", index=False)
    cleaned_df.to_csv(OUTPUT_DIR / "cleaned_encounters.csv", index=False)
    flagged_df.to_csv(OUTPUT_DIR / "flagged_encounters.csv", index=False)

    report = generate_report(analytics_df, cleaned_df, flagged_df)
    (OUTPUT_DIR / "pipeline_report.txt").write_text(report)

    print("\n" + report)
    print(f"\nAll outputs written to {OUTPUT_DIR}/")

    # Print sample outputs
    print("\n--- ANALYTICS SUMMARY (first 10 rows) ---")
    print(analytics_df.head(10).to_string(index=False))

    print("\n--- FLAGGED RECORDS ---")
    if flagged_df.empty:
        print("  No records flagged.")
    else:
        print(flagged_df.to_string(index=False))

    return 0


if __name__ == "__main__":
    sys.exit(main())
