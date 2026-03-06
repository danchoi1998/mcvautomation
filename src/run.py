"""
MCV Automation - Entry Point
=============================
Edit Config, DATE_RANGES, and REFERENCE_FILES below, then run:
    cd src && python run.py
"""

import time
import os
from datetime import date
from pathlib import Path

import settings
from filegenerator import (
    connect_salesforce,
    fetch_salesforce_data,
    run_purchase_pipeline,
)
from master_file_creator import create_master_from_dfs
from master_cleaner import load_target_ids, filter_targeted_accounts, aggregate_master, add_calculated_columns, aggregate_summary


# =============================================================================
# CONFIG SECTION  -  Edit these values before running
# =============================================================================
class Config:
    def __init__(
        self,
        file_name="Example",
        save_files_to=Path(r"/mnt/c/Users/DanielChoi/OneDrive - Buyers Edge Platform/Desktop/Python/MCVAutomation/test_files/test_validations"),
        MIN_file="file name",
        MIN_sheet_name="sheet name",
        exclusion_effective_date=date(2026, 12, 31),
        filters=None,
    ):
        self.file_name = file_name
        self.save_files_to = save_files_to
        self.MIN_file = MIN_file
        self.MIN_sheet_name = MIN_sheet_name
        self.exclusion_effective_date = exclusion_effective_date
        self.filters = filters or {
            "manufacturer": {
                "enabled": True,
                "ids": ["MA-1000012"],
            },
            "category": {
                "enabled": True,
                "values": ["Towels (Disposable)"],
                "handle_apostrophe": False,
            },
            "min": {
                "enabled": False,
                "ids": ["MIN1", "MIN2", "MIN3"],
                "check_min_file": False,
            },
            "S1 communication filter": {
                "enabled": False,
            },
            "remove entirely": {
                "enabled": False,
                "mfr_ids": ["MA-1047966"],
            },
            "brand": {
                "enabled": False,
                "brands": ["brand1", "brand2"],
                "handle_apostrophe": True,
            },
        }


# ── Date ranges to loop over (the ONLY thing that changes between runs) ─────
DATE_RANGES = [
    (date(2025, 5, 1), date(2025, 8, 31)),
    (date(2025, 9, 1), date(2026, 1, 15)),   # ← add/edit your second range
]

# ── External Excel files containing "Account Platform ID" for targeting ──────
REFERENCE_FILES = [
    # "/path/to/reference_file_1.xlsx",
    # "/path/to/reference_file_2.xlsx",
]


# =============================================================================
# MAIN
# =============================================================================
def main():
    overall_start = time.time()

    # ── Connect ──────────────────────────────────────────────────────────
    sf = connect_salesforce(settings.SF_USERNAME, settings.SF_PASSWORD, settings.SF_SECURITY_TOKEN, False)

    # ── Config ───────────────────────────────────────────────────────────
    myconf = Config()

    # ── Fetch SF data ONCE (date-independent) ────────────────────────────
    sf_data = fetch_salesforce_data(sf, myconf)

    # ── Run pipeline for EACH date range ─────────────────────────────────
    results = []
    for from_date, to_date in DATE_RANGES:
        myconf.from_date = from_date
        myconf.to_date = to_date
        df = run_purchase_pipeline(sf, myconf, sf_data)
        results.append(df)

    # ── Create master DataFrame (before stacked on top of during) ────────
    if len(results) == 2:
        master = create_master_from_dfs(results[0], results[1])

        # ── Filter to targeted accounts ──────────────────────────────────
        if REFERENCE_FILES:
            target_ids = load_target_ids(REFERENCE_FILES)
            master = filter_targeted_accounts(master, target_ids)

        # ── Aggregate and add calculated columns ─────────────────────────
        master = aggregate_master(master)
        item_detail = add_calculated_columns(master, DATE_RANGES[0], DATE_RANGES[1])

        # Second aggregation: account-level summary
        summary = aggregate_summary(item_detail)

        print(f"\nItem Detail: {item_detail.shape[0]:,} rows, {item_detail.shape[1]} columns")
        print(f"Summary:     {summary.shape[0]:,} rows, {summary.shape[1]} columns")

        item_detail.to_csv("item_detail_preview.csv", index=False)
        summary.to_csv("summary_preview.csv", index=False)
        print("Saved item_detail_preview.csv and summary_preview.csv for inspection")

    elapsed = time.time() - overall_start
    print("=" * 60)
    print(f"All done!  Total wall time: {elapsed / 60:.1f} minutes")
    print("=" * 60)


if __name__ == "__main__":
    main()
