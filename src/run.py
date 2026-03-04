"""
MCV Automation - Entry Point
=============================
Edit Config and DATE_RANGES below, then run:
    cd src && python run.py
"""

import sys
import os
import time
from datetime import date

# Ensure src/ is on the path so subpackage imports work regardless of cwd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import settings
from pipeline.filegenerator import (
    connect_salesforce,
    fetch_salesforce_data,
    run_purchase_pipeline,
)
from pipeline.master_file_creator import create_master_from_dfs


# =============================================================================
# CONFIG SECTION  -  Edit these values before running
# =============================================================================
class Config:
    def __init__(self):
        # Output file name (date/time stamp is appended automatically)
        self.file_name = "Example"

        # Folder where final Excel files are saved
        self.save_files_to = r"Z:\Shared\GPO Operations\GPO Analytics & Support\Data Skills Learning\GPO Analytics Python Trainings\Daniel's Files"

        # MIN file name and sheet (only used when check_min_file is True)
        self.MIN_file = "file name"
        self.MIN_sheet_name = "sheet name"

        # Date configurations  -  overridden per-run in DATE_RANGES below
        self.from_date = date(2025, 5, 1)
        self.to_date = date(2025, 8, 31)
        self.exclusion_effective_date = date(2026, 12, 31)

        # Filter configurations
        self.filters = {
            "manufacturer": {
                "enabled": True,
                "ids": ["MA-1000277"],
            },
            "category": {
                "enabled": True,
                "values": ["Art Supplies"],
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
    (date(2024, 9, 1), date(2024, 11, 30)),   # ← add/edit your second range
]


# =============================================================================
# MAIN
# =============================================================================
def main():
    overall_start = time.time()

    # ── Connect ──────────────────────────────────────────────────────────
    sf = connect_salesforce(
        settings.SF_USERNAME, settings.SF_PASSWORD,
        settings.SF_SECURITY_TOKEN, False
    )

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

    # ── Create master file (before stacked on top of during) ─────────────
    if len(results) == 2:
        master_path = os.path.join(
            myconf.save_files_to, f"{myconf.file_name} - MASTER.xlsx"
        )
        create_master_from_dfs(results[0], results[1], master_path)

    elapsed = time.time() - overall_start
    print("=" * 60)
    print(f"All done!  Total wall time: {elapsed / 60:.1f} minutes")
    print("=" * 60)


if __name__ == "__main__":
    main()
