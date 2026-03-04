"""
Master File Creator
===================
Combines two DataFrames (or Excel files) into a single master file via a
union (vertical stack).

The "before" DataFrame gets:
    Total Quantity  ->  Before Marketing Period Quantity
    Total Case QTY  ->  Before Marketing Period Case QTY
    + empty During Marketing Period Quantity / Case QTY columns

The "during" DataFrame gets:
    Total Quantity  ->  During Marketing Period Quantity
    Total Case QTY  ->  During Marketing Period Case QTY
    + empty Before Marketing Period Quantity / Case QTY columns

The "before" data is stacked on top of the "during" data.

Usage:
    Called automatically by filegenerator.py, or run standalone by setting
    BEFORE_FILE, DURING_FILE, and OUTPUT_DIR below.
"""

import pandas as pd
import os
from pathlib import Path

# =============================================================================
# CONFIG  -  Only needed for standalone usage
# =============================================================================

BEFORE_FILE = Path(r"/mnt/c/Users/DanielChoi/OneDrive - Buyers Edge Platform/Desktop/Python/MCVAutomation/test_files/previously_completed_validations/Essity Mfold Towel Campaign Validation - Before.xlsx")
DURING_FILE = Path(r"/mnt/c/Users/DanielChoi/OneDrive - Buyers Edge Platform/Desktop/Python/MCVAutomation/test_files/previously_completed_validations/Essity Mfold Towel Campaign Validation - During.xlsx")
OUTPUT_DIR = Path(r"/mnt/c/Users/DanielChoi/OneDrive - Buyers Edge Platform/Desktop/Python/MCVAutomation/test_files/test_validations")

SHEET_NAME = "ALL Item Level Detail"

# Original column names in both files
QTY_COL = "Total Quantity"
CASE_QTY_COL = "Total Case QTY"

# =============================================================================
# HELPERS
# =============================================================================

def build_output_path(before_file, during_file, output_dir):
    """Generate an output filename from the common prefix of the two input filenames."""
    before_name = os.path.splitext(os.path.basename(before_file))[0]
    during_name = os.path.splitext(os.path.basename(during_file))[0]
    common = os.path.commonprefix([before_name, during_name]).rstrip(" -")
    if not common:
        common = before_name
    filename = f"{common} - MASTER.xlsx"
    return os.path.join(output_dir, filename)


def load_sheet(filepath, sheet_name):
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    print(f"Reading: {filepath}")
    df = pd.read_excel(filepath, sheet_name=sheet_name)
    print(f"  {len(df):,} rows, {len(df.columns)} columns")
    return df


def prepare_before(df):
    """Rename quantity columns and add empty 'during' columns."""
    df = df.rename(columns={
        QTY_COL: "Before Marketing Period Quantity",
        CASE_QTY_COL: "Before Marketing Period Case QTY",
    })

    # Insert the two empty "during" columns right after the "before" columns
    case_qty_idx = df.columns.get_loc("Before Marketing Period Case QTY")
    df.insert(case_qty_idx + 1, "During Marketing Period Quantity", pd.NA)
    df.insert(case_qty_idx + 2, "During Marketing Period Case QTY", pd.NA)

    return df


def prepare_during(df):
    """Rename quantity columns and add empty 'before' columns."""
    df = df.rename(columns={
        QTY_COL: "During Marketing Period Quantity",
        CASE_QTY_COL: "During Marketing Period Case QTY",
    })

    # Insert the two empty "before" columns right before the "during" columns
    qty_idx = df.columns.get_loc("During Marketing Period Quantity")
    df.insert(qty_idx, "Before Marketing Period Quantity", pd.NA)
    df.insert(qty_idx + 1, "Before Marketing Period Case QTY", pd.NA)

    return df


# =============================================================================
# MAIN
# =============================================================================

def build_master(df_before, df_during):
    """Prepare columns and union into a single DataFrame."""
    df_before = prepare_before(df_before)
    df_during = prepare_during(df_during)

    master = pd.concat([df_before, df_during], ignore_index=True)
    print(f"Master: {len(master):,} rows, {len(master.columns)} columns")
    return master


def create_master_from_dfs(df_before, df_during):
    """Create master DataFrame directly from DataFrames (called by filegenerator)."""
    return build_master(df_before, df_during)


def create_master_file(before_file, during_file, output_dir, sheet_name=SHEET_NAME):
    """Create master file from Excel files (standalone usage)."""
    output_file = build_output_path(before_file, during_file, output_dir)
    df_before = load_sheet(before_file, sheet_name)
    df_during = load_sheet(during_file, sheet_name)
    return _build_and_save(df_before, df_during, output_file)


if __name__ == "__main__":
    if not BEFORE_FILE or not DURING_FILE or not OUTPUT_DIR:
        raise ValueError("Please set BEFORE_FILE, DURING_FILE, and OUTPUT_DIR in the CONFIG section.")
    create_master_file(BEFORE_FILE, DURING_FILE, OUTPUT_DIR)
