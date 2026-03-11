"""
Master Cleaner
==============
Cleans the combined master DataFrame:
  1. Filters to targeted accounts (via external Excel files).
  2. Aggregates Case QTY columns by account/product grouping.
  3. Adds annualized QTY, percent growth, and marketing success columns.
"""

import pandas as pd
import numpy as np


PLATFORM_ID_COLUMNS = [
    "SF Location: Platform ID",
    "SF PA: Platform ID",
    "SF GPA: Platform ID",
    "SF GGPA: Platform ID",
]


def load_target_ids(file_paths):
    """
    Read external Excel files and return a set of unique Account Platform IDs.

    Parameters
    ----------
    file_paths : list of str
        Paths to Excel files, each containing an "Account Platform ID" column.

    Returns
    -------
    set
        All unique Account Platform IDs across the files.
    """
    all_ids = set()
    for path in file_paths:
        print(f"Reading target IDs from: {path}")
        if str(path).endswith(".csv"):
            df = pd.read_csv(path)
        else:
            df = pd.read_excel(path)
        ids = df["Account Platform ID"].dropna().unique()
        all_ids.update(ids)
        print(f"  Found {len(ids):,} IDs ({len(all_ids):,} unique total)")
    return all_ids


def filter_targeted_accounts(master, target_ids):
    """
    Keep only rows where any of the 4 SF Platform ID columns matches a target ID.

    Parameters
    ----------
    master : pd.DataFrame
        The combined master DataFrame.
    target_ids : set
        Set of Account Platform IDs to keep.

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame with only targeted account rows.
    """
    before_count = len(master)

    mask = pd.Series(False, index=master.index)
    for col in PLATFORM_ID_COLUMNS:
        if col in master.columns:
            mask = mask | master[col].isin(target_ids)

    master = master[mask].copy()

    # Add "Targeted Account" column after "SF Location: Name"
    loc_name_idx = master.columns.get_loc("SF Location: Name")
    master.insert(loc_name_idx + 1, "Targeted Account", "Yes")

    print(f"Filtered to targeted accounts: {before_count:,} → {len(master):,} rows "
          f"({before_count - len(master):,} removed)")
    return master


def filter_targeted_mins(master, target_mins):
    """
    Keep only rows where the MIN column matches the provided list.

    Parameters
    ----------
    master : pd.DataFrame
        The master DataFrame.
    target_mins : list of str
        MINs to keep.

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame with only targeted MIN rows.
    """
    before_count = len(master)
    master = master[master["MIN"].isin(target_mins)].copy()
    print(f"Filtered to targeted MINs: {before_count:,} → {len(master):,} rows "
          f"({before_count - len(master):,} removed)")
    return master


# =============================================================================
# AGGREGATION
# =============================================================================

GROUP_COLUMNS = [
    "SF PA: GPO Brands-MAP",
    "SF Highest Group Name",
    "SF Highest Group PLID",
    "SF Location: Name",
    "Targeted Account",
    "Manufacturer",
    "MIN",
    "Product Description",
    "Brand",
    "Pack Size",
]

AGG_VALUES = [
    "Before Marketing Period Case QTY",
    "During Marketing Period Case QTY",
]


def aggregate_master(master):
    """Aggregate Case QTY columns by account/product grouping."""
    aggregated = (
        master
        .groupby(GROUP_COLUMNS, dropna=False)[AGG_VALUES]
        .sum()
        .reset_index()
    )
    print(f"Aggregated: {len(master):,} → {len(aggregated):,} rows")
    return aggregated


# =============================================================================
# CALCULATED COLUMNS
# =============================================================================

MARKETING_SUCCESS_THRESHOLD = 0.2  # 20% growth


def add_calculated_columns(master, before_date_range, during_date_range):
    """
    Add annualized QTY, percent growth, and marketing success columns.

    Parameters
    ----------
    master : pd.DataFrame
        Aggregated master DataFrame.
    before_date_range : tuple of (date, date)
        (from_date, to_date) for the before period.
    during_date_range : tuple of (date, date)
        (from_date, to_date) for the during period.
    """
    before_days = (before_date_range[1] - before_date_range[0]).days + 1
    during_days = (during_date_range[1] - during_date_range[0]).days + 1

    before_qty = master["Before Marketing Period Case QTY"]
    during_qty = master["During Marketing Period Case QTY"]

    # Annualized columns
    master["Before Marketing Period - Annualized QTY"] = (before_qty / before_days) * 365
    master["During Marketing Period - Annualized QTY"] = (during_qty / during_days) * 365

    annualized_before = master["Before Marketing Period - Annualized QTY"]
    annualized_during = master["During Marketing Period - Annualized QTY"]

    # Percent Growth: (during - before) / before
    # Use replace to avoid ZeroDivisionError (np.where evaluates all branches)
    safe_before = annualized_before.replace(0, np.nan)
    master["Percent Growth"] = np.where(
        annualized_before == 0,
        np.where(annualized_during > 0, np.nan, 0),
        (annualized_during - annualized_before) / safe_before,
    )

    # Annualized QTY difference
    master["Annualized QTY"] = annualized_during - annualized_before

    # Marketing Success: insert after "SF Location: Name"
    marketing_success = np.where(
        annualized_before == 0,
        annualized_during > 0,
        master["Percent Growth"] >= MARKETING_SUCCESS_THRESHOLD,
    )
    targeted_idx = master.columns.get_loc("Targeted Account")
    master.insert(targeted_idx + 1, "Marketing Success", marketing_success)

    print(f"Added calculated columns. Marketing Success: "
          f"{master['Marketing Success'].sum():,} / {len(master):,} rows")
    return master


# =============================================================================
# SUMMARY AGGREGATION
# =============================================================================

SUMMARY_GROUP_COLUMNS = [
    "SF PA: GPO Brands-MAP",
    "SF Highest Group Name",
    "SF Highest Group PLID",
    "Targeted Account",
    "Marketing Success",
]

SUMMARY_AGG_VALUES = [
    "Before Marketing Period - Annualized QTY",
    "During Marketing Period - Annualized QTY",
]


def aggregate_summary(master):
    """
    Second aggregation: group by account-level columns + Marketing Success,
    sum annualized QTY columns, then compute Percent Growth.

    Parameters
    ----------
    master : pd.DataFrame
        The item-detail DataFrame (after add_calculated_columns).

    Returns
    -------
    pd.DataFrame
        Summary-level DataFrame.
    """
    summary = (
        master
        .groupby(SUMMARY_GROUP_COLUMNS, dropna=False)[SUMMARY_AGG_VALUES]
        .sum()
        .reset_index()
    )

    ann_before = summary["Before Marketing Period - Annualized QTY"]
    ann_during = summary["During Marketing Period - Annualized QTY"]

    safe_before = ann_before.replace(0, np.nan)
    summary["Percent Growth"] = np.where(
        ann_before == 0,
        np.where(ann_during > 0, np.nan, 0),
        (ann_during - ann_before) / safe_before,
    )

    print(f"Summary aggregation: {len(master):,} → {len(summary):,} rows")
    return summary
