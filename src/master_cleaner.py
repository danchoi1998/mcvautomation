"""
Master Cleaner
==============
Cleans the combined master DataFrame by filtering to targeted accounts.

Targeted accounts are identified by external Excel files containing an
"Account Platform ID" column. Rows are kept if any of the 4 SF Platform ID
columns match a targeted Account Platform ID.
"""

import pandas as pd


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
    print(f"Filtered to targeted accounts: {before_count:,} → {len(master):,} rows "
          f"({before_count - len(master):,} removed)")
    return master
