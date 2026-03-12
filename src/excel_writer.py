"""
Excel Writer
=============
Exports Item Detail and Summary DataFrames to a formatted .xlsx file.
"""

import os
import xlsxwriter


# Column rename mapping (applied to DataFrames before writing)
COLUMN_RENAMES = {
    "Before Marketing Period Case QTY": "Prior to Marketing Period Case QTY",
    "During Marketing Period Case QTY": "Marketing Period Case QTY",
    "Before Marketing Period - Annualized QTY": "Prior to Marketing Period - Annualized QTY",
    "During Marketing Period - Annualized QTY": "Marketing Period - Annualized QTY",
}

# Columns that get a SUBTOTAL(9, ...) formula on each sheet
ITEM_DETAIL_SUBTOTAL_COLS = [
    "Prior to Marketing Period Case QTY",
    "Marketing Period Case QTY",
    "Prior to Marketing Period - Annualized QTY",
    "Marketing Period - Annualized QTY",
    "Annualized QTY",
]

SUMMARY_SUBTOTAL_COLS = [
    "Prior to Marketing Period - Annualized QTY",
    "Marketing Period - Annualized QTY",
]

# Columns to sort (largest to smallest) before writing
ITEM_DETAIL_SORT_COL = "Annualized QTY"
SUMMARY_SORT_COL = "Marketing Period - Annualized QTY"

# Layout constants
HEADER_ROW = 6        # 0-indexed (row 7 in Excel)
DATA_START_ROW = 7    # 0-indexed (row 8 in Excel)
SUBTOTAL_ROW = 5      # 0-indexed (row 6 in Excel, above headers)


def _col_letter(col_idx):
    """Convert a 0-based column index to an Excel column letter (A, B, ..., Z, AA, ...)."""
    result = ""
    while True:
        result = chr(col_idx % 26 + ord("A")) + result
        col_idx = col_idx // 26 - 1
        if col_idx < 0:
            break
    return result


def _write_sheet(workbook, sheet_name, df, title, date_line, subtotal_cols, sort_col):
    """Write a single formatted sheet."""
    # Sort the DataFrame before writing
    if sort_col in df.columns:
        df = df.sort_values(sort_col, ascending=False).reset_index(drop=True)

    worksheet = workbook.add_worksheet(sheet_name)
    num_rows = len(df)
    num_cols = len(df.columns)
    last_data_row = DATA_START_ROW + num_rows - 1  # 0-indexed

    # ── Formats ───────────────────────────────────────────────────────────
    title_fmt = workbook.add_format({"bold": True, "font_size": 11})
    date_fmt = workbook.add_format({"font_size": 11})
    note_fmt = workbook.add_format({"italic": True, "font_size": 11})
    header_fmt = workbook.add_format({
        "bold": True,
        "bg_color": "#B5E6A2",
        "border": 1,
        "text_wrap": True,
        "align": "center",
        "valign": "vcenter",
    })
    cell_fmt = workbook.add_format({"border": 1})
    cell_center_fmt = workbook.add_format({"border": 1, "align": "center"})
    number_fmt = workbook.add_format({"border": 1, "num_format": "#,##0"})
    pct_fmt = workbook.add_format({"border": 1, "num_format": "0.00%"})
    subtotal_fmt = workbook.add_format({
        "num_format": "#,##0",
        "bold": True,
        "bottom": 1,
    })

    # ── Rows 1-3: Title, date range, note ─────────────────────────────────
    worksheet.write(0, 0, title, title_fmt)
    worksheet.write(1, 0, date_line, date_fmt)
    worksheet.write(
        2, 0,
        "Note: Members who purchased new cases or increased cases due to marketing campaign",
        note_fmt,
    )

    # ── Row 7: Column headers ─────────────────────────────────────────────
    worksheet.set_row(HEADER_ROW, 45)
    for col_idx, col_name in enumerate(df.columns):
        worksheet.write(HEADER_ROW, col_idx, col_name, header_fmt)

    # ── Row 6: SUBTOTAL formulas above specific columns ───────────────────
    col_name_to_idx = {name: i for i, name in enumerate(df.columns)}
    for col_name in subtotal_cols:
        if col_name not in col_name_to_idx:
            continue
        col_idx = col_name_to_idx[col_name]
        letter = _col_letter(col_idx)
        first_cell = f"{letter}{DATA_START_ROW + 1}"       # e.g. K8
        last_cell = f"{letter}{last_data_row + 1}"         # e.g. K5007
        formula = f"=SUBTOTAL(9,{first_cell}:{last_cell})"
        worksheet.write_formula(SUBTOTAL_ROW, col_idx, formula, subtotal_fmt)

    # ── Data rows ─────────────────────────────────────────────────────────
    for row_idx in range(num_rows):
        for col_idx, col_name in enumerate(df.columns):
            value = df.iloc[row_idx, col_idx]

            # Choose format based on column
            if col_name == "Percent Growth":
                fmt = pct_fmt
            elif col_name in subtotal_cols:
                fmt = number_fmt
            elif col_name in ("GPO Brand", "SF PA: GPO Brands-MAP"):
                fmt = cell_center_fmt
            else:
                fmt = cell_fmt

            # Handle NaN / None
            if value is None or (isinstance(value, float) and value != value):
                worksheet.write_blank(DATA_START_ROW + row_idx, col_idx, None, fmt)
            else:
                worksheet.write(DATA_START_ROW + row_idx, col_idx, value, fmt)

    # ── Autofilter on header row ──────────────────────────────────────────
    worksheet.autofilter(HEADER_ROW, 0, last_data_row, num_cols - 1)

    # ── Column widths ──────────────────────────────────────────────────────
    for col_idx, col_name in enumerate(df.columns):
        if "QTY" in col_name and col_name not in ("Percent Growth", "Annualized QTY"):
            worksheet.set_column(col_idx, col_idx, 22.14)
        elif col_name in ("Percent Growth", "Annualized QTY") and sheet_name == "Item Detail":
            worksheet.set_column(col_idx, col_idx, 15.48)
        else:
            # Auto-fit (approximate)
            max_len = len(str(col_name))
            for row_idx in range(min(num_rows, 100)):
                val = df.iloc[row_idx, col_idx]
                if val is not None and not (isinstance(val, float) and val != val):
                    max_len = max(max_len, len(str(val)))
            worksheet.set_column(col_idx, col_idx, min(max_len + 2, 40))


def export_to_excel(
    item_detail,
    summary,
    title,
    before_start_date,
    during_end_date,
    output_path,
):
    """
    Write Item Detail and Summary DataFrames to a formatted Excel file.

    Parameters
    ----------
    item_detail : pd.DataFrame
    summary : pd.DataFrame
    title : str
        Bold title for cell A1 on both sheets.
    before_start_date : date
        Start of the before period (for the date line).
    during_end_date : date
        End of the during period (for the date line).
    output_path : str or Path
        Full path for the output .xlsx file.
    """
    date_line = f"Data: {before_start_date.strftime('%m/%d/%Y')} - {during_end_date.strftime('%m/%d/%Y')}"

    # Filter summary to only rows where Marketing Success = "Yes"
    summary = summary[summary["Marketing Success"] == "Yes"].reset_index(drop=True)

    # Rename columns for display
    item_detail = item_detail.rename(columns=COLUMN_RENAMES)
    summary = summary.rename(columns={
        **COLUMN_RENAMES,
        "SF PA: GPO Brands-MAP": "GPO Brand",
    })

    workbook = xlsxwriter.Workbook(str(output_path))

    # Summary sheet first, then Item Detail
    _write_sheet(
        workbook, "Summary", summary,
        title, date_line,
        SUMMARY_SUBTOTAL_COLS, SUMMARY_SORT_COL,
    )
    _write_sheet(
        workbook, "Item Detail", item_detail,
        title, date_line,
        ITEM_DETAIL_SUBTOTAL_COLS, ITEM_DETAIL_SORT_COL,
    )

    workbook.close()
    print(f"Saved: {output_path}")
