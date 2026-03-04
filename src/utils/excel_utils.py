"""
Excel Utilities
===============
Reusable ExcelCreation class for writing formatted Excel workbooks.
"""

import time
import numpy as np
import pandas as pd
import xlsxwriter

from config.columns import (
    EXCEL_DM_FORMAT_COLUMNS,
    EXCEL_PROGRAM_COLUMNS,
    EXCEL_WEIGHT_COLUMNS,
    EXCEL_QUANT_COLUMNS,
    EXCEL_MONEY_COLUMNS,
    EXCEL_MIN_COLUMNS,
    EXCEL_SF_TEXT_COLUMNS,
)


class ExcelCreation:
    def __init__(self):
        self.FORMAT_MAPPINGS = {
            "DM_Columns_2": {"header": "DMheader", "data": "format_data"},
            "Weight":       {"header": "DMheader", "data": "weightn"},
            "Min":          {"header": "DMheader", "data": "minn"},
            "Quant":        {"header": "DMheader", "data": "quantn"},
            "Money":        {"header": "DMheader", "data": "moneyn"},
            "Programs":     {"header": "DMheader", "data": "format_data"},
            "SF_Text":      {"header": "SFheader", "data": "text_format"},
            "default":      {"header": "SFheader", "data": "format_data"},
        }
        self.column_to_format = {}
        for col in EXCEL_DM_FORMAT_COLUMNS:
            self.column_to_format[col] = "DM_Columns_2"
        for col in EXCEL_PROGRAM_COLUMNS:
            self.column_to_format[col] = "Programs"
        for col in EXCEL_WEIGHT_COLUMNS:
            self.column_to_format[col] = "Weight"
        for col in EXCEL_QUANT_COLUMNS:
            self.column_to_format[col] = "Quant"
        for col in EXCEL_MONEY_COLUMNS:
            self.column_to_format[col] = "Money"
        for col in EXCEL_MIN_COLUMNS:
            self.column_to_format[col] = "Min"
        for col in EXCEL_SF_TEXT_COLUMNS:
            self.column_to_format[col] = "SF_Text"

    def write_formatted_excel(self, dfs, file_path):
        total_start = time.time()
        with pd.ExcelWriter(file_path, engine="xlsxwriter",
                            engine_kwargs={"options": {"nan_inf_to_errors": True}}) as writer:
            workbook = writer.book
            formats = {
                "SFheader":    workbook.add_format({"bold": True, "align": "left", "bg_color": "#F2F2F2"}),
                "DMheader":    workbook.add_format({"bold": True, "align": "left", "bg_color": "#FDE9D9"}),
                "text_format": workbook.add_format({"align": "left", "num_format": "@"}),
                "format_data": workbook.add_format({"align": "left"}),
                "moneyn":      workbook.add_format({"num_format": 44}),
                "minn":        workbook.add_format({"num_format": "00000"}),
                "weightn":     workbook.add_format({"num_format": "#,##0.00"}),
                "quantn":      workbook.add_format({"num_format": "#,##0"}),
            }
            for sheetname, df in dfs.items():
                if df.empty:
                    continue
                print(f"\nProcessing sheet: {sheetname}")
                print(f"Total rows: {len(df):,}")
                df.replace([np.nan, np.inf, -np.inf], "", inplace=True)
                t0 = time.time()
                df.to_excel(writer, sheet_name=sheetname, index=False, startrow=0)
                worksheet = writer.sheets[sheetname]
                for col_num, col_name in enumerate(df.columns):
                    fmt_type = self.column_to_format.get(col_name, "default")
                    hdr_fmt  = formats[self.FORMAT_MAPPINGS[fmt_type]["header"]]
                    dat_fmt  = formats[self.FORMAT_MAPPINGS[fmt_type]["data"]]
                    max_len  = max(len(str(col_name)), df[col_name].astype(str).apply(len).max())
                    width    = 45 if max_len >= 43 else (max_len + 2)
                    worksheet.write(0, col_num, col_name, hdr_fmt)
                    worksheet.set_column(col_num, col_num, width, dat_fmt)
                last_col = xlsxwriter.utility.xl_col_to_name(df.shape[1] - 1)
                worksheet.autofilter(f"A1:{last_col}{df.shape[0] + 1}")
                print(f"Writing data took: {time.time() - t0:.2f} seconds")
        print(f"\nTotal Excel file creation time: {time.time() - total_start:.2f} seconds")
        print(f"Excel file created at: {file_path}")
