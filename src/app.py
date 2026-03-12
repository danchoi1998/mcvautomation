"""
MCV Automation - Streamlit Web App
===================================
Launch with:  streamlit run src/app.py
"""

import sys
import os
import io
import time
import tempfile
import contextlib
import traceback
from datetime import date
from pathlib import Path

import streamlit as st

# Ensure src/ is on the import path
sys.path.insert(0, str(Path(__file__).resolve().parent))

import settings
from filegenerator import (
    connect_salesforce,
    fetch_salesforce_data,
    run_purchase_pipeline,
)
from master_file_creator import create_master_from_dfs
from master_cleaner import (
    load_target_ids,
    filter_targeted_accounts,
    filter_targeted_mins,
    aggregate_master,
    add_calculated_columns,
    aggregate_summary,
)
from excel_writer import export_to_excel


# =============================================================================
# PAGE CONFIG
# =============================================================================
st.set_page_config(page_title="MCV Automation", layout="wide")
st.title("Marketing Campaign Validation")

# =============================================================================
# STDOUT CAPTURE  –  shows pipeline print() output in the UI
# =============================================================================
class StreamCapture(io.StringIO):
    """Captures stdout and mirrors it to a Streamlit placeholder."""

    def __init__(self, placeholder):
        super().__init__()
        self.placeholder = placeholder
        self.lines = []

    def write(self, text):
        super().write(text)
        if text.strip():
            self.lines.append(text.strip())
            self.placeholder.code("\n".join(self.lines[-30:]), language=None)
        return len(text)


# =============================================================================
# HELPER – save uploaded files to temp dir so load_target_ids can read them
# =============================================================================
def save_uploaded_files(uploaded_files):
    tmp_dir = tempfile.mkdtemp()
    paths = []
    for uf in uploaded_files:
        path = os.path.join(tmp_dir, uf.name)
        with open(path, "wb") as f:
            f.write(uf.getbuffer())
        paths.append(path)
    return paths


# =============================================================================
# SIDEBAR – Configuration Form
# =============================================================================
with st.sidebar:
    st.header("Configuration")

    with st.form("config_form"):
        # ── Campaign name ────────────────────────────────────────────────
        file_name = st.text_input("Campaign Name", value="")

        # ── Exclusion effective date ─────────────────────────────────────
        exclusion_date = st.date_input(
            "Exclusion Effective Date", value=date(2026, 12, 31)
        )

        # ── Date ranges ─────────────────────────────────────────────────
        st.subheader("Date Ranges")
        st.caption("Before Marketing Period")
        b_col1, b_col2 = st.columns(2)
        before_from = b_col1.date_input("From", value=date(2025, 5, 1), key="bf")
        before_to = b_col2.date_input("To", value=date(2025, 8, 31), key="bt")

        st.caption("During Marketing Period")
        d_col1, d_col2 = st.columns(2)
        during_from = d_col1.date_input("From", value=date(2025, 9, 1), key="df")
        during_to = d_col2.date_input("To", value=date(2026, 1, 15), key="dt")

        # ── Filters ─────────────────────────────────────────────────────
        st.subheader("Filters")

        mfr_enabled = st.checkbox("Manufacturer filter", value=True)
        mfr_ids = st.text_input(
            "Manufacturer IDs (comma-separated)", value="MA-1000012"
        )

        cat_enabled = st.checkbox("Category filter", value=True)
        cat_values = st.text_input(
            "Categories (comma-separated)", value="Towels (Disposable)"
        )
        cat_apostrophe = st.checkbox("Handle apostrophe (category)", value=False)

        min_enabled = st.checkbox("MIN filter", value=False)
        min_ids = st.text_input("MIN IDs (comma-separated)", value="")
        check_min_file = st.checkbox("Check MIN file", value=False)
        min_file_name = st.text_input("MIN file name", value="")
        min_sheet = st.text_input("MIN sheet name", value="")

        s1_enabled = st.checkbox("S1 communication filter", value=False)

        remove_enabled = st.checkbox("Remove entirely filter", value=False)
        remove_ids = st.text_input(
            "MFR IDs to remove (comma-separated)", value="MA-1047966"
        )

        brand_enabled = st.checkbox("Brand filter", value=False)
        brand_values = st.text_input("Brands (comma-separated)", value="")
        brand_apostrophe = st.checkbox("Handle apostrophe (brand)", value=True)

        # ── Target MINs ─────────────────────────────────────────────────
        st.subheader("Target MINs")
        target_mins_text = st.text_area(
            "MINs to keep (one per line, leave empty to keep all)", value=""
        )

        # ── Submit ───────────────────────────────────────────────────────
        submitted = st.form_submit_button("Run Pipeline", type="primary")


# =============================================================================
# MAIN AREA – File uploaders + results
# =============================================================================

# Reference files for targeted account filtering
st.subheader("Reference Files")
st.caption("Upload CSV/Excel files containing 'Account Platform ID' columns")
uploaded_ref_files = st.file_uploader(
    "Reference files",
    accept_multiple_files=True,
    type=["csv", "xlsx", "xls"],
    label_visibility="collapsed",
)

st.divider()

# =============================================================================
# PIPELINE EXECUTION
# =============================================================================
if submitted:
    if not file_name.strip():
        st.error("Please enter a campaign name.")
        st.stop()

    # ── Build Config object ──────────────────────────────────────────────
    class Config:
        pass

    conf = Config()
    conf.file_name = file_name.strip()
    conf.save_files_to = Path(tempfile.mkdtemp())
    conf.MIN_file = min_file_name.strip() or "file name"
    conf.MIN_sheet_name = min_sheet.strip() or "sheet name"
    conf.exclusion_effective_date = exclusion_date

    def parse_list(text):
        return [x.strip() for x in text.split(",") if x.strip()]

    conf.filters = {
        "manufacturer": {
            "enabled": mfr_enabled,
            "ids": parse_list(mfr_ids),
        },
        "category": {
            "enabled": cat_enabled,
            "values": parse_list(cat_values),
            "handle_apostrophe": cat_apostrophe,
        },
        "min": {
            "enabled": min_enabled,
            "ids": parse_list(min_ids),
            "check_min_file": check_min_file,
        },
        "S1 communication filter": {
            "enabled": s1_enabled,
        },
        "remove entirely": {
            "enabled": remove_enabled,
            "mfr_ids": parse_list(remove_ids),
        },
        "brand": {
            "enabled": brand_enabled,
            "brands": parse_list(brand_values),
            "handle_apostrophe": brand_apostrophe,
        },
    }

    date_ranges = [
        (before_from, before_to),
        (during_from, during_to),
    ]

    target_mins = [
        m.strip() for m in target_mins_text.strip().splitlines() if m.strip()
    ]

    ref_file_paths = save_uploaded_files(uploaded_ref_files) if uploaded_ref_files else []

    # ── Run ──────────────────────────────────────────────────────────────
    progress = st.progress(0, text="Starting pipeline...")
    log_placeholder = st.empty()
    capture = StreamCapture(log_placeholder)

    try:
        with contextlib.redirect_stdout(capture):
            # Step 1: Connect
            progress.progress(0.05, text="Connecting to Salesforce...")
            sf = connect_salesforce(
                settings.SF_USERNAME,
                settings.SF_PASSWORD,
                settings.SF_SECURITY_TOKEN,
                False,
            )

            # Step 2: Fetch SF data
            progress.progress(0.10, text="Fetching Salesforce data (this takes a few minutes)...")
            sf_data = fetch_salesforce_data(sf, conf)

            # Step 3: Run pipeline for each date range
            MAX_RETRIES = 3
            results = []
            for i, (from_date, to_date) in enumerate(date_ranges):
                pct = 0.20 + i * 0.30
                progress.progress(pct, text=f"Processing {from_date} to {to_date}...")
                conf.from_date = from_date
                conf.to_date = to_date
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        sf = connect_salesforce(
                            settings.SF_USERNAME,
                            settings.SF_PASSWORD,
                            settings.SF_SECURITY_TOKEN,
                            False,
                        )
                        df = run_purchase_pipeline(sf, conf, sf_data)
                        results.append(df)
                        break
                    except Exception as e:
                        print(f"Pipeline attempt {attempt}/{MAX_RETRIES} failed: {e}")
                        if attempt == MAX_RETRIES:
                            raise
                        print("Retrying with fresh SF connection...")

            if len(results) != 2:
                st.error("Expected 2 date ranges but got a different number of results.")
                st.stop()

            # Step 4: Create master
            progress.progress(0.80, text="Creating master DataFrame...")
            master = create_master_from_dfs(results[0], results[1])

            # Step 5: Filter targeted accounts
            if ref_file_paths:
                target_ids = load_target_ids(ref_file_paths)
                master = filter_targeted_accounts(master, target_ids)

            # Step 6: Filter targeted MINs
            if target_mins:
                master = filter_targeted_mins(master, target_mins)

            # Step 7: Aggregate
            progress.progress(0.88, text="Aggregating and calculating metrics...")
            master = aggregate_master(master)
            item_detail = add_calculated_columns(master, date_ranges[0], date_ranges[1])
            summary = aggregate_summary(item_detail)

            # Step 8: Export to Excel
            progress.progress(0.95, text="Generating Excel file...")
            tmp_xlsx = os.path.join(tempfile.mkdtemp(), f"{conf.file_name}.xlsx")
            export_to_excel(
                item_detail,
                summary,
                title=conf.file_name,
                before_start_date=date_ranges[0][0],
                during_end_date=date_ranges[1][1],
                output_path=tmp_xlsx,
            )

            with open(tmp_xlsx, "rb") as f:
                excel_bytes = f.read()

        progress.progress(1.0, text="Done!")

        # ── Results ──────────────────────────────────────────────────────
        st.success(
            f"Pipeline complete! Item Detail: {item_detail.shape[0]:,} rows | "
            f"Summary: {summary.shape[0]:,} rows"
        )

        st.download_button(
            label="Download Excel Report",
            data=excel_bytes,
            file_name=f"{conf.file_name}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
        )

        tab1, tab2 = st.tabs(["Item Detail", "Summary"])
        with tab1:
            st.dataframe(item_detail, use_container_width=True)
        with tab2:
            st.dataframe(summary, use_container_width=True)

    except Exception:
        progress.empty()
        st.error("Pipeline failed. See details below.")
        st.code(traceback.format_exc())
