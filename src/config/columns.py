"""
Column Definitions
==================
Centralized column name constants, rename maps, and ordering lists
used across the pipeline.
"""

# ── Purchase query result columns ────────────────────────────────────────
PURCHASE_COLUMNS = [
    "DM Customer Platform ID", "DM Customer ID",
    "Distributor Parent", "DSTR PA Platform ID", "Distributor House",
    "DSTR Platform ID", "Manufacturer Ori", "MFR Platform ID Ori",
    "Manufacturer Group Ori", "MFR Group Platform ID Ori",
    "MIN", "DIN", "GTIN", "Pack Size", "Product Description",
    "DSTR Product ID", "MFR Product ID", "Unit", "Middle Category",
    "Brand", "Brand Owner value",
    "Total Quantity", "Total Case QTY", "Total Weight", "Total Price",
]

# ── Output column ordering ───────────────────────────────────────────────
DM_COLUMNS = [
    "Distributor Parent", "DSTR PA Platform ID", "Distributor House", "DSTR Platform ID",
    "Manufacturer", "MFR Platform ID", "MIN", "DIN", "GTIN", "MFR Product ID",
    "DSTR Product ID", "Pack Size", "Product Description", "Unit", "Middle Category",
    "Brand", "Brand Owner", "Total Quantity", "Total Case QTY", "Total Weight", "Total Price",
]

SF_COLUMNS_2 = [
    "SF Highest Group Name", "SF Highest Group PLID", "SF Location: Name",
    "SF Location: Platform ID", "SF Location: Account ID",
]

SF_COLUMNS_3 = [
    "SF PA: Name", "SF PA: Platform ID", "SF PA: Account ID",
    "SF GPA: Name", "SF GPA: Platform ID", "SF GPA: Account ID",
    "SF GGPA: Name", "SF GGPA: Platform ID", "SF GGPA: Account ID",
    "SF Location: Market Sector", "SF Location: Market Segment", "SF Location: Menu Type",
    "SF Location: Billing Street", "SF Location: Billing City",
    "SF Location: Billing Postal Code", "SF Location: Billing State",
    "SF PA: GPO Brands-MAP", "SF PA: Subscription Tier", "SF PA: Channel Partners",
    "SF PA: Client Manager",
    "SF Highest Group: Channel Partner(s)", "SF Highest Group: DSTR Sales Rep",
    "SF Highest Group: Primary Contact", "SF Highest Group: Primary Contact Email",
    "SF Highest Group: Primary Contact Phone",
]

# ── Manufacturer agreement rename map ────────────────────────────────────
MFR_AGREEMENT_RENAME = {
    "Location__r.Name":                                "Exclusion: Location Name",
    "Platform_Client_ID__c":                           "Exclusion: Location Platform ID",
    "Manufacturer1__r.Name":                           "Exclusion: Manufacturer",
    "Manufacturer1__r.Platform_Manufacturer_ID__c":    "Exclusion: Manufacturer ID",
    "Effective_Date__c":                                "Exclusion: Effective Date",
    "Date_Removed__c":                                 "Exclusion: Date Removed",
    "Agreement_Type__c":                               "Exclusion: Agreement Type",
    "Source_of_Exclusion__c":                           "Exclusion: Source of Agreement",
    "Status__c":                                       "Exclusion: Status",
}

# ── Master file column constants ─────────────────────────────────────────
QTY_COL = "Total Quantity"
CASE_QTY_COL = "Total Case QTY"

# ── Excel format column groupings ────────────────────────────────────────
EXCEL_DM_FORMAT_COLUMNS = [
    "Distributor Parent", "DSTR PA Platform ID", "Distributor House",
    "DSTR Platform ID", "Manufacturer", "MFR Platform ID",
    "MFR Product ID", "DSTR Product ID", "Pack Size",
    "Product Description", "Unit", "Middle Category", "Brand", "Brand Owner",
]

EXCEL_PROGRAM_COLUMNS = ["On MAP?", "In MIN File?"]

EXCEL_WEIGHT_COLUMNS = ["Total Weight", "Total Case QTY"]
EXCEL_QUANT_COLUMNS = ["Total Quantity"]
EXCEL_MONEY_COLUMNS = ["Total Price"]
EXCEL_MIN_COLUMNS = ["MIN", "DIN", "GTIN"]

EXCEL_SF_TEXT_COLUMNS = [
    "SF GPA: Name", "SF GPA: Platform ID", "SF GPA: Account ID",
    "SF GGPA: Name", "SF GGPA: Platform ID", "SF GGPA: Account ID",
    "SF PA: Client Manager", "SF Highest Group: Channel Partner(s)",
    "SF Highest Group: DSTR Sales Rep", "SF Highest Group: Primary Contact",
    "SF Highest Group: Primary Contact Email", "SF Highest Group: Primary Contact Phone",
    "SF PA: Name", "SF PA: Platform ID", "SF PA: Account ID",
    "SF Location: Name", "SF Location: Platform ID", "SF Location: Account ID",
    "SF Location: Market Sector", "SF Location: Market Segment", "SF Location: Menu Type",
    "SF Location: Billing Street", "SF Location: Billing City",
    "SF Location: Billing Postal Code", "SF Location: Billing State",
    "SF PA: GPO Brands-MAP", "SF PA: Subscription Tier", "SF PA: Channel Partners",
]
