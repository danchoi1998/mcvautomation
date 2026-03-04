"""
Item Usage Report - Refactored for Reuse
=========================================
Wraps the original notebook into a callable pipeline so you can run it
multiple times (e.g. with different date ranges) without re-fetching
date-independent Salesforce data.

Usage:
    1. Edit the CONFIG SECTION below (credentials, filters, date ranges).
    2. Edit .env (bottommost section) for SF credentials.
    3. Run:  python item_usage_report.py
"""

# =============================================================================
# IMPORTS
# =============================================================================
from datetime import date, datetime
import pandas as pd
import numpy as np
from simple_salesforce import Salesforce
import pytz
import time
import os
import psycopg2
import xlsxwriter
from pathlib import Path
import settings
from master_file_creator import create_master_from_dfs
from master_cleaner import load_target_ids, filter_targeted_accounts, pivot_master


# =============================================================================
# CONFIG SECTION  –  Edit these values before running
# =============================================================================
class Config:
    def __init__(self):
        # Output file name (date/time stamp is appended automatically)
        self.file_name = "Example"

        # Folder where final Excel files are saved
        self.save_files_to = Path(r"/mnt/c/Users/DanielChoi/OneDrive - Buyers Edge Platform/Desktop/Python/MCVAutomation/test_files/test_validations")

        # MIN file name and sheet (only used when check_min_file is True)
        self.MIN_file = "file name"
        self.MIN_sheet_name = "sheet name"

        self.exclusion_effective_date = date(2026, 12, 31)

        # Filter configurations
        self.filters = {
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
# CONNECTION HELPERS
# =============================================================================
def connect_salesforce(username: str, password: str, security_token: str, is_test: bool) -> Salesforce:
    """Returns a Salesforce connection object. If test, connects to SF sandbox."""
    domain = None
    if is_test:
        domain = "test"
        username += ".full"
    return Salesforce(username, password, security_token, domain=domain)


def connect_datawarehouse():
    """Returns a psycopg2 connection to the data warehouse."""
    return psycopg2.connect(
        host=settings.DB_HOST,
        database=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        port=settings.DB_PORT,
    )


def sf_to_df(sf, query):
    """Execute a Salesforce SOQL query and return a clean DataFrame."""
    result = sf.query_all(query)
    df = pd.json_normalize(result["records"])
    df = df.drop(df.filter(regex="attribute").columns, axis=1)
    return df


# =============================================================================
# UTILITY CLASSES  (unchanged from notebook)
# =============================================================================
class FileUtils:
    @staticmethod
    def build_file_path(config):
        now = datetime.now(pytz.timezone("US/Eastern"))
        today = now.strftime("%Y-%m-%d at %H-%M")
        return os.path.join(config.save_files_to, f"{config.file_name} on {today}.xlsx")

    @staticmethod
    def process_mins(config, df):
        df["MIN"] = df["MIN"].astype(str).replace({",": "00", "": "00", "nan": "00"})
        if config.filters["min"]["check_min_file"]:
            min_file_path = os.path.join(config.save_files_to, config.MIN_file)
            mins_df = pd.read_excel(min_file_path, sheet_name=config.MIN_sheet_name)
            mins_df = mins_df[["MIN"]]
            mins_df["MIN"] = mins_df["MIN"].astype(str).replace({",": "00", "": "00", "nan": "00"})
            min_set = set(mins_df["MIN"].dropna().unique())
            df["In MIN File?"] = df["MIN"].apply(lambda x: "Yes" if x in min_set else "No")
        return df


class QueryBuilder:
    def __init__(self, config):
        self.config = config
        self.mfr_names_data = None
        self.names_id_filt = None
        self.mfr_platform_ids = None

    # ── private helpers ──────────────────────────────────────────────────
    def _handle_apostrophe_replacement(self, value):
        if isinstance(value, str):
            return value.replace("X", "''")
        return value

    def _handle_apostrophe_list(self, values):
        if isinstance(values, (list, tuple)):
            return [self._handle_apostrophe_replacement(v) for v in values]
        return self._handle_apostrophe_replacement(values)

    def _build_in_clause(self, column, values, handle_apostrophe=False):
        if handle_apostrophe:
            values = self._handle_apostrophe_list(values)
        if isinstance(values, (list, tuple)):
            if len(values) == 1:
                return f"{column} = '{values[0]}'"
            if len(values) >= 40 or len(values) == 0:
                return ""
            quoted = [f"'{v}'" for v in values]
            return f"{column} IN ({', '.join(quoted)})"
        return f"{column} = '{values}'"

    def _build_or_condition(self, conditions, handle_apostrophe=False):
        parts = []
        for column, values, enabled in conditions:
            if enabled:
                if handle_apostrophe:
                    values = self._handle_apostrophe_list(values)
                part = self._build_in_clause(column, values)
                parts.append(part)
        if not parts:
            return ""
        return f" AND ({' OR '.join(parts)})"

    def _clean_query(self, query):
        return " ".join(query.replace("\n", " ").split())

    def _lenquestion(self, tuplename):
        return str(tuplename[0]) if len(tuplename) == 1 else tuplename

    def _filter_by_manufacturer_ids(self, df, ids):
        if isinstance(ids, (tuple, list)):
            return df.loc[df["Exclusion: Manufacturer ID"].isin(ids)]
        return df.loc[df["Exclusion: Manufacturer ID"] == ids]

    # ── query builders ───────────────────────────────────────────────────
    def build_mfr_names_query(self):
        cfg = self.config
        conditions = [
            ("internalcategory",                      cfg.filters["category"]["values"],     cfg.filters["category"]["enabled"]),
            ("manufacturerplatformid",                 cfg.filters["manufacturer"]["ids"],    cfg.filters["manufacturer"]["enabled"]),
            ("manufacturerproductgroupplatformid",     cfg.filters["manufacturer"]["ids"],    cfg.filters["manufacturer"]["enabled"]),
            ("mpc",                                    cfg.filters["min"]["ids"],             cfg.filters["min"]["enabled"]),
        ]
        cond_clause = self._build_or_condition(conditions, handle_apostrophe=True)
        q = f"""
            SELECT manufacturername, manufacturerplatformid,
                   manufacturerproductgroupname, manufacturerproductgroupplatformid,
                   internalcategory
            FROM distributor_invoice_line_full dilf
            WHERE purchasedate BETWEEN '{cfg.from_date}' AND '{cfg.to_date}'
              AND invoicestatus = 'VALID'
              AND resolvedtime > '{cfg.from_date}' {cond_clause}
            GROUP BY manufacturername, manufacturerplatformid,
                     manufacturerproductgroupname, manufacturerproductgroupplatformid,
                     internalcategory
        """
        return self._clean_query(q)

    def process_mfr_names_results(self, results_df):
        df = results_df.copy()
        df = df.set_axis(
            ["Manufacturer Ori", "MFR Platform ID Ori", "MFR Group",
             "MFR Group Platform ID", "Middle Category"], axis=1
        ).drop_duplicates()
        df["Manufacturer"] = np.where(df["MFR Group"].notna(), df["MFR Group"], df["Manufacturer Ori"])
        df["MFR Platform ID"] = np.where(df["MFR Group Platform ID"].notna(),
                                         df["MFR Group Platform ID"], df["MFR Platform ID Ori"])
        self.mfr_names_data = df

        filtered_df = df.drop(columns=["Middle Category"]).drop_duplicates()
        mfr_names = tuple(set(filtered_df["Manufacturer"]))
        self.names_id_filt = self._lenquestion(mfr_names)
        self.mfr_platform_ids = list(set(filtered_df["MFR Platform ID"].dropna()))
        return df

    def build_purchase_query(self):
        if self.names_id_filt is None:
            raise ValueError("Must process MFRNames results before building purchase query")
        cfg = self.config
        conditions = [
            ("internalcategory",                      cfg.filters["category"]["values"],     cfg.filters["category"]["enabled"]),
            ("manufacturerplatformid",                 cfg.filters["manufacturer"]["ids"],    cfg.filters["manufacturer"]["enabled"]),
            ("manufacturerproductgroupplatformid",     cfg.filters["manufacturer"]["ids"],    cfg.filters["manufacturer"]["enabled"]),
            ("mpc",                                    cfg.filters["min"]["ids"],             cfg.filters["min"]["enabled"]),
        ]
        cond_clause = self._build_or_condition(conditions, handle_apostrophe=True)

        brand_clause = ""
        if cfg.filters.get("brand", {}).get("enabled", False):
            brand_values = cfg.filters["brand"]["brands"]
            if cfg.filters["brand"].get("handle_apostrophe", False):
                brand_values = self._handle_apostrophe_list(brand_values)
            if brand_values:
                if isinstance(brand_values, str):
                    brand_values = [brand_values]
                quoted = [f"'{b}'" for b in brand_values]
                brand_clause = f" AND manufacturerproductbrandname IN ({', '.join(quoted)})"

        q = f"""
            SELECT customerplatformid, customerlocation_id,
                   distributorcompanyname, distributorcompanyplatformid,
                   distributorlocationname, distributorlocationplatformid,
                   manufacturername, manufacturerplatformid,
                   manufacturerproductgroupname, manufacturerproductgroupplatformid,
                   mpc, dpc, gtin, mppack, manufacturerproductdescription,
                   distributorproduct_id, manufacturerproduct_id, chargebytypedesc,
                   internalcategory, manufacturerproductbrandname, manufacturerproductbrandtype,
                   sum(quantity) as totalquantity, sum(casequantity) as totalcasequantity,
                   sum(weight) as totalweight, sum(totalprice) as ttotalprice
            FROM distributor_invoice_line_full dilf
            LEFT JOIN unpacked_dm_customer udc ON udc.location_id = dilf.customerlocation_id
            WHERE purchasedate BETWEEN '{cfg.from_date}' AND '{cfg.to_date}'
              AND invoicestatus = 'VALID'
              AND resolvedtime > '{cfg.from_date}'
              AND quantity >= 1 {cond_clause} {brand_clause}
            GROUP BY customerplatformid, customerlocation_id,
                     distributorcompanyname, distributorcompanyplatformid,
                     distributorlocationname, distributorlocationplatformid,
                     manufacturername, manufacturerplatformid,
                     manufacturerproductgroupname, manufacturerproductgroupplatformid,
                     mpc, dpc, gtin, mppack, manufacturerproductdescription,
                     distributorproduct_id, manufacturerproduct_id, chargebytypedesc,
                     internalcategory, manufacturerproductbrandname, manufacturerproductbrandtype
        """
        return self._clean_query(q)

    def build_mfr_agreement_query(self):
        print(self.mfr_platform_ids)
        mfr_cond = ""
        clause = self._build_in_clause(
            "Manufacturer1__r.Platform_Manufacturer_ID__c", self.mfr_platform_ids
        )
        if clause:
            mfr_cond = f" AND {clause}"

        q = f"""
            SELECT Manufacturer_Name_Value__c, Location__r.Name, Platform_Client_ID__c,
                   Manufacturer1__r.Name, Manufacturer1__r.Platform_Manufacturer_ID__c,
                   Effective_Date__c, Date_Removed__c,
                   Agreement_Type__c, Source_of_Exclusion__c, Status__c
            FROM Manufacturer_Agreement__c
            WHERE RecordTypeId = '012C0000000IJ3HIAW'
              AND Date_Removed__c = null
              AND Status__c = 'Approved'
              AND Effective_Date__c <= {self.config.exclusion_effective_date}
              AND Agreement_Type__c != 'On Platform Deal'
              {mfr_cond}
        """
        filter_after = "Filter After" if mfr_cond == "" else ""
        return self._clean_query(q), filter_after

    def build_opp_query(self):
        cfg = self.config
        base = """
            SELECT
                Program__r.AccountID18__c,
                Program__r.Account.Name,
                Program__r.Account.Platform_Client_ID__c,
                Program__r.Account.Parent.Name,
                Program__r.Account.Parent_Platform_ID__c,
                Program__r.Account.Parent.AccountID18__c,
                Program__r.Account.Parent.Parent.Name,
                Program__r.Account.Parent.Parent_Platform_ID__c,
                Program__r.Account.Parent.Parent.AccountID18__c,
                Program__r.Account.Parent.Parent.Parent.Name,
                Program__r.Account.Parent.Parent.Parent_Platform_ID__c,
                Program__r.Account.Parent.Parent.Parent.AccountID18__c,
                Program__r.Account.Parent.GPO_BrandsMAP__c,
                Program__r.Account.Parent.Subscription_Level__c,
                Program__r.Account.Market_Sector__c,
                Program__r.Account.Market_Segment_e__c,
                Program__r.Account.Parent.Internal_Channel_Partners__c,
                Program__r.Account.Parent.Channel_Partners__c,
                Program__r.Account.Menu_Type__c,
                Program__r.Account.Parent.Client_Manager__r.Name,
                Program__r.Account.BillingStreet,
                Program__r.Account.BillingCity,
                Program__r.Account.BillingState,
                Program__r.Account.BillingPostalCode"""
        if cfg.filters.get("S1 communication filter", {}).get("enabled", False):
            base += ",\n                Program__r.Account.Parent.Communication_Restrictions__c"
        base += """
            FROM Participation_Summary__c
            WHERE Program__r.Type = 'MAP'
              AND Program__r.Account.IsParent1__c = False
              AND Program__r.Account.Test_Account_1__c = False
              AND Program__r.StageName IN ('Active', 'Awaiting Revenue')
              AND Program__r.Account.Platform_Client_ID__c != ''
              AND Program__r.Account.Platform_Client_ID__c != null
              AND Program__r.Account.Subscription_Level__c != 'Do Not Submit'"""
        if cfg.filters.get("S1 communication filter", {}).get("enabled", False):
            base += """
              AND Program__r.Account.Parent.Communication_Restrictions__c IN ('', 'No Restrictions')"""
        return self._clean_query(base)

    def get_column_mapping(self):
        mapping = {
            "Program__r.AccountID18__c":                                "SF Location: Account ID",
            "Program__r.Account.Name":                                  "SF Location: Name",
            "Program__r.Account.Platform_Client_ID__c":                 "SF Location: Platform ID",
            "Program__r.Account.Parent.GPO_BrandsMAP__c":               "SF PA: GPO Brands-MAP",
            "Program__r.Account.Parent.Subscription_Level__c":          "SF PA: Subscription Tier",
            "Program__r.Account.Parent.Channel_Partners__c":            "SF PA: External CP",
            "Program__r.Account.Parent.Internal_Channel_Partners__c":   "SF PA: Internal CP",
            "Program__r.Account.Parent.Client_Manager__r.Name":         "SF PA: Client Manager",
            "Program__r.Account.Market_Sector__c":                      "SF Location: Market Sector",
            "Program__r.Account.Market_Segment_e__c":                   "SF Location: Market Segment",
            "Program__r.Account.Menu_Type__c":                          "SF Location: Menu Type",
            "Program__r.Account.BillingStreet":                         "SF Location: Billing Street",
            "Program__r.Account.BillingCity":                           "SF Location: Billing City",
            "Program__r.Account.BillingPostalCode":                     "SF Location: Billing Postal Code",
            "Program__r.Account.BillingState":                          "SF Location: Billing State",
            "Program__r.Account.Parent.Name":                           "SF PA: Name",
            "Program__r.Account.Parent_Platform_ID__c":                 "SF PA: Platform ID",
            "Program__r.Account.Parent.AccountID18__c":                 "SF PA: Account ID",
            "Program__r.Account.Parent.Parent.Name":                    "SF GPA: Name",
            "Program__r.Account.Parent.Parent_Platform_ID__c":          "SF GPA: Platform ID",
            "Program__r.Account.Parent.Parent.AccountID18__c":          "SF GPA: Account ID",
            "Program__r.Account.Parent.Parent.Parent.Name":             "SF GGPA: Name",
            "Program__r.Account.Parent.Parent.Parent_Platform_ID__c":   "SF GGPA: Platform ID",
            "Program__r.Account.Parent.Parent.Parent.AccountID18__c":   "SF GGPA: Account ID",
        }
        if self.config.filters.get("S1 communication filter", {}).get("enabled", False):
            mapping["Program__r.Account.Parent.Communication_Restrictions__c"] = "SF PA: Communication Restrictions"
        return mapping

    def process_manufacturer_agreements(self, sf, mfr_agreement_query, mfr_names_pull,
                                        manufacturer_ids, filter_after):
        print(f"Exclusions Query: {mfr_agreement_query}")
        MfrAg = sf_to_df(sf, mfr_agreement_query)
        MfrAg = MfrAg.rename(columns={
            "Location__r.Name":                                "Exclusion: Location Name",
            "Platform_Client_ID__c":                           "Exclusion: Location Platform ID",
            "Manufacturer1__r.Name":                           "Exclusion: Manufacturer",
            "Manufacturer1__r.Platform_Manufacturer_ID__c":    "Exclusion: Manufacturer ID",
            "Effective_Date__c":                                "Exclusion: Effective Date",
            "Date_Removed__c":                                 "Exclusion: Date Removed",
            "Agreement_Type__c":                               "Exclusion: Agreement Type",
            "Source_of_Exclusion__c":                           "Exclusion: Source of Agreement",
            "Status__c":                                       "Exclusion: Status",
        })
        MfrAg = MfrAg[["Exclusion: Location Name", "Exclusion: Location Platform ID",
                        "Exclusion: Manufacturer", "Exclusion: Manufacturer ID",
                        "Exclusion: Source of Agreement"]].copy()

        if filter_after == "Filter After":
            MfrAg = MfrAg[MfrAg["Exclusion: Manufacturer ID"].isin(self.mfr_platform_ids)]

        MfrAg.loc[:, "PLID & Source"] = (
            MfrAg["Exclusion: Location Platform ID"].astype(str)
            + ": " + MfrAg["Exclusion: Source of Agreement"].astype(str)
        )

        MFRNames2 = (
            mfr_names_pull[["Manufacturer", "MFR Platform ID"]]
            .copy()
            .loc[lambda d: d["MFR Platform ID"].notna()]
            .rename(columns={"MFR Platform ID": "Exclusion: Manufacturer ID"})
            .drop_duplicates()
        )
        MfrAg2 = MfrAg.merge(MFRNames2, how="left", on="Exclusion: Manufacturer ID")

        # Filtered by specific manufacturer IDs
        if self.config.filters["manufacturer"]["enabled"]:
            filtered = self._filter_by_manufacturer_ids(MfrAg2, manufacturer_ids)
            filtered = (
                pd.DataFrame(filtered)
                .drop_duplicates()
                .assign(**{"Excluded from MFR": lambda x: "Excluded from " + x["Manufacturer"].astype(str) + "?"})
                .copy()
            )
        else:
            filtered = None

        # All exclusions (for MiddCatOnlyExcl)
        all_excl = (
            MfrAg2[["Exclusion: Location Platform ID", "Manufacturer",
                     "Exclusion: Manufacturer ID", "Exclusion: Source of Agreement",
                     "PLID & Source"]]
            .copy().drop_duplicates()
        )
        all_excl.loc[:, "Excluded from Purchased Manufacturer?"] = np.where(
            all_excl["Exclusion: Manufacturer ID"].notna(), "Excluded", "Error"
        )
        final_all = all_excl[["Exclusion: Location Platform ID", "Manufacturer",
                               "Excluded from Purchased Manufacturer?",
                               "Exclusion: Source of Agreement", "PLID & Source"]].copy()
        return final_all, filtered


# =============================================================================
# SALESFORCE DATA FETCH  –  date-independent, run ONCE
# =============================================================================
def fetch_salesforce_data(sf, config):
    """
    Fetch all Salesforce data that does NOT depend on date ranges.
    Returns a dict of DataFrames that can be reused across runs.
    """
    print("=" * 60)
    print("Fetching date-independent Salesforce data (once) ...")
    print("=" * 60)
    t0 = time.time()

    qb = QueryBuilder(config)

    # ── Opportunities ────────────────────────────────────────────────────
    opp_query = qb.build_opp_query()
    column_mapping = qb.get_column_mapping()

    def combine_cp(row):
        ext = str(row["SF PA: External CP"]) if pd.notna(row["SF PA: External CP"]) else ""
        internal = str(row["SF PA: Internal CP"]) if pd.notna(row["SF PA: Internal CP"]) else ""
        return ",".join(filter(None, [ext, internal]))

    df = sf_to_df(sf, opp_query)
    opp = (
        df.rename(columns=column_mapping)
        .assign(**{"SF PA: Channel Partners": lambda x: x.apply(combine_cp, axis=1)})
        .drop(columns=[c for c in df.columns if c not in column_mapping]
              + ["SF PA: External CP", "SF PA: Internal CP"])
        .drop_duplicates()
    )

    # ── Channel Partner Agreements ───────────────────────────────────────
    cpa_query = """
        SELECT Name, Account__r.Platform_Client_ID__c,
               Channel_Partner__r.Distributor__r.Sales_Rep__r.Name,
               Channel_Partner__r.Name, CreatedDate
        FROM Channel_Partner_Agreement__c
        WHERE Account__r.Test_Account_1__c = False
          AND Account__r.IsParent1__c = True
          AND Account__r.Platform_Client_ID__c != ''
          AND Account__r.Subscription_Level__c != 'Do Not Submit'
    """
    cpa = (
        sf_to_df(sf, cpa_query)
        .rename(columns={
            "Name": "CPA Name",
            "CreatedDate": "Created Date",
            "Account__r.Platform_Client_ID__c": "SF Highest Group PLID",
            "Channel_Partner__r.Distributor__r.Sales_Rep__r.Name": "SF Highest Group: DSTR Sales Rep",
            "Channel_Partner__r.Name": "SF Highest Group: Channel Partner(s)",
        })
        .drop(columns=["Channel_Partner__r.Distributor__r",
                        "Channel_Partner__r.Distributor__r.Sales_Rep__r"])
        .assign(**{"Created Date": lambda x: pd.to_datetime(x["Created Date"]).dt.date})
        .drop_duplicates()
        .fillna("")
        .sort_values(by=["SF Highest Group PLID", "Created Date"])
        .drop(columns=["CPA Name", "Created Date"])
        .drop_duplicates(subset=["SF Highest Group PLID",
                                  "SF Highest Group: Channel Partner(s)",
                                  "SF Highest Group: DSTR Sales Rep"], keep="first")
        .groupby("SF Highest Group PLID")
        .agg({
            "SF Highest Group: Channel Partner(s)": lambda x: ", ".join(filter(None, set(x))),
            "SF Highest Group: DSTR Sales Rep":     lambda x: ", ".join(filter(None, set(x))),
        })
        .reset_index()
    )

    # ── Primary Contacts ─────────────────────────────────────────────────
    contact_query = """
        SELECT Platform_Client_ID__c,
               Primary_Contact__r.Name,
               Primary_Contact__r.Email,
               Primary_Contact__r.Phone
        FROM Account
        WHERE Test_Account_1__c = False
          AND Platform_Client_ID__c != ''
          AND Primary_Contact__r.Phone != ''
    """
    contacts = sf_to_df(sf, contact_query).rename(columns={
        "Platform_Client_ID__c":       "SF Highest Group PLID",
        "Primary_Contact__r.Name":     "SF Highest Group: Primary Contact",
        "Primary_Contact__r.Email":    "SF Highest Group: Primary Contact Email",
        "Primary_Contact__r.Phone":    "SF Highest Group: Primary Contact Phone",
    })

    print(f"Salesforce fetch complete in {time.time() - t0:.1f}s\n")
    return {
        "opp": opp,
        "cpa": cpa,
        "contacts": contacts,
        "column_mapping": column_mapping,
    }


# =============================================================================
# DATE-DEPENDENT PIPELINE  –  run once PER date range
# =============================================================================
def run_purchase_pipeline(sf, config, sf_data):
    """
    Execute the full date-dependent pipeline:
      1. Build & run DW queries (MFR names, purchases)
      2. Build exclusions
      3. Merge with (pre-fetched) SF data
      4. Write formatted Excel
    """
    opp          = sf_data["opp"]
    cpa          = sf_data["cpa"]
    contacts     = sf_data["contacts"]

    print("=" * 60)
    print(f"Running pipeline for {config.from_date} → {config.to_date}")
    print("=" * 60)
    pipeline_start = time.time()

    # ── 1. Query Builder: MFR names, agreements, purchase query ──────────
    qb = QueryBuilder(config)

    mfr_names_query = qb.build_mfr_names_query()
    print(f"MFR Names Query: {mfr_names_query}")
    with connect_datawarehouse() as cnx:
        cur = cnx.cursor()
        cur.execute(mfr_names_query.replace("X", "''"))
        results = cur.fetchall()
        field_names = [i[0] for i in cur.description]

    mfr_names = qb.process_mfr_names_results(pd.DataFrame(results, columns=field_names))

    mfr_agreement_query, filter_after = qb.build_mfr_agreement_query()
    manufacturer_ids = config.filters["manufacturer"]["ids"]
    mfr_agreement_exclusions, mfr_id_filter = qb.process_manufacturer_agreements(
        sf, mfr_agreement_query, mfr_names, manufacturer_ids, filter_after
    )

    purchase_query = qb.build_purchase_query()

    # ── 2. Fetch purchase data from DW ───────────────────────────────────
    print(f"Purchases Query: {purchase_query}")
    with connect_datawarehouse() as cnx:
        with cnx.cursor() as cur:
            cur.execute(purchase_query)
            table_rows = cur.fetchall()

    new_columns = [
        "DM Customer Platform ID", "DM Customer ID",
        "Distributor Parent", "DSTR PA Platform ID", "Distributor House",
        "DSTR Platform ID", "Manufacturer Ori", "MFR Platform ID Ori",
        "Manufacturer Group Ori", "MFR Group Platform ID Ori",
        "MIN", "DIN", "GTIN", "Pack Size", "Product Description",
        "DSTR Product ID", "MFR Product ID", "Unit", "Middle Category",
        "Brand", "Brand Owner value",
        "Total Quantity", "Total Case QTY", "Total Weight", "Total Price",
    ]
    LocPurchase = pd.DataFrame(table_rows, columns=new_columns)
    LocPurchase = LocPurchase.assign(
        Manufacturer=lambda d: d["Manufacturer Group Ori"].fillna(d["Manufacturer Ori"]),
        MFR_Platform_ID=lambda d: d["MFR Group Platform ID Ori"].fillna(d["MFR Platform ID Ori"]),
        Brand_Owner=lambda d: pd.Series(
            np.select(
                [d["Brand Owner value"] == 1, d["Brand Owner value"] == 2,
                 d["Brand Owner value"] == 9, d["Brand Owner value"].isna()],
                ["Manufacturer", "Distributor", "Customer", None],
                default=np.nan,
            ), index=d.index,
        ),
    ).rename(columns={"MFR_Platform_ID": "MFR Platform ID", "Brand_Owner": "Brand Owner"})

    drop_cols = {"Brand Owner value", "Manufacturer Ori", "MFR Platform ID Ori",
                 "Manufacturer Group Ori", "MFR Group Platform ID Ori"}
    LocPurchase = LocPurchase[[c for c in LocPurchase.columns if c not in drop_cols]]

    # ── 3. Merge purchases with SF opp data, process exclusions ──────────
    PurAccDat1 = (
        pd.merge(LocPurchase, opp, how="left",
                 left_on="DM Customer Platform ID", right_on="SF Location: Platform ID")
        .drop_duplicates()
        .drop(columns=["DM Customer Platform ID", "DM Customer ID"])
    )
    PurAccDat = FileUtils.process_mins(config, PurAccDat1)

    Exclusionaccounts = (
        PurAccDat[["SF Location: Platform ID", "SF PA: Platform ID",
                   "SF GPA: Platform ID", "SF GGPA: Platform ID"]]
        .drop_duplicates()
        .dropna(subset=["SF Location: Platform ID"])
    )
    plat_cols = ["SF Location: Platform ID", "SF PA: Platform ID",
                 "SF GPA: Platform ID", "SF GGPA: Platform ID"]

    # MFR-specific exclusions
    Excluded1 = None
    if config.filters["manufacturer"]["enabled"] and mfr_id_filter is not None:
        Excluded1 = (
            pd.concat([
                mfr_id_filter.merge(Exclusionaccounts, how="left",
                                    left_on="Exclusion: Location Platform ID", right_on=col)
                for col in plat_cols
            ])
            .dropna(subset=["SF Location: Platform ID"])
            .drop_duplicates()
            .sort_values(by=["SF Location: Platform ID", "Exclusion: Manufacturer",
                             "Exclusion: Source of Agreement"])
            .pipe(lambda df: pd.merge(
                df.groupby(["SF Location: Platform ID", "Exclusion: Manufacturer"])
                  .agg({"PLID & Source": lambda x: ", ".join(filter(None, set(x)))})
                  .reset_index()
                  .assign(**{"Excluded from PLID & Source":
                             lambda x: x["Exclusion: Manufacturer"] + "-- " + x["PLID & Source"]})
                  .groupby("SF Location: Platform ID")
                  .agg({"Excluded from PLID & Source": lambda x: ";  ".join(filter(None, set(x)))})
                  .reset_index(),
                df[["SF Location: Platform ID", "Excluded from MFR"]]
                  .drop_duplicates()
                  .groupby(["SF Location: Platform ID", "Excluded from MFR"])["Excluded from MFR"]
                  .count().unstack().reset_index()
                  .mask(lambda x: x.apply(lambda y: pd.to_numeric(y, errors="coerce")).notnull(), "Excluded")
                  .fillna("Not Excluded").drop_duplicates(),
                how="left", on="SF Location: Platform ID",
            ))
            .drop_duplicates()
        )

    # Category-level exclusions
    MiddCatOnlyExcl4 = (
        pd.concat([
            mfr_agreement_exclusions.merge(Exclusionaccounts, how="left",
                                           left_on="Exclusion: Location Platform ID", right_on=col)
            for col in plat_cols
        ])
        .dropna(subset=["SF Location: Platform ID"])
        .sort_values(by=["SF Location: Platform ID", "Manufacturer",
                         "Exclusion: Source of Agreement"])
        .pipe(lambda df: pd.merge(
            df.groupby(["SF Location: Platform ID", "Manufacturer"])
              .agg({"PLID & Source": lambda x: ", ".join(filter(None, set(x)))})
              .reset_index()
              .assign(**{"Purchase Excluded PLID & Source":
                         lambda x: x["Manufacturer"] + "-- " + x["PLID & Source"]})
              [["SF Location: Platform ID", "Manufacturer", "Purchase Excluded PLID & Source"]],
            df[["SF Location: Platform ID", "Manufacturer",
                "Excluded from Purchased Manufacturer?"]].drop_duplicates(),
            how="left", on=["SF Location: Platform ID", "Manufacturer"],
        ))
        .drop_duplicates()
    )

    # ── 4. High-group logic & final merges ───────────────────────────────
    def highgroup(df):
        conditions = [df["SF GGPA: Name"].notna(), df["SF GPA: Name"].notna(), df["SF PA: Name"].notna()]
        name_choices  = [df["SF GGPA: Name"], df["SF GPA: Name"], df["SF PA: Name"]]
        plid_choices  = [df["SF GGPA: Platform ID"], df["SF GPA: Platform ID"], df["SF PA: Platform ID"]]
        default = "No matching PLID in SF"
        return pd.DataFrame({
            "SF Highest Group Name": np.select(conditions, name_choices, default),
            "SF Highest Group PLID": np.select(conditions, plid_choices, default),
        })

    PurAccDat4 = (
        PurAccDat
        .assign(**{"On MAP?": lambda x: np.where(x["SF Location: Platform ID"].isnull(),
                                                  "No, not on MAP", "Yes, on MAP")})
        .assign(**highgroup(PurAccDat))
        .pipe(lambda df: pd.merge(df, Excluded1, how="left", on="SF Location: Platform ID")
              if config.filters["manufacturer"]["enabled"] and Excluded1 is not None else df)
        .pipe(lambda df: pd.merge(df, MiddCatOnlyExcl4, how="left",
                                  on=["SF Location: Platform ID", "Manufacturer"]))
        .drop_duplicates()
        .pipe(lambda df: pd.merge(df, contacts, how="left", on="SF Highest Group PLID"))
        .drop_duplicates()
        .pipe(lambda df: pd.merge(df, cpa, how="left", on="SF Highest Group PLID"))
        .drop_duplicates()
    )

    # ── 5. Column ordering ───────────────────────────────────────────────
    dm_columns = [
        "Distributor Parent", "DSTR PA Platform ID", "Distributor House", "DSTR Platform ID",
        "Manufacturer", "MFR Platform ID", "MIN", "DIN", "GTIN", "MFR Product ID",
        "DSTR Product ID", "Pack Size", "Product Description", "Unit", "Middle Category",
        "Brand", "Brand Owner", "Total Quantity", "Total Case QTY", "Total Weight", "Total Price",
    ]
    sf_columns1 = ["On MAP?"]
    if config.filters["min"]["check_min_file"]:
        sf_columns1.append("In MIN File?")
    sf_columns2 = [
        "SF Highest Group Name", "SF Highest Group PLID", "SF Location: Name",
        "SF Location: Platform ID", "SF Location: Account ID",
    ]
    sf_columns3 = [
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
    if config.filters["S1 communication filter"]["enabled"]:
        idx = sf_columns3.index("SF PA: Client Manager") + 1
        sf_columns3.insert(idx, "SF PA: Communication Restrictions")

    allcols = sf_columns1 + sf_columns2 + dm_columns + sf_columns3
    PurAccDat4 = PurAccDat4[
        sf_columns1
        + [c for c in PurAccDat4.columns if c not in allcols]
        + sf_columns2 + dm_columns + sf_columns3
    ]
    for c in PurAccDat4.columns:
        if c not in allcols:
            PurAccDat4[c] = PurAccDat4[c].fillna("Not Excluded")

    # ── 6. Filter to MAP-only, apply removals ────────────────────────────
    PurAccDat5 = PurAccDat4.loc[PurAccDat4["On MAP?"] == "Yes, on MAP"].copy()
    print(f"{len(PurAccDat5):,} records are on MAP")

    if config.filters["remove entirely"]["enabled"]:
        ids_remove = config.filters["remove entirely"]["mfr_ids"]
        if isinstance(ids_remove, str):
            ids_remove = [ids_remove]
        before = len(PurAccDat5)
        PurAccDat5 = PurAccDat5.loc[~PurAccDat5["MFR Platform ID"].isin(ids_remove)].copy()
        print(f"Removed {before - len(PurAccDat5):,} records for MFR IDs: {', '.join(ids_remove)}")

    print(f"Pipeline for {config.from_date} → {config.to_date} done in {time.time() - pipeline_start:.1f}s\n")
    return PurAccDat5


# =============================================================================
# EXCEL WRITER  (unchanged from notebook)
# =============================================================================
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
        for col in ["Distributor Parent", "DSTR PA Platform ID", "Distributor House",
                     "DSTR Platform ID", "Manufacturer", "MFR Platform ID",
                     "MFR Product ID", "DSTR Product ID", "Pack Size",
                     "Product Description", "Unit", "Middle Category", "Brand", "Brand Owner"]:
            self.column_to_format[col] = "DM_Columns_2"
        for col in ["On MAP?", "In MIN File?"]:
            self.column_to_format[col] = "Programs"
        for col in ["Total Weight", "Total Case QTY"]:
            self.column_to_format[col] = "Weight"
        for col in ["Total Quantity"]:
            self.column_to_format[col] = "Quant"
        for col in ["Total Price"]:
            self.column_to_format[col] = "Money"
        for col in ["MIN", "DIN", "GTIN"]:
            self.column_to_format[col] = "Min"
        for col in [
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
        ]:
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
                df = df.replace([np.nan, np.inf, -np.inf], "")
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


# =============================================================================
# MAIN  –  entry point
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
            master = pivot_master(master)

    elapsed = time.time() - overall_start
    print("=" * 60)
    print(f"All done!  Total wall time: {elapsed / 60:.1f} minutes")
    print("=" * 60)


if __name__ == "__main__":
    main()
