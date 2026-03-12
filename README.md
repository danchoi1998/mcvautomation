# Marketing Campaign Validation (MCV) Automation

Automates marketing campaign validation by pulling purchase data from Salesforce and a PostgreSQL data warehouse, comparing before vs. during campaign periods, and producing a formatted Excel report.

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Create a `.env` file at the project root:
   ```
   SF_USERNAME=your_sf_username
   SF_PASSWORD=your_sf_password
   SF_SECURITY_TOKEN=your_sf_token
   DB_HOST=localhost
   DB_USER=your_db_user
   DB_PASSWORD=your_db_password
   DB_NAME=your_db_name
   DB_PORT=5432
   ```

3. Make sure the PostgreSQL data warehouse tunnel is running on your machine.

## Configuration

Open `src/run.py` and edit the **Config section** at the top:

- **`Config` class** — campaign name, output folder, manufacturer/category/MIN/brand filters
- **`DATE_RANGES`** — two date tuples: the "before" period and the "during" period
- **`REFERENCE_FILES`** — list of CSV/Excel files containing `Account Platform ID` columns (used to filter to targeted accounts)
- **`TARGET_MINS`** — list of MINs to keep in the final output (leave empty to keep all)

## Running

```bash
cd src && python run.py
```

The pipeline takes ~15–25 minutes depending on the data volume. Progress is printed to the console.

## Output

An `.xlsx` file is saved to the `save_files_to` path configured in `Config`. It contains two sheets:

- **Summary** — Account-level view with annualized quantities and percent growth (only accounts where Marketing Success = Yes)
- **Item Detail** — Product-level detail with before/during case quantities, annualized metrics, percent growth, and marketing success classification

## Project Structure

```
src/
├── run.py                  # Entry point and configuration
├── filegenerator.py        # Salesforce + PostgreSQL data extraction and merging
├── master_file_creator.py  # Unions before/during period DataFrames
├── master_cleaner.py       # Filtering, aggregation, and calculated columns
├── excel_writer.py         # Formatted Excel output
└── settings.py             # Loads .env credentials
```
