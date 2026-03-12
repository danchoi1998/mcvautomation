# Marketing Campaign Validation (MCV) Automation

Automates marketing campaign validation by pulling purchase data from Salesforce and a PostgreSQL data warehouse, comparing before vs. during campaign periods, and producing a formatted Excel report.

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Create a `.env` file at the project root (see `.env.example` for the template):
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

## Running

### Web App (recommended)

Start the Streamlit web app:
```bash
./start_app.sh
```

Then open the URL shown in the terminal. The app provides a form-based interface for all configuration options, file uploads, and a download button for the Excel report.

To run in the background:
```bash
nohup ./start_app.sh > app.log 2>&1 &
```

To stop:
```bash
pkill -f "streamlit run src/app.py"
```

### Team Access

Team members on the same network can access the app at `http://<host-ip>:8501`. The host IP is printed when the app starts.

You may need to open port 8501 in your firewall. On Windows (run as Administrator):
```powershell
netsh advfirewall firewall add rule name="Streamlit MCV App" dir=in action=allow protocol=TCP localport=8501
```

### Command Line

For scripted or headless runs, edit the Config section at the top of `src/run.py`, then:
```bash
cd src && python run.py
```

## Configuration

When using `run.py`, edit these values at the top of the file:

- **`Config` class** — campaign name, output folder, manufacturer/category/MIN/brand filters
- **`DATE_RANGES`** — two date tuples: the "before" period and the "during" period
- **`REFERENCE_FILES`** — list of CSV/Excel files containing `Account Platform ID` columns (used to filter to targeted accounts)
- **`TARGET_MINS`** — list of MINs to keep in the final output (leave empty to keep all)

The pipeline takes ~15–25 minutes depending on the data volume.

## Output

An `.xlsx` file with two sheets:

- **Summary** — Account-level view with annualized quantities and percent growth (only accounts where Marketing Success = Yes)
- **Item Detail** — Product-level detail with before/during case quantities, annualized metrics, percent growth, and marketing success classification

## Project Structure

```
├── start_app.sh              # Launch script for the web app
├── requirements.txt
├── .env                      # Credentials (not committed)
└── src/
    ├── app.py                # Streamlit web interface
    ├── run.py                # CLI entry point and configuration
    ├── filegenerator.py      # Salesforce + PostgreSQL data extraction and merging
    ├── master_file_creator.py# Unions before/during period DataFrames
    ├── master_cleaner.py     # Filtering, aggregation, and calculated columns
    ├── excel_writer.py       # Formatted Excel output
    └── settings.py           # Loads .env credentials
```
