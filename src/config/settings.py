"""
Settings
========
Loads credentials from the .env file at the project root.
Fill in your values in .env before running.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# Salesforce
# =============================================================================
SF_USERNAME       = os.getenv("SF_USERNAME", "")
SF_PASSWORD       = os.getenv("SF_PASSWORD", "")
SF_SECURITY_TOKEN = os.getenv("SF_SECURITY_TOKEN", "")

# =============================================================================
# Data warehouse (PostgreSQL)
# =============================================================================
DB_HOST     = os.getenv("DB_HOST", "")
DB_USER     = os.getenv("DB_USER", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME     = os.getenv("DB_NAME", "")
DB_PORT     = os.getenv("DB_PORT", "5432")
