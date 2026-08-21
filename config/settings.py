"""
Centralized configuration for the Retail Intelligence Platform.
All constants, bucket names, paths, and environment variable
loading live here. Import from this module -- never hardcode
values directly in ingeation or processing scripts.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── AWS ───────────────────────────────────────────────────────────────────────
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# ── S3 Buckets ───────────────────────────────────────────────────────────────────────
BRONZE_BUCKET = "retail-intel-bronze"
SILVER_BUCKET = "retail-intel-silver"
GOLD_BUCKET = "retail-intel-gold"
ARCHIVE_BUCKET = "retail-intel-archive"

# ── Snowflake ───────────────────────────────────────────────────────────────────────
SF_ACCOUNT = os.getenv("SF_ACCOUNT")
SF_PIPELINE_USER = os.getenv("SF_PIPELINE_USER")
SF_PIPELINE_PASSWORD = os.getenv("SF_PIPELINE_PASSWORD")
SF_REPORTING_USER = os.getenv("SF_REPORTING_USER")
SF_REPORTING_PASSWORD = os.getenv("SF_REPORTING_PASSWORD")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE", "ANALYTICS_WH")
SF_BRONZE_DB = os.getenv("SF_BRONZE_DB", "BRONZE_DB")
SF_SILVER_DB = os.getenv("SF_SILVER_DB", "SILVER_DB")
SF_GOLD_DB = os.getenv("SF_GOLD_DB", "GOLD_DB")

# ── Databricks ────────────────────────────────────────────────────────────────
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

# ── Ingestion ─────────────────────────────────────────────────────────────────
TAXI_START_YEAR = 2024
TAXI_START_MONTH = 1
TAXI_END_MONTH = 6

COMPLAINTS_START_DATE = "2024-01-01"
COMPLAINTS_END_DATE = "2024-01-31"
COMPLAINTS_PAGE_SIZE = 1000
COMPLAINTS_MAX_RETRIES = 3
COMPLAINTS_RETRY_BACKOFF = 2

WEATHER_START_DATE = "2024-01-01"
WEATHER_END_DATE = "2024-06-30"
WEATHER_LATITUDE = 40.7128
WEATHER_LONGITUDE = -74.0060

# ── Streaming ─────────────────────────────────────────────────────────────────
CLICKSTREAM_BATCH_SIZE = 50
CLICKSTREAM_INTERVAL_SECONDS = 30

NYC_STORES = [
    {"store_id": "NYC-001", "borough": "MANHATTAN"},
    {"store_id": "NYC-002", "borough": "BROOKLYN"},
    {"store_id": "NYC-003", "borough": "QUEENS"},
    {"store_id": "NYC-004", "borough": "BRONX"},
    {"store_id": "NYC-005", "borough": "STATEN ISLAND"},
]

PRODUCT_CATEGORIES = [
    "Electronics", "Clothing", "Home", "Sports", "Beauty", "Food"
]