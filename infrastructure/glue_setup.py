"""
Glue crawler setup for the Retail Intelligence Platform.
Creates one crawler per Bronze source. Each crawler scnas its S3 prefix,
infers schema, and registers tables in the Glue Data Catalog.
Run once to set up -- crawlers can then be triggered manually or on schedule.
"""

import boto3
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(messages)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
REGION = "us-east-1"
GLUE_ROLE_ARN = "arn:aws:iam::866409326585:role/retail-intel-glue-role"
DATABASE_NAME = "retail_intel_bronze"
BRONZE_BUCKET = "retail-intel-bronze"

CRAWLERS = [
    {
        "name": "retail-intel-taxi-crawler",
        "s3_path": f"s3://{BRONZE_BUCKET}/taxi/",
        "table_prefix": "taxi_",
        "description": "Crawls NYC Taxi Parquet files partitioned by year/month"
    },
    {
        "name": "retail-intel-weather-crawler",
        "s3_path": f"s3://{BRONZE_BUCKET}/weather/organized/",
        "table_prefix": "weather_",
        "description": "Crawls organized daily weather JSON files"
    },
    {
        "name": "retail-intel-zones-crawler",
        "s3_path": f"s3://{BRONZE_BUCKET}/zones/",
        "table_prefix": "zones_",
        "description": "Crawls NYC Zone Lookup CSV reference file"
    },
    {
        "name": "retail-intel-complaints-crawler",
        "s3_path": f"s3://{BRONZE_BUCKET}/complaints/organized/",
        "table_prefix": "complaints_",
        "description": "Crawls organized daily 311 Complaints JSON files"
    },
    {
        "name": "retail-intel-clickstream-crawler",
        "s3_path": f"s3://{BRONZE_BUCKET}/clickstream/",
        "table_prefix": "clickstream_",
        "description": "Crawls streaming clickstream events partitioned by year/month/day/hour"
    },
]