"""
Glue crawler setup for the Retail Intelligence Platform.
Creates one crawler per Bronze source. Each crawler scans its S3 prefix,
infers schema, and registers tables in the Glue Data Catalog.
Run once to set up — crawlers can then be triggered manually or on schedule.
"""

import json
import logging
import sys

import boto3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
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

CRAWLER_CONFIG = json.dumps({
    "Version": 1.0,
    "CrawlerOutput": {
        "Partitions": {"AddOrUpdateBehavior": "InheritFromTable"}
    }
})


def create_glue_database(glue_client: boto3.client) -> None:
    """Create the Glue database if it does not already exist."""
    try:
        glue_client.get_database(Name=DATABASE_NAME)
        logger.info(f"Database '{DATABASE_NAME}' already exists — skipping")
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_database(
            DatabaseInput={
                "Name": DATABASE_NAME,
                "Description": "Bronze layer catalog for Retail Intelligence Platform"
            }
        )
        logger.info(f"Created database '{DATABASE_NAME}'")


def crawler_exists(glue_client: boto3.client, name: str) -> bool:
    """Return True if a crawler with the given name already exists."""
    try:
        glue_client.get_crawler(Name=name)
        return True
    except glue_client.exceptions.EntityNotFoundException:
        return False


def create_crawler(glue_client: boto3.client, crawler: dict) -> None:
    """Create a single Glue crawler if it does not already exist."""
    name = crawler["name"]

    if crawler_exists(glue_client, name):
        logger.info(f"Crawler '{name}' already exists — skipping")
        return

    glue_client.create_crawler(
        Name=name,
        Role=GLUE_ROLE_ARN,
        DatabaseName=DATABASE_NAME,
        Description=crawler["description"],
        Targets={"S3Targets": [{"Path": crawler["s3_path"]}]},
        TablePrefix=crawler["table_prefix"],
        SchemaChangePolicy={
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "LOG"
        },
        RecrawlPolicy={"RecrawlBehavior": "CRAWL_EVERYTHING"},
        Configuration=CRAWLER_CONFIG
    )
    logger.info(f"Created crawler '{name}' → {crawler['s3_path']}")


def run_crawler(glue_client: boto3.client, name: str) -> None:
    """Trigger a crawler to run immediately."""
    try:
        glue_client.start_crawler(Name=name)
        logger.info(f"Started crawler '{name}'")
    except glue_client.exceptions.CrawlerRunningException:
        logger.info(f"Crawler '{name}' is already running — skipping")


def main() -> None:
    """Create Glue database, all crawlers, and trigger initial runs."""
    # boto3 auto-detects credentials from ~/.aws/credentials (set via aws configure)
    glue_client = boto3.client("glue", region_name=REGION)

    logger.info("Setting up Glue Data Catalog...")
    create_glue_database(glue_client)

    logger.info("Creating crawlers...")
    for crawler in CRAWLERS:
        create_crawler(glue_client, crawler)

    logger.info("Triggering initial crawler runs...")
    for crawler in CRAWLERS:
        run_crawler(glue_client, crawler["name"])

    logger.info("Glue setup complete")


if __name__ == "__main__":
    main()