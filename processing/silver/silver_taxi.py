"""
Silver Taxi Processing
Reads NYC Yellow Taxi Parquet files from S3 Bronze (Jan-Jun 2024),
cleans and enriches with derived columns, and writes partitioned
Parquet files to S3 Silver (one file per month).
"""

import logging
import os
import sys
from io import BytesIO

import boto3
import pandas as pd
from dotenv import load_dotenv

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ── Credentials ───────────────────────────────────────────────────────────────
load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION     = os.getenv("AWS_REGION", "us-east-1")

# ── Config ────────────────────────────────────────────────────────────────────
BRONZE_BUCKET = "retail-intel-bronze"
SILVER_BUCKET = "retail-intel-silver"

PAYMENT_MAP = {1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute"}


def build_s3_client() -> boto3.client:
    """Build boto3 S3 client using credentials from environment."""
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )


def list_taxi_keys(s3: boto3.client) -> list:
    """List all Bronze taxi Parquet file keys."""
    response = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix="taxi/")
    keys = [obj["Key"] for obj in response.get("Contents", [])]
    logger.info(f"Found {len(keys)} taxi files")
    return keys


def process_month(s3: boto3.client, key: str) -> pd.DataFrame:
    """Read one month of taxi data, clean and enrich with pandas."""
    month = key.split("month=")[1].split("/")[0]
    logger.info(f"Processing month {month}...")

    obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)
    pdf = pd.read_parquet(BytesIO(obj["Body"].read()), engine="pyarrow")

    # Rename columns
    pdf = pdf.rename(columns={
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "trip_distance": "trip_distance_miles",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
    })

    # Cast types
    pdf["vendor_id"] = pdf["vendor_id"].astype("Int32")
    pdf["passenger_count"] = pdf["passenger_count"].astype("Int32")
    pdf["pickup_location_id"] = pdf["pickup_location_id"].astype("Int32")
    pdf["dropoff_location_id"] = pdf["dropoff_location_id"].astype("Int32")
    pdf["payment_type"] = pdf["payment_type"].astype("Int32")
    pdf["pickup_datetime"] = pd.to_datetime(pdf["pickup_datetime"])
    pdf["dropoff_datetime"] = pd.to_datetime(pdf["dropoff_datetime"])

    # Derived columns
    pdf["pickup_date"] = pdf["pickup_datetime"].dt.date
    pdf["pickup_hour"] = pdf["pickup_datetime"].dt.hour
    pdf["day_of_week"] = pdf["pickup_datetime"].dt.dayofweek + 1
    pdf["trip_duration_minutes"] = (
        pdf["dropoff_datetime"] - pdf["pickup_datetime"]
    ).dt.total_seconds() / 60
    pdf["payment_type_desc"] = pdf["payment_type"].map(PAYMENT_MAP).fillna("Unknown")

    # Data quality filters
    pdf = pdf[pdf["fare_amount"] > 0]
    pdf = pdf[pdf["trip_distance_miles"] > 0]
    pdf = pdf[pdf["trip_duration_minutes"] > 0]
    pdf = pdf[pdf["passenger_count"] > 0]
    pdf["pickup_date"] = pd.to_datetime(pdf["pickup_date"])
    pdf = pdf[pdf["pickup_date"] >= "2024-01-01"]
    pdf = pdf[pdf["pickup_date"] <= "2024-06-30"]

    # Select final columns
    pdf = pdf[[
        "vendor_id", "pickup_datetime", "dropoff_datetime",
        "pickup_date", "pickup_hour", "day_of_week",
        "trip_duration_minutes", "trip_distance_miles",
        "pickup_location_id", "dropoff_location_id",
        "passenger_count", "payment_type", "payment_type_desc",
        "fare_amount", "tip_amount", "total_amount"
    ]]

    logger.info(f"  Month {month}: {len(pdf):,} clean rows")
    return pdf, month


def write_month(s3: boto3.client, pdf: pd.DataFrame, month: str) -> None:
    """Write one month of Silver taxi data to S3 as Parquet."""
    key = f"taxi/year=2024/month={month}/taxi_trips.parquet"
    buffer = BytesIO()
    pdf.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    s3.put_object(Bucket=SILVER_BUCKET, Key=key, Body=buffer.getvalue())
    logger.info(f"Written s3://retail-intel-silver/{key}")


def validate(s3: boto3.client) -> None:
    """List Silver taxi partitions and sizes."""
    response = s3.list_objects_v2(Bucket=SILVER_BUCKET, Prefix="taxi/")
    for obj in response.get("Contents", []):
        size_mb = obj["Size"] / 1024 / 1024
        logger.info(f"  {obj['Key']} — {size_mb:.1f} MB")


def main() -> None:
    s3 = build_s3_client()
    keys = list_taxi_keys(s3)

    total_rows = 0
    for key in keys:
        pdf_month, month = process_month(s3, key)
        write_month(s3, pdf_month, month)
        total_rows += len(pdf_month)

    logger.info(f"Total Silver taxi rows written: {total_rows:,}")
    logger.info("Validating Silver taxi partitions...")
    validate(s3)
    logger.info("Silver taxi complete")


if __name__ == "__main__":
    main()