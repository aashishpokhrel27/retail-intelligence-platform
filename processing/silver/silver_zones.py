"""
Silver Zones Processing
Reads NYC Zone Lookup CSV from S3 Bronze, cleans and types columns,
and writes a clean Parquet file to S3 Silver.
"""

import os
from io import BytesIO, StringIO

import boto3
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql.functions import col, trim, upper

# ── Credentials ───────────────────────────────────────────────────────────────
load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION     = os.getenv("AWS_REGION", "us-east-1")

# ── Config ────────────────────────────────────────────────────────────────────
BRONZE_BUCKET  = "retail-intel-bronze"
SILVER_BUCKET  = "retail-intel-silver"
BRONZE_KEY     = "zones/taxi_zone_lookup.csv"
SILVER_KEY     = "zones/taxi_zone_lookup.parquet"

# ── S3 Client ─────────────────────────────────────────────────────────────────
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)


def read_bronze_zones() -> pd.DataFrame:
    """Read NYC Zone Lookup CSV from S3 Bronze."""
    obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=BRONZE_KEY)
    csv_content = obj["Body"].read().decode("utf-8")
    pdf = pd.read_csv(StringIO(csv_content))
    print(f"Bronze row count: {len(pdf)}")
    return pdf


def clean_zones(pdf: pd.DataFrame):
    """Convert to Spark, cast types, clean strings."""
    df = spark.createDataFrame(pdf)

    df_silver = (
        df
        .withColumnRenamed("LocationID", "location_id")
        .withColumnRenamed("Borough", "borough")
        .withColumnRenamed("Zone", "zone")
        .withColumnRenamed("service_zone", "service_zone")
        .withColumn("location_id", col("location_id").cast("integer"))
        .withColumn("borough", trim(upper(col("borough"))))
        .withColumn("zone", trim(col("zone")))
        .withColumn("service_zone", trim(col("service_zone")))
    )

    print(f"Silver row count: {df_silver.count()}")
    df_silver.printSchema()
    return df_silver


def write_silver_zones(df_silver) -> None:
    """Write Silver zones to S3 as Parquet via boto3."""
    pdf_silver = pd.DataFrame([row.asDict() for row in df_silver.collect()])

    buffer = BytesIO()
    pdf_silver.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    s3.put_object(
        Bucket=SILVER_BUCKET,
        Key=SILVER_KEY,
        Body=buffer.getvalue()
    )
    print(f"Written {len(pdf_silver)} rows to s3://{SILVER_BUCKET}/{SILVER_KEY}")


def validate_silver_zones() -> None:
    """Read back and validate the Silver zones file."""
    obj = s3.get_object(Bucket=SILVER_BUCKET, Key=SILVER_KEY)
    pdf_check = pd.read_parquet(BytesIO(obj["Body"].read()))

    print(f"Validation — Row count: {len(pdf_check)}")
    print(f"Validation — Columns: {list(pdf_check.columns)}")
    print(f"Validation — Boroughs: {sorted(pdf_check['borough'].dropna().unique())}")


def main():
    pdf_bronze = read_bronze_zones()
    df_silver  = clean_zones(pdf_bronze)
    write_silver_zones(df_silver)
    validate_silver_zones()
    print("Silver zones complete")


if __name__ == "__main__":
    main()