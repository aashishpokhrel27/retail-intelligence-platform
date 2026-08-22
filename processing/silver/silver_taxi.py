# Databricks notebook source
import boto3
import pandas as pd
import json
import os
from io import BytesIO
from pyspark.sql.functions import col, to_timestamp, hour, dayofweek, unix_timestamp
from pyspark.sql.functions import round as spark_round

# Fill in your credentials from .env
from dotenv import load_dotenv

load_dotenv()
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")

BRONZE_BUCKET  = "retail-intel-bronze"
SILVER_BUCKET  = "retail-intel-silver"

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)
print("Ready")

# COMMAND ----------

response = s3.list_objects_v2(
    Bucket=BRONZE_BUCKET,
    Prefix="taxi/"
)
for obj in response.get("Contents", []):
    size_mb = obj["Size"] / 1024 / 1024
    print(f"{obj['Key']} - {size_mb:.1f} MB")

# COMMAND ----------

import pyarrow.parquet as pq

# Read just the schema of January file
key = "taxi/year=2024/month=01/yellow_tripdata_2024-01.parquet"
obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)
pdf_sample = pd.read_parquet(BytesIO(obj["Body"].read()), engine="pyarrow")

print(f"Row count: {len(pdf_sample)}")
print(f"Columns: {list(pdf_sample.columns)}")
print(pdf_sample.dtypes)
pdf_sample.head(3)

# COMMAND ----------

from pyspark.sql.functions import (
    col, hour, dayofweek, unix_timestamp,
    when, lit, to_date
)

def process_taxi_month(key: str, zones_pdf: pd.DataFrame):
    """Read one month of taxi data, clean and enrich with zones."""
    obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)
    pdf = pd.read_parquet(BytesIO(obj["Body"].read()), engine="pyarrow")
    
    df = spark.createDataFrame(pdf)
    
    df_clean = (
        df
        .withColumnRenamed("VendorID", "vendor_id")
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumnRenamed("passenger_count", "passenger_count")
        .withColumnRenamed("trip_distance", "trip_distance_miles")
        .withColumnRenamed("PULocationID", "pickup_location_id")
        .withColumnRenamed("DOLocationID", "dropoff_location_id")
        .withColumnRenamed("fare_amount", "fare_amount")
        .withColumnRenamed("tip_amount", "tip_amount")
        .withColumnRenamed("total_amount", "total_amount")
        .withColumnRenamed("payment_type", "payment_type")
        # Cast types
        .withColumn("vendor_id", col("vendor_id").cast("integer"))
        .withColumn("passenger_count", col("passenger_count").cast("integer"))
        .withColumn("trip_distance_miles", col("trip_distance_miles").cast("double"))
        .withColumn("fare_amount", col("fare_amount").cast("double"))
        .withColumn("tip_amount", col("tip_amount").cast("double"))
        .withColumn("total_amount", col("total_amount").cast("double"))
        .withColumn("payment_type", col("payment_type").cast("integer"))
        .withColumn("pickup_location_id", col("pickup_location_id").cast("integer"))
        .withColumn("dropoff_location_id", col("dropoff_location_id").cast("integer"))
        # Derived columns
        .withColumn("pickup_date", to_date(col("pickup_datetime")))
        .withColumn("pickup_hour", hour(col("pickup_datetime")))
        .withColumn("day_of_week", dayofweek(col("pickup_datetime")))
        .withColumn("trip_duration_minutes",
            (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60
        )
        .withColumn("payment_type_desc",
            when(col("payment_type") == 1, "Credit card")
            .when(col("payment_type") == 2, "Cash")
            .when(col("payment_type") == 3, "No charge")
            .when(col("payment_type") == 4, "Dispute")
            .otherwise("Unknown")
        )
        # Data quality filters
        .filter(col("fare_amount") > 0)
        .filter(col("trip_distance_miles") > 0)
        .filter(col("trip_duration_minutes") > 0)
        .filter(col("passenger_count") > 0)
        .filter(col("pickup_date") >= "2024-01-01")
        .filter(col("pickup_date") <= "2024-06-30")
        # Select final columns
        .select(
            "vendor_id", "pickup_datetime", "dropoff_datetime",
            "pickup_date", "pickup_hour", "day_of_week",
            "trip_duration_minutes", "trip_distance_miles",
            "pickup_location_id", "dropoff_location_id",
            "passenger_count", "payment_type", "payment_type_desc",
            "fare_amount", "tip_amount", "total_amount"
        )
    )
    return df_clean

# Load zones for enrichment
zones_obj = s3.get_object(Bucket=SILVER_BUCKET, Key="zones/taxi_zone_lookup.parquet")
zones_pdf = pd.read_parquet(BytesIO(zones_obj["Body"].read()))
print(f"Zones loaded: {len(zones_pdf)} rows")

# List all taxi files
taxi_keys = [
    obj["Key"] for obj in 
    s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix="taxi/")["Contents"]
]
print(f"Taxi files to process: {len(taxi_keys)}")

# COMMAND ----------

# ── Cell 5: Process all months with pandas only ───────────────────────────────
all_pdfs = []

for key in taxi_keys:
    month = key.split("month=")[1].split("/")[0]
    print(f"Processing month {month}...")
    
    obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)
    pdf = pd.read_parquet(BytesIO(obj["Body"].read()), engine="pyarrow")
    
    # Clean and transform with pandas
    pdf = pdf.rename(columns={
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance_miles",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "fare_amount": "fare_amount",
        "tip_amount": "tip_amount",
        "total_amount": "total_amount",
        "payment_type": "payment_type"
    })
    
    # Derived columns
    pdf["pickup_date"] = pd.to_datetime(pdf["pickup_datetime"]).dt.date
    pdf["pickup_hour"] = pd.to_datetime(pdf["pickup_datetime"]).dt.hour
    pdf["day_of_week"] = pd.to_datetime(pdf["pickup_datetime"]).dt.dayofweek + 1
    pdf["trip_duration_minutes"] = (
        pd.to_datetime(pdf["dropoff_datetime"]) - 
        pd.to_datetime(pdf["pickup_datetime"])
    ).dt.total_seconds() / 60
    
    # Payment description
    payment_map = {1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute"}
    pdf["payment_type_desc"] = pdf["payment_type"].map(payment_map).fillna("Unknown")
    
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
    
    print(f"  Month {month}: {len(pdf):,} clean rows")
    all_pdfs.append(pdf)

# Combine all months
pdf_silver = pd.concat(all_pdfs, ignore_index=True)
print(f"\nTotal Silver taxi rows: {len(pdf_silver):,}")

# COMMAND ----------

# ── Write Silver taxi to S3 as Parquet ───────────────────────────────────────
print("Writing to S3... this may take a few minutes for 17M rows")

buffer = BytesIO()
pdf_silver.to_parquet(buffer, index=False, engine="pyarrow")
buffer.seek(0)

s3.put_object(
    Bucket=SILVER_BUCKET,
    Key="taxi/taxi_trips_silver.parquet",
    Body=buffer.getvalue()
)
print(f"Written {len(pdf_silver):,} rows to s3://retail-intel-silver/taxi/taxi_trips_silver.parquet")

# ── Validate ──────────────────────────────────────────────────────────────────
obj = s3.get_object(Bucket=SILVER_BUCKET, Key="taxi/taxi_trips_silver.parquet")
pdf_check = pd.read_parquet(BytesIO(obj["Body"].read()))
print(f"Validation — Rows: {len(pdf_check):,}")
print(f"Validation — Date range: {pdf_check['pickup_date'].min()} to {pdf_check['pickup_date'].max()}")
print(f"Validation — Avg fare: ${pdf_check['fare_amount'].mean():.2f}")
print(f"Validation — Payment types:\n{pdf_check['payment_type_desc'].value_counts()}")