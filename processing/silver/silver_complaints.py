# Databricks notebook source
import boto3
import pandas as pd
import json
import os
from io import BytesIO, StringIO

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
    Prefix="complaints/organized/",
    MaxKeys=5
)
for obj in response.get("Contents", []):
    print(obj["Key"])

# COMMAND ----------

import gzip

# Inspect one file
key = "complaints/organized/year=2024/month=01/day=01/complaints_2024-01-01.json.gz"
obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)
content = gzip.decompress(obj["Body"].read()).decode("utf-8")
records = json.loads(content)

print(f"Records in day 1: {len(records)}")
print(f"Keys: {list(records[0].keys())}")
print(json.dumps(records[0], indent=2))

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, trim, upper, length, when

spark = SparkSession.builder.appName("silver-complaints").getOrCreate()

# ── Read all complaint files from S3 ─────────────────────────────────────────
response = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix="complaints/organized/")
keys = [obj["Key"] for obj in response.get("Contents", [])]
print(f"Total daily files: {len(keys)}")

records = []
for key in keys:
    obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)
    content = gzip.decompress(obj["Body"].read()).decode("utf-8")
    records.extend(json.loads(content))

pdf = pd.DataFrame(records)
print(f"Total rows: {len(pdf)}")
print(pdf.dtypes)
pdf.head(3)

# COMMAND ----------

# ── Clean with Spark ──────────────────────────────────────────────────────────
df = spark.createDataFrame(pdf)

df_silver = (
    df
    .withColumn("unique_key", col("unique_key").cast("long"))
    .withColumn("created_date", to_timestamp(col("created_date"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
    .withColumn("complaint_type", trim(col("complaint_type")))
    .withColumn("descriptor", trim(col("descriptor")))
    .withColumn("incident_zip", trim(col("incident_zip")))
    .withColumn("borough", trim(upper(col("borough"))))
    .withColumn("ingestion_timestamp_utc", col("ingestion_timestamp_utc").cast("timestamp"))
    # Drop rows with no borough or complaint type
    .filter(col("borough").isNotNull() & (col("borough") != ""))
    .filter(col("complaint_type").isNotNull() & (col("complaint_type") != ""))
    # Add date column for easier partitioning
    .withColumn("complaint_date", col("created_date").cast("date"))
)

print(f"Silver rows: {df_silver.count()}")
df_silver.printSchema()
df_silver.show(5)

# COMMAND ----------

# ── Write Silver complaints to S3 as Parquet ──────────────────────────────────
pdf_silver = pd.DataFrame([row.asDict() for row in df_silver.collect()])

buffer = BytesIO()
pdf_silver.to_parquet(buffer, index=False, engine="pyarrow")
buffer.seek(0)

s3.put_object(
    Bucket=SILVER_BUCKET,
    Key="complaints/complaints_silver.parquet",
    Body=buffer.getvalue()
)
print(f"Written {len(pdf_silver)} rows to s3://retail-intel-silver/complaints/complaints_silver.parquet")

# ── Validate ──────────────────────────────────────────────────────────────────
obj = s3.get_object(Bucket=SILVER_BUCKET, Key="complaints/complaints_silver.parquet")
pdf_check = pd.read_parquet(BytesIO(obj["Body"].read()))
print(f"Validation — Rows: {len(pdf_check)}")
print(f"Validation — Date range: {pdf_check['complaint_date'].min()} to {pdf_check['complaint_date'].max()}")
print(f"Validation — Boroughs: {sorted(pdf_check['borough'].unique())}")
print(f"Validation — Top 5 complaint types:\n{pdf_check['complaint_type'].value_counts().head()}")