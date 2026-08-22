# Databricks notebook source
# MAGIC %pip install boto3

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# ── Cell 1: Imports and AWS credentials ──────────────────────────────────────
import boto3
import pandas as pd
from io import StringIO
from pyspark.sql.functions import col, trim, upper

# Fill in your credentials from .env
import os
from dotenv import load_dotenv

load_dotenv()
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

BRONZE_BUCKET  = "retail-intel-bronze"
SILVER_BUCKET  = "retail-intel-silver"

# ── S3 client ─────────────────────────────────────────────────────────────────
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION
)

print("boto3 client ready")

# COMMAND ----------

# ── Cell 2: Read zones CSV from S3 Bronze ────────────────────────────────────
obj = s3.get_object(Bucket=BRONZE_BUCKET, Key="zones/taxi_zone_lookup.csv")
csv_content = obj["Body"].read().decode("utf-8")

# Convert to pandas first, then to Spark
pdf = pd.read_csv(StringIO(csv_content))

print(f"Row count: {len(pdf)}")
print(f"Columns: {list(pdf.columns)}")
pdf.head()

# COMMAND ----------

# ── Cell 3: Clean and enrich zones ───────────────────────────────────────────
# Convert pandas to Spark DataFrame
df = spark.createDataFrame(pdf)

# Clean: cast types, trim whitespace, standardize borough names
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
df_silver.show(10)

# COMMAND ----------

# ── Cell 4: Write Silver zones to S3 as Parquet via boto3 ────────────────────
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq

# Convert to pandas — collect to driver first
pdf_silver = df_silver.collect()
pdf_silver = pd.DataFrame([row.asDict() for row in pdf_silver])

# Write parquet to buffer
buffer = BytesIO()
pdf_silver.to_parquet(buffer, index=False, engine="pyarrow")
buffer.seek(0)

# Upload to S3 Silver
s3.put_object(
    Bucket=SILVER_BUCKET,
    Key="zones/taxi_zone_lookup.parquet",
    Body=buffer.getvalue()
)

print(f"Written {len(pdf_silver)} rows to s3://retail-intel-silver/zones/taxi_zone_lookup.parquet")

# COMMAND ----------

# ── Cell 5: Validate Silver zones ────────────────────────────────────────────
# Read back from S3 to confirm write was successful
obj = s3.get_object(Bucket=SILVER_BUCKET, Key="zones/taxi_zone_lookup.parquet")
pdf_check = pd.read_parquet(BytesIO(obj["Body"].read()))

print(f"Row count: {len(pdf_check)}")
print(f"Columns: {list(pdf_check.columns)}")
print(f"Boroughs: {sorted(pdf_check['borough'].dropna().unique())}")
pdf_check.head()