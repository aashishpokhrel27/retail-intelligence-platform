# Databricks notebook source
import boto3
import pandas as pd
import json
import gzip
import os
from io import BytesIO

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
    Prefix="clickstream/",
    MaxKeys=5
)
for obj in response.get("Contents", []):
    print(obj["Key"])

# COMMAND ----------

key = "clickstream/year=2026/month=08/day=20/hour=04/events_20260820_042418.json.gz"
obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)
content = gzip.decompress(obj["Body"].read()).decode("utf-8")
records = json.loads(content)

print(f"Records in file: {len(records)}")
print(f"Keys: {list(records[0].keys())}")
print(json.dumps(records[0], indent=2))

# COMMAND ----------

# ── Read all clickstream files ────────────────────────────────────────────────
response = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix="clickstream/")
keys = [obj["Key"] for obj in response.get("Contents", [])]
print(f"Total clickstream files: {len(keys)}")

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

# ── Clean ─────────────────────────────────────────────────────────────────────
pdf["event_timestamp_utc"] = pd.to_datetime(pdf["event_timestamp_utc"])
pdf["business_date"] = pd.to_datetime(pdf["business_date"]).dt.date
pdf["price"] = pdf["price"].astype("float64")
pdf["quantity"] = pdf["quantity"].astype("int32")
pdf["revenue"] = pdf["revenue"].astype("float64")

# Validate event types
valid_event_types = ["page_view", "add_to_cart", "purchase", "remove_from_cart"]
pdf = pdf[pdf["event_type"].isin(valid_event_types)]

# Add derived columns
pdf["event_hour"] = pdf["event_timestamp_utc"].dt.hour
pdf["event_date"] = pdf["event_timestamp_utc"].dt.date

print(f"Clean rows: {len(pdf):,}")
print(f"Event types: {pdf['event_type'].value_counts().to_dict()}")
print(f"Date range: {pdf['event_date'].min()} to {pdf['event_date'].max()}")

# ── Write partitioned by day ───────────────────────────────────────────────────
for date, pdf_day in pdf.groupby("event_date"):
    date_str = str(date)
    year = date_str[:4]
    month = date_str[5:7]
    day = date_str[8:10]

    buffer = BytesIO()
    pdf_day.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    s3_key = f"clickstream/year={year}/month={month}/day={day}/clickstream_events.parquet"
    s3.put_object(Bucket=SILVER_BUCKET, Key=s3_key, Body=buffer.getvalue())
    print(f"Written {len(pdf_day):,} rows → s3://retail-intel-silver/{s3_key}")

print("\nSilver clickstream complete")