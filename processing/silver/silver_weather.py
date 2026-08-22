# Databricks notebook source
import boto3
import pandas as pd
from io import BytesIO
import json
import os

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

# List what's in the organized weather folder
response = s3.list_objects_v2(
    Bucket=BRONZE_BUCKET,
    Prefix="weather/organized/",
    MaxKeys=5
)

for obj in response.get("Contents", []):
    print(obj["Key"])

# COMMAND ----------

obj = s3.get_object(Bucket=BRONZE_BUCKET, Key="weather/organized/year=2024/month=01/day=01/weather_2024-01-01.json")
content = json.loads(obj["Body"].read().decode("utf-8"))
print(json.dumps(content, indent=2))

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when

spark = SparkSession.builder.appName("silver-complaints").getOrCreate()


# ── Read all organized weather files from S3 ──────────────────────────────────
response = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix="weather/organized/")
keys = [obj["Key"] for obj in response.get("Contents", [])]
print(f"Total daily files: {len(keys)}")

# Read all into a list of dicts
records = []
for key in keys:
    obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)
    records.append(json.loads(obj["Body"].read().decode("utf-8")))

pdf = pd.DataFrame(records)
print(f"Total rows: {len(pdf)}")
print(pdf.dtypes)
pdf.head()

# COMMAND ----------

# ── Clean with Spark ──────────────────────────────────────────────────────────
from pyspark.sql.functions import col, to_date, when

df = spark.createDataFrame(pdf)

df_silver = (
    df
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    .withColumn("latitude", col("latitude").cast("double"))
    .withColumn("longitude", col("longitude").cast("double"))
    .withColumn("temperature_max_c", col("temperature_max_c").cast("double"))
    .withColumn("temperature_min_c", col("temperature_min_c").cast("double"))
    .withColumn("precipitation_mm", col("precipitation_mm").cast("double"))
    .withColumn("windspeed_max_kmh", col("windspeed_max_kmh").cast("double"))
    .withColumn("weathercode", col("weathercode").cast("integer"))
    .withColumn("ingestion_timestamp_utc", col("ingestion_timestamp_utc").cast("timestamp"))
    .withColumn("weather_description",
        when(col("weathercode") == 0, "Clear sky")
        .when(col("weathercode").isin(1, 2, 3), "Partly cloudy")
        .when(col("weathercode").isin(51, 53, 55), "Drizzle")
        .when(col("weathercode").isin(61, 63, 65), "Rain")
        .when(col("weathercode").isin(71, 73, 75), "Snow")
        .when(col("weathercode").isin(80, 82, 82), "Rain showers")
        .when(col("weathercode").isin(95, 96, 99), "Thunderstorm")
        .otherwise("Other")
    )
    .drop("timezone") # constant value, not needed in Silver
)

print(f"Silver rows: {df_silver.count()}")
df_silver.printSchema()
df_silver.show(5)


# COMMAND ----------

# ── Write Silver weather to S3 as Parquet ────────────────────────────────────
pdf_silver = pd.DataFrame([row.asDict() for row in df_silver.collect()])

buffer = BytesIO()
pdf_silver.to_parquet(buffer, index=False, engine="pyarrow")
buffer.seek(0)

s3.put_object(
    Bucket=SILVER_BUCKET,
    Key="weather/weather_daily.parquet",
    Body=buffer.getvalue()
)
print(f"Written {len(pdf_silver)} rows to s3://retail-intel-silver/weather/weather_daily.parquet")

# ── Validate ────────────────────────────────────
obj = s3.get_object(Bucket=SILVER_BUCKET, Key="weather/weather_daily.parquet")
pdf_check = pd.read_parquet(BytesIO(obj["Body"].read()))
print(f"Validation -- Rows: {len(pdf_check)}")
print(f"Validation -- Date range: {pdf_check['date'].min()} to {pdf_check['date'].max()}")
print(f"Validation -- Weather types: {pdf_check["weather_description"].value_counts().to_dict()}")