-- ── Athena Validation Queries ──────────────────────────────────────────────
-- Run after Glue crawlers complete to validate Bronze layer data quality.
-- Set Athena output location first:
-- s3://retail-intel-bronze/athena-results/

-- ── TAXI ──────────────────────────────────────────────────────────────────
-- Row count per month
SELECT year, month, COUNT(*) as row_count
FROM retail_intel_bronze.taxi_yellow_tripdata_2024
GROUP BY year, month
ORDER BY year, month;

-- Date range check
SELECT
    MIN(tpep_pickup_datetime) AS earliest_pickup,
    MAX(tpep_pickup_datetime) AS latest_pickup
FROM retail_intel_bronze.taxi_yellow_tripdata_2024;

-- Null check on critical fields
SELECT
    SUM(CASE WHEN trip_distance IS NULL THEN 1 ELSE 0 END) AS null_distance,
    SUM(CASE WHEN fare_amount IS NULL THEN 1 ELSE 0 END)   AS null_fare,
    SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END)  AS null_total
FROM retail_intel_bronze.taxi_yellow_tripdata_2024;

-- ── WEATHER ───────────────────────────────────────────────────────────────
-- Row count (should be one row per day)
SELECT COUNT(*) AS total_days
FROM retail_intel_bronze.weather_organized;

-- Date range check
SELECT
    MIN(date) AS earliest_date,
    MAX(date) AS latest_date
FROM retail_intel_bronze.weather_organized;

-- ── ZONES ─────────────────────────────────────────────────────────────────
-- Total zones (should be 265)
SELECT COUNT(*) AS total_zones
FROM retail_intel_bronze.zones_taxi_zone_lookup;

-- Distinct boroughs
SELECT borough, COUNT(*) AS zone_count
FROM retail_intel_bronze.zones_taxi_zone_lookup
GROUP BY borough
ORDER BY zone_count DESC;

-- ── COMPLAINTS ────────────────────────────────────────────────────────────
-- Row count per day
SELECT year, month, day, COUNT(*) AS complaint_count
FROM retail_intel_bronze.complaints_organized
GROUP BY year, month, day
ORDER BY year, month, day;

-- Total records (should be ~287,000)
SELECT COUNT(*) AS total_complaints
FROM retail_intel_bronze.complaints_organized;

-- Top complaint types
SELECT complaint_type, COUNT(*) AS count
FROM retail_intel_bronze.complaints_organized
GROUP BY complaint_type
ORDER BY count DESC
LIMIT 10;

-- ── CLICKSTREAM ───────────────────────────────────────────────────────────
-- Events per hour
SELECT year, month, day, hour, COUNT(*) AS event_count
FROM retail_intel_bronze.clickstream_events
GROUP BY year, month, day, hour
ORDER BY year, month, day, hour;

-- Event type distribution
SELECT event_type, COUNT(*) AS count
FROM retail_intel_bronze.clickstream_events
GROUP BY event_type
ORDER BY count DESC;