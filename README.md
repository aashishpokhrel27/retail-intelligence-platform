# Retail Intelligence Platform

A production-grade, multi-source data platform ingesting batch and streaming data from 5 sources, processing through a medallion architecture (Bronze → Silver → Gold), and serving analytics via Snowflake and Apache Superset.

---

## Architecture

```
Sources
  NYC Taxi Parquet (batch)
  Open-Meteo Weather API (batch)
  NYC Zone Lookup CSV (static)
  NYC 311 Complaints (batch)
  Synthetic Clickstream (streaming)
        │
        ▼
AWS S3 Bronze (raw, immutable, partitioned)
        │
        ▼
Databricks + PySpark (Silver — cleaned, enriched, Delta Lake)
        │
        ▼
Snowflake (Gold — dbt marts, PIPELINE_ROLE / REPORTING_ROLE)
        │
        ▼
Apache Superset (dashboards — batch + live metrics)

EC2 t2.micro → Clickstream Generator → S3 Bronze (every 30s)
```

---

## Data Sources

| Source | Type | Ingestion | Format |
|---|---|---|---|
| NYC Taxi Trip Data | Batch monthly | File download | Parquet |
| Open-Meteo Weather | Batch daily | REST API (free, no key) | JSON |
| NYC Zone Lookup | Static reference | One-time download | CSV |
| NYC 311 Complaints | Batch daily | Paginated Socrata API | JSON |
| Synthetic Clickstream | Streaming 30s | Python generator on EC2 | JSON |

---

## Tech Stack

| Category | Technologies |
|---|---|
| Ingestion | Python, boto3, AWS S3, AWS Glue, AWS Athena, AWS EC2 |
| Processing | Databricks, PySpark, Delta Lake |
| Warehouse | Snowflake (Bronze DB, Silver DB, Gold DB) |
| Transformation | dbt Core |
| Orchestration | Apache Airflow |
| Data Quality | Great Expectations |
| Visualization | Apache Superset |
| Version Control | Git, GitHub (multi-branch workflow) |

---

## Project Structure

```
retail-intelligence-platform/
├── ingestion/              # Bronze ingestion scripts per source
│   ├── taxi/
│   ├── weather/
│   ├── zones/
│   └── complaints/
├── processing/             # PySpark Silver and Gold transformations
│   ├── silver/
│   └── gold/
├── streaming/              # Clickstream event generator
│   └── clickstream/
├── orchestration/          # Airflow DAGs
│   └── dags/
├── dbt/                    # dbt models
│   └── models/
│       ├── staging/
│       └── marts/
├── quality/                # Great Expectations suites
│   └── expectations/
├── dashboard/              # Superset config and screenshots
├── infrastructure/         # AWS and Snowflake setup guides and SQL
├── config/                 # Centralized settings and constants
├── tests/                  # Unit tests
└── docs/                   # Architecture diagrams and study notes
```

---

## Setup

### Prerequisites

- Python 3.12+
- AWS CLI configured (`aws configure`)
- Snowflake trial account
- Databricks Community Edition account
- Git

### Install dependencies

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### Environment setup

```bash
cp .env.example .env
# Fill in your actual credentials in .env
```

### Run ingestion (Bronze layer)

```bash
python ingestion/taxi/ingest_taxi.py
python ingestion/weather/ingest_weather.py
python ingestion/zones/ingest_zones.py
python ingestion/complaints/ingest_complaints.py
```

### Run streaming generator (local test)

```bash
python streaming/clickstream/clickstream_generator.py
```

For EC2 deployment see [infrastructure/README.md](infrastructure/README.md).

---

## Infrastructure

See [infrastructure/README.md](infrastructure/README.md) for:
- AWS S3 bucket creation
- Snowflake setup SQL (warehouses, databases, schemas, roles, users)
- EC2 instance setup and clickstream generator deployment

---

## Phase Status

| Phase | Description | Status |
|---|---|---|
| 1 — Bronze | Multi-source ingestion to S3 | ✅ Complete |
| 2 — Silver | PySpark cleaning and enrichment | 🔄 In Progress |
| 3 — Gold | dbt marts in Snowflake | ⏳ Pending |
| 4 — Orchestration | Airflow DAGs | ⏳ Pending |
| 5 — Quality | Great Expectations | ⏳ Pending |
| 6 — Dashboard | Superset + ML forecasting | ⏳ Pending |

---

## Design Decisions

Key architectural choices and known limitations are documented in [docs/design_decisions.md](docs/design_decisions.md).

---

## Git Workflow

```
main          ← production only, tagged releases
  └── develop ← integration branch
        ├── feature/silver-processing
        ├── feature/glue-athena
        ├── feature/project-standards
        └── feature/*
```

Commits follow [Conventional Commits](https://www.conventionalcommits.org/):
`feat:`, `fix:`, `refactor:`, `docs:`, `test:`