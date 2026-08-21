# Design Decisions

Architectural choices made during development of the Retail Intelligence Platform,
including the reasoning behind each decision and known trade-offs.
This document is updated as new decisions are made.

---

## DD-001: Four Separate S3 Buckets Instead of One

**Decision:** Use four separate S3 buckets (`retail-intel-bronze`, `retail-intel-silver`,
`retail-intel-gold`, `retail-intel-archive`) instead of a single bucket with layer prefixes.

**Industry standard:** Single bucket with prefixes (`my-platform/bronze/`, `/silver/`, `/gold/`).

**Why we deviated:** Ingestion scripts were already built and tested against individual bucket
names before this decision was evaluated. Restructuring would require re-running all ingestion
jobs with no meaningful benefit at this project scale.

**Trade-off:** Four buckets instead of one means four IAM policy targets, four S3 lifecycle
rules, and slightly more complex cross-bucket operations. Acceptable for a single-project setup.

**When to fix:** If this project scales to multiple pipelines or teams, consolidate to a
single bucket with prefix-based separation.

---

## DD-002: Lake-First Architecture — BRONZE_DB Removed from Snowflake

**Decision:** Raw data (Bronze layer) remains in S3, queryable via Glue Catalog and Athena.
BRONZE_DB was not created in Snowflake. Only SILVER_DB and GOLD_DB exist in Snowflake.

**Industry standard:** Two patterns exist — lake-first (S3 + Glue + Athena for Bronze)
and warehouse-first (everything in Snowflake). Both are used in production.

**Why lake-first:** Databricks reads directly from S3 for Silver processing — loading Bronze
into Snowflake first would be a redundant copy. Snowflake credits are expensive at scale;
keeping raw data in S3 and using Athena (pay-per-query) for Bronze validation is significantly
cheaper. Athena at $5/TB scanned vs Snowflake at credit consumption per second.

**Trade-off:** Two query interfaces (Athena for Bronze, Snowflake for Silver/Gold). Engineers
need to know which system holds which layer.

**When to revisit:** If the team standardizes entirely on Snowflake and drops Databricks,
warehouse-first becomes simpler. Not applicable for this stack.

---

## DD-003: Headlines Source Dropped

**Decision:** NewsAPI and GDELT were evaluated as streaming unstructured text sources.
Both were dropped. NYC 311 Complaints is the sole unstructured text source.

**Why NewsAPI dropped:** Free tier limits to articles published in the last 30 days.
January 2024 historical data unavailable — would not align with taxi and weather data.

**Why GDELT dropped:** GDELT DOC API officially supports only the last 3 months.
January 2024 data was not reliably available via the free API tier.

**Impact:** Project has one unstructured source (311 Complaints) instead of two.
287,000 complaint records for January 2024 is sufficient for NLP pipeline demonstration.

---

## DD-004: S3 Micro-Batch Instead of Kafka for Streaming (Phase 1)

**Decision:** Clickstream generator writes JSON batches to S3 every 30 seconds.
Databricks Structured Streaming reads these files as a micro-batch stream.
Apache Kafka is not used in Phase 1.

**Industry standard:** Kafka is the industry standard for real-time streaming.

**Why deferred:** Kafka requires a dedicated broker server running 24/7 — additional EC2
cost before the pipeline is proven end-to-end. S3 micro-batch provides equivalent learning
value at zero additional cost and achieves acceptable latency (30 seconds) for retail analytics.

**Architecture decision:** The Databricks Structured Streaming consumer code is written to
be source-agnostic. Swapping S3 micro-batch for Kafka requires only changing the source
configuration — consumer logic stays identical.

**Planned:** Kafka added in Phase 2 alongside Kafka Connect for direct Snowflake sinking.

---

## DD-005: Python boto3 for Infrastructure Provisioning Instead of Terraform

**Decision:** AWS Glue crawlers, S3 buckets, and IAM roles provisioned via Python boto3
scripts and AWS Console. Terraform not used in Phase 1.

**Industry standard:** Terraform (or AWS CDK) for infrastructure as code — reproducible,
version-controlled, team-safe provisioning.

**Why deferred:** Terraform adds learning overhead before the data pipeline itself is proven.
boto3 scripts achieve the same result for a single developer and are already familiar from
ingestion work.

**Planned:** Terraform introduced in Phase 4 to manage all AWS infrastructure as code.
boto3 provisioning scripts retained as reference but superseded by Terraform.

---

## DD-006: Separate IAM Role Per AWS Service

**Decision:** Each AWS service has its own dedicated IAM role:
`retail-intel-ec2-role` (EC2), `retail-intel-glue-role` (Glue).

**Industry standard:** This IS the industry standard — principle of least privilege.

**Why:** Each service only has permissions it needs. If one role is compromised, the blast
radius is contained to that service only. An attacker with the EC2 role can only write to S3 —
they cannot access Glue, Athena, or any other service.

---

## DD-007: Python 3.9 on EC2 (Known Issue)

**Issue:** Amazon Linux 2023 defaults to Python 3.9. boto3 issued a deprecation warning:
Python 3.9 support ends April 29, 2026.

**Current status:** Generator runs correctly. Warning is non-blocking.

**Planned fix:** Upgrade to Python 3.10+ on EC2 in Phase 2 before the deprecation deadline.

---

## DD-008: Snowflake Roles — PIPELINE_ROLE and REPORTING_ROLE

**Decision:** Two service roles created in Snowflake rather than using ACCOUNTADMIN for
all operations.

- `PIPELINE_ROLE` — full read/write on SILVER_DB and GOLD_DB. Used by Databricks and Airflow.
- `REPORTING_ROLE` — read-only on GOLD_DB.MARTS and GOLD_DB.LIVE. Used by Superset.

**Why:** Principle of least privilege. Superset dashboard cannot accidentally modify Silver
or Gold data. Databricks pipeline cannot drop databases. Each component has exactly the
permissions it needs.