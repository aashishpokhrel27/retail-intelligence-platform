# Infrastructure Setup

## Snowflake

Run `snowflake_setup.sql` as ACCOUNTADMIN to set up:
- Warehouse: ANALYTICS_WH (X-Small, auto-suspends 60s)
- Databases: BRONZE_DB, SILVER_DB, GOLD_DB
- Schemas per medallion layer
- Roles: PIPELINE_ROLE, REPORTING_ROLE
- Users: PIPELINE_USER, REPORTING_USER

## AWS

S3 buckets created via AWS CLI:
- retail-intel-bronze (versioning enabled)
- retail-intel-silver
- retail-intel-gold
- retail-intel-archive

## Design Decisions

See `docs/design_decisions.md` for architectural choices and known limitations.