-- One-time Phase 5 (Week 6) setup, run once in a Snowflake worksheet under
-- ACCOUNTADMIN (docs/PLAN.md: "sign up for Snowflake, XS warehouse,
-- AUTO_SUSPEND=60"). Not applied via Terraform/dbt -- this is account-level
-- setup that happens exactly once, the same way the account signup itself
-- was a manual step.
--
-- Creates a dedicated TRANSFORMER role instead of running the pipeline as
-- ACCOUNTADMIN day to day -- least privilege, and it's what profiles.yml's
-- snowflake target already defaults SNOWFLAKE_ROLE to.

use role accountadmin;

create warehouse if not exists hindcast_xs
  warehouse_size = 'XSMALL'
  auto_suspend = 60
  auto_resume = true
  initially_suspended = true
  comment = 'docs/PLAN.md Phase 5 Week 6 -- sized for ~140k rows/month, never needs to scale up.';

create role if not exists transformer;
grant usage on warehouse hindcast_xs to role transformer;

create database if not exists hindcast
  comment = 'Marts + RAW landing tables loaded via external stage COPY INTO (docs/PLAN.md Phase 5 Week 6).';

-- RAW: COPY INTO lands the 3 silver tables here (mirrors the Databricks
-- side-track's workspace.hindcast_silver, same Parquet snapshots produced
-- by export_silver_snapshot.py). Not dbt-managed -- staging models read it
-- directly via silver_delta_scan(), so dbt never needs write access here.
create schema if not exists hindcast.raw
  comment = 'Landing zone for load_into_snowflake.py COPY INTO. Not a dbt-managed schema.';

-- MARTS (and MARTS_core / MARTS_facts, per dbt_project.yml's +schema config)
-- are created automatically by `dbt build` the first time it runs, as long
-- as TRANSFORMER can create schemas in this database.
grant usage, create schema on database hindcast to role transformer;
grant all on schema hindcast.raw to role transformer;

-- Grant the role to the login already used for the account (NAHIANALINDO),
-- rather than creating a second dedicated service user -- this is a
-- single-operator student project, not a team, so the extra user/key-pair
-- management wouldn't buy anything real.
grant role transformer to user nahianalindo;

-- Verify: should show HINDCAST_XS / TRANSFORMER / HINDCAST / RAW, not the
-- ACCOUNTADMIN defaults -- confirms the role switch actually took, since
-- SQL after this point runs as TRANSFORMER, not ACCOUNTADMIN.
use role transformer;
use warehouse hindcast_xs;
use database hindcast;
use schema raw;
select current_role(), current_warehouse(), current_database(), current_schema();
