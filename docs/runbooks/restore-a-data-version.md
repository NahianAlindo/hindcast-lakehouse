# Restore a data version

Reproduces exactly what the marts looked like at a given weekly snapshot
(`data-vYYYY.WW`), using only what's in git + the DVC remote — no live
Snowflake credentials, no running VM, no Airflow. This is the primary
pipeline's (Snowflake/DuckDB) restore path; see the bottom of this doc for
the Databricks side-track, which doesn't use DVC at all.

## Why this works with nothing else running

`data/exports/*.parquet` is a DuckDB-sourced snapshot (not Snowflake) —
see `docs/PLAN.md` Phase 6 for why: DuckDB is the one target that doesn't
depend on a live warehouse, a trial that hasn't expired, or the VM being up.
Restoring a tag only needs three things: this git repo, the DVC remote
(Azure Blob, `dvc` container), and a local Python environment.

## Procedure

```bash
# 1. Pick a tag. List them if you don't already know the one you want:
git tag -l 'data-v*'

# 2. Check out that snapshot's commit.
git checkout data-v2026.34

# 3. Pull the actual Parquet data the tag's dvc.lock points at. Needs
#    AZURE_STORAGE_CONNECTION_STRING set (Key Vault secret
#    `storage-connection-string`, or ask whoever has it — this one
#    credential is the only thing not fully self-contained in the repo).
export AZURE_STORAGE_CONNECTION_STRING="<value>"
uv run dvc pull

# 4. Rebuild the star schema from the pulled Parquet, entirely locally.
#    This does NOT re-run dbt against ADLS/Snowflake -- data/exports/*.parquet
#    IS the star schema at that snapshot; this step just loads it into a
#    fresh local DuckDB file so it's queryable the normal way.
uv run python warehouse/dvc/load_exports_into_duckdb.py

# 5. Verify: row counts / spot-check values should match what's recorded
#    in that snapshot's commit message and dvc.lock. Tables land in
#    main_core / main_facts, mirroring dbt_project.yml's own schema split.
uv run duckdb hindcast_restored.duckdb -c "select count(*) from main_facts.fct_forecast_slot"
```

## What's actually reproducible here, and what isn't

- **Reproducible exactly:** every mart table's contents at snapshot time —
  `dim_date`, `dim_location`, `fct_forecast_slot`, all of it, byte-for-byte
  via DVC's content-addressed storage.
- **Not reproducible, and not meant to be:** re-deriving that same output
  from bronze from scratch. Bronze itself isn't replayable (OpenWeatherMap
  doesn't sell you their own past forecasts — this project's core domain
  constraint, `docs/PLAN.md` §2). DVC versions the *output* of a point in
  time, not a time machine for the input.

## Databricks side-track: different mechanism, no DVC involved

The Databricks marts were never DVC-tracked (`docs/PLAN.md` Phase 6 explains
why: Delta's own transaction log is a strictly better fit there — it doesn't
expire like the Snowflake trial, and that side-track isn't a source of truth
anything else restores from). Restoring a past version of *that* side is one
query, no `dvc pull`, no git checkout:

```sql
SELECT * FROM workspace.hindcast_silver.obs_weather VERSION AS OF 12;
-- or:
SELECT * FROM workspace.hindcast_silver.obs_weather
  TIMESTAMP AS OF '2026-08-01 00:00:00';
```

Run `DESCRIBE HISTORY workspace.hindcast_silver.obs_weather` first to see
available versions/timestamps.
