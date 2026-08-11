"""dbx-export/<table> (plain Parquet, written by export_silver_snapshot.py)
-> Databricks workspace.hindcast_silver.<table> (managed Delta), via COPY
INTO with an inline, short-lived SAS credential.

Why COPY INTO with an inline SAS token, not a permanent external location:
Databricks Free Edition only offers serverless SQL warehouses -- no classic
clusters, no custom JAR/connector installs, and no configurable external
storage locations (confirmed live: CREATE TABLE against a raw
`abfss://...` path with the hadoop-azure connector isn't available in this
tier). COPY INTO's inline `WITH (CREDENTIAL (AZURE_SAS_TOKEN = ...))`
clause is the one documented mechanism that doesn't need any of that --
verified live against the actual serverless warehouse here despite a
documented (but apparently stale, or specific to a different compute path)
issue suggesting SAS auth fails on serverless.

Credentials: AZURE_STORAGE_ACCOUNT_KEY (same env var the rest of this repo
uses), DATABRICKS_HOST, DATABRICKS_TOKEN. Never hardcoded, never committed.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone

import requests
from azure.storage.blob import ContainerSasPermissions, generate_container_sas

from datadog_metrics import submit_gauge

STORAGE_ACCOUNT = "sthindcastjlbpfz"
EXPORT_CONTAINER = "dbx-export"
DATABRICKS_SCHEMA = "workspace.hindcast_silver"
WAREHOUSE_ID = "3e7e1236bdf67349"

# (table_name, DDL column list) -- must match export_silver_snapshot.py's
# source tables' actual silver schemas exactly (COPY INTO does not infer
# or evolve schema here).
TABLES: dict[str, str] = {
    "obs_weather": """
        location_id STRING, run_id STRING, requested_at TIMESTAMP, source_dt TIMESTAMP,
        temp_c DOUBLE, feels_like_c DOUBLE, temp_min_c DOUBLE, temp_max_c DOUBLE,
        pressure_hpa BIGINT, humidity_pct BIGINT, wind_speed_ms DOUBLE, wind_deg BIGINT,
        wind_gust_ms DOUBLE, clouds_pct BIGINT, visibility_m BIGINT, weather_code BIGINT,
        weather_main STRING, weather_description STRING, weather_icon STRING,
        payload_sha256 STRING
    """,
    "obs_air_quality": """
        location_id STRING, run_id STRING, requested_at TIMESTAMP, source_dt TIMESTAMP,
        aqi INT, co DOUBLE, no DOUBLE, no2 DOUBLE, o3 DOUBLE, so2 DOUBLE,
        pm2_5 DOUBLE, pm10 DOUBLE, nh3 DOUBLE, payload_sha256 STRING
    """,
    "fct_forecast_issue_raw": """
        location_id STRING, run_id STRING, issued_at TIMESTAMP, payload_sha256 STRING,
        valid_ts TIMESTAMP, temp_c DOUBLE, feels_like_c DOUBLE, temp_min_c DOUBLE,
        temp_max_c DOUBLE, pressure_hpa BIGINT, humidity_pct BIGINT, pop DOUBLE,
        wind_speed_ms DOUBLE, wind_deg BIGINT, weather_code BIGINT, weather_main STRING,
        weather_description STRING, weather_icon STRING, lead_time_minutes DOUBLE
    """,
}


def generate_sas() -> str:
    account_key = os.environ["AZURE_STORAGE_ACCOUNT_KEY"]
    expiry = datetime.now(timezone.utc) + timedelta(hours=3)
    return generate_container_sas(
        account_name=STORAGE_ACCOUNT,
        container_name=EXPORT_CONTAINER,
        account_key=account_key,
        permission=ContainerSasPermissions(read=True, list=True),
        expiry=expiry,
    )


def run_statement(host: str, token: str, sql: str) -> dict:
    resp = requests.post(
        f"{host}/api/2.0/sql/statements",
        headers={"Authorization": f"Bearer {token}"},
        json={"warehouse_id": WAREHOUSE_ID, "wait_timeout": "50s", "statement": sql},
        timeout=60,
    )
    resp.raise_for_status()
    body = resp.json()
    if body.get("status", {}).get("state") == "FAILED":
        raise RuntimeError(f"Statement failed: {body['status']['error']}")
    return body


def main() -> None:
    started = time.monotonic()
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    token = os.environ["DATABRICKS_TOKEN"]
    sas = generate_sas()

    run_statement(host, token, f"CREATE SCHEMA IF NOT EXISTS {DATABRICKS_SCHEMA}")

    for table, ddl in TABLES.items():
        target = f"{DATABRICKS_SCHEMA}.{table}"
        source = f"abfss://{EXPORT_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{table}"

        run_statement(
            host, token, f"CREATE TABLE IF NOT EXISTS {target} ({ddl}) USING DELTA"
        )
        # TRUNCATE first: COPY INTO tracks which source files it has already
        # loaded and skips them on re-run (its idempotency guarantee) --
        # exactly wrong for this use case, where export_silver_snapshot.py
        # overwrites the *same* file path with fresh data every run. A
        # truncate-then-reload makes this script safely re-runnable.
        run_statement(host, token, f"TRUNCATE TABLE {target}")
        result = run_statement(
            host,
            token,
            f"COPY INTO {target} FROM '{source}' "
            f"WITH (CREDENTIAL (AZURE_SAS_TOKEN = '{sas}')) "
            f"FILEFORMAT = PARQUET COPY_OPTIONS ('force' = 'true')",
        )
        rows = result["result"]["data_array"][0]
        print(f"[{table}] copy into {target}: {rows[1]} rows inserted")
        # docs/PLAN.md Phase 7: hindcast.databricks.copy_into_rowcount
        submit_gauge("hindcast.databricks.copy_into_rowcount", float(rows[1]), tags=[f"table:{table}"])

    # docs/PLAN.md Phase 7: hindcast.databricks.sync_duration_s -- this
    # script's own runtime (SAS generation + all 3 tables' CREATE/TRUNCATE/
    # COPY INTO), not the whole databricks_sync DAG (which also runs the
    # Spark export before this and a dbt build after, in two other
    # containers -- there's no single process that sees all three).
    submit_gauge("hindcast.databricks.sync_duration_s", time.monotonic() - started)


if __name__ == "__main__":
    main()
