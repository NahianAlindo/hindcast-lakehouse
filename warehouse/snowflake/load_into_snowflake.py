"""exports/<table> (plain Parquet, written by export_silver_snapshot.py) ->
Snowflake HINDCAST.RAW.<table>, via an external stage + COPY INTO with an
inline, short-lived SAS credential.

Mirrors warehouse/databricks/load_into_databricks.py's shape deliberately --
same export-then-COPY-INTO pattern, same reason (Delta's overwrite/merge
writes leave superseded parquet files on disk until VACUUM, so COPY INTO
must read a Spark-exported clean snapshot, never the Delta directory
itself). What's actually different: Snowflake authenticates the stage with
a CREATE STAGE object instead of an inline WITH (CREDENTIAL (...)) clause on
the COPY statement, and needs MATCH_BY_COLUMN_NAME since plain
FILEFORMAT=PARQUET COPY INTO loads Parquet rows into a single VARIANT column
by default -- Databricks' COPY INTO maps columns positionally without that
option, Snowflake's doesn't.

Credentials: AZURE_STORAGE_ACCOUNT_KEY (same env var the rest of this repo
uses), SNOWFLAKE_ACCOUNT/USER/PASSWORD/ROLE/WAREHOUSE/DATABASE (same names
warehouse/dbt/hindcast/profiles.yml's snowflake target reads). Never
hardcoded, never committed.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import snowflake.connector
from azure.storage.blob import ContainerSasPermissions, generate_container_sas

STORAGE_ACCOUNT = "sthindcastjlbpfz"
EXPORT_CONTAINER = os.environ.get("EXPORT_CONTAINER", "exports")
RAW_SCHEMA = "RAW"

# Identical DDL to load_into_databricks.py's TABLES dict -- STRING/BIGINT/
# DOUBLE/TIMESTAMP are all native Snowflake types too (STRING is a built-in
# VARCHAR alias there, not a Databricks-only spelling), so the same export
# schema loads unmodified on both engines.
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


def connect() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "HINDCAST_XS"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "HINDCAST"),
        schema=RAW_SCHEMA,
    )


def main() -> None:
    sas = generate_sas()
    conn = connect()
    cur = conn.cursor()
    try:
        cur.execute(
            f"CREATE OR REPLACE STAGE export_stage "
            f"URL = 'azure://{STORAGE_ACCOUNT}.blob.core.windows.net/{EXPORT_CONTAINER}/' "
            f"CREDENTIALS = (AZURE_SAS_TOKEN = '{sas}')"
        )

        for table, ddl in TABLES.items():
            cur.execute(f"CREATE TABLE IF NOT EXISTS {table} ({ddl})")
            # TRUNCATE first: COPY INTO tracks which source files it has
            # already loaded and skips them on re-run (its idempotency
            # guarantee) -- exactly wrong here, since export_silver_snapshot.py
            # overwrites the *same* file path with fresh data every run.
            # A truncate-then-reload makes this script safely re-runnable.
            cur.execute(f"TRUNCATE TABLE {table}")
            cur.execute(
                f"COPY INTO {table} FROM @export_stage/{table} "
                # Spark's export directory also has a 0-byte _SUCCESS marker
                # (and possibly .crc sidecars) alongside the real part file --
                # unlike Databricks' COPY INTO, Snowflake's loads every file
                # under the path by default with no format-based filtering,
                # so PATTERN is required, not optional (hit this live as
                # "Parquet file size is 0 bytes" against _SUCCESS).
                f"PATTERN = '.*[.]parquet' "
                f"FILE_FORMAT = (TYPE = PARQUET) "
                f"MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE "
                f"FORCE = TRUE"
            )
            rows = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"[{table}] copy into RAW.{table}: {rows} rows present")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
