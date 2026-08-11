"""Queries fct_forecast_slot after each dbt build and emits the three
warehouse-derived metrics docs/PLAN.md Phase 7 calls for that can't be
computed from any single job's own local state -- they're properties of
the accumulating snapshot as a whole:

  hindcast.slot.awaiting_actual_count -- gauge, count of open slots stuck
    waiting on an observation that should have landed by now.
  hindcast.slot.closed_no_actual_ratio -- gauge, the project's primary SLI
    (docs/PLAN.md: "the one number that says the pipeline is failing to do
    the thing it exists to do"). Denominator is slots that actually
    *reached* closure (closed + closed_no_actual), not every slot ever
    created -- an in-flight slot isn't evidence of failure yet.
  hindcast.match.offset_minutes_p95 -- gauge, computed server-side by
    Snowflake's PERCENTILE_CONT rather than pulled client-side, since the
    underlying row count only grows.

Runs against Snowflake specifically (the critical-path warehouse), as a
step in the silver_to_snowflake DAG after dbt_build_snowflake.
"""

from __future__ import annotations

import os

import snowflake.connector
from datadog_metrics import submit_gauge


def connect() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "HINDCAST_XS"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "HINDCAST"),
        schema=f"{os.environ.get('SNOWFLAKE_SCHEMA', 'MARTS')}_FACTS",
    )


def main() -> None:
    conn = connect()
    cur = conn.cursor()
    try:
        awaiting_actual = cur.execute(
            "SELECT COUNT(*) FROM fct_forecast_slot WHERE slot_status = 'awaiting_actual'"
        ).fetchone()[0]

        closed_no_actual_ratio = cur.execute(
            """
            SELECT
                COUNT_IF(slot_status = 'closed_no_actual')::FLOAT
                / NULLIF(COUNT_IF(slot_status IN ('closed', 'closed_no_actual')), 0)
            FROM fct_forecast_slot
            """
        ).fetchone()[0]

        offset_p95 = cur.execute(
            """
            SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ABS(match_offset_minutes))
            FROM fct_forecast_slot
            WHERE match_offset_minutes IS NOT NULL
            """
        ).fetchone()[0]

        submit_gauge("hindcast.slot.awaiting_actual_count", awaiting_actual)
        print(f"hindcast.slot.awaiting_actual_count = {awaiting_actual}")

        if closed_no_actual_ratio is not None:
            submit_gauge("hindcast.slot.closed_no_actual_ratio", closed_no_actual_ratio)
            print(f"hindcast.slot.closed_no_actual_ratio = {closed_no_actual_ratio}")
        else:
            print("hindcast.slot.closed_no_actual_ratio: no closed slots yet, skipping")

        if offset_p95 is not None:
            submit_gauge("hindcast.match.offset_minutes_p95", float(offset_p95))
            print(f"hindcast.match.offset_minutes_p95 = {offset_p95}")
        else:
            print("hindcast.match.offset_minutes_p95: no matched observations yet, skipping")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
