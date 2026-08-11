"""Loads the 3 relevant silver Delta tables into Snowflake and rebuilds the
star schema against them (docs/PLAN.md Phase 5 Week 6) -- the
critical-path warehouse, unlike databricks_sync's demo side-track.

Same three-container-chain shape as databricks_sync (see that DAG's
docstring for why each step exists), reused deliberately rather than
reinvented:
  1. export_silver_snapshot.py (hindcast-spark:local) -- same script
     databricks_sync uses, pointed at the Terraform-managed `exports`
     container instead of `dbx-export` via EXPORT_CONTAINER, since this
     pipeline needs its own @hourly-fresh snapshot, not databricks_sync's
     @daily one.
  2. load_into_snowflake.py (hindcast-spark:local, same image) -- generates
     a short-lived SAS token, stages it, and COPY INTOs those snapshots
     into HINDCAST.RAW via Snowflake's external-stage mechanism (the
     Snowflake-side equivalent of load_into_databricks.py's inline
     WITH (CREDENTIAL (...)) COPY INTO).
  3. hindcast-dbt:local, given "snowflake" as its command -- `dbt build
     --target snowflake`, rebuilding the full star schema against what #2
     just loaded.
  4. emit_pipeline_metrics.py (hindcast-spark:local, same image) --
     docs/PLAN.md Phase 7's warehouse-derived metrics (slot.
     awaiting_actual_count, slot.closed_no_actual_ratio, match.
     offset_minutes_p95), queried straight from fct_forecast_slot now that
     #3 just rebuilt it.

docs/PLAN.md originally sketched this as two separate DAGs
(`silver_to_snowflake` + `dbt_build`) linked by Airflow Assets. Consolidated
into one linear DAG instead, matching databricks_sync's already-verified
pattern: dbt_build has no reason to run before load_into_snowflake finishes
loading the same run's data, so a hard task dependency inside one DAG is
simpler than a cross-DAG Asset for a dependency that's always 1:1 anyway.

Shares the spark_jobs pool with bronze_to_silver_spark and databricks_sync's
export task -- same CPU-starvation guard (see bronze_to_silver_spark's
docstring for the incident this prevents). @hourly, not @daily: this is the
warehouse the rest of the plan (Phase 7 DQ, Phase 9 analysis) depends on, so
it needs to stay fresh on the pipeline's real cadence.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag

SPARK_IMAGE = "hindcast-spark:local"
DBT_IMAGE = "hindcast-dbt:local"
DOCKER_SOCKET_URL = "unix://var/run/docker.sock"


@dag(
    dag_id="silver_to_snowflake",
    schedule="@hourly",
    start_date=datetime(2026, 8, 9),
    catchup=False,
    max_active_runs=1,
    tags=["snowflake", "dbt", "warehouse"],
)
def silver_to_snowflake():
    export = DockerOperator(
        task_id="export_silver_snapshot",
        image=SPARK_IMAGE,
        command="export_silver_snapshot.py",
        environment={"EXPORT_CONTAINER": "exports"},
        docker_url=DOCKER_SOCKET_URL,
        auto_remove="success",
        mount_tmp_dir=False,
        pool="spark_jobs",
    )

    load = DockerOperator(
        task_id="load_into_snowflake",
        image=SPARK_IMAGE,
        command="load_into_snowflake.py",
        docker_url=DOCKER_SOCKET_URL,
        auto_remove="success",
        mount_tmp_dir=False,
    )

    dbt_build = DockerOperator(
        task_id="dbt_build_snowflake",
        image=DBT_IMAGE,
        command="snowflake",
        docker_url=DOCKER_SOCKET_URL,
        auto_remove="success",
        mount_tmp_dir=False,
    )

    emit_metrics = DockerOperator(
        task_id="emit_pipeline_metrics",
        image=SPARK_IMAGE,
        command="emit_pipeline_metrics.py",
        docker_url=DOCKER_SOCKET_URL,
        auto_remove="success",
        mount_tmp_dir=False,
    )

    export >> load >> dbt_build >> emit_metrics


silver_to_snowflake()
