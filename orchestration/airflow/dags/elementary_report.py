"""Generates Phase 7's DQ report (docs/PLAN.md §5/§9: "elementary_report |
@daily | DQ report -> ADLS") and uploads it to ADLS's exports container as
exports/dq_report.html, overwritten each run.

Doesn't re-run dbt build itself -- it reads elementary's own tables (test
results, run history, freshness) from whatever silver_to_snowflake's last
several @hourly dbt builds already populated via elementary's on-run-end
hook (verified live: this already happens automatically on every dbt build
against every target). @daily is a report-generation cadence, not a data
cadence -- the underlying test-result history it reports on is already
fresher than daily.

Snowflake-only, deliberately (docs/PLAN.md Phase 7's "elementary-data stays
primary-pipeline-only"): the report needs the @hourly cadence/history the
primary pipeline has to say anything meaningful about trends, which the
Databricks side-track's @daily demo path doesn't have.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag

IMAGE = "hindcast-dbt:local"


@dag(
    dag_id="elementary_report",
    schedule="@daily",
    start_date=datetime(2026, 8, 10),
    catchup=False,
    max_active_runs=1,
    tags=["dq", "elementary", "snowflake"],
)
def elementary_report():
    DockerOperator(
        task_id="generate_and_upload_report",
        image=IMAGE,
        command="elementary_report",
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        mount_tmp_dir=False,
    )


elementary_report()
