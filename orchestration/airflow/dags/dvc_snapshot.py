"""Weekly data-versioning snapshot (docs/PLAN.md Phase 6): a single one-shot
container (hindcast-dvc:local) that git-clones the repo fresh, runs `dvc
repro` (dbt build --target duckdb, then export every mart table to Parquet),
`dvc push`es the result to the Azure Blob remote, and commits+tags+pushes
`data-vYYYY.WW` back to GitHub -- see docker/dvc/entrypoint-wrapper.sh for
the full sequence and docker/dvc/Dockerfile's docstring for why this is the
one image in the project that clones at runtime instead of baking code in.

Sourced from DuckDB, not Snowflake, even though Snowflake is the
critical-path warehouse (docs/PLAN.md Phase 6's "both pipelines" note):
DuckDB is the one target this snapshot can rebuild from with zero live
warehouse credentials, and it's the permanent post-trial/post-teardown
state, so a DuckDB-sourced snapshot survives losing Snowflake entirely.

@weekly, not @hourly/@daily like the other DAGs -- a data *version*, not a
live sync; the whole point is a small number of durable, git-ref-addressable
checkpoints, not maximum freshness. No spark_jobs pool: this is a DuckDB
workload, not a Spark JVM, and doesn't compete for the resource that pool
guards (see bronze_to_silver_spark's docstring for that incident).
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag

IMAGE = "hindcast-dvc:local"


@dag(
    dag_id="dvc_snapshot",
    schedule="@weekly",
    start_date=datetime(2026, 8, 9),
    catchup=False,
    max_active_runs=1,
    tags=["dvc", "versioning"],
)
def dvc_snapshot():
    DockerOperator(
        task_id="snapshot_and_publish",
        image=IMAGE,
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        mount_tmp_dir=False,
    )


dvc_snapshot()
