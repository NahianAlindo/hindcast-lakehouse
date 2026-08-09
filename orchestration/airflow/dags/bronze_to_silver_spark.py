"""Runs the four Phase 4 Spark jobs as one-shot containers (docs/PLAN.md
Phase 3's DAG table, Phase 4's transform jobs).

Each job is `docker run --rm hindcast-spark:local <job>.py` under the hood,
via DockerOperator against the VM's real Docker daemon (sibling containers,
not nested Docker-in-Docker -- the scheduler container has /var/run/
docker.sock bind-mounted for exactly this). Verified directly on the VM
before this DAG existed: memory recovers fully after each container exits,
no impact on Airflow's own containers (see docker/spark/Dockerfile's commit
for the numbers).

current/air_quality/forecast have no *data* dependency on each other, but
they're chained sequentially anyway (current >> air_quality >> forecast >>
milestones), and max_active_runs=1 caps the DAG to one run at a time. Hit a
real incident from running them in parallel: on a 2 vCPU VM, 3 simultaneous
Spark JVMs starved the guest OS badly enough that even SSH and Azure's
out-of-band VM-agent channel stopped responding (confirmed still
`PowerState/running` at the control plane the whole time). Resized to 4
vCPU/8GB (Standard_B4als_v2) to fix the immediate cause, but on recovery the
scheduler had multiple DagRuns queued from the outage window and fired all of
their parallel tasks again -- 6 JVMs this time, same starvation, even on 4
vCPUs. Real production volume here is tiny (1,739 blobs total as of Phase 4)
so there's no throughput reason to parallelize; serializing is strictly
safer and costs nothing.

catchup=False, @hourly -- matches docs/PLAN.md's bronze_to_silver_spark
schedule.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag

IMAGE = "hindcast-spark:local"


def _spark_task(task_id: str, script: str) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=IMAGE,
        command=script,
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        mount_tmp_dir=False,
        # spark_jobs (1 slot): caps concurrent Spark JVM containers across
        # *every* DAG on this VM, not just within this one. max_active_runs
        # only serializes this DAG against itself -- without a shared pool,
        # this DAG's tasks and databricks_sync's export task could still
        # overlap and reproduce the exact CPU-starvation incident described
        # above, just across two DAGs instead of one.
        pool="spark_jobs",
    )


@dag(
    dag_id="bronze_to_silver_spark",
    schedule="@hourly",
    start_date=datetime(2026, 8, 9),
    catchup=False,
    max_active_runs=1,
    tags=["transform", "spark"],
)
def bronze_to_silver_spark():
    current = _spark_task("bronze_current_to_silver", "bronze_current_to_silver.py")
    air_quality = _spark_task(
        "bronze_air_quality_to_silver", "bronze_air_quality_to_silver.py"
    )
    forecast = _spark_task("bronze_forecast_to_silver", "bronze_forecast_to_silver.py")
    milestones = _spark_task(
        "silver_forecast_milestones", "silver_forecast_milestones.py"
    )

    current >> air_quality >> forecast >> milestones


bronze_to_silver_spark()
