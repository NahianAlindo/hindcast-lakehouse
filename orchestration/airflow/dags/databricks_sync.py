"""Keeps the Databricks Free Edition side-track (docs/PLAN.md's Phase 5
addendum -- a demo environment proving the same star schema portable to a
third engine, not on the critical Azure/Snowflake path) in sync with real
accruing data, instead of the one-time manual snapshot it started as.

Three strictly sequential one-shot containers, each depending on the last
one's actual output existing:
  1. export_silver_snapshot.py (hindcast-spark:local) -- reads the 3
     relevant silver Delta tables via Spark's transaction-log-aware reader
     and writes clean Parquet snapshots to ADLS's dbx-export container.
     Must go through Spark, not a raw file copy: Delta's overwrite/merge
     writes leave superseded parquet files on disk until an explicit
     VACUUM (never run here), so reading the Delta table's own directory
     directly would double-count stale data.
  2. load_into_databricks.py (hindcast-spark:local, same image -- it's a
     lightweight requests+azure-storage-blob script, not a Spark job, but
     reusing the image already built for #1 avoids maintaining a second
     near-identical one for one small script) -- generates a short-lived
     SAS token and COPY INTOs those snapshots into Databricks-native Delta
     tables via the SQL Statement Execution API.
  3. hindcast-dbt:local's default entrypoint -- `dbt build --target
     databricks`, rebuilding the full star schema against what #2 just
     loaded.

Only #1 needs the shared spark_jobs pool (docker-compose's actual VM only
has 4 vCPUs, and bronze_to_silver_spark's own tasks already share this same
pool -- see that DAG's docstring for the CPU-starvation incident this
guards against). #2 and #3 are network-bound, not local-JVM-heavy, so they
don't compete for the same resource.

@daily, not @hourly like bronze_to_silver_spark: this is a demo path, not
the core pipeline -- there's no reason to burn VM cycles, Key Vault calls,
or Databricks Free Edition serverless-warehouse time keeping it fresher
than a portfolio demo actually needs.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag

SPARK_IMAGE = "hindcast-spark:local"
DBT_IMAGE = "hindcast-dbt:local"
DOCKER_SOCKET_URL = "unix://var/run/docker.sock"


@dag(
    dag_id="databricks_sync",
    schedule="@daily",
    start_date=datetime(2026, 8, 9),
    catchup=False,
    max_active_runs=1,
    tags=["databricks", "dbt", "demo"],
)
def databricks_sync():
    export = DockerOperator(
        task_id="export_silver_snapshot",
        image=SPARK_IMAGE,
        command="export_silver_snapshot.py",
        docker_url=DOCKER_SOCKET_URL,
        auto_remove="success",
        mount_tmp_dir=False,
        pool="spark_jobs",
    )

    load = DockerOperator(
        task_id="load_into_databricks",
        image=SPARK_IMAGE,
        command="load_into_databricks.py",
        docker_url=DOCKER_SOCKET_URL,
        auto_remove="success",
        mount_tmp_dir=False,
    )

    dbt_build = DockerOperator(
        task_id="dbt_build_databricks",
        image=DBT_IMAGE,
        docker_url=DOCKER_SOCKET_URL,
        auto_remove="success",
        mount_tmp_dir=False,
    )

    export >> load >> dbt_build


databricks_sync()
