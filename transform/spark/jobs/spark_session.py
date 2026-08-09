"""Shared SparkSession builder: ADLS (abfs) + Delta Lake config.

Bronze/silver live in ADLS Gen2 (hierarchical namespace on), so this uses the
`abfs://` scheme via Hadoop's AzureBlobFileSystem driver -- not the legacy
`wasb://` one, which doesn't understand HNS directories correctly.

JAR versions are pinned to what Spark 3.5.x actually bundles, not guessed:
pyspark 3.5.9 ships Hadoop 3.3.4, and hadoop-azure 3.3.4's own POM pins
com.microsoft.azure:azure-storage to 7.0.1 -- confirmed against Maven
Central's dependency metadata before downloading anything. Delta's own jars
(delta-spark, delta-storage, antlr4-runtime) are the exact ones
`configure_spark_with_delta_pip` resolves for delta-spark==3.2.1, captured
once locally rather than re-resolved from Maven Central on every job run
(too slow, and a needless external dependency for a job that runs on a
schedule) -- see docker/spark/Dockerfile, which bakes all five into the
image.

Local dev (WSL2, CLAUDE.md: Spark never runs on native Windows): point
SPARK_ADLS_JARS_DIR at wherever the jars were downloaded (see this repo's
Phase 4 notes) and set AZURE_STORAGE_ACCOUNT_KEY from `az storage account
keys list` -- never commit that key. Deployed: the jars are baked into the
image at the same env var's default path, and the key comes from Key Vault
via the entrypoint-wrapper pattern already used for Airflow.
"""

import os

from pyspark.sql import SparkSession

STORAGE_ACCOUNT = "sthindcastjlbpfz"

_JAR_NAMES = [
    "hadoop-azure-3.3.4.jar",
    "azure-storage-7.0.1.jar",
    "io.delta_delta-spark_2.12-3.2.1.jar",
    "io.delta_delta-storage-3.2.1.jar",
    "org.antlr_antlr4-runtime-4.9.3.jar",
]


def build_spark_session(app_name: str) -> SparkSession:
    jars_dir = os.environ.get("SPARK_ADLS_JARS_DIR", "/opt/spark-jars")
    storage_key = os.environ["AZURE_STORAGE_ACCOUNT_KEY"]
    jars = ",".join(f"{jars_dir}/{name}" for name in _JAR_NAMES)

    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.jars", jars)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            f"spark.hadoop.fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
            "SharedKey",
        )
        .config(
            f"spark.hadoop.fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
            storage_key,
        )
        .getOrCreate()
    )


def bronze_path(endpoint: str) -> str:
    return f"abfs://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/endpoint={endpoint}"


def silver_path(table: str) -> str:
    return f"abfs://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/{table}"
