"""silver/<table> (Delta) -> dbx-export/<table> (plain Parquet, single clean
snapshot) -- the pre-step before load_into_databricks.py's COPY INTO.

Why this step exists and isn't just "COPY INTO straight from the Delta
table's own directory": Delta's overwrite/merge writes don't delete
superseded parquet files immediately -- they stay on disk, unreferenced by
the latest transaction log version, until an explicit VACUUM (never run on
this project's silver tables). obs_weather and obs_air_quality are full
overwrite every run, so their raw file directories can accumulate many
stale full copies. Reading the directory naively (as COPY INTO's
FILEFORMAT=PARQUET does -- it has no Delta transaction-log awareness) would
silently multiply-count that stale data. Spark's own `.format("delta")`
reader is transaction-log-aware, so exporting through it first guarantees
exactly the current logical rows land in the clean export.

Same three tables Phase 6's Snowflake COPY INTO reads (docs/PLAN.md):
obs_weather, obs_air_quality, fct_forecast_issue_raw. Not
fct_forecast_milestones -- that Spark-computed table isn't part of the
star-schema source layer dbt reads from (see warehouse/dbt/hindcast's
_sources.yml).
"""

from spark_session import build_spark_session, silver_path

TABLES = ["obs_weather", "obs_air_quality", "fct_forecast_issue_raw"]
EXPORT_CONTAINER = "dbx-export"
STORAGE_ACCOUNT = "sthindcastjlbpfz"


def export_path(table: str) -> str:
    return f"abfs://{EXPORT_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{table}"


def main() -> None:
    spark = build_spark_session("export_silver_snapshot")
    for table in TABLES:
        df = spark.read.format("delta").load(silver_path(table))
        # coalesce(1): these tables are small enough (see the Phase 4
        # benchmark's whole point) that a single output file is simpler for
        # COPY INTO to read and doesn't cost anything real at this volume.
        df.coalesce(1).write.mode("overwrite").parquet(export_path(table))
        # Re-read the just-written export (not the source df) for the
        # logged count -- counting the source again here would be a second,
        # separate action against a table that ingestion keeps writing to
        # live, and could report a different number than what actually
        # landed in the export (hit this live: a source overwrite raced
        # between this job's write and a naive post-write source count).
        written = spark.read.parquet(export_path(table)).count()
        print(f"[{table}] exported {written} rows to {export_path(table)}")
    spark.stop()


if __name__ == "__main__":
    main()
