"""ZORDER is a Delta-table property (set via `OPTIMIZE ... ZORDER BY`), not
a per-query flag like AQE or join strategy -- so it gets its own script:
write the synthetic dataset as a local Delta table once, run the real
milestones query against it (baseline), OPTIMIZE + ZORDER BY (location_id,
valid_ts) -- the same columns the milestones window partitions/filters on
-- then run the identical query again and compare.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from functools import partial
from pathlib import Path

from metrics import timed_run
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from run_spark_benchmark import MILESTONE_HOURS, build_dim_location


def run_milestones_simple(spark, source, dim):
    milestones = spark.createDataFrame([(float(h),) for h in MILESTONE_HOURS], ["milestone_hours"])
    candidates = (
        source.withColumn("lead_time_hours", F.col("lead_time_minutes") / 60)
        .crossJoin(milestones)
        .withColumn(
            "distance_hours",
            F.abs(F.col("lead_time_hours") - F.col("milestone_hours")),
        )
    )
    window = Window.partitionBy("location_id", "valid_ts", "milestone_hours").orderBy(
        F.col("distance_hours").asc()
    )
    selected = (
        candidates.withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") == 1)
        .drop("_rn", "distance_hours")
    )
    return selected.join(F.broadcast(dim), "location_id", "left")


def _count_milestones(spark, delta_df, dim) -> int:
    return run_milestones_simple(spark, delta_df, dim).count()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source", required=True, help="Parquet path to load and re-write as Delta"
    )
    parser.add_argument("--delta-path", required=True)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--results", required=True)
    args = parser.parse_args()

    jars_dir = os.environ.get("SPARK_ADLS_JARS_DIR", "/home/nahian/spark-jars")
    jar_names = [
        "io.delta_delta-spark_2.12-3.2.1.jar",
        "io.delta_delta-storage-3.2.1.jar",
        "org.antlr_antlr4-runtime-4.9.3.jar",
    ]
    jars = ",".join(f"{jars_dir}/{name}" for name in jar_names)
    spark = (
        SparkSession.builder.appName("zorder_benchmark")
        .master("local[4]")
        .config("spark.driver.memory", "3g")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.jars", jars)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    # Local benchmark CLI, not network-facing -- --source/--delta-path/
    # --results are meant to point wherever the developer running it
    # chooses. Resolving through pathlib normalizes '..'/'.' segments
    # rather than passing raw CLI text straight into filesystem calls.
    source_dir = Path(args.source).resolve()
    delta_path = Path(args.delta_path).resolve()
    results_path = Path(args.results).resolve()

    source = spark.read.parquet(str(source_dir))
    source.write.format("delta").mode("overwrite").save(str(delta_path))
    dim = build_dim_location(spark)

    for zorder in (False, True):
        if zorder:
            spark.sql(f"OPTIMIZE delta.`{delta_path}` ZORDER BY (location_id, valid_ts)")

        delta_df = spark.read.format("delta").load(str(delta_path))

        # functools.partial binds delta_df's *current* value immediately,
        # rather than a closure that would capture the loop variable by
        # reference (classic Python loop-closure pitfall -- not live here
        # since timed_run calls it within the same iteration, but the
        # pattern is worth avoiding outright rather than relying on that).
        metrics = timed_run(spark, partial(_count_milestones, spark, delta_df, dim))
        record = {
            "timestamp": time.time(),
            "engine": "spark",
            "rows": args.rows,
            "zorder": zorder,
            "label": "zorder_on" if zorder else "zorder_off",
            **metrics,
        }
        with results_path.open("a") as f:
            f.write(json.dumps(record) + "\n")
        print(json.dumps(record, indent=2))

    spark.stop()


if __name__ == "__main__":
    main()
