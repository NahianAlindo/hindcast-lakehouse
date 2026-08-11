"""Parameterized re-run of the real silver_forecast_milestones job (Window
partitionBy + row_number over 8 lead-time milestones) plus a join to a
synthetic dim_location, with the toggles docs/PLAN.md Phase 4 specifies:
partitioned vs unpartitioned source, AQE on/off, broadcast vs sort-merge
join to dim_location, salted vs unsalted skew handling on that join.

ZORDER is benchmarked separately (run_zorder_benchmark.py) since it's a
Delta-table-level operation (OPTIMIZE ... ZORDER BY), not a per-query flag.

Appends one JSON line per run to a results file so a full sweep (many
processes, since each run gets a fresh SparkSession/JVM to avoid one run's
cached state or JIT warm-up biasing the next) accumulates a single dataset.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from metrics import timed_run
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

MILESTONE_HOURS = [3, 6, 12, 24, 48, 72, 96, 120]
N_SALT_BUCKETS = 12  # > local[*] core count, so salted skew actually spreads


def build_dim_location(spark: SparkSession):
    """Small synthetic dim table (10,003 rows: 3 mega-cities + 10,000
    regular, matching generate_synthetic's location_id universe) -- big
    enough that a naive sort-merge join actually shuffles both sides, small
    enough that Spark's default broadcast threshold would pick it up on its
    own (which is exactly why the broadcast-vs-sort-merge comparison has to
    force the join strategy explicitly rather than rely on the default)."""
    mega = spark.range(3).select(
        F.concat(F.lit("mega_city_"), F.col("id").cast("string")).alias("location_id")
    )
    regular = spark.range(9997).select(
        F.concat(F.lit("city_"), F.lpad((F.col("id") + 3).cast("string"), 5, "0")).alias(
            "location_id"
        )
    )
    dim = mega.union(regular).withColumn(
        "country", (F.abs(F.hash("location_id")) % 190).cast("string")
    )
    return dim


def run_milestones(spark, source, dim, *, join_strategy: str, salted: bool):
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

    if dim is None:
        return selected

    if join_strategy == "broadcast":
        joined = selected.join(F.broadcast(dim), "location_id", "left")
    else:
        # Force a real sort-merge shuffle join by disabling auto-broadcast
        # for this query (set globally by the CLI before calling this).
        if salted:
            salt = (F.abs(F.hash("location_id", F.lit("salt"))) % N_SALT_BUCKETS).cast("int")
            left = selected.withColumn("_salt", salt).withColumn(
                "_join_key", F.concat_ws("_", "location_id", "_salt")
            )
            # Explode the dim side across every salt bucket so each salted
            # left-side key still finds its match -- the standard salted
            # skew-join pattern.
            buckets = spark.range(N_SALT_BUCKETS).select(F.col("id").cast("int").alias("_salt"))
            right = (
                dim.crossJoin(buckets)
                .withColumn("_join_key", F.concat_ws("_", "location_id", "_salt"))
                .drop("location_id", "_salt")
            )
            joined = left.join(right, "_join_key", "left").drop("_join_key", "_salt")
        else:
            joined = selected.join(dim, "location_id", "left")

    return joined


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--rows", type=int, required=True, help="row count label")
    parser.add_argument("--aqe", choices=["on", "off"], default="on")
    parser.add_argument("--join-strategy", choices=["broadcast", "sort_merge"], default="broadcast")
    parser.add_argument("--salted", action="store_true")
    parser.add_argument(
        "--partitioned",
        action="store_true",
        help="label only -- reflects whether --source points at a partitionBy(issue_date) layout",
    )
    parser.add_argument(
        "--skip-join",
        action="store_true",
        help=(
            "omit the dim_location join entirely -- used for the scale sweep so the query "
            "exactly matches run_duckdb_benchmark.py's (join-free) query"
        ),
    )
    parser.add_argument("--label", default="")
    parser.add_argument("--results", required=True)
    args = parser.parse_args()

    builder = (
        SparkSession.builder.appName(f"spark_benchmark_{args.label}")
        .master("local[4]")
        .config("spark.driver.memory", "3g")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.sql.adaptive.enabled", str(args.aqe == "on").lower())
    )
    if args.join_strategy == "sort_merge":
        builder = builder.config("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark = builder.getOrCreate()

    # Local benchmark CLI, not network-facing -- --source/--results are
    # meant to point wherever the developer running it chooses. Resolving
    # through pathlib normalizes '..'/'.' segments rather than passing raw
    # CLI text straight into filesystem calls.
    source_dir = Path(args.source).resolve()
    results_path = Path(args.results).resolve()

    source = spark.read.parquet(str(source_dir))
    dim = None if args.skip_join else build_dim_location(spark)

    def action():
        out = run_milestones(
            spark, source, dim, join_strategy=args.join_strategy, salted=args.salted
        )
        return out.count()

    metrics = timed_run(spark, action)

    record = {
        "timestamp": time.time(),
        "engine": "spark",
        "rows": args.rows,
        "aqe": args.aqe,
        "join_strategy": args.join_strategy,
        "salted": args.salted,
        "partitioned": args.partitioned,
        "skip_join": args.skip_join,
        "label": args.label,
        "source": str(source_dir),
        **metrics,
    }
    with results_path.open("a") as f:
        f.write(json.dumps(record) + "\n")

    print(json.dumps(record, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
