"""silver/fct_forecast_issue_raw -> silver/fct_forecast_milestones.

The shuffle-heavy job that justifies Spark (docs/PLAN.md Phase 4): for each
(location_id, valid_ts) forecast slot, pick out the single forecast closest
to each of 8 standard lead-time milestones (3h, 6h, 12h, 24h, 48h, 72h, 96h,
120h -- matching the plan's 8-row dim_lead_time_bucket). This is the "how did
the forecast for this exact future moment evolve as lead time decreased"
view Phase 9's slot-lifecycle chart is built on.

Window.partitionBy(location_id, valid_ts, milestone_hours).orderBy(distance)
+ row_number(): one row per (slot, milestone) pair, picking whichever actual
poll landed closest to that milestone. Meaningful milestone coverage needs
real accrual over weeks (many issued_at polls per slot) -- this job is
correct from day one, but early runs will mostly see one poll "winning" each
milestone by default since there's nothing closer yet.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_session import build_spark_session, silver_path

SOURCE_TABLE = "fct_forecast_issue_raw"
TARGET_TABLE = "fct_forecast_milestones"
MILESTONE_HOURS = [3, 6, 12, 24, 48, 72, 96, 120]


def main() -> None:
    spark = build_spark_session("silver_forecast_milestones")

    source = spark.read.format("delta").load(silver_path(SOURCE_TABLE))
    milestones = spark.createDataFrame(
        [(float(h),) for h in MILESTONE_HOURS], ["milestone_hours"]
    )

    candidates = (
        source.withColumn("lead_time_hours", F.col("lead_time_minutes") / 60)
        .crossJoin(milestones)
        .withColumn(
            "distance_hours",
            F.abs(F.col("lead_time_hours") - F.col("milestone_hours")),
        )
    )

    window = Window.partitionBy(
        "location_id", "valid_ts", "milestone_hours"
    ).orderBy(F.col("distance_hours").asc())

    selected = (
        candidates.withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") == 1)
        .drop("_rn", "distance_hours")
    )

    selected.write.format("delta").mode("overwrite").save(silver_path(TARGET_TABLE))
    print(f"wrote {selected.count()} rows to {TARGET_TABLE}")
    spark.stop()


if __name__ == "__main__":
    main()
