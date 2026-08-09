"""bronze/endpoint=forecast -> silver/fct_forecast_issue_raw.

The real Spark job: explode(payload.list) -- ~40 rows per response -- flatten,
derive lead_time_minutes, and Delta MERGE on (location_id, issued_at, valid_ts).

`issued_at` is the envelope's `requested_at` -- minted by the extractor at
request time, never re-derived here (CLAUDE.md's single most load-bearing
extractor rule: the entire lead-time analysis depends on this being the
actual request time, not something inferred from a schedule). Every poll
gets its own issued_at, so this key is already unique per poll on its own;
the MERGE exists for idempotency (safe to reprocess the same bronze file
without duplicating rows), not to collapse genuinely repeated OWM model
runs -- those stay as distinct rows on purpose, since each one is a
revision in the accumulating-snapshot story Phase 5's fct_forecast_slot
builds on top of this raw layer.
"""

import os

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from schemas import ForecastSchema, check
from spark_session import bronze_path, build_spark_session, silver_path

ENDPOINT = "forecast"
TABLE = "fct_forecast_issue_raw"


def main() -> None:
    spark = build_spark_session(f"bronze_{ENDPOINT}_to_silver")

    path = bronze_path(ENDPOINT) + os.environ.get("BRONZE_PATH_SUFFIX", "")
    raw = spark.read.json(path).where(F.col("http_status") == 200)

    exploded = raw.select(
        F.col("location_id"),
        F.col("run_id"),
        F.to_timestamp("requested_at").alias("issued_at"),
        F.col("payload_sha256"),
        F.explode("payload.list").alias("timestep"),
    )

    flat = exploded.select(
        "location_id",
        "run_id",
        "issued_at",
        "payload_sha256",
        F.to_timestamp(F.from_unixtime(F.col("timestep.dt"))).alias("valid_ts"),
        F.col("timestep.main.temp").alias("temp_c"),
        F.col("timestep.main.feels_like").alias("feels_like_c"),
        F.col("timestep.main.temp_min").alias("temp_min_c"),
        F.col("timestep.main.temp_max").alias("temp_max_c"),
        F.col("timestep.main.pressure").alias("pressure_hpa"),
        F.col("timestep.main.humidity").alias("humidity_pct"),
        F.col("timestep.pop").alias("pop"),
        F.col("timestep.weather")[0]["main"].alias("weather_main"),
        F.col("timestep.weather")[0]["description"].alias("weather_description"),
        F.col("timestep.weather")[0]["icon"].alias("weather_icon"),
    ).withColumn(
        "lead_time_minutes",
        (F.col("valid_ts").cast("long") - F.col("issued_at").cast("long")) / 60,
    )

    # Hard-fail cross-field check: a violation here would silently poison
    # every lead-time bucket downstream (docs/PLAN.md Phase 4).
    bad_rows = flat.where(F.col("valid_ts") <= F.col("issued_at")).count()
    if bad_rows:
        raise ValueError(
            f"[{ENDPOINT}] {bad_rows} rows have valid_ts <= issued_at -- "
            "lead-time computation would be wrong for these; refusing to write."
        )

    check(flat, ForecastSchema, f"bronze_{ENDPOINT}_to_silver")

    target_path = silver_path(TABLE)

    if DeltaTable.isDeltaTable(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)
        (
            target.alias("t")
            .merge(
                flat.alias("s"),
                "t.location_id = s.location_id AND t.issued_at = s.issued_at "
                "AND t.valid_ts = s.valid_ts",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        flat.write.format("delta").mode("overwrite").save(target_path)

    print(f"[{ENDPOINT}] merged {flat.count()} rows into {TABLE}")
    spark.stop()


if __name__ == "__main__":
    main()
