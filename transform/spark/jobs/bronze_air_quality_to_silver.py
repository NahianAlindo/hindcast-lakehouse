"""bronze/endpoint=air_quality -> silver/obs_air_quality.

Same shape as bronze_current_to_silver.py: flatten, dedup on
(location_id, source_dt) keeping first-seen, full overwrite each run (see
that job's docstring for why full overwrite is the right call at this data
volume). OWM's air_pollution response nests one reading per poll under
`payload.list[0]` (the endpoint technically supports multi-timestep
responses, but the current-pollution call this project makes always returns
exactly one).
"""

import os

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from schemas import ObsAirQualitySchema, check
from spark_session import bronze_path, build_spark_session, silver_path

ENDPOINT = "air_quality"
TABLE = "obs_air_quality"


def main() -> None:
    spark = build_spark_session(f"bronze_{ENDPOINT}_to_silver")

    path = bronze_path(ENDPOINT) + os.environ.get("BRONZE_PATH_SUFFIX", "")
    raw = spark.read.json(path).where(F.col("http_status") == 200)

    item = F.col("payload.list")[0]
    flat = raw.select(
        F.col("location_id"),
        F.col("run_id"),
        F.to_timestamp("requested_at").alias("requested_at"),
        F.to_timestamp(F.from_unixtime(item["dt"])).alias("source_dt"),
        item["main"]["aqi"].cast("int").alias("aqi"),
        item["components"]["co"].alias("co"),
        item["components"]["no"].alias("no"),
        item["components"]["no2"].alias("no2"),
        item["components"]["o3"].alias("o3"),
        item["components"]["so2"].alias("so2"),
        item["components"]["pm2_5"].alias("pm2_5"),
        item["components"]["pm10"].alias("pm10"),
        item["components"]["nh3"].alias("nh3"),
        F.col("payload_sha256"),
    )

    check(flat, ObsAirQualitySchema, f"bronze_{ENDPOINT}_to_silver")

    dedup_window = Window.partitionBy("location_id", "source_dt").orderBy(
        F.col("requested_at").asc()
    )
    deduped = (
        flat.withColumn("_rn", F.row_number().over(dedup_window))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )

    # overwriteSchema is safe here for the same reason as
    # bronze_current_to_silver.py: full overwrite every run means the
    # table's schema *is* this file's SELECT statement.
    deduped.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        silver_path(TABLE)
    )
    print(f"[{ENDPOINT}] wrote {deduped.count()} rows to {TABLE}")
    spark.stop()


if __name__ == "__main__":
    main()
