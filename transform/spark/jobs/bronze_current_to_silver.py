"""bronze/endpoint=current -> silver/obs_weather.

Flattens main/wind/weather[0]/clouds; dedups on (location_id, source_dt)
keeping first-seen (by requested_at). Full overwrite each run, not an
incremental MERGE: real volume here is a few hundred rows total (this
project's whole point is that its actual data volume is small -- see the
Spark-vs-DuckDB crossover benchmark), so re-reading all of bronze and
rewriting all of silver is simple, trivially correct, and still fast at this
scale. bronze_forecast_to_silver.py earns a real MERGE; this doesn't.
"""

import os

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from schemas import ObsWeatherSchema, check
from spark_session import bronze_path, build_spark_session, silver_path

ENDPOINT = "current"
TABLE = "obs_weather"


def main() -> None:
    spark = build_spark_session(f"bronze_{ENDPOINT}_to_silver")

    # BRONZE_PATH_SUFFIX lets local testing scope to a single hour's worth of
    # files instead of the full bronze history -- unset in production.
    path = bronze_path(ENDPOINT) + os.environ.get("BRONZE_PATH_SUFFIX", "")
    raw = spark.read.json(path).where(F.col("http_status") == 200)

    weather0 = F.col("payload.weather")[0]
    flat = raw.select(
        F.col("location_id"),
        F.col("run_id"),
        F.to_timestamp("requested_at").alias("requested_at"),
        F.to_timestamp(F.from_unixtime("payload.dt")).alias("source_dt"),
        F.col("payload.main.temp").alias("temp_c"),
        F.col("payload.main.feels_like").alias("feels_like_c"),
        F.col("payload.main.temp_min").alias("temp_min_c"),
        F.col("payload.main.temp_max").alias("temp_max_c"),
        F.col("payload.main.pressure").alias("pressure_hpa"),
        F.col("payload.main.humidity").alias("humidity_pct"),
        F.col("payload.wind.speed").alias("wind_speed_ms"),
        F.col("payload.wind.deg").alias("wind_deg"),
        F.col("payload.wind.gust").alias("wind_gust_ms"),
        F.col("payload.clouds.all").alias("clouds_pct"),
        F.col("payload.visibility").alias("visibility_m"),
        weather0["id"].alias("weather_code"),
        weather0["main"].alias("weather_main"),
        weather0["description"].alias("weather_description"),
        weather0["icon"].alias("weather_icon"),
        F.col("payload_sha256"),
    )

    check(flat, ObsWeatherSchema, f"bronze_{ENDPOINT}_to_silver")

    dedup_window = Window.partitionBy("location_id", "source_dt").orderBy(
        F.col("requested_at").asc()
    )
    deduped = (
        flat.withColumn("_rn", F.row_number().over(dedup_window))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )

    # overwriteSchema is safe (not a footgun) here specifically because this
    # job is a full overwrite every run -- the table's schema *is* this
    # file's SELECT statement, so a mismatch only ever means the code
    # changed, which is exactly when the schema should follow.
    deduped.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        silver_path(TABLE)
    )
    print(f"[{ENDPOINT}] wrote {deduped.count()} rows to {TABLE}")
    spark.stop()


if __name__ == "__main__":
    main()
