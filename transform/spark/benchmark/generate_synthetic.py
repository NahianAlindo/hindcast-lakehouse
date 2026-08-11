"""Synthetic fct_forecast_issue_raw generator for the Phase 4 scale benchmark
(docs/PLAN.md Phase 4: 10,000 cities x 90 days x 8 issues/day x 40 timesteps
~= 288M rows, ~9GB Parquet, with deliberate skew: 3 "mega-cities" holding
20% of rows).

Generated entirely with Spark's distributed `spark.range()` + column
expressions -- never collected to the driver -- so it scales to the full
288M rows on a single laptop. A literal city x day x issue x timestep nested
loop (crossJoin) was considered and rejected: it can't produce skew without
either blowing up the row count or a second re-sampling pass, and it forces
a much larger intermediate shuffle just to build the base grid. Deriving
every column from a single flat row index is the standard way to generate
large synthetic Spark datasets and gets skew "for free" via a weighted
per-row location_id draw.

Fixed seed (`SEED`) so the generated dataset -- and therefore every
benchmark number derived from it -- is reproducible. DVC tracks this
script's seed value, not its ~9GB output (docs/PLAN.md Phase 6).
"""

from __future__ import annotations

import argparse

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

SEED = 20260809
N_CITIES = 10_000
N_MEGA_CITIES = 3
MEGA_CITY_SHARE = 0.20  # fraction of all rows owned by the 3 mega-cities
N_DAYS = 90
ISSUES_PER_DAY = 8  # matches the real 3-hourly poll cadence
TIMESTEPS = 40  # matches OWM's real 5-day/3-hour forecast list length
BASE_DATE = "2026-01-01"

FULL_ROW_COUNT = N_CITIES * N_DAYS * ISSUES_PER_DAY * TIMESTEPS  # 288,000,000


def _location_id_expr() -> Column:
    """20% of rows land on one of 3 mega-cities, the remaining 80% spread
    uniformly across the other 9,997 regular cities. A per-row weighted
    draw (not a nested loop) is what makes the skew exact and
    scale-independent."""
    mega_pick = (F.abs(F.hash(F.col("row_id"), F.lit(SEED))) % N_MEGA_CITIES).cast("int")
    regular_pick = F.abs(F.hash(F.col("row_id"), F.lit(SEED + 1))) % (N_CITIES - N_MEGA_CITIES)
    is_mega = F.rand(SEED) < MEGA_CITY_SHARE
    return F.when(is_mega, F.concat(F.lit("mega_city_"), mega_pick.cast("string"))).otherwise(
        F.concat(
            F.lit("city_"),
            F.lpad((regular_pick + N_MEGA_CITIES).cast("string"), 5, "0"),
        )
    )


def _issued_at_expr() -> Column:
    """One of 90 days x one of 8 fixed 3-hourly issue slots, derived
    deterministically from row_id so the dataset is reproducible."""
    day_offset = F.abs(F.hash(F.col("row_id"), F.lit(SEED + 2))) % N_DAYS
    issue_slot = F.abs(F.hash(F.col("row_id"), F.lit(SEED + 3))) % ISSUES_PER_DAY
    return (
        F.to_timestamp(F.lit(BASE_DATE))
        + F.make_interval(days=day_offset)
        + F.make_interval(hours=issue_slot * F.lit(3))
    )


def _timestep_index_expr() -> Column:
    """0..39, i.e. +3h increments out to the 5-day horizon."""
    return F.abs(F.hash(F.col("row_id"), F.lit(SEED + 4))) % TIMESTEPS


def _temp_c_expr() -> Column:
    """Diurnal (hour-of-day) + seasonal (day-of-year) sinusoidal signal
    plus Gaussian noise -- realistic enough to exercise real
    partitioning/skew behavior without needing an actual climate model."""
    two_pi = 2 * 3.141592653589793
    hour_of_day = F.hour("valid_ts")
    day_of_year = F.dayofyear("valid_ts")
    return (
        F.lit(15.0)
        + F.lit(10.0) * F.sin(F.lit(two_pi) * day_of_year / 365.0)
        + F.lit(6.0) * F.sin(F.lit(two_pi) * hour_of_day / 24.0)
        + F.randn(SEED + 5) * F.lit(1.5)
    )


def generate(spark: SparkSession, total_rows: int) -> DataFrame:
    """Generates `total_rows` synthetic forecast rows with the same skew
    profile regardless of scale, so results at different `total_rows` are
    comparable points on the same underlying distribution."""
    df = spark.range(total_rows).withColumnRenamed("id", "row_id")
    timestep_index = _timestep_index_expr()
    # Dict order matters: Spark applies these sequentially, and valid_ts
    # references issued_at while temp_c references valid_ts -- both defined
    # earlier in the same call.
    df = df.withColumns(
        {
            "location_id": _location_id_expr(),
            "issued_at": _issued_at_expr(),
            "valid_ts": F.col("issued_at") + F.make_interval(hours=timestep_index * F.lit(3)),
            "lead_time_minutes": (timestep_index * 180).cast("double"),
            "temp_c": _temp_c_expr(),
        }
    )

    return df.select("location_id", "issued_at", "valid_ts", "lead_time_minutes", "temp_c")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=FULL_ROW_COUNT)
    parser.add_argument("--out", required=True)
    parser.add_argument("--partitioned", action="store_true")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("generate_synthetic")
        .master("local[4]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "64")
        .getOrCreate()
    )

    df = generate(spark, args.rows)

    writer = df.write.mode("overwrite")
    if args.partitioned:
        df = df.withColumn("issue_date", F.to_date("issued_at"))
        writer = df.write.mode("overwrite").partitionBy("issue_date")
    writer.parquet(args.out)

    print(f"wrote {args.rows} rows to {args.out} (partitioned={args.partitioned})")
    spark.stop()


if __name__ == "__main__":
    main()
