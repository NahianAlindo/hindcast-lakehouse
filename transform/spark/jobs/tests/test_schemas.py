"""schemas.check()'s contract: raise ValueError on an out-of-range row,
stay silent on a valid one. wind_deg=360 is a deliberate regression case --
OpenWeatherMap really does report exactly 360 for due-north wind (hit this
live: a real bronze row failed a `less_than(360)` bound before the schema
was corrected to `le=360`, see schemas.py's docstring).
"""

import pytest
from pyspark.sql import types as T
from schemas import ForecastSchema, ObsAirQualitySchema, ObsWeatherSchema, check

OBS_WEATHER_FIELDS = T.StructType(
    [
        T.StructField("temp_c", T.DoubleType()),
        T.StructField("feels_like_c", T.DoubleType()),
        T.StructField("humidity_pct", T.LongType()),
        T.StructField("wind_deg", T.LongType()),
        T.StructField("weather_code", T.LongType()),
    ]
)


def test_valid_obs_weather_row_passes(spark):
    df = spark.createDataFrame([(21.5, 20.9, 55, 180, 800)], OBS_WEATHER_FIELDS)
    check(df, ObsWeatherSchema, "test")


def test_wind_deg_of_exactly_360_is_valid(spark):
    # The documented live incident: due-north wind reports as 360, not 0.
    df = spark.createDataFrame([(21.5, 20.9, 55, 360, 800)], OBS_WEATHER_FIELDS)
    check(df, ObsWeatherSchema, "test")


def test_temp_c_out_of_range_raises(spark):
    df = spark.createDataFrame([(500.0, 20.9, 55, 180, 800)], OBS_WEATHER_FIELDS)
    with pytest.raises(ValueError, match="Pandera validation failed"):
        check(df, ObsWeatherSchema, "test")


def test_wind_deg_of_361_raises(spark):
    df = spark.createDataFrame([(21.5, 20.9, 55, 361, 800)], OBS_WEATHER_FIELDS)
    with pytest.raises(ValueError, match="Pandera validation failed"):
        check(df, ObsWeatherSchema, "test")


def test_weather_code_outside_owm_range_raises(spark):
    df = spark.createDataFrame([(21.5, 20.9, 55, 180, 999)], OBS_WEATHER_FIELDS)
    with pytest.raises(ValueError, match="Pandera validation failed"):
        check(df, ObsWeatherSchema, "test")


def test_valid_forecast_row_passes(spark):
    fields = T.StructType(
        [
            T.StructField("temp_c", T.DoubleType()),
            T.StructField("feels_like_c", T.DoubleType()),
            T.StructField("humidity_pct", T.LongType()),
            T.StructField("pop", T.DoubleType()),
            T.StructField("wind_deg", T.LongType()),
            T.StructField("weather_code", T.LongType()),
        ]
    )
    df = spark.createDataFrame([(15.0, 14.2, 80, 0.4, 90, 500)], fields)
    check(df, ForecastSchema, "test")


def test_pop_above_one_raises(spark):
    fields = T.StructType(
        [
            T.StructField("temp_c", T.DoubleType()),
            T.StructField("feels_like_c", T.DoubleType()),
            T.StructField("humidity_pct", T.LongType()),
            T.StructField("pop", T.DoubleType()),
            T.StructField("wind_deg", T.LongType()),
            T.StructField("weather_code", T.LongType()),
        ]
    )
    df = spark.createDataFrame([(15.0, 14.2, 80, 1.5, 90, 500)], fields)
    with pytest.raises(ValueError, match="Pandera validation failed"):
        check(df, ForecastSchema, "test")


def test_valid_air_quality_row_passes(spark):
    fields = T.StructType([T.StructField("aqi", T.IntegerType())])
    df = spark.createDataFrame([(3,)], fields)
    check(df, ObsAirQualitySchema, "test")


def test_aqi_outside_owm_1_to_5_scale_raises(spark):
    fields = T.StructType([T.StructField("aqi", T.IntegerType())])
    df = spark.createDataFrame([(6,)], fields)
    with pytest.raises(ValueError, match="Pandera validation failed"):
        check(df, ObsAirQualitySchema, "test")
