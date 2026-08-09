"""Pandera schemas enforced at the bronze->silver boundary -- the one place
dbt tests can't reach, since dbt only sees what's already landed in the
warehouse (docs/PLAN.md Phase 4).

pandera.pyspark's `.validate()` doesn't raise on failure -- it returns the
same DataFrame with error metadata attached, so `check(df, Schema)` here is
what actually turns a failed check into a hard failure. Range bounds match
docs/PLAN.md's Phase 4 spec (temp_c in [-90, 60] -- Earth's actual recorded
extremes -- humidity_pct in [0, 100], pop in [0, 1]). wind_deg is [0, 360]
inclusive, not the exclusive [0, 360) originally spec'd -- OpenWeatherMap
really does report exactly 360 for due-north wind (hit this live: a real
bronze row failed `less_than(360)`), so 360 and 0 are both valid encodings
of the same direction.
"""

from pyspark.sql import DataFrame, types as T

import pandera.pyspark as pa
from pandera.pyspark import DataFrameModel, Field


class ObsWeatherSchema(DataFrameModel):
    temp_c: T.DoubleType = Field(ge=-90, le=60, nullable=True)
    feels_like_c: T.DoubleType = Field(ge=-90, le=60, nullable=True)
    humidity_pct: T.LongType = Field(ge=0, le=100, nullable=True)
    wind_deg: T.LongType = Field(ge=0, le=360, nullable=True)


class ObsAirQualitySchema(DataFrameModel):
    aqi: T.IntegerType = Field(ge=1, le=5, nullable=True)


class ForecastSchema(DataFrameModel):
    temp_c: T.DoubleType = Field(ge=-90, le=60, nullable=True)
    feels_like_c: T.DoubleType = Field(ge=-90, le=60, nullable=True)
    humidity_pct: T.LongType = Field(ge=0, le=100, nullable=True)
    pop: T.DoubleType = Field(ge=0, le=1, nullable=True)


def check(df: DataFrame, schema: type[DataFrameModel], job_name: str) -> None:
    """Raises ValueError with the pandera error detail if validation fails."""
    result = schema.validate(df)
    errors = result.pandera.errors
    if errors:
        raise ValueError(f"[{job_name}] Pandera validation failed: {dict(errors)}")
