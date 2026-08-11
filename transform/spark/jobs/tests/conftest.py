"""A plain local[1] SparkSession -- schemas.py's check() only validates
in-memory DataFrames, no Delta/ADLS JARs or real bronze/silver data needed.
Session-scoped: PySpark's JVM startup (~10-15s) is the dominant cost here,
not test count.
"""

import os
import sys

import pytest
from pyspark.sql import SparkSession

# Spark's Python workers otherwise resolve `python3` off PATH, which can
# diverge from the interpreter running pytest (hit this live in WSL2 as
# "Please check environment variables PYSPARK_PYTHON and
# PYSPARK_DRIVER_PYTHON are correctly set") -- pin both to sys.executable.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("hindcast_schema_tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
