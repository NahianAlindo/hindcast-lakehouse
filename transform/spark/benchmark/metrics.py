"""Pulls real shuffle/spill metrics for a single benchmark run out of
Spark's own UI REST API (http://localhost:<ui_port>/api/v1/...) -- the
same data the Spark UI's Stages tab renders, just queried directly instead
of eyeballed. Recording the max stage id before and after each run and
diffing is what scopes the query to just that run's stages, since local[*]
keeps one SparkSession (and one Spark UI) alive across the whole sweep.
"""

from __future__ import annotations

import time

import requests
from pyspark.sql import SparkSession


def ui_base_url(spark: SparkSession) -> str:
    ui_web_url = spark.sparkContext.uiWebUrl
    assert ui_web_url is not None, "Spark UI is disabled -- benchmark needs it for stage metrics"
    port = ui_web_url.split(":")[-1]
    return f"http://localhost:{port}/api/v1/applications/{spark.sparkContext.applicationId}"


def max_stage_id(spark: SparkSession) -> int:
    resp = requests.get(f"{ui_base_url(spark)}/stages", timeout=10)
    stages = resp.json()
    return max((s["stageId"] for s in stages), default=-1)


def stage_metrics_since(spark: SparkSession, since_stage_id: int) -> dict:
    """Sums shuffle read/write and memory/disk spill bytes across every
    stage with stageId > since_stage_id -- i.e. everything the just-run
    action triggered."""
    resp = requests.get(f"{ui_base_url(spark)}/stages", timeout=10)
    stages = [s for s in resp.json() if s["stageId"] > since_stage_id]
    return {
        "shuffle_read_bytes": sum(s.get("shuffleReadBytes", 0) for s in stages),
        "shuffle_write_bytes": sum(s.get("shuffleWriteBytes", 0) for s in stages),
        "memory_spill_bytes": sum(s.get("memoryBytesSpilled", 0) for s in stages),
        "disk_spill_bytes": sum(s.get("diskBytesSpilled", 0) for s in stages),
        "stage_count": len(stages),
    }


def timed_run(spark: SparkSession, action_fn) -> dict:
    """Runs `action_fn()` (must trigger a Spark action), returns wall-clock
    seconds plus shuffle/spill metrics scoped to exactly this run."""
    before = max_stage_id(spark)
    start = time.perf_counter()
    result = action_fn()
    elapsed = time.perf_counter() - start
    # Spark UI updates asynchronously after the action returns; a short
    # settle delay avoids reading a stages list that's still catching up.
    time.sleep(0.5)
    metrics = stage_metrics_since(spark, before)
    metrics["wall_clock_seconds"] = elapsed
    metrics["result"] = result
    return metrics
