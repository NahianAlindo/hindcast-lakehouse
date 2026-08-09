"""Fan out over locations via dynamic task mapping (docs/PLAN.md Phase 3).

Cadence matches the standalone extractor's: hourly. catchup=False -- a
missed air-quality poll is meaningless, the moment is gone (CLAUDE.md
non-negotiable constraint). The one-off historical backfill is not a DAG --
it already ran once by hand (run_air_quality_backfill.py) and re-running it
is safe but pointless once done.
"""

from __future__ import annotations

import sys
import uuid
from datetime import datetime

sys.path.insert(0, "/opt/airflow/ingestion/hindcast_extract")

from airflow.sdk import dag, task
from config import load_locations
from run_air_quality import fetch_and_land


@dag(
    dag_id="owm_air_quality_ingest",
    schedule="@hourly",
    start_date=datetime(2026, 8, 9),
    catchup=False,
    tags=["ingest", "air_quality"],
)
def owm_air_quality_ingest():
    @task
    def make_extractor_run_id() -> str:
        return uuid.uuid4().hex[:12]

    @task
    def get_locations() -> list[dict]:
        return load_locations()

    # `run_id` collides with Airflow's own reserved task-context variable name
    # (every task gets one injected automatically for the DAG run) -- TaskFlow's
    # partial() rejects it outright rather than silently shadowing it.
    @task
    def fetch_one(loc: dict, extractor_run_id: str) -> bool:
        return fetch_and_land(loc, extractor_run_id)

    fetch_one.partial(extractor_run_id=make_extractor_run_id()).expand(loc=get_locations())


owm_air_quality_ingest()
