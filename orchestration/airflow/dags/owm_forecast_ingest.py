"""Fan out over locations via dynamic task mapping (docs/PLAN.md Phase 3).

Cadence matches the standalone extractor's: every 3 hours. catchup=False --
a missed forecast poll is meaningless, the moment is gone (CLAUDE.md
non-negotiable constraint). `requested_at`, minted inside fetch_and_land,
IS `issued_at` for every timestep in the response -- the field the entire
lead-time analysis is built on.

fetch_one runs in a dedicated 1-slot pool (created by deploy.sh via
`airflow pools set forecast_serial 1 ...`), not left to run in parallel like
the other two ingest DAGs. run_forecast.fetch_and_land does its own
read-modify-write of the shared forecast dedup-state blob per location;
two truly concurrent calls would race on it (each reads the same snapshot,
last write wins). Serializing keeps per-location task instances -- and
their independent retry/observability in the Airflow UI, the actual point
of dynamic task mapping -- without needing to make state.py's storage
format any more complicated than a single JSON blob.
"""

from __future__ import annotations

import sys
import uuid
from datetime import datetime

sys.path.insert(0, "/opt/airflow/ingestion/hindcast_extract")

from airflow.sdk import dag, task
from config import load_locations
from run_forecast import fetch_and_land

FORECAST_POOL = "forecast_serial"


@dag(
    dag_id="owm_forecast_ingest",
    schedule="0 */3 * * *",
    start_date=datetime(2026, 8, 9),
    catchup=False,
    tags=["ingest", "forecast"],
)
def owm_forecast_ingest():
    @task
    def make_extractor_run_id() -> str:
        return uuid.uuid4().hex[:12]

    @task
    def get_locations() -> list[dict]:
        return load_locations()

    # `run_id` collides with Airflow's own reserved task-context variable name
    # (every task gets one injected automatically for the DAG run) -- TaskFlow's
    # partial() rejects it outright rather than silently shadowing it.
    @task(pool=FORECAST_POOL)
    def fetch_one(loc: dict, extractor_run_id: str) -> tuple[bool, bool]:
        return fetch_and_land(loc, extractor_run_id)

    fetch_one.partial(extractor_run_id=make_extractor_run_id()).expand(loc=get_locations())


owm_forecast_ingest()
