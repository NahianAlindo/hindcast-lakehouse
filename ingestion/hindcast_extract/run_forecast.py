"""Poll the 5-day/3-hour forecast for every configured location.

The critical extractor: `requested_at` minted here IS `issued_at` for every
one of the ~40 timesteps in the response -- the field the entire lead-time
analysis is built on (docs/PLAN.md / CLAUDE.md non-negotiable constraint).

Also tracks `is_new_model_run`: compares this poll's payload hash against the
last one seen for that location. OWM refreshes forecasts on its own cadence,
so two polls 3h apart can return the same underlying model run -- recorded,
not filtered, since knowing the forecast *didn't* update is itself a finding
(docs/PLAN.md §5 Phase 2).

Cadence: every 3 hours. Run standalone (python run_forecast.py), via GitHub
Actions accrual-fallback, or -- Phase 3 -- one task per location, dynamically
mapped, in Airflow's `owm_forecast_ingest` DAG. fetch_and_land() is the
per-location unit both paths call; main() is a thin loop over it for
standalone/CLI use.
"""

import hashlib
import json
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

from client import get
from config import load_locations
from envelope import land_bronze
from models import validate
from state import read_last_forecast_hashes, write_last_forecast_hashes

from observability import init_sentry, with_sentry_scope

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "observability"))
from datadog_metrics import submit_count  # noqa: E402

ENDPOINT = "forecast"


@with_sentry_scope(ENDPOINT)
def fetch_and_land(loc: dict, run_id: str) -> tuple[bool, bool]:
    """Fetch the forecast for one location and land it. Returns (landed, is_new_model_run).

    Reads and writes this location's dedup-state entry directly rather than
    batching across all locations, so this is safe to call independently per
    mapped Airflow task instance. That does mean two truly concurrent calls
    could race on the shared state blob (each reads the same snapshot, last
    write wins) -- Airflow's owm_forecast_ingest DAG avoids this by running
    its mapped tasks through a dedicated 1-slot pool rather than in parallel,
    trading the full parallelism dynamic task mapping would otherwise give
    for a state model that stays simple. Standalone/CLI use (main(), below)
    is single-threaded already and was never at risk.
    """
    requested_at = datetime.now(UTC)
    response = get(
        "/data/2.5/forecast",
        {"lat": loc["lat"], "lon": loc["lon"], "units": "metric"},
        endpoint=ENDPOINT,
    )
    payload = response.json() if response.status_code == 200 else {"error": response.text}

    if response.status_code != 200:
        land_bronze(
            endpoint=ENDPOINT,
            location_id=loc["location_id"],
            requested_at=requested_at,
            http_status=response.status_code,
            url_redacted=str(response.url).split("appid=")[0] + "appid=***",
            payload=payload,
            run_id=run_id,
        )
        print(f"[{ENDPOINT}] {loc['location_id']} -> HTTP {response.status_code}")
        return False, False

    is_valid, validation_error = validate(ENDPOINT, payload)
    if not is_valid:
        print(
            f"[{ENDPOINT}] {loc['location_id']} -> "
            f"validation failed (landing anyway): {validation_error}"
        )

    payload_hash = hashlib.sha256(json.dumps(payload).encode()).hexdigest()
    last_hashes = read_last_forecast_hashes()
    is_new_model_run = last_hashes.get(loc["location_id"]) != payload_hash
    last_hashes[loc["location_id"]] = payload_hash
    write_last_forecast_hashes(last_hashes)

    # docs/PLAN.md Phase 7: hindcast.forecast.new_model_run_ratio. Emitted
    # per-location here, not as a pre-computed ratio in main() -- Airflow's
    # owm_forecast_ingest DAG maps this over locations as separate task
    # instances (see that DAG's dynamic task mapping) and never calls
    # main()'s aggregate loop at all, so a main()-only ratio would silently
    # never fire under the actual production execution path. The ratio
    # itself is a dashboard-side formula (sum is_new:true / sum total),
    # same pattern as monthly_call_budget_pct.
    submit_count(
        "hindcast.forecast.new_model_run",
        1,
        tags=[f"is_new:{str(is_new_model_run).lower()}"],
    )

    land_bronze(
        endpoint=ENDPOINT,
        location_id=loc["location_id"],
        requested_at=requested_at,
        http_status=response.status_code,
        url_redacted=str(response.url).split("appid=")[0] + "appid=***",
        payload=payload,
        run_id=run_id,
        is_new_model_run=is_new_model_run,
        validation_error=validation_error,
    )
    n_steps = len(payload.get("list", []))
    print(
        f"[{ENDPOINT}] {loc['location_id']} -> {n_steps} timesteps, "
        f"new_model_run={is_new_model_run}, issued_at={requested_at.isoformat()}"
    )
    return True, is_new_model_run


def main() -> None:
    init_sentry()
    run_id = uuid.uuid4().hex[:12]
    locations = load_locations()
    landed = 0
    new_model_run_count = 0

    for loc in locations:
        ok, is_new_model_run = fetch_and_land(loc, run_id)
        if ok:
            landed += 1
            if is_new_model_run:
                new_model_run_count += 1

    new_model_run_ratio = new_model_run_count / landed if landed else 0.0
    print(
        f"[{ENDPOINT}] landed {landed}/{len(locations)} locations, "
        f"{new_model_run_count} new model runs ({new_model_run_ratio:.0%}), run_id={run_id}"
    )


if __name__ == "__main__":
    main()
