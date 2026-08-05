"""Poll the 5-day/3-hour forecast for every configured location.

The critical extractor: `requested_at` minted here IS `issued_at` for every
one of the ~40 timesteps in the response -- the field the entire lead-time
analysis is built on (docs/PLAN.md / CLAUDE.md non-negotiable constraint).

Also tracks `is_new_model_run`: compares this poll's payload hash against the
last one seen for that location. OWM refreshes forecasts on its own cadence,
so two polls 3h apart can return the same underlying model run -- recorded,
not filtered, since knowing the forecast *didn't* update is itself a finding
(docs/PLAN.md §5 Phase 2).

Cadence: every 3 hours. Run by hand, GitHub Actions accrual-fallback, or a
systemd timer for now; Airflow's `owm_forecast_ingest` DAG takes over in
Phase 3.
"""

import hashlib
import json
import uuid
from datetime import datetime, timezone

from client import get
from config import load_locations
from envelope import land_bronze
from models import validate
from state import read_last_forecast_hashes, write_last_forecast_hashes

ENDPOINT = "forecast"


def main() -> None:
    run_id = uuid.uuid4().hex[:12]
    locations = load_locations()
    last_hashes = read_last_forecast_hashes()
    landed = 0
    new_model_run_count = 0

    for loc in locations:
        requested_at = datetime.now(timezone.utc)
        response = get(
            "/data/2.5/forecast",
            {"lat": loc["lat"], "lon": loc["lon"], "units": "metric"},
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
            continue

        is_valid, validation_error = validate(ENDPOINT, payload)
        if not is_valid:
            print(f"[{ENDPOINT}] {loc['location_id']} -> validation failed (landing anyway): {validation_error}")

        payload_hash = hashlib.sha256(json.dumps(payload).encode()).hexdigest()
        is_new_model_run = last_hashes.get(loc["location_id"]) != payload_hash
        last_hashes[loc["location_id"]] = payload_hash
        if is_new_model_run:
            new_model_run_count += 1

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
        landed += 1
        n_steps = len(payload.get("list", []))
        print(
            f"[{ENDPOINT}] {loc['location_id']} -> {n_steps} timesteps, "
            f"new_model_run={is_new_model_run}, issued_at={requested_at.isoformat()}"
        )

    write_last_forecast_hashes(last_hashes)
    new_model_run_ratio = new_model_run_count / landed if landed else 0.0
    print(
        f"[{ENDPOINT}] landed {landed}/{len(locations)} locations, "
        f"{new_model_run_count} new model runs ({new_model_run_ratio:.0%}), run_id={run_id}"
    )


if __name__ == "__main__":
    main()
