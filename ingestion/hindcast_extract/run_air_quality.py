"""Poll current air pollution for every configured location.

Cadence: hourly. Run standalone (python run_air_quality.py), via GitHub
Actions accrual-fallback, or -- Phase 3 -- one task per location, dynamically
mapped, in Airflow's `owm_air_quality_ingest` DAG. fetch_and_land() is the
per-location unit both paths call; main() is a thin loop over it for
standalone/CLI use. The one-off historical backfill lives in
run_air_quality_backfill.py.
"""

import uuid
from datetime import datetime, timezone

from client import get
from config import load_locations
from envelope import land_bronze
from models import validate
from observability import init_sentry, with_sentry_scope

ENDPOINT = "air_quality"


@with_sentry_scope(ENDPOINT)
def fetch_and_land(loc: dict, run_id: str) -> bool:
    """Fetch current air pollution for one location and land it. Returns True if HTTP 200."""
    requested_at = datetime.now(timezone.utc)
    response = get(
        "/data/2.5/air_pollution",
        {"lat": loc["lat"], "lon": loc["lon"]},
        endpoint=ENDPOINT,
    )
    payload = response.json() if response.status_code == 200 else {"error": response.text}

    validation_error = None
    if response.status_code == 200:
        is_valid, validation_error = validate(ENDPOINT, payload)
        if not is_valid:
            print(f"[{ENDPOINT}] {loc['location_id']} -> validation failed (landing anyway): {validation_error}")

    land_bronze(
        endpoint=ENDPOINT,
        location_id=loc["location_id"],
        requested_at=requested_at,
        http_status=response.status_code,
        url_redacted=str(response.url).split("appid=")[0] + "appid=***",
        payload=payload,
        run_id=run_id,
        validation_error=validation_error,
    )
    if response.status_code != 200:
        print(f"[{ENDPOINT}] {loc['location_id']} -> HTTP {response.status_code}")
    return response.status_code == 200


def main() -> None:
    init_sentry()
    run_id = uuid.uuid4().hex[:12]
    locations = load_locations()
    landed = sum(fetch_and_land(loc, run_id) for loc in locations)
    print(f"[{ENDPOINT}] landed {landed}/{len(locations)} locations, run_id={run_id}")


if __name__ == "__main__":
    main()
