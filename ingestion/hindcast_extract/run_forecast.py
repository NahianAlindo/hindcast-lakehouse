"""Poll the 5-day/3-hour forecast for every configured location.

The critical extractor: `requested_at` minted here IS `issued_at` for every
one of the ~40 timesteps in the response — the field the entire lead-time
analysis is built on (docs/PLAN.md CLAUDE.md non-negotiable constraint).
Cadence: every 3 hours. Run by hand or a timer for now; Airflow's
`owm_forecast_ingest` DAG takes over in Phase 3.
"""

import uuid
from datetime import datetime, timezone

from client import get
from config import load_locations
from envelope import land_bronze

ENDPOINT = "forecast"


def main() -> None:
    run_id = uuid.uuid4().hex[:12]
    locations = load_locations()
    landed = 0

    for loc in locations:
        requested_at = datetime.now(timezone.utc)
        response = get(
            "/data/2.5/forecast",
            {"lat": loc["lat"], "lon": loc["lon"], "units": "metric"},
        )
        payload = response.json() if response.status_code == 200 else {"error": response.text}
        land_bronze(
            endpoint=ENDPOINT,
            location_id=loc["location_id"],
            requested_at=requested_at,
            http_status=response.status_code,
            url_redacted=str(response.url).split("appid=")[0] + "appid=***",
            payload=payload,
            run_id=run_id,
        )
        if response.status_code == 200:
            landed += 1
            n_steps = len(payload.get("list", []))
            print(f"[{ENDPOINT}] {loc['location_id']} -> {n_steps} timesteps @ issued_at={requested_at.isoformat()}")
        else:
            print(f"[{ENDPOINT}] {loc['location_id']} -> HTTP {response.status_code}")

    print(f"[{ENDPOINT}] landed {landed}/{len(locations)} locations, run_id={run_id}")


if __name__ == "__main__":
    main()
