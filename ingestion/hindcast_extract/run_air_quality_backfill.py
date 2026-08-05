"""One-off historical backfill: Air Pollution history, free back to 2020-11-27.

Gives the warehouse a genuine multi-year historical fact table from day one
instead of starting empty (docs/PLAN.md §5 Phase 2). Run once by hand, not on
a schedule -- re-running is safe (idempotent landing, same blob paths) but
pointless once it's done.
"""

import time
import uuid
from datetime import datetime, timedelta, timezone

from client import get
from config import load_locations
from envelope import land_bronze

ENDPOINT = "air_quality_history"
BACKFILL_START = datetime(2020, 11, 27, tzinfo=timezone.utc)
CHUNK_DAYS = 30


def main() -> None:
    run_id = uuid.uuid4().hex[:12]
    locations = load_locations()
    now = datetime.now(timezone.utc)
    total_calls = 0

    for loc in locations:
        chunk_start = BACKFILL_START
        while chunk_start < now:
            chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS), now)
            requested_at = datetime.now(timezone.utc)
            response = get(
                "/data/2.5/air_pollution/history",
                {
                    "lat": loc["lat"],
                    "lon": loc["lon"],
                    "start": int(chunk_start.timestamp()),
                    "end": int(chunk_end.timestamp()),
                },
            )
            total_calls += 1
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
                n = len(payload.get("list", []))
                print(
                    f"[{ENDPOINT}] {loc['location_id']} {chunk_start.date()}..{chunk_end.date()} -> {n} hourly records"
                )
            else:
                print(
                    f"[{ENDPOINT}] {loc['location_id']} {chunk_start.date()}..{chunk_end.date()} -> HTTP {response.status_code}"
                )

            chunk_start = chunk_end
            time.sleep(1)  # ~700 calls total across all locations; stay well under 60/min

    print(f"[{ENDPOINT}] backfill complete: {total_calls} calls, run_id={run_id}")


if __name__ == "__main__":
    main()
