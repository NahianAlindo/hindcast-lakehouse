"""Minimal OpenWeatherMap HTTP client.

Retries 429 (honouring Retry-After) and 5xx with backoff. Never raises on a
final bad status — the caller lands the envelope either way (bronze is
lossless by contract: a 401/404 gets recorded, not silently dropped).
"""

import sys
import time
from pathlib import Path

import httpx
from config import OWM_API_KEY, OWM_BASE_URL

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "observability"))
from datadog_metrics import submit_count, submit_distribution  # noqa: E402


def get(
    path: str, params: dict, endpoint: str = "unknown", max_attempts: int = 5
) -> httpx.Response:
    query = {**params, "appid": OWM_API_KEY}
    response = None
    started = time.monotonic()
    for attempt in range(1, max_attempts + 1):
        response = httpx.get(f"{OWM_BASE_URL}{path}", params=query, timeout=10.0)
        if response.status_code == 429:
            wait_s = float(response.headers.get("Retry-After", 2**attempt))
            time.sleep(wait_s)
            continue
        if response.status_code >= 500:
            time.sleep(2**attempt)
            continue
        break
    assert response is not None, "max_attempts must be >= 1"

    # docs/PLAN.md Phase 7: hindcast.ingest.calls / .latency_ms. Tagged by
    # final status only (not each retried attempt) -- one call from the
    # caller's perspective, regardless of how many HTTP round trips it took.
    latency_ms = (time.monotonic() - started) * 1000
    tags = [f"endpoint:{endpoint}", f"status:{response.status_code}"]
    submit_count("hindcast.ingest.calls", 1, tags=tags)
    submit_distribution("hindcast.ingest.latency_ms", latency_ms, tags=[f"endpoint:{endpoint}"])
    return response
