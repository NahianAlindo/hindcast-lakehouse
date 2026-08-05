"""Minimal OpenWeatherMap HTTP client.

Retries 429 (honouring Retry-After) and 5xx with backoff. Never raises on a
final bad status — the caller lands the envelope either way (bronze is
lossless by contract: a 401/404 gets recorded, not silently dropped).
"""

import time

import httpx

from config import OWM_API_KEY, OWM_BASE_URL


def get(path: str, params: dict, max_attempts: int = 5) -> httpx.Response:
    query = {**params, "appid": OWM_API_KEY}
    response = None
    for attempt in range(1, max_attempts + 1):
        response = httpx.get(f"{OWM_BASE_URL}{path}", params=query, timeout=10.0)
        if response.status_code == 429:
            wait_s = float(response.headers.get("Retry-After", 2**attempt))
            time.sleep(wait_s)
            continue
        if response.status_code >= 500:
            time.sleep(2**attempt)
            continue
        return response
    return response
