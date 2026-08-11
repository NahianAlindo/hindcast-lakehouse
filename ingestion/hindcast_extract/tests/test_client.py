"""client.get()'s retry contract (docs/PLAN.md §5 Phase 2): retry 429
(honouring Retry-After) and 5xx with backoff, never retry anything else --
bronze lands whatever the final status was, lossless by contract. respx
mocks httpx at the transport level, so these never make a real request.
"""

import time

import httpx
import respx
from client import get

URL = "https://api.openweathermap.org/data/2.5/weather"


@respx.mock
def test_returns_immediately_on_200():
    route = respx.get(URL).mock(return_value=httpx.Response(200, json={"cod": 200}))
    response = get("/data/2.5/weather", {"lat": 1, "lon": 1}, endpoint="current")
    assert response.status_code == 200
    assert route.call_count == 1


@respx.mock
def test_does_not_retry_a_401():
    # A bad API key won't fix itself on retry -- and bronze is lossless by
    # contract, so the caller lands the 401 rather than the extractor
    # silently looping on it.
    route = respx.get(URL).mock(
        return_value=httpx.Response(401, json={"cod": 401, "message": "Invalid API key."})
    )
    response = get("/data/2.5/weather", {"lat": 1, "lon": 1}, endpoint="current")
    assert response.status_code == 401
    assert route.call_count == 1


@respx.mock
def test_retries_a_429_honouring_retry_after(monkeypatch):
    slept_for = []
    monkeypatch.setattr(time, "sleep", slept_for.append)
    route = respx.get(URL).mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "7"}),
            httpx.Response(200, json={"cod": 200}),
        ]
    )
    response = get("/data/2.5/weather", {"lat": 1, "lon": 1}, endpoint="current")
    assert response.status_code == 200
    assert route.call_count == 2
    assert slept_for == [7.0]


@respx.mock
def test_retries_5xx_with_backoff(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda s: None)
    route = respx.get(URL).mock(
        side_effect=[httpx.Response(503), httpx.Response(200, json={"cod": 200})]
    )
    response = get("/data/2.5/weather", {"lat": 1, "lon": 1}, endpoint="current")
    assert response.status_code == 200
    assert route.call_count == 2


@respx.mock
def test_gives_up_after_max_attempts_and_returns_the_last_bad_response(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda s: None)
    route = respx.get(URL).mock(return_value=httpx.Response(503))
    response = get("/data/2.5/weather", {"lat": 1, "lon": 1}, endpoint="current", max_attempts=3)
    assert response.status_code == 503
    assert route.call_count == 3
