"""Submits metrics directly to Datadog's HTTP API (docs/PLAN.md Phase 7) --
no Datadog Agent involved, deliberately: every component that emits a
metric in this project is a one-shot container (extractor runner, Spark
job, dbt run, sync script), not a long-lived service a local dogstatsd
agent could sit next to. Stdlib-only (urllib), not requests/httpx, so this
one small file can be copied as-is into every image that needs it
(ingestion/hindcast_extract already ships in the Airflow image; the same
copy-a-specific-file pattern export_silver_snapshot.py etc. already use)
without adding a dependency to Dockerfiles that don't otherwise need one.

Silent no-op if DATADOG_API_KEY isn't set, matching ingestion/hindcast_
extract/observability.py's Sentry pattern -- metrics are a nice-to-have,
never a hard dependency for the pipeline to run. Every call is wrapped in a
broad except, for the same reason: a Datadog outage or a typo'd site name
must never be what breaks an ingest poll or a dbt build.
"""

from __future__ import annotations

import json
import os
import time
import urllib.request

DD_SITE = os.environ.get("DATADOG_SITE", "us5.datadoghq.com")
_TIMEOUT_S = 5


def _post(path: str, body: dict) -> None:
    api_key = os.environ.get("DATADOG_API_KEY")
    if not api_key:
        return
    req = urllib.request.Request(
        f"https://api.{DD_SITE}/api/v1/{path}",
        data=json.dumps(body).encode(),
        headers={"DD-API-KEY": api_key, "Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=_TIMEOUT_S)
    except Exception:
        pass


def _submit_series(metric: str, value: float, metric_type: str, tags: list[str] | None) -> None:
    _post(
        "series",
        {
            "series": [
                {
                    "metric": metric,
                    "points": [[int(time.time()), value]],
                    "type": metric_type,
                    "tags": tags or [],
                }
            ]
        },
    )


def submit_count(metric: str, value: float, tags: list[str] | None = None) -> None:
    _submit_series(metric, value, "count", tags)


def submit_gauge(metric: str, value: float, tags: list[str] | None = None) -> None:
    _submit_series(metric, value, "gauge", tags)


def submit_distribution(metric: str, value: float, tags: list[str] | None = None) -> None:
    # Distributions are a separate top-level endpoint, not a `type` on the
    # series endpoint -- Datadog computes percentiles (p50/p95/p99) server-
    # side from the raw samples, which is what histogram-shaped metrics
    # here (latency, job duration) need (docs/PLAN.md's
    # `hindcast.match.offset_minutes_p95` etc.), rather than this code
    # having to pre-aggregate percentiles itself. points value is a *list*
    # of samples per timestamp (even though this call always submits one),
    # matching the API's own shape.
    _post(
        "distribution_points",
        {
            "series": [
                {
                    "metric": metric,
                    "points": [[int(time.time()), [value]]],
                    "tags": tags or [],
                }
            ]
        },
    )
