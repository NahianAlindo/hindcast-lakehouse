"""Sentry integration for the extractor (docs/PLAN.md Phase 7): every
uncaught exception tagged with location_id/endpoint/run_id, so a Sentry
issue points straight at which location+endpoint+run broke instead of just
"the extractor crashed somewhere." A silent no-op if SENTRY_DSN_INGEST isn't
set (local dev, or the accrual-fallback GH Actions runner before that
secret exists there too) -- never a hard dependency for ingestion to run.

Release is the running git commit, not yet an image digest: docs/PLAN.md
Phase 7 calls for digest-based release tagging, but that needs Phase 8's
build-images.yml (Buildx -> GHCR, digest recorded in deploy/manifest.json)
to exist first -- a digest isn't knowable from inside the image that
produces it. A commit SHA is the meaningful, available-today stand-in;
swap GIT_COMMIT for the real digest once Phase 8 ships it.
"""

import functools
import os

import sentry_sdk

_initialized = False


def init_sentry() -> None:
    """Idempotent -- safe to call from standalone main() *and* have
    with_sentry_scope() call it again defensively. Guards against double
    sentry_sdk.init() calls, which is wasteful but not itself harmful; the
    real reason this needs to be idempotent-safe is the two different call
    sites below, not re-entrancy within one process."""
    global _initialized
    if _initialized:
        return
    dsn = os.environ.get("SENTRY_DSN_INGEST")
    if not dsn:
        return
    sentry_sdk.init(
        dsn=dsn,
        release=os.environ.get("GIT_COMMIT", "unknown"),
        # Error tracking only -- this is a small, low-QPS extractor, not a
        # service worth paying APM's per-transaction overhead for.
        traces_sample_rate=0.0,
    )
    _initialized = True


def with_sentry_scope(endpoint: str):
    """Decorates a `fetch_and_land(loc, run_id)`-shaped function: tags any
    exception it raises with endpoint/location_id/run_id, reports it to
    Sentry, then re-raises unchanged -- this never alters control flow
    (a bad location still aborts the run exactly as it did before), it only
    adds visibility into an exception that would already have propagated.

    Calls init_sentry() itself, not just relying on each runner's main() to
    have done so -- Airflow's ingest DAGs import fetch_and_land directly via
    TaskFlow (see owm_current_ingest.py etc.) and never call main() at all,
    so main()-only init would silently leave Sentry uninitialized under
    Airflow specifically (hit this live: traced through why a deliberately
    broken run_current.py import still reported nothing under Airflow).
    """

    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(loc, run_id, *args, **kwargs):
            init_sentry()
            with sentry_sdk.new_scope() as scope:
                scope.set_tag("endpoint", endpoint)
                scope.set_tag("location_id", loc["location_id"])
                scope.set_tag("run_id", run_id)
                try:
                    return fn(loc, run_id, *args, **kwargs)
                except Exception as exc:
                    sentry_sdk.capture_exception(exc)
                    raise

        return wrapper

    return decorator
