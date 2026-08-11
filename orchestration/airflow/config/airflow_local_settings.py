"""Airflow cluster policy (docs/PLAN.md Phase 7: "Airflow task failure
callback" to Sentry) -- lives here, not as `on_failure_callback` wired into
every individual DAG file, because `task_policy` runs once per task during
DAG parsing and can attach the callback to *every* task across *every* DAG
automatically. Adding a 9th DAG later gets Sentry reporting for free instead
of one more place someone has to remember to wire it in.

Loaded automatically by Airflow from $AIRFLOW_HOME/config/airflow_local_
settings.py -- no explicit import anywhere makes this run; that's the
mechanism, not a missing wire-up.
"""

from __future__ import annotations

import os

import sentry_sdk


def _init_sentry_airflow() -> None:
    if sentry_sdk.is_initialized():
        return
    dsn = os.environ.get("SENTRY_DSN_AIRFLOW")
    if not dsn:
        return
    sentry_sdk.init(
        dsn=dsn,
        release=os.environ.get("GIT_COMMIT", "unknown"),
        traces_sample_rate=0.0,
    )


def _sentry_task_failure_callback(context: dict) -> None:
    _init_sentry_airflow()
    if not sentry_sdk.is_initialized():
        return
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context.get("run_id", "unknown")
    with sentry_sdk.new_scope() as scope:
        scope.set_tag("dag_id", dag_id)
        scope.set_tag("task_id", task_id)
        scope.set_tag("run_id", run_id)
        exception = context.get("exception")
        if exception is not None:
            sentry_sdk.capture_exception(exception)
        else:
            sentry_sdk.capture_message(
                f"Airflow task failed: {dag_id}.{task_id} (run_id={run_id})",
                level="error",
            )


def task_policy(task) -> None:
    # Chain onto whatever on_failure_callback(s) a task already has (list or
    # single callable, Airflow supports both) rather than replacing them --
    # this is an addition, not a takeover of failure handling.
    existing = task.on_failure_callback
    if existing is None:
        existing_callbacks = []
    elif isinstance(existing, list):
        existing_callbacks = existing
    else:
        existing_callbacks = [existing]

    task.on_failure_callback = [_sentry_task_failure_callback, *existing_callbacks]
