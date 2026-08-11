# Phase 7 (docs/PLAN.md): one dashboard, four rows (Ingest health / Slot
# lifecycle / Transform / Warehouse+DQ), plus the two monitors the plan
# calls for. As code, not a manual UI dashboard nobody can diff or rebuild.
#
# Two metrics are deliberately *not* emitted directly by any job --
# monthly_call_budget_pct and forecast.new_model_run_ratio are dashboard-
# side query formulas over the raw hindcast.ingest.calls / hindcast.
# forecast.new_model_run counts instead (see ingestion/hindcast_extract/
# client.py and run_forecast.py's own comments for why: a derived
# percentage computed from one source-of-truth counter is simpler and more
# robust than a second stateful counter the extractor would have to
# maintain itself).
#
# Monitor paging: the plan calls for these to page via Slack. No Slack
# integration is configured in this Datadog org (out of scope here -- that
# needs the user to connect it under Datadog's own Integrations settings),
# so these are real, functioning monitors with clear alert messages, but
# won't actually notify anyone until a Slack (or email/PagerDuty/etc.)
# recipient is added to the `message` field's @-mention.

resource "datadog_dashboard" "hindcast_pipeline_health" {
  title       = "Hindcast Pipeline Health"
  description = "docs/PLAN.md Phase 7 -- ingest health, slot lifecycle, transform, warehouse+DQ."
  layout_type = "ordered"

  widget {
    group_definition {
      title       = "Ingest Health"
      layout_type = "ordered"

      widget {
        timeseries_definition {
          title = "Calls by endpoint + status"
          request {
            q            = "sum:hindcast.ingest.calls{*} by {endpoint,status}.as_count()"
            display_type = "bars"
          }
        }
      }

      widget {
        timeseries_definition {
          title = "Latency p95 (ms)"
          request {
            q = "p95:hindcast.ingest.latency_ms{*} by {endpoint}"
          }
        }
      }

      widget {
        query_value_definition {
          title = "Monthly call budget used (%, rolling 30d / 1M cap)"
          request {
            q          = "sum:hindcast.ingest.calls{*}.rollup(sum, 2592000) / 1000000 * 100"
            aggregator = "last"
          }
        }
      }
    }
  }

  widget {
    group_definition {
      title       = "Slot Lifecycle"
      layout_type = "ordered"

      widget {
        query_value_definition {
          title = "Awaiting actual (count)"
          request {
            q          = "avg:hindcast.slot.awaiting_actual_count{*}"
            aggregator = "last"
          }
        }
      }

      widget {
        query_value_definition {
          title = "Closed, no actual (%) -- primary SLI"
          request {
            q          = "avg:hindcast.slot.closed_no_actual_ratio{*} * 100"
            aggregator = "last"
          }
        }
      }

      widget {
        query_value_definition {
          title = "New model run ratio (%)"
          request {
            q          = "sum:hindcast.forecast.new_model_run{is_new:true}.as_count() / sum:hindcast.forecast.new_model_run{*}.as_count() * 100"
            aggregator = "last"
          }
        }
      }
    }
  }

  widget {
    group_definition {
      title       = "Transform"
      layout_type = "ordered"

      widget {
        timeseries_definition {
          title = "Spark job duration (s) by job"
          request {
            q = "avg:hindcast.spark.job_duration_s{*} by {job}"
          }
        }
      }

      widget {
        timeseries_definition {
          title = "Databricks sync duration (s)"
          request {
            q = "avg:hindcast.databricks.sync_duration_s{*}"
          }
        }
      }

      widget {
        timeseries_definition {
          title = "Databricks COPY INTO rows by table"
          request {
            q = "avg:hindcast.databricks.copy_into_rowcount{*} by {table}"
          }
        }
      }
    }
  }

  widget {
    group_definition {
      title       = "Warehouse + DQ"
      layout_type = "ordered"

      widget {
        timeseries_definition {
          title = "dbt test failures by target"
          request {
            q            = "sum:hindcast.dbt.test_failures{*} by {target}.as_count()"
            display_type = "bars"
          }
        }
      }

      widget {
        query_value_definition {
          title = "Match offset p95 (min)"
          request {
            q          = "avg:hindcast.match.offset_minutes_p95{*}"
            aggregator = "last"
          }
        }
      }
    }
  }
}

resource "datadog_monitor" "closed_no_actual_ratio" {
  name    = "hindcast: closed_no_actual_ratio > 5%"
  type    = "metric alert"
  message = <<-EOT
    The pipeline's primary SLI (docs/PLAN.md Phase 7): more than 5% of forecast
    slots that reached closure never got a matching actual observation. This is
    "the pipeline failing to do the thing it exists to do" -- check the
    owm_current_ingest DAG and int_observation_slot_matched's join tolerance.
  EOT
  query   = "avg(last_1h):avg:hindcast.slot.closed_no_actual_ratio{*} > 0.05"

  monitor_thresholds {
    critical = 0.05
  }

  notify_no_data    = false
  renotify_interval = 60
  tags              = ["project:hindcast", "phase:7"]
}

resource "datadog_monitor" "ingest_401" {
  name    = "hindcast: ingest getting HTTP 401 from OpenWeatherMap"
  type    = "metric alert"
  message = <<-EOT
    The extractor is getting 401s from OpenWeatherMap -- the API key is likely
    invalid, expired, or revoked. This blocks ALL ingestion across every
    endpoint, and a missed poll is permanently lost (docs/PLAN.md's core
    domain constraint: OWM doesn't sell its own past forecasts). Check the key
    in Key Vault (owm-api-key) against OWM's billing/API-keys page.
  EOT
  query   = "sum(last_5m):sum:hindcast.ingest.calls{status:401}.as_count() > 0"

  monitor_thresholds {
    critical = 0
  }

  notify_no_data    = false
  renotify_interval = 60
  tags              = ["project:hindcast", "phase:7"]
}
