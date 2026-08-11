-- docs/PLAN.md's marts DQ suite: "no slot has an actual timestamped before
-- its forecast was issued". An actual observation landing before the first
-- forecast for that slot even existed is a logical impossibility -- if
-- this test ever returns rows, it means a timestamp is wrong somewhere
-- upstream (clock skew, a mis-parsed timezone), not a real event.
select
    location_id,
    valid_ts_utc,
    first_forecast_at,
    actual_obs_ts
from {{ ref('fct_forecast_slot') }}
where
    actual_obs_ts is not null
    and actual_obs_ts < first_forecast_at
