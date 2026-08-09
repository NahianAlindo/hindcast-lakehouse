-- Transaction fact, one row per (location, distinct source observation
-- timestamp) -- docs/PLAN.md §6.1. Surrogate keys throughout; no natural
-- keys carried except as degenerate dimensions (location_id, run_id) for
-- traceability back to bronze.
select
    {{ dbt_utils.generate_surrogate_key(['o.location_id', 'o.obs_ts_utc']) }} as weather_observation_key,
    l.location_key,
    d.date_key,
    t.time_key,
    wc.weather_condition_key,
    pr.pipeline_run_key,
    o.location_id,
    o.run_id,
    o.obs_ts_utc,
    o.requested_at_utc,
    o.temp_c,
    o.feels_like_c,
    o.temp_min_c,
    o.temp_max_c,
    o.pressure_hpa,
    o.humidity_pct,
    o.wind_speed_ms,
    o.wind_deg,
    o.wind_gust_ms,
    o.clouds_pct,
    o.visibility_m
from {{ ref('stg_owm__current') }} o
left join {{ ref('dim_location') }} l on l.location_id = o.location_id
left join {{ ref('dim_date') }} d on d.date_day = cast(o.obs_ts_utc as date)
left join {{ ref('dim_time') }} t on t.hour = extract(hour from o.obs_ts_utc)
left join {{ ref('dim_weather_condition') }} wc on wc.code = o.weather_code
left join {{ ref('dim_pipeline_run') }} pr on pr.run_id = o.run_id
