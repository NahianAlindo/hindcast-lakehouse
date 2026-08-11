-- 1:1 with silver.fct_forecast_issue_raw: rename/cast only. lead_time_minutes
-- is already extractor/Spark-derived from issued_at (never re-inferred from a
-- schedule, per CLAUDE.md's single most load-bearing extractor rule) -- kept
-- as-is here, re-derived as lead_time_hours in int_forecast_with_leadtime.
select
    location_id,
    run_id,
    issued_at::timestamp as issued_at_utc,
    valid_ts::timestamp as valid_ts_utc,
    lead_time_minutes::double as lead_time_minutes,
    temp_c::double as temp_c,
    feels_like_c::double as feels_like_c,
    temp_min_c::double as temp_min_c,
    temp_max_c::double as temp_max_c,
    pressure_hpa::bigint as pressure_hpa,
    humidity_pct::bigint as humidity_pct,
    pop::double as pop,
    wind_speed_ms::double as wind_speed_ms,
    wind_deg::bigint as wind_deg,
    weather_code::bigint as weather_code,
    weather_main,
    weather_description,
    weather_icon,
    payload_sha256
from {{ silver_delta_scan('fct_forecast_issue_raw') }}
