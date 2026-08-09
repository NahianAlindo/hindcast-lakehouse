-- 1:1 with silver.obs_weather: rename/cast only, no business logic here
-- (docs/PLAN.md §5 Phase 5's staging-layer rule).
select
    location_id,
    run_id,
    requested_at::timestamp as requested_at_utc,
    source_dt::timestamp    as obs_ts_utc,
    temp_c::double           as temp_c,
    feels_like_c::double     as feels_like_c,
    temp_min_c::double       as temp_min_c,
    temp_max_c::double       as temp_max_c,
    pressure_hpa::bigint     as pressure_hpa,
    humidity_pct::bigint     as humidity_pct,
    wind_speed_ms::double    as wind_speed_ms,
    wind_deg::bigint         as wind_deg,
    wind_gust_ms::double     as wind_gust_ms,
    clouds_pct::bigint       as clouds_pct,
    visibility_m::bigint     as visibility_m,
    weather_code::bigint     as weather_code,
    weather_main,
    weather_description,
    weather_icon,
    payload_sha256
from {{ silver_delta_scan('obs_weather') }}
