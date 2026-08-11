-- 1:1 with silver.obs_air_quality: rename/cast only.
select
    location_id,
    run_id,
    requested_at::timestamp as requested_at_utc,
    source_dt::timestamp as obs_ts_utc,
    aqi::int as aqi,
    co::double as co,
    no::double as no,
    no2::double as no2,
    o3::double as o3,
    so2::double as so2,
    pm2_5::double as pm2_5,
    pm10::double as pm10,
    nh3::double as nh3,
    payload_sha256
from {{ silver_delta_scan('obs_air_quality') }}
