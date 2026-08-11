-- Transaction fact, one row per (location, hour) -- docs/PLAN.md §6.1.
select
    {{ dbt_utils.generate_surrogate_key(['o.location_id', 'o.obs_ts_utc']) }} as air_quality_observation_key,
    l.location_key,
    d.date_key,
    t.time_key,
    aq.air_quality_band_key,
    pr.pipeline_run_key,
    o.location_id,
    o.run_id,
    o.obs_ts_utc,
    o.requested_at_utc,
    o.aqi,
    o.co,
    o.no,
    o.no2,
    o.o3,
    o.so2,
    o.pm2_5,
    o.pm10,
    o.nh3
from {{ ref('stg_owm__air_quality') }} as o
left join {{ ref('dim_location') }} as l on o.location_id = l.location_id
left join {{ ref('dim_date') }} as d on d.date_day = cast(o.obs_ts_utc as date)
left join {{ ref('dim_time') }} as t on t.hour = extract(hour from o.obs_ts_utc)
left join {{ ref('dim_air_quality_band') }} as aq on o.aqi = aq.aqi
left join {{ ref('dim_pipeline_run') }} as pr on o.run_id = pr.run_id
