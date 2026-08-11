-- Transaction fact (append-only ledger), one row per (location, issued_at,
-- valid_ts) -- every predicted timestep from every API response, unlike
-- fct_forecast_slot which keeps only the 8 milestone selections per slot
-- (docs/PLAN.md §6.1). This is the full raw history the milestone pivot
-- and the accumulating snapshot both read from.
--
-- dim_time role-plays twice: valid_time_utc_key (straightforward) and
-- valid_time_local_key, computed via the location's IANA tz -- "was the
-- afternoon forecast worse?" is a local question, and DST correctness
-- depends on doing this conversion with a real zone name, not a stored
-- offset (docs/PLAN.md's DST decision, §2 row 6).
with base as (
    select
        f.*,
        l.location_key,
        l.iana_tz,
        {{ to_local_timestamp('f.valid_ts_utc', 'l.iana_tz') }} as valid_ts_local
    from {{ ref('int_forecast_with_leadtime') }} as f
    left join {{ ref('dim_location') }} as l on f.location_id = l.location_id
)

select
    {{ dbt_utils.generate_surrogate_key(['b.location_id', 'b.issued_at_utc', 'b.valid_ts_utc']) }}
        as forecast_issue_key,
    b.location_key,
    dd.date_key as valid_date_key,
    tu.time_key as valid_time_utc_key,
    tl.time_key as valid_time_local_key,
    wc.weather_condition_key,
    pr.pipeline_run_key,
    b.location_id,
    b.run_id,
    b.issued_at_utc,
    b.valid_ts_utc,
    b.valid_ts_local,
    b.lead_time_minutes,
    b.lead_time_hours,
    b.temp_c,
    b.feels_like_c,
    b.temp_min_c,
    b.temp_max_c,
    b.pressure_hpa,
    b.humidity_pct,
    b.pop
from base as b
left join {{ ref('dim_date') }} as dd on dd.date_day = cast(b.valid_ts_utc as date)
left join {{ ref('dim_time') }} as tu on tu.hour = extract(hour from b.valid_ts_utc)
left join {{ ref('dim_time') }} as tl on tl.hour = extract(hour from b.valid_ts_local)
left join {{ ref('dim_weather_condition') }} as wc on b.weather_code = wc.code
left join {{ ref('dim_pipeline_run') }} as pr on b.run_id = pr.run_id
