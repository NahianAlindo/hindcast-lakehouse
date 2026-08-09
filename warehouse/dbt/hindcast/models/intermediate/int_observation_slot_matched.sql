-- Actual-matching (docs/PLAN.md §6.3b): forecast slots sit on exact 3h UTC
-- boundaries, observations don't, so an equality join is impossible.
-- Tolerance is half a slot width (var: actual_match_tolerance_minutes = 90)
-- so windows never overlap and no observation can be claimed by two slots.
-- LEFT JOIN, so a missing observation produces a row with actual_obs_ts
-- null rather than silently dropping the slot -- fct_forecast_slot turns
-- that into slot_status = 'closed_no_actual'.
{% set tolerance = var('actual_match_tolerance_minutes') %}
{% set tolerance_interval = "interval '" ~ tolerance ~ " minutes'" %}

with slots as (
    select distinct location_id, valid_ts_utc
    from {{ ref('int_forecast_with_leadtime') }}
),

matched as (
    select
        s.location_id,
        s.valid_ts_utc,
        o.obs_ts_utc                                            as actual_obs_ts,
        {{ datediff_minutes('o.obs_ts_utc', 's.valid_ts_utc') }} as match_offset_minutes,
        o.temp_c                                                as temp_actual_c,
        o.wind_speed_ms                                         as wind_speed_actual_ms,
        o.wind_deg                                              as wind_deg_actual,
        o.weather_code                                          as weather_code_actual
    from slots s
    left join {{ ref('stg_owm__current') }} o
           on o.location_id = s.location_id
          and o.obs_ts_utc between s.valid_ts_utc - {{ tolerance_interval }}
                                and s.valid_ts_utc + {{ tolerance_interval }}
    qualify row_number() over (
        partition by s.location_id, s.valid_ts_utc
        order by abs({{ datediff_seconds('o.obs_ts_utc', 's.valid_ts_utc') }}) asc,
                 o.obs_ts_utc asc
    ) = 1
),

-- Secondary windowed measures (docs/PLAN.md §6.3b): mean temp across every
-- observation in the window (smoother than the single point actual) and
-- summed precip -- precipitation is additive, averaging it is wrong. This
-- project doesn't ingest a direct precip-amount field from /weather, so
-- windowed precip is left for when that's added; obs_count_in_window is
-- real today.
windowed as (
    select
        s.location_id,
        s.valid_ts_utc,
        avg(o.temp_c)  as temp_actual_window_mean_c,
        count(o.obs_ts_utc) as obs_count_in_window
    from slots s
    left join {{ ref('stg_owm__current') }} o
           on o.location_id = s.location_id
          and o.obs_ts_utc between s.valid_ts_utc - {{ tolerance_interval }}
                                and s.valid_ts_utc + {{ tolerance_interval }}
    group by s.location_id, s.valid_ts_utc
)

select
    m.*,
    w.temp_actual_window_mean_c,
    w.obs_count_in_window
from matched m
left join windowed w
       on w.location_id = m.location_id
      and w.valid_ts_utc = m.valid_ts_utc
