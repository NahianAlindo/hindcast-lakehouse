-- The persistence baseline (docs/PLAN.md §6.2's `temp_persistence_c`,
-- `pop_persistence`): the naive "assume it'll be like it was 24h ago"
-- predictor every forecast-skill metric needs to beat to be worth
-- anything. Looked up the same way actuals are matched to slots --
-- nearest real observation within +/-90min, this time of (valid_ts - 24h)
-- rather than valid_ts itself.
--
-- is_precipitation is inlined via the same OWM code ranges
-- dim_weather_condition uses, not a join to that mart -- intermediate
-- models feed marts, not the other way around.
{% set tolerance = var('actual_match_tolerance_minutes') %}

with slots as (
    select distinct location_id, valid_ts_utc
    from {{ ref('int_forecast_with_leadtime') }}
),

lookback as (
    select
        location_id,
        valid_ts_utc,
        valid_ts_utc - interval '24 hours' as lookback_ts
    from slots
),

matched as (
    select
        l.location_id,
        l.valid_ts_utc,
        o.temp_c as temp_persistence_c,
        case
            when o.weather_code is null then null
            when o.weather_code between 300 and 622 then 1.0
            else 0.0
        end as pop_persistence
    from lookback l
    left join {{ ref('stg_owm__current') }} o
           on o.location_id = l.location_id
          and o.obs_ts_utc between l.lookback_ts - interval '{{ tolerance }} minutes'
                                and l.lookback_ts + interval '{{ tolerance }} minutes'
    qualify row_number() over (
        partition by l.location_id, l.valid_ts_utc
        order by abs(datediff('second', l.lookback_ts, o.obs_ts_utc)) asc
    ) = 1
)

select * from matched
