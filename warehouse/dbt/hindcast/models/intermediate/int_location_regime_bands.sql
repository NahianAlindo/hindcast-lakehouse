-- Feeds dim_location_regime, the one earned SCD2 mini-dimension
-- (docs/PLAN.md §6.5) -- Phase 5b, optional. Computes skill directly from
-- the intermediate layer rather than from fct_forecast_slot, deliberately:
-- a mart that's itself keyed by location_regime_key can't be the source of
-- the band that key depends on without a circular dependency.
--
-- Honest data-maturity note: forecastability_tier needs real forecast-vs-
-- actual comparisons accrued over time, and thermal_regime/volatility_band
-- need a real trailing-30-day window. Ingestion has been live only a few
-- days as of this build (docs/PLAN.md's accrual clock started Phase 2,
-- Week 2) -- early runs of this model will band on whatever partial window
-- actually exists, not a full 30/90 days. That's expected, not a bug: the
-- bands get more meaningful every day ingestion keeps running, which is
-- exactly the "this dataset accrues in wall-clock time" property this
-- whole project is built around.
with thermal as (
    select
        location_id,
        avg(temp_c) as trailing_mean_temp_c
    from {{ ref('stg_owm__current') }}
    where obs_ts_utc >= current_timestamp - interval '30 days'
    group by location_id
),

daily_temp as (
    select
        location_id,
        date_trunc('day', obs_ts_utc) as obs_date,
        avg(temp_c) as daily_mean_temp_c
    from {{ ref('stg_owm__current') }}
    where obs_ts_utc >= current_timestamp - interval '30 days'
    group by location_id, date_trunc('day', obs_ts_utc)
),

daily_diffs as (
    select
        location_id,
        daily_mean_temp_c - lag(daily_mean_temp_c) over (
            partition by location_id order by obs_date
        ) as day_over_day_diff
    from daily_temp
),

volatility as (
    select
        location_id,
        stddev(day_over_day_diff) as trailing_temp_volatility
    from daily_diffs
    group by location_id
),

-- Skill: MAE between the shortest-lead forecast available for a slot and
-- that slot's matched actual, vs. the persistence baseline's MAE for the
-- same slots. skill_score > 0 means the forecast beats naive persistence.
forecast_nearest as (
    select
        location_id,
        valid_ts_utc,
        temp_c,
        row_number() over (
            partition by location_id, valid_ts_utc order by lead_time_hours asc
        ) as rn
    from {{ ref('int_forecast_with_leadtime') }}
),

skill as (
    select
        f.location_id,
        avg(abs(f.temp_c - m.temp_actual_c)) as forecast_mae,
        avg(abs(p.temp_persistence_c - m.temp_actual_c)) as persistence_mae
    from forecast_nearest as f
    inner join {{ ref('int_observation_slot_matched') }} as m
        on f.location_id = m.location_id and f.valid_ts_utc = m.valid_ts_utc
    inner join {{ ref('int_persistence_baseline') }} as p
        on f.location_id = p.location_id and f.valid_ts_utc = p.valid_ts_utc
    where
        f.rn = 1
        and m.temp_actual_c is not null
        and p.temp_persistence_c is not null
    group by f.location_id
)

select
    t.trailing_mean_temp_c,
    v.trailing_temp_volatility,
    coalesce(t.location_id, v.location_id, s.location_id) as location_id,
    case
        when s.persistence_mae is null or s.persistence_mae = 0 then null
        else 1.0 - (s.forecast_mae / s.persistence_mae)
    end as skill_score,
    case
        when t.trailing_mean_temp_c is null then null
        when t.trailing_mean_temp_c < 0 then 'Cold'
        when t.trailing_mean_temp_c < 10 then 'Cool'
        when t.trailing_mean_temp_c < 20 then 'Mild'
        when t.trailing_mean_temp_c < 28 then 'Warm'
        else 'Hot'
    end as thermal_regime,
    case
        when v.trailing_temp_volatility is null then null
        when v.trailing_temp_volatility < 2 then 'Low'
        when v.trailing_temp_volatility < 5 then 'Medium'
        else 'High'
    end as volatility_band,
    case
        when s.persistence_mae is null then null
        when 1.0 - (s.forecast_mae / nullif(s.persistence_mae, 0)) >= 0.3 then 'High'
        when 1.0 - (s.forecast_mae / nullif(s.persistence_mae, 0)) >= 0.0 then 'Medium'
        else 'Low'
    end as forecastability_tier
from thermal as t
full outer join volatility as v on t.location_id = v.location_id
-- coalesce, not t.location_id alone: a full outer join chain means
-- t.location_id can be null for a row that only matched in v, and this
-- join still needs to find that row's skill data by location.
full outer join skill as s on coalesce(t.location_id, v.location_id) = s.location_id
