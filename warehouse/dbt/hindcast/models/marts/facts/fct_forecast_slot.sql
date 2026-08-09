-- The flagship accumulating snapshot (docs/PLAN.md §6.2). Grain: one row
-- per (location, valid_timestamp_utc). A row is born the first time a
-- 5-day forecast mentions that slot (~120h before it happens), rewritten
-- in place as milestones fill in, and closed the day after it occurs.
--
-- materialized as incremental/merge on forecast_slot_key: the MERGE *is*
-- the accumulating snapshot's defining behaviour (docs/PLAN.md's Phase 5
-- key decision) -- every run re-evaluates every open slot (there's no
-- append-only shortcut for a table whose whole point is being rewritten in
-- place), which is fine at this project's real data volume (the same
-- volume that makes DuckDB the right engine, per the Phase 4 benchmark).
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='forecast_slot_key',
    )
}}

{% set milestones = var('milestone_hours') %}

with pivoted as (
    select
        location_id,
        valid_ts_utc,
        {% for h in milestones -%}
        max(case when milestone_hours = {{ h }} then temp_c end)        as temp_fcst_{{ h }}h,
        max(case when milestone_hours = {{ h }} then pop end)           as pop_fcst_{{ h }}h,
        max(case when milestone_hours = {{ h }} then wind_speed_ms end) as wind_speed_fcst_{{ h }}h,
        max(case when milestone_hours = {{ h }} then wind_deg end)      as wind_deg_fcst_{{ h }}h,
        max(case when milestone_hours = {{ h }} then weather_code end)  as weather_code_fcst_{{ h }}h,
        max(case when milestone_hours = {{ h }} then issued_at_utc end) as issued_at_{{ h }}h,
        max(case when milestone_hours = {{ h }} then actual_lead_hours end) as actual_lead_hours_{{ h }}h{{ "," if not loop.last }}
        {% endfor %}
    from {{ ref('int_forecast_milestone_pivot') }}
    group by location_id, valid_ts_utc
),

lifecycle as (
    select
        location_id,
        valid_ts_utc,
        min(issued_at_utc)          as first_forecast_at,
        max(issued_at_utc)          as last_forecast_at,
        count(*)                    as revision_count,
        count(distinct issued_at_utc) as distinct_model_runs
    from {{ ref('int_forecast_with_leadtime') }}
    group by location_id, valid_ts_utc
),

actuals as (
    select
        location_id,
        valid_ts_utc,
        actual_obs_ts,
        match_offset_minutes,
        temp_actual_c,
        wind_speed_actual_ms,
        wind_deg_actual,
        weather_code_actual,
        case
            when weather_code_actual is null then null
            when weather_code_actual between 300 and 622 then true
            else false
        end as pop_actual_binary,
        temp_actual_window_mean_c,
        obs_count_in_window
    from {{ ref('int_observation_slot_matched') }}
),

baseline as (
    select location_id, valid_ts_utc, temp_persistence_c, pop_persistence
    from {{ ref('int_persistence_baseline') }}
),

joined as (
    select
        p.*,
        lc.first_forecast_at,
        lc.last_forecast_at,
        lc.revision_count,
        lc.distinct_model_runs,
        a.actual_obs_ts,
        a.match_offset_minutes,
        a.temp_actual_c,
        a.wind_speed_actual_ms,
        a.wind_deg_actual,
        a.weather_code_actual,
        a.pop_actual_binary,
        a.temp_actual_window_mean_c,
        a.obs_count_in_window,
        b.temp_persistence_c,
        b.pop_persistence
    from pivoted p
    left join lifecycle lc using (location_id, valid_ts_utc)
    left join actuals a    using (location_id, valid_ts_utc)
    left join baseline b   using (location_id, valid_ts_utc)
),

with_status as (
    select
        j.*,
        -- 5-state lifecycle (docs/PLAN.md §6.2). 'pending' vs 'forecasting'
        -- split at 24h out: inside 24h is the window every milestone <=24h
        -- can actually start filling in, so it's meaningfully more "in
        -- progress" than a slot that's only been seen at 96h/120h lead.
        case
            when j.actual_obs_ts is not null then 'closed'
            when j.valid_ts_utc < current_timestamp - interval '{{ var("actual_match_tolerance_minutes") }} minutes'
                then 'closed_no_actual'
            when j.valid_ts_utc <= current_timestamp then 'awaiting_actual'
            when j.valid_ts_utc - current_timestamp <= interval '24 hours' then 'forecasting'
            else 'pending'
        end as slot_status,
        -- dq_status priority: a missing actual past its window is the most
        -- actionable signal, then a big-but-still-in-tolerance match, then
        -- sparse milestone coverage (only meaningful once the slot is old
        -- enough that every milestone <= its age should exist).
        case
            when j.actual_obs_ts is null
                 and j.valid_ts_utc < current_timestamp - interval '{{ var("actual_match_tolerance_minutes") }} minutes'
                then 'no_actual'
            when j.match_offset_minutes is not null and abs(j.match_offset_minutes) > {{ (var("actual_match_tolerance_minutes") / 2) | int }}
                then 'wide_match_offset'
            when j.valid_ts_utc <= current_timestamp
                 and ({{ milestones | length }} - (
                    {% for h in milestones -%}
                    (case when j.temp_fcst_{{ h }}h is not null then 1 else 0 end){{ " + " if not loop.last }}
                    {% endfor -%}
                 )) > 4
                then 'sparse_forecasts'
            else 'ok'
        end as dq_status
    from joined j
)

select
    {{ dbt_utils.generate_surrogate_key(['ws.location_id', 'ws.valid_ts_utc']) }} as forecast_slot_key,
    l.location_key,
    lr.location_regime_key,
    dd.date_key as valid_date_key,
    tu.time_key as valid_time_utc_key,
    tl.time_key as valid_time_local_key,
    pr.pipeline_run_key,

    ws.location_id,
    ws.valid_ts_utc,

    {% for h in milestones -%}
    ws.temp_fcst_{{ h }}h,
    ws.pop_fcst_{{ h }}h,
    ws.wind_speed_fcst_{{ h }}h,
    wc_{{ h }}.weather_condition_key as condition_key_fcst_{{ h }}h,
    ws.issued_at_{{ h }}h,
    ws.actual_lead_hours_{{ h }}h,
    {% endfor %}

    ws.temp_actual_c,
    ws.pop_actual_binary,
    cast(null as double) as precip_actual_mm, -- not collected by /weather (no direct precip-amount field); left null, not fabricated
    ws.wind_speed_actual_ms,
    ws.wind_deg_actual,
    wc_actual.weather_condition_key as condition_key_actual,
    ws.actual_obs_ts,
    ws.match_offset_minutes,

    ws.temp_actual_window_mean_c,
    cast(null as double) as precip_actual_window_sum_mm, -- same reason as precip_actual_mm
    ws.obs_count_in_window,

    ws.temp_persistence_c,
    ws.pop_persistence,

    ws.first_forecast_at,
    ws.last_forecast_at,
    ws.revision_count,
    ws.distinct_model_runs,
    ws.slot_status,
    case when ws.slot_status in ('closed', 'closed_no_actual') then ws.last_forecast_at end as slot_closed_at,
    ws.dq_status,

    {% for h in milestones -%}
    abs(ws.temp_fcst_{{ h }}h - ws.temp_actual_c) as abs_err_temp_{{ h }}h,
    (ws.temp_fcst_{{ h }}h - ws.temp_actual_c)     as signed_err_temp_{{ h }}h,
    power(ws.temp_fcst_{{ h }}h - ws.temp_actual_c, 2) as sq_err_temp_{{ h }}h,
    power(ws.pop_fcst_{{ h }}h - (case when ws.pop_actual_binary then 1.0 else 0.0 end), 2) as brier_{{ h }}h,
    least(
        abs(ws.wind_deg_fcst_{{ h }}h - ws.wind_deg_actual),
        360 - abs(ws.wind_deg_fcst_{{ h }}h - ws.wind_deg_actual)
    ) as wind_dir_circ_err_{{ h }}h{{ "," if not loop.last }}
    {% endfor %}

from with_status ws
left join {{ ref('dim_location') }} l on l.location_id = ws.location_id
left join {{ ref('dim_location_regime') }} lr
       on lr.location_id = ws.location_id
      and ws.first_forecast_at >= lr.valid_from
      and (lr.valid_to is null or ws.first_forecast_at < lr.valid_to)
left join {{ ref('dim_date') }} dd on dd.date_day = cast(ws.valid_ts_utc as date)
left join {{ ref('dim_time') }} tu on tu.hour = extract(hour from ws.valid_ts_utc)
left join {{ ref('dim_time') }} tl on tl.hour = extract(hour from (ws.valid_ts_utc at time zone l.iana_tz))
left join {{ ref('dim_pipeline_run') }} pr on pr.run_id = (
    select run_id from {{ ref('int_forecast_with_leadtime') }} f
    where f.location_id = ws.location_id and f.valid_ts_utc = ws.valid_ts_utc
    order by f.issued_at_utc desc limit 1
)
left join {{ ref('dim_weather_condition') }} wc_actual on wc_actual.code = ws.weather_code_actual
{% for h in milestones -%}
left join {{ ref('dim_weather_condition') }} wc_{{ h }} on wc_{{ h }}.code = ws.weather_code_fcst_{{ h }}h
{% endfor %}

{% if is_incremental() %}
-- Accumulating-snapshot incremental pattern: a closed slot's data can never
-- change again (its actual has already landed and every milestone that
-- will ever exist for it does), so skip recomputing anything already
-- closed in the target. Everything still open (or brand new) gets
-- recomputed and re-merged every run -- that recomputation, not an
-- append, is what makes this an accumulating snapshot rather than a
-- transaction fact.
where not exists (
    select 1 from {{ this }} t
    where t.location_id = ws.location_id
      and t.valid_ts_utc = ws.valid_ts_utc
      and t.slot_status = 'closed'
)
{% endif %}
