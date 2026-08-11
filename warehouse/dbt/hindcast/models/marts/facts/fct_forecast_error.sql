-- Derived from fct_forecast_slot by unpivot (docs/PLAN.md §6.1) -- exactly
-- one source of truth. The snapshot is the pipeline's state machine and
-- the modeling showpiece; this long fact (one row per location, valid_ts,
-- lead-time milestone) is the actual query/BI surface -- 8 wide measure
-- columns would force 8 near-identical DAX measures and break slicing by
-- lead time (docs/PLAN.md §9).
{% set milestones = var('milestone_hours') %}

with slot as (
    select * from {{ ref('fct_forecast_slot') }}
),

unpivoted as (
    {% for h in milestones %}
        select
            location_key,
            location_regime_key,
            valid_date_key,
            valid_time_utc_key,
            valid_time_local_key,
            pipeline_run_key,
            location_id,
            valid_ts_utc,
            {{ h }} as milestone_hours,
            temp_fcst_{{ h }}h as temp_fcst,
            pop_fcst_{{ h }}h as pop_fcst,
            wind_speed_fcst_{{ h }}h as wind_speed_fcst,
            condition_key_fcst_{{ h }}h as condition_key_fcst,
            issued_at_{{ h }}h as issued_at,
            actual_lead_hours_{{ h }}h as actual_lead_hours,
            temp_actual_c,
            pop_actual_binary,
            wind_speed_actual_ms,
            wind_deg_actual,
            condition_key_actual,
            abs_err_temp_{{ h }}h as abs_err_temp,
            signed_err_temp_{{ h }}h as signed_err_temp,
            sq_err_temp_{{ h }}h as sq_err_temp,
            brier_{{ h }}h as brier,
            wind_dir_circ_err_{{ h }}h as wind_dir_circ_err,
            slot_status,
            dq_status
        from slot
        {{ "union all" if not loop.last }}
    {% endfor %}
)

select
    {{ dbt_utils.generate_surrogate_key(['u.location_id', 'u.valid_ts_utc', 'u.milestone_hours']) }}
        as forecast_error_key,
    u.location_key,
    u.location_regime_key,
    u.valid_date_key,
    u.valid_time_utc_key,
    u.valid_time_local_key,
    lb.lead_time_bucket_key,
    u.pipeline_run_key,
    u.location_id,
    u.valid_ts_utc,
    u.milestone_hours,
    u.temp_fcst,
    u.pop_fcst,
    u.wind_speed_fcst,
    u.condition_key_fcst,
    u.issued_at,
    u.actual_lead_hours,
    u.temp_actual_c,
    u.pop_actual_binary,
    u.wind_speed_actual_ms,
    u.wind_deg_actual,
    u.condition_key_actual,
    u.abs_err_temp,
    u.signed_err_temp,
    u.sq_err_temp,
    u.brier,
    u.wind_dir_circ_err,
    u.slot_status,
    u.dq_status
from unpivoted as u
left join {{ ref('dim_lead_time_bucket') }} as lb on u.milestone_hours = lb.milestone_hours
