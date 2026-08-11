-- Semantic test on the model's central claim (docs/PLAN.md Phase 7), not a
-- structural one: MAE at 96h lead time should be >= MAE at 24h, across the
-- trailing 30 days. If this ever fails, either the lead-time computation is
-- inverted or the actual-matching join is wrong -- a forecast issued
-- further out should never be *more* accurate on average than one issued
-- closer in. Warn-level, not error-level: a genuine skill-score surprise
-- worth a human look, not necessarily a broken pipeline.
{{ config(severity='warn') }}

with recent_closed as (
    select
        milestone_hours,
        abs_err_temp
    from {{ ref('fct_forecast_error') }}
    where
        slot_status = 'closed'
        and valid_ts_utc >= current_timestamp - interval '30 days'
        and milestone_hours in (24, 96)
),

mae_by_bucket as (
    select
        milestone_hours,
        avg(abs_err_temp) as mae_temp
    from recent_closed
    group by milestone_hours
)

select
    (
        select mae_temp from mae_by_bucket
        where milestone_hours = 24
    ) as mae_24h,
    (
        select mae_temp from mae_by_bucket
        where milestone_hours = 96
    ) as mae_96h
where
    (
        select mae_temp from mae_by_bucket
        where milestone_hours = 96
    )
    < (
        select mae_temp from mae_by_bucket
        where milestone_hours = 24
    )
