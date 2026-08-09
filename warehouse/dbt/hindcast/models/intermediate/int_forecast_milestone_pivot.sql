-- Lead-time milestone selection (docs/PLAN.md §6.3a). Polls happen every 3
-- hours, so no forecast is ever issued at exactly T-Lh. Rule: for milestone
-- L, pick the forecast whose lead_time_hours is the smallest value >= L --
-- i.e. the last forecast issued *before* the L-hour mark. Deliberately
-- >= L, not "nearest": that guarantees the reported figure is never more
-- informed than its label claims (a "24h forecast" that was actually
-- issued 23h out would flatter the accuracy curve).
{% set milestones = var('milestone_hours') %}

with milestone_hours as (
    select * from (values
        {% for h in milestones %}
        ({{ h }}){% if not loop.last %},{% endif %}
        {% endfor %}
    ) as t(milestone_hours)
),

candidates as (
    select
        f.location_id,
        f.valid_ts_utc,
        f.issued_at_utc,
        f.lead_time_hours,
        f.temp_c,
        f.feels_like_c,
        f.pop,
        f.wind_speed_ms,
        f.wind_deg,
        f.weather_code,
        f.weather_main,
        m.milestone_hours
    from {{ ref('int_forecast_with_leadtime') }} f
    inner join milestone_hours m
        on f.lead_time_hours >= m.milestone_hours
       and f.lead_time_hours <  m.milestone_hours + 3
)

select
    *,
    -- Stored so the imprecision is auditable rather than hidden -- tested
    -- (schema.yml) to always fall in [L, L+3).
    lead_time_hours as actual_lead_hours
from candidates
qualify row_number() over (
    partition by location_id, valid_ts_utc, milestone_hours
    order by lead_time_hours asc, issued_at_utc desc
) = 1
