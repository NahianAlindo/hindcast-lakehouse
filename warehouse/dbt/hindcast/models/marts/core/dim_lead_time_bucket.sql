-- 8 rows, one per milestone hour (docs/PLAN.md §6.4) -- the analysis's
-- primary slicer. milestone_hours comes from dbt_project.yml's var so
-- int_forecast_milestone_pivot and this dimension can never drift apart.
{% set milestones = var('milestone_hours') %}

with milestones as (
    select * from (
        values
        {% for h in milestones %}
            ({{ h }}){% if not loop.last %},{% endif %}
        {% endfor %}
    ) as t (milestone_hours)
)

select
    {{ dbt_utils.generate_surrogate_key(['milestone_hours']) }} as lead_time_bucket_key,
    milestone_hours,
    milestone_hours - 3 as min_hours,
    milestone_hours as max_hours,
    case
        when milestone_hours <= 6 then 'nowcast'
        when milestone_hours <= 24 then 'short'
        else 'medium'
    end as horizon_class,
    lpad(cast(milestone_hours as string), 3, '0') || 'h' as label,
    row_number() over (order by milestone_hours) as sort_order
from milestones
