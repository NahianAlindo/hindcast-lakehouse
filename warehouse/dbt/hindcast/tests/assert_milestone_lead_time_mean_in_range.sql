-- One mean-in-range check per milestone (docs/PLAN.md Phase 7:
-- "expect_column_mean_to_be_between(lead_time_hours, ...) per bucket"),
-- generated from the same milestone_hours var every other milestone-aware
-- model reads from. Lives here, not in int_forecast_milestone_pivot's own
-- schema.yml, because dbt's YAML property files don't support a Jinja
-- for-loop generating multiple test entries (verified live -- a real
-- parse error, not a style choice) the way a .sql test file does.
--
-- A dbt test fails on any row returned, so this unions one row per
-- milestone whose mean actual_lead_hours falls *outside* [L, L+3) --
-- passing (zero rows) is the expected, silent case.
{% set milestones = var('milestone_hours') %}

{% for h in milestones %}
select
    {{ h }} as milestone_hours,
    avg(actual_lead_hours) as mean_actual_lead_hours
from {{ ref('int_forecast_milestone_pivot') }}
where milestone_hours = {{ h }}
having avg(actual_lead_hours) < {{ h }} or avg(actual_lead_hours) >= {{ h + 3 }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
