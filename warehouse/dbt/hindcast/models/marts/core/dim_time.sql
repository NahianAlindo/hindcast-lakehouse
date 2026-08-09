-- 24-row hour-grain dimension (docs/PLAN.md §6.4's "24 (hour)" option) --
-- minute-grain would add 1,440 rows for precision this project never uses:
-- forecast slots are 3h-boundary-aligned and observations are matched to
-- them within a ±90min tolerance (int_observation_slot_matched), so hour
-- is the finest grain any fact actually needs. Role-plays twice in
-- fct_forecast_slot: valid_time_utc_key and valid_time_local_key --
-- "was the afternoon forecast worse?" is a *local* question.
with hours as (
    select cast(h as int) as hour from {{ hour_series() }}
)

select
    {{ dbt_utils.generate_surrogate_key(['hour']) }} as time_key,
    hour,
    -- floor()+cast, not `hour / 3 * 3`: DuckDB's `/` between integers is
    -- true (floating-point) division, not floor division (same gotcha
    -- dim_date's season_index hit) -- the naive formula silently returned
    -- `hour` unchanged for every row instead of flooring to the 8 daily
    -- 3-hour boundaries (0,3,6,...,21), verified live against all 24 hours.
    cast(floor(hour / 3.0) * 3 as int) as three_hour_slot,
    case
        when hour between 21 and 23 or hour between 0 and 4 then 'night'
        when hour between 5 and 6   then 'dawn'
        when hour between 7 and 11  then 'morning'
        when hour between 12 and 16 then 'afternoon'
        else 'evening'
    end as daypart
from hours
