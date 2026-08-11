-- Static calendar dimension, generated (not sourced) -- 2025-01-01 through
-- 2031-12-31 comfortably spans this project's real operating lifetime.
-- Hemisphere-aware season (docs/PLAN.md §6.4) because the location set
-- (dim_location) spans both -- Sydney's summer is Oshawa's winter, and a
-- single `season` column would silently be wrong for half the fact rows.
with days as (
    {{ date_series("'2025-01-01'", "'2032-01-01'") }}
),

-- Season *index* (0=Winter, 1=Spring, 2=Summer, 3=Fall) derived from month
-- by integer arithmetic -- (month % 12) / 3, integer division -- rather
-- than a season-name literal repeated per month. Southern is exactly 2
-- season-steps (6 months) ahead of northern. Each season name string
-- appears exactly once in the whole file, in season_names below, instead
-- of 4-6 times across a CASE or VALUES block.
season_names as (
    select * from (values (0, 'Winter'), (1, 'Spring'), (2, 'Summer'), (3, 'Fall'))
        as t (season_index, season_name)
),

{% set date_col = 'indexed.date_day' %}

indexed as (
    select
        days.date_day,
        -- floor(), not `/`: DuckDB's `/` between integers is true
        -- (floating-point) division, not floor division -- hit this live,
        -- every non-exact month (everything except 3/6/9) silently failed
        -- to join below since e.g. 1/3 = 0.333 != the integer 0 in
        -- season_names. floor()+cast is the one spelling both engines
        -- agree on, rather than a third dialect-specific operator
        -- (DuckDB's `//` vs Spark SQL's `DIV`).
        cast(floor((cast(extract(month from days.date_day) as int) % 12) / 3.0) as int) as season_index_n
    from days
)

select
    {{ dbt_utils.generate_surrogate_key([date_col]) }} as date_key,
    indexed.date_day,
    extract(year from indexed.date_day) as year,
    extract(month from indexed.date_day) as month,
    extract(day from indexed.date_day) as day_of_month,
    extract(doy from indexed.date_day) as day_of_year,
    extract(week from indexed.date_day) as iso_week,
    extract(dow from indexed.date_day) as day_of_week,   -- 0=Sunday
    {{ day_name(date_col) }} as day_name,
    {{ month_name(date_col) }} as month_name,
    extract(dow from indexed.date_day) in (0, 6) as is_weekend,
    sn_north.season_name as season_northern,
    sn_south.season_name as season_southern
from indexed
left join season_names as sn_north on indexed.season_index_n = sn_north.season_index
left join season_names as sn_south on sn_south.season_index = (indexed.season_index_n + 2) % 4
