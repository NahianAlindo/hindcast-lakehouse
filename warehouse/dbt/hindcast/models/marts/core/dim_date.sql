-- Static calendar dimension, generated (not sourced) -- 2025-01-01 through
-- 2031-12-31 comfortably spans this project's real operating lifetime.
-- Hemisphere-aware season (docs/PLAN.md §6.4) because the location set
-- (dim_location) spans both -- Sydney's summer is Oshawa's winter, and a
-- single `season` column would silently be wrong for half the fact rows.
with days as (
    {%- if target.type == 'databricks' -%}
    -- Spark SQL's range() is integer-only (no date bounds like DuckDB's);
    -- sequence()+explode() is the Spark-native way to generate a date series.
    select explode(sequence(cast('2025-01-01' as date), cast('2031-12-31' as date), interval 1 day)) as date_day
    {%- else -%}
    select
        cast(d as date) as date_day
    from range(cast('2025-01-01' as date), cast('2032-01-01' as date), interval 1 day) as t(d)
    {%- endif %}
),

-- Month -> season mapping, one row per month rather than a season name
-- literal repeated across a dozen CASE branches (each of the 4 season
-- names would otherwise appear 4-6 times between the two hemisphere
-- columns). Northern and southern are exactly 6 months out of phase.
month_seasons as (
    select * from (values
        (12, 'Winter', 'Summer'), (1, 'Winter', 'Summer'), (2, 'Winter', 'Summer'),
        (3, 'Spring', 'Fall'),    (4, 'Spring', 'Fall'),    (5, 'Spring', 'Fall'),
        (6, 'Summer', 'Winter'),  (7, 'Summer', 'Winter'),  (8, 'Summer', 'Winter'),
        (9, 'Fall',   'Spring'),  (10, 'Fall',  'Spring'),  (11, 'Fall',  'Spring')
    ) as t(month, season_northern, season_southern)
)

select
    {{ dbt_utils.generate_surrogate_key(['days.date_day']) }} as date_key,
    days.date_day,
    extract(year from days.date_day)    as year,
    extract(month from days.date_day)   as month,
    extract(day from days.date_day)     as day_of_month,
    extract(doy from days.date_day)     as day_of_year,
    extract(week from days.date_day)    as iso_week,
    extract(dow from days.date_day)     as day_of_week,   -- 0=Sunday
    {{ day_name('days.date_day') }}     as day_name,
    {{ month_name('days.date_day') }}   as month_name,
    extract(dow from days.date_day) in (0, 6) as is_weekend,
    ms.season_northern,
    ms.season_southern
from days
left join month_seasons ms on ms.month = extract(month from days.date_day)
