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
)

select
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_key,
    date_day,
    extract(year from date_day)    as year,
    extract(month from date_day)   as month,
    extract(day from date_day)     as day_of_month,
    extract(doy from date_day)     as day_of_year,
    extract(week from date_day)    as iso_week,
    extract(dow from date_day)     as day_of_week,   -- 0=Sunday
    {{ day_name('date_day') }}     as day_name,
    {{ month_name('date_day') }}   as month_name,
    extract(dow from date_day) in (0, 6) as is_weekend,
    case extract(month from date_day)
        when 12 then 'Winter' when 1 then 'Winter' when 2 then 'Winter'
        when 3 then 'Spring'  when 4 then 'Spring'  when 5 then 'Spring'
        when 6 then 'Summer'  when 7 then 'Summer'  when 8 then 'Summer'
        else 'Fall'
    end as season_northern,
    case extract(month from date_day)
        when 12 then 'Summer' when 1 then 'Summer' when 2 then 'Summer'
        when 3 then 'Fall'    when 4 then 'Fall'    when 5 then 'Fall'
        when 6 then 'Winter'  when 7 then 'Winter'  when 8 then 'Winter'
        else 'Spring'
    end as season_southern
from days
