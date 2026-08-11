-- Static vendor enum (OWM's documented condition codes, 200-804) -- Type 1,
-- SCD2 would be theatre (docs/PLAN.md §6.5). condition_group/severity_rank/
-- is_precipitation/is_severe are derived here rather than baked into the
-- seed, since they're business classification, not raw vendor data.
with base as (
    select
        code,
        description,
        icon
    from {{ ref('owm_weather_conditions') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['code']) }} as weather_condition_key,
    code,
    description,
    icon,
    case
        when code between 200 and 232 then 'Thunderstorm'
        when code between 300 and 321 then 'Drizzle'
        when code between 500 and 531 then 'Rain'
        when code between 600 and 622 then 'Snow'
        when code between 701 and 781 then 'Atmosphere'
        when code = 800 then 'Clear'
        when code between 801 and 804 then 'Clouds'
    end as condition_group,
    -- Higher = more severe. Clear/Clouds are benign; thunderstorms and
    -- tornadoes (781) top the scale regardless of numeric code order.
    case
        when code = 781 then 10
        when code between 200 and 232 then 9
        when code between 502 and 504 then 8
        when code between 611 and 622 then 6
        when code between 300 and 622 then 5
        when code between 701 and 771 then 4
        when code between 801 and 804 then 2
        when code = 800 then 1
    end as severity_rank,
    (code between 300 and 622) as is_precipitation,
    (code = 781 or code between 200 and 232 or code between 502 and 504) as is_severe
from base
