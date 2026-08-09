-- 5 rows: OWM's own AQI 1-5 scale (docs/PLAN.md §6.4). Static enum, no SCD2.
with base as (
    select * from (
        values
            (1, 'Good',      'Air quality is satisfactory; poses little or no risk.'),
            (2, 'Fair',      'Air quality is acceptable; moderate health concern for a very small number of unusually sensitive people.'),
            (3, 'Moderate',  'Members of sensitive groups may experience health effects.'),
            (4, 'Poor',      'Everyone may begin to experience health effects; sensitive groups may experience more serious effects.'),
            (5, 'Very Poor', 'Health warnings of emergency conditions; the entire population is more likely to be affected.')
    ) as t(aqi, label, health_guidance)
)

select
    {{ dbt_utils.generate_surrogate_key(['aqi']) }} as air_quality_band_key,
    aqi,
    label,
    health_guidance
from base
