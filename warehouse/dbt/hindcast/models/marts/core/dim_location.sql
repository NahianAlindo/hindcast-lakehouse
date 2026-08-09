-- Type 1, static (docs/PLAN.md §6.4/§6.5) -- name/coordinates/timezone
-- genuinely don't change, so SCD2 here would be theatre. `country` doubles
-- as iso2 (locations.yml already stores 2-letter codes); `admin_area`
-- (state/province) isn't collected by the extractor's config and isn't
-- fabricated here -- add it to ingestion/config/locations.yml first if a
-- future report needs it.
select
    {{ dbt_utils.generate_surrogate_key(['location_id']) }} as location_key,
    location_id,
    name,
    country,
    country as iso2,
    lat,
    lon,
    iana_tz,
    case when lat >= 0 then 'Northern' else 'Southern' end as hemisphere,
    climate_class as koppen_class,
    personal_relevance,
    personal_note,
    active_from
from {{ ref('stg_seed__locations') }}
