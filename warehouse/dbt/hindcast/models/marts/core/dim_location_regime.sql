-- Wraps snap_location_regime (the dbt snapshot doing the actual SCD2 work)
-- with the column names docs/PLAN.md §6.4 specifies (valid_from/valid_to/
-- is_current) instead of dbt's own dbt_valid_from/dbt_valid_to/dbt_scd_id.
select
    {{ dbt_utils.generate_surrogate_key(['location_id', 'dbt_valid_from']) }} as location_regime_key,
    location_id,
    forecastability_tier,
    thermal_regime,
    volatility_band,
    dbt_valid_from as valid_from,
    dbt_valid_to   as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('snap_location_regime') }}
