-- dbt's native SCD2 mechanism, used for the one dimension that earns it
-- (docs/PLAN.md §6.5): a derived, banded attribute that drifts, where the
-- historical value changes how a past forecast should be read. strategy
-- 'check' (not 'timestamp') because int_location_regime_bands has no
-- natural updated_at -- a version is created whenever any banded column's
-- *value* changes, which is exactly the semantics wanted here (banding is
-- what makes this tractable at all: raw trailing skill would version every
-- single day).
{% snapshot snap_location_regime %}

{{
    config(
      target_schema='snapshots',
      unique_key='location_id',
      strategy='check',
      check_cols=['thermal_regime', 'volatility_band', 'forecastability_tier'],
    )
}}

select
    location_id,
    thermal_regime,
    volatility_band,
    forecastability_tier
from {{ ref('int_location_regime_bands') }}

{% endsnapshot %}
