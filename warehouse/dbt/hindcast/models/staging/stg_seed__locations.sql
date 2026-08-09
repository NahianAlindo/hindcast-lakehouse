-- 1:1 with the locations seed. Source of truth for edits is
-- ingestion/config/locations.yml (the extractor's own config); seeds/locations.csv
-- is a transcription kept in sync by hand, not generated at build time, since
-- dbt seeds must be plain CSV and the YAML carries extractor-only fields
-- (e.g. active_from's role in state.py) this model doesn't need.
select
    location_id,
    name,
    country,
    lat::double        as lat,
    lon::double        as lon,
    iana_tz,
    personal_relevance,
    personal_note,
    climate_class,
    active_from::date  as active_from
from {{ ref('locations') }}
