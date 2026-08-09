-- lead_time_hours re-derived from the extractor-minted lead_time_minutes
-- (never re-inferred from a schedule -- CLAUDE.md's single most
-- load-bearing extractor rule). This is the shared base every downstream
-- intermediate model (milestone pivot, persistence baseline) reads from.
select
    *,
    lead_time_minutes / 60.0 as lead_time_hours
from {{ ref('stg_owm__forecast') }}
