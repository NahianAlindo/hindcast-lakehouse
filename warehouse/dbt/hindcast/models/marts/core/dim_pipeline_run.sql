-- Audit dimension (docs/PLAN.md §6.4/§7) -- every fact carries
-- pipeline_run_key, which is what makes "which code and which data
-- produced this row" a single SQL join instead of a paragraph in a README.
--
-- Honest gap, not swept under the rug: `run_id` is already minted by the
-- extractor and lands in every silver row (ingestion/hindcast_extract), so
-- started_at/ended_at/rows_written below are real, derived from actual
-- data. git_sha, git_tag, image_digest, dvc_data_version, and dag_run_id
-- are NOT yet captured anywhere queryable -- that needs the Airflow DAGs
-- (Phase 3) or the extractor itself to persist run metadata somewhere this
-- model can read, which hasn't been built. Those columns exist here as
-- NULL placeholders so fact tables can join against the right grain
-- (`pipeline_run_key`) today, without fabricating values, and get
-- backfilled once that instrumentation exists (tracked for Phase 7).
with runs as (
    select
        run_id,
        requested_at_utc as event_ts
    from {{ ref('stg_owm__current') }}
    union all
    select
        run_id,
        requested_at_utc as event_ts
    from {{ ref('stg_owm__air_quality') }}
    union all
    select
        run_id,
        issued_at_utc as event_ts
    from {{ ref('stg_owm__forecast') }}
),

agg as (
    select
        run_id,
        min(event_ts) as started_at,
        max(event_ts) as ended_at,
        count(*) as rows_written
    from runs
    group by run_id
)

select
    {{ dbt_utils.generate_surrogate_key(['run_id']) }} as pipeline_run_key,
    run_id,
    cast(null as string) as dag_id,
    cast(null as string) as dag_run_id,
    cast(null as string) as git_sha,
    cast(null as string) as git_tag,
    cast(null as string) as image_digest,
    cast(null as string) as extractor_version,
    cast(null as string) as dvc_data_version,
    started_at,
    ended_at,
    'unknown' as status,
    rows_written
from agg
