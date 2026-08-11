"""DuckDB counter-benchmark: the same lead-time-milestone-selection query
(cross join to 8 milestones, pick the closest forecast per
(location, valid_ts, milestone) via a window function) against the same
synthetic Parquet, run in DuckDB instead of Spark. This is "the honest
half" of the benchmark (docs/PLAN.md Phase 4) -- it's expected to win at
real production volume (~140k rows/month) and the scale sweep is what finds
the row count where Spark actually overtakes it.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import duckdb

MILESTONE_HOURS = [3, 6, 12, 24, 48, 72, 96, 120]

QUERY_TEMPLATE = """
with milestones as (
    select unnest({milestones}) as milestone_hours
),
candidates as (
    select
        s.location_id,
        s.valid_ts,
        s.lead_time_minutes,
        s.temp_c,
        m.milestone_hours,
        abs(s.lead_time_minutes / 60.0 - m.milestone_hours) as distance_hours
    from source s
    cross join milestones m
),
ranked as (
    select
        *,
        row_number() over (
            partition by location_id, valid_ts, milestone_hours
            order by distance_hours asc
        ) as rn
    from candidates
)
select count(*) from ranked where rn = 1
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory-limit", default="3GB")
    parser.add_argument("--temp-dir", default="/home/nahian/benchmark_data/duckdb_tmp")
    parser.add_argument("--label", default="")
    parser.add_argument("--results", required=True)
    args = parser.parse_args()

    # This is a local benchmark CLI, not a network-facing service --
    # --source/--results/--temp-dir are meant to point wherever the
    # developer running it chooses (that's the actual feature, not a bug).
    # Resolving through pathlib still normalizes any '..'/'.' segments
    # rather than passing raw CLI text straight into filesystem calls.
    source_dir = Path(args.source).resolve()
    results_path = Path(args.results).resolve()
    temp_dir = Path(args.temp_dir).resolve()

    temp_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"PRAGMA threads={args.threads}")
    con.execute(f"PRAGMA memory_limit='{args.memory_limit}'")
    con.execute(f"PRAGMA temp_directory='{temp_dir}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"CREATE VIEW source AS SELECT * FROM read_parquet('{source_dir}/**/*.parquet')")

    query = QUERY_TEMPLATE.format(milestones=MILESTONE_HOURS)

    start = time.perf_counter()
    result_row = con.execute(query).fetchone()
    assert result_row is not None
    result = result_row[0]
    elapsed = time.perf_counter() - start

    record = {
        "timestamp": time.time(),
        "engine": "duckdb",
        "rows": args.rows,
        "threads": args.threads,
        "label": args.label,
        "source": str(source_dir),
        "wall_clock_seconds": elapsed,
        "result": result,
    }
    with results_path.open("a") as f:
        f.write(json.dumps(record) + "\n")

    print(json.dumps(record, indent=2))


if __name__ == "__main__":
    main()
