"""hindcast.duckdb (marts.core / marts.facts, just built by `dbt build
--target duckdb`) -> data/exports/<table>.parquet -- the curated snapshot
DVC's weekly dvc_snapshot job version-tags (docs/PLAN.md Phase 6).

DuckDB, not Snowflake, is the source here even though Snowflake is the
critical-path warehouse: DuckDB is the one target every phase of this
project can rebuild from with zero live credentials (dbt's Week 5 target
and the permanent post-teardown/post-trial state, docs/PLAN.md's Phase 6
"both pipelines" note), so a snapshot sourced from it survives losing
Snowflake entirely -- exactly the property this snapshot exists for.

Every table in marts.core and marts.facts, discovered from
information_schema rather than a hardcoded list, so a new mart added later
doesn't silently fall out of the weekly snapshot.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

DUCKDB_PATH = os.environ.get("DBT_DUCKDB_PATH", "warehouse/dbt/hindcast/hindcast.duckdb")
EXPORT_DIR = Path("data/exports")
MART_SCHEMAS = ("main_core", "main_facts")


def main() -> None:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH, read_only=True)

    tables = con.execute(
        "select table_schema, table_name from information_schema.tables "
        "where table_schema in ? order by table_schema, table_name",
        [list(MART_SCHEMAS)],
    ).fetchall()

    if not tables:
        raise RuntimeError(
            f"No tables found in {MART_SCHEMAS} -- has `dbt build --target "
            f"duckdb` actually run against {DUCKDB_PATH}?"
        )

    for schema, table in tables:
        out_path = EXPORT_DIR / f"{table}.parquet"
        con.execute(f"COPY {schema}.{table} TO '{out_path}' (FORMAT parquet)")
        rows = con.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
        print(f"[{schema}.{table}] exported {rows} rows to {out_path}")

    con.close()


if __name__ == "__main__":
    main()
