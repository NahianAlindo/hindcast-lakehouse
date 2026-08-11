"""data/exports/<table>.parquet -> hindcast_restored.duckdb -- the restore-
side complement to export_marts_duckdb.py, used by
docs/runbooks/restore-a-data-version.md. Recreates the same main_core /
main_facts schema split the original dbt build produced, from a DVC-pulled
snapshot alone, with no live warehouse and no dbt run involved.

Table-to-schema assignment mirrors warehouse/dbt/hindcast/dbt_project.yml's
`+schema: core` / `+schema: facts` config exactly (dim_* -> core, fct_* ->
facts) rather than re-deriving it from dbt, since this script's whole point
is working without dbt/ADLS/a warehouse being reachable at all.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

EXPORT_DIR = Path("data/exports")
OUT_DB = "hindcast_restored.duckdb"


def schema_for(table_name: str) -> str:
    return "main_core" if table_name.startswith("dim_") else "main_facts"


def main() -> None:
    parquet_files = sorted(EXPORT_DIR.glob("*.parquet"))
    if not parquet_files:
        raise RuntimeError(f"No Parquet files in {EXPORT_DIR} -- run `dvc pull` first.")

    con = duckdb.connect(OUT_DB)
    for schema in ("main_core", "main_facts"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    for path in parquet_files:
        table = path.stem
        schema = schema_for(table)
        con.execute(
            f"CREATE OR REPLACE TABLE {schema}.{table} AS SELECT * FROM read_parquet('{path}')"
        )
        count_row = con.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()
        assert count_row is not None
        rows = count_row[0]
        print(f"[{schema}.{table}] loaded {rows} rows from {path}")

    con.close()
    print(f"\nRestored to {OUT_DB} -- query it directly, e.g.:")
    print(f'  duckdb {OUT_DB} -c "select count(*) from main_facts.fct_forecast_slot"')


if __name__ == "__main__":
    main()
