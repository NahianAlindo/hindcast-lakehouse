# hindcast-lakehouse

A lakehouse that grades the weather forecast. Every three hours it snapshots
OpenWeatherMap's 5-day/3-hour forecast for a curated set of locations; every
thirty minutes it records what the weather actually did; then it joins the
two across time to measure how forecast accuracy decays with lead time.

The flagship artifact is `fct_forecast_slot` — an accumulating snapshot fact
table with one row per (location, future 3-hour slot), rewritten in place as
successive forecasts revise the prediction and closed once the actual lands.

## Stack

- **Ingestion** — a Python extractor polling OpenWeatherMap's free tier
  (current weather, 5-day/3-hour forecast, air pollution), landing raw JSON
  in Azure ADLS Gen2 (bronze layer).
- **Orchestration** — Apache Airflow 3.x, running in Docker on an Azure VM,
  backed by a Postgres container on a persistent managed disk.
- **Transformation** — PySpark 3.5 + Delta Lake, bronze → validated,
  deduplicated silver Delta tables.
- **Modeling** — dbt-core, a Kimball star schema, dual-targeted at DuckDB
  (dev) and Snowflake (prod), with a Databricks Free Edition target as a
  third-engine portability demo.
- **Infra** — Terraform (Terraform Cloud remote state), Azure (VM, ADLS
  Gen2, Key Vault, Managed Identity), two separate workspaces for compute
  and data.
- **CI/CD** — GitHub Actions: lint/type/test/dbt-build on every push,
  versioned Docker image builds with SBOM + provenance on tagged releases,
  dbt docs published to GitHub Pages.
- **Observability & DQ** — Datadog dashboards/monitors, Sentry error
  tracking, elementary-data data-quality reports, dbt tests across every
  layer from staging to marts.
- **Versioning** — DVC for warehouse exports, git tags resolved to
  immutable image digests for full pipeline-run traceability.

## Docs

- [`docs/PLAN.md`](docs/PLAN.md) — full architecture, star schema design,
  and phased build plan.
- [dbt docs site](http://blaze-dev.me/hindcast-lakehouse/) — model lineage,
  column descriptions, test coverage.

## Status

Ingestion has been live since Week 2 and has not stopped. Phases 0–8 are
built and verified live: ingestion, infrastructure, transformation, the
star schema, observability/data quality, and CI/CD.
