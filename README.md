# hindcast-lakehouse

An end-to-end lakehouse that **grades the weather forecast**. Every three hours it
snapshots OpenWeatherMap's 5-day/3-hour forecast for a curated set of
personally-meaningful locations; every thirty minutes it records what the weather
actually did; then it measures how forecast accuracy decays with lead time — MAE/RMSE
by lead-time bucket, Brier score on precipitation probability, skill vs. a persistence
baseline. Airflow + Spark on a DigitalOcean droplet, Delta on Azure ADLS Gen2, dbt into
Snowflake, Terraform for everything, Datadog/Sentry/OpenLineage on top, Power BI to
report — built and torn down for $0.

**Status:** scaffolding / Phase 0 (accounts + local environment). No pipeline code yet.

See [`docs/PLAN.md`](docs/PLAN.md) for the full architecture, star schema design, and
phased build plan, and [`CLAUDE.md`](CLAUDE.md) for working conventions. This project
originally targeted Spotify listening history; it pivoted after Spotify introduced a
Premium-subscription requirement for personal-data API access — see `CLAUDE.md` for why.
