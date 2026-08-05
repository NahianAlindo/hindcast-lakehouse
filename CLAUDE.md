# hindcast-lakehouse

A portfolio data engineering project that **grades the weather forecast**. Every three
hours it snapshots OpenWeatherMap's 5-day/3-hour forecast for a curated set of
personally-meaningful locations; every thirty minutes it records what the weather
actually did; then it joins the two across time to measure how forecast accuracy decays
with lead time. The flagship artifact is an **accumulating snapshot fact table**
(`fct_forecast_slot`) — one row per (location, future 3-hour slot), rewritten ~40 times
as successive forecasts revise the prediction, closed once the actual lands.

Stack: Airflow 3.x + PySpark 3.5/Delta + a **Postgres container** (Airflow metadata, on a
persistent Managed Disk) all on one Azure VM, + Azure ADLS Gen2/Key Vault for the data
plane, all on a **single Azure for Students subscription** (kept in separate Terraform
workspaces for blast-radius hygiene, not separate subscriptions), Snowflake (trial) →
DuckDB (post-trial fallback), dbt-core, DVC, Terraform (+ Terraform Cloud), GitHub Actions,
Datadog + Sentry + OpenLineage/Marquez, Power BI.

**Full build plan — architecture, star schema, phase-by-phase deliverables, cost model:
[`docs/PLAN.md`](docs/PLAN.md). Read it before proposing architecture changes.**

## This project pivoted twice — know why

This repo originally targeted **Spotify Web API personal listening history**. That was
abandoned mid-build: as of Feb/March 2026 Spotify requires the Developer-app-owner
account to hold an active Premium subscription for any `/me/*` endpoint, and even after
activating Premium the account stayed stuck on a 403 (unresolved, possibly a platform-side
rollout bug). Rather than keep fighting an external, undebuggable blocker, the data domain
was switched to **OpenWeatherMap**, which the user already has a working, simple API-key
(no OAuth) for. All Spotify-specific code, docs, and constraints have been removed — if you
see any reference to Spotify, `bootstrap_oauth.py`, refresh tokens, or `recently-played`
anywhere, it's stale and should be deleted, not resurrected.

Separately, the compute host changed from a **DigitalOcean droplet to an Azure VM**:
DigitalOcean wound down its GitHub Student Developer Pack participation, and all credit
— including credit already redeemed — expired **2026-08-01**. This wasn't a design
change, it was forced by an external partner exiting the program; `education.github.com/pack`
still lists the DO offer, but it's stale and not actually redeemable. If you see references
to a "droplet" anywhere, it means an Azure VM now, or the doc is stale and should be fixed.

**Status: Phase 0, Phase 1, and Phase 2 complete, ingestion live.** Local tooling, git
repo (public, pushed via SSH to `github.com/NahianAlindo/hindcast-lakehouse`), OWM API
key (verified, Classic free tier, no card), and `ingestion/config/locations.yml` (10
curated locations) all exist. `ingestion/hindcast_extract/` is fully built: Pydantic
response-validation models (`models.py`), forecast model-run deduplication tracking
(`is_new_model_run`, state kept in ADLS at `bronze/_state/`), and a one-off Air Pollution
historical backfill script (`run_air_quality_backfill.py`, free back to 2020-11-27) on
top of the three live pollers. **Accrual is live and writes directly to ADLS** (bronze
container) via `.github/workflows/accrual-fallback.yml` — GitHub Actions cron on all
three cadences, authenticating with a storage account connection string (not Managed
Identity — this runner isn't an Azure resource, and this tenant blocks Service
Principal/OIDC creation for students). The earlier "commit bronze to git" stopgap is
retired; the ~70 files it had accrued were migrated into ADLS first, nothing lost.

**Phase 1 (IaC foundation) is applied and verified, zero drift in both Terraform
workspaces:**
- `hindcast-azure-data` (eastus): resource group, ADLS Gen2 (4 containers + lifecycle
  policy), Key Vault (RBAC, OWM key + Postgres password seeded), a persistent 8 GB
  Managed Disk for Postgres's data directory.
- `hindcast-azure-compute` (**westus2**, not eastus — see `docs/PLAN.md` §5 Phase 1 for
  why): VM (`Standard_B2ats_v2`, Ubuntu 24.04), NSG locked to home IP, the persistent
  disk attached, Managed Identity with RBAC roles onto ADLS + Key Vault. **This VM was
  created via the Azure Portal, then `terraform import`-ed** after every Ubuntu VM size
  failed with capacity restrictions through the CLI/Terraform path in `eastus` — the
  Portal's own size picker succeeded in `westus2` where scripted creation kept failing on
  the same sizes. Worth knowing as a real fallback pattern, not just a one-off workaround.
- Docker + a Postgres 16 container are running on the VM (verified via SSH, `pg_isready`
  passing), data on the persistent disk at `/mnt/postgres-data/pgdata` (a subdirectory,
  not the mount point — `initdb` refuses a non-empty directory, and a fresh ext4 disk
  always has `lost+found`).

Next up: Phase 3 (Airflow deployment on this VM). Don't jump ahead further than Phase 3
(e.g. don't sign up for Snowflake before Week 6; don't build dbt marts before Spark/silver
exists; don't build the report before enough wall-clock accrual has happened for the
analysis to mean anything — see the critical-path note in `docs/PLAN.md` §12).

## Tech stack

Python 3.11 · Apache Spark 3.5 (+ delta-spark 3.2) · Apache Airflow 3.x · dbt-core (dual
target: `duckdb` dev, `snowflake` prod) · Snowflake · DuckDB · DVC · Terraform (+
Terraform Cloud remote state) · Docker Compose · Azure (single subscription: a VM for
compute, ADLS Gen2 / Key Vault / PostgreSQL for the data plane — separate Terraform
workspaces, not separate subscriptions) · GitHub Actions · Datadog · Sentry ·
OpenLineage/Marquez · Power BI.

## Non-negotiable constraints

These will cause real breakage, wasted spend, or a ruined analysis if ignored — flag it
to me before silently working around any of these:

- **Total spend must be $0. Hard requirement, not a preference.** See `docs/PLAN.md` §10.
  Down to **one** real-money risk point now (dropping Spotify eliminated the
  Premium-trial clock; DigitalOcean's Student Pack partnership ending forced a move to
  an Azure VM, which turned out to eliminate that risk too — Azure for Students has no
  card on file at all and hard-stops at $100). The one thing left: **Snowflake**'s
  30-day trial needs no card, but converting to on-demand does — never do that, flip the
  dbt target to `duckdb` instead (already built as a first-class target from Week 5, not
  a last-minute scramble). Keep the GitHub repo **public** (unlimited free Actions
  minutes).
- **The project deliberately uses only ONE of the user's two available $100 Azure
  credits** (user's choice, not a technical requirement — don't "helpfully" spread
  resources onto the second one).
- **Postgres runs as a Docker container on the VM (data on a persistent Azure Managed
  Disk), not Azure PostgreSQL Flexible Server.** The managed service failed twice in
  real provisioning attempts — a 403 region restriction in `eastus`, then
  `CapacityNotAvailable` in `centralus` after 17 minutes — so it was dropped in favor of
  something that doesn't depend on Azure capacity outside our control. The disk (not the
  VM's OS disk) is what makes this still work like "disposable compute": `terraform
  destroy`/`apply` on the compute workspace tears down and rebuilds the VM, and the disk
  (with Postgres's data / Airflow's run history) reattaches automatically. Don't
  reintroduce a managed Postgres service without a real reason — this was hard-won.
- Session-scoped VM runtime (`terraform destroy`/`apply` between work sessions) is good
  practice, not strictly required for budget anymore — dropping managed Postgres means
  the VM alone (~$30/mo) fits the $100 credit for the full build even always-on. Do it
  anyway for the cost-discipline habit and because it's a genuinely good demo line.
- **Only use OpenWeatherMap's Classic free tier** (Current Weather, 5-day/3-hour
  Forecast, Air Pollution + history, Geocoding) — 60 calls/min, 1M calls/month, no card,
  no expiry. **Never use One Call API 3.0/4.0** — it requires a card on file for its
  pay-as-you-go overage, which would reintroduce exactly the real-money risk class that
  dropping Spotify eliminated. This is a deliberate, load-bearing decision, not a
  cost-cutting afterthought — see `docs/PLAN.md` §2 decision #2.
- **`issued_at` (the forecast request timestamp) must always be minted and recorded by
  the extractor, never inferred from the DAG schedule.** The entire lead-time analysis
  is built on this field. If it's ever computed after the fact from `logical_date` instead
  of the actual request time, every downstream lead-time bucket is silently wrong.
- **Ingestion must go live in Week 2 and never stop.** Unlike the old Spotify design,
  this dataset **accrues in wall-clock time and cannot be backfilled** — OpenWeatherMap
  doesn't sell you their own past forecasts. A missed poll is a permanently missing data
  point. `catchup=False` on all ingest DAGs is a deliberate domain decision, not laziness.
- **Don't sign up for Snowflake until Week 6.** Build the entire star schema against the
  `duckdb` dbt target first (Week 5) — no warehouse needed for that work, and starting
  the 30-day trial early just burns runway for nothing.
- **Spark runs in WSL2 or the devcontainer, never native Windows.** No `winutils.exe`
  workarounds.
- **Don't force SCD Type 2 onto `dim_location`.** Name/lat/lon/country/timezone are
  static — that would be theater. The one earned SCD2 candidate is the narrow mini-dimension
  `dim_location_regime` (banded, derived, seasonally-drifting attributes) — see
  `docs/PLAN.md` §6.5. The flagship modeling artifact is the **accumulating snapshot**
  fact `fct_forecast_slot`, not an SCD2.
- **Secrets never go into Terraform variables, `.env` in git, or committed files.** They
  live in Azure Key Vault, written out-of-band. `.env` (git-ignored) is the local-only
  stopgap before Key Vault exists.
- **DAG/Spark/dbt code ships baked into versioned Docker images, not `git-sync`'d from
  `main`.** Git tag → image digest is what makes "which pipeline version produced this
  row" answerable via the `dim_pipeline_run` audit dimension joined to every fact.
- **An Azure VM that's `Stop`ped but not `Stop (deallocate)`d still bills for compute.**
  Only `terraform destroy` or an explicit deallocate stops the meter — a plain OS
  shutdown does not.
- **No Kubernetes, no Airbyte/Fivetran/Meltano, no Great Expectations** unless the
  reasoning in `docs/PLAN.md`'s ADRs changes — deliberate scope calls, not oversights.
  `dlt` was seriously considered for ingestion and rejected (recorded as ADR-003's named
  runner-up) specifically because OWM's forecast response has no authoritative model-run
  timestamp — minting `issued_at` correctly is domain logic a transport library doesn't
  own.

## Conventions (once code exists)

- Package/env management: `uv`. Task running: `Taskfile.yml` (`Task`), not Make.
- Lint/format: `ruff` + `ruff-format` (Python), `sqlfluff` (dbt SQL), `terraform fmt` +
  `tflint`/`checkov` (IaC), `detect-secrets` — wired via `pre-commit`.
- Every non-obvious architectural choice gets a short ADR in `docs/adr/`, not just a code
  comment. `docs/PLAN.md` §5 Phase 10 lists the ~10 ADRs this project expects.
- Follow the monorepo layout implied by `docs/PLAN.md` (`ingestion/hindcast_extract/`,
  `transform/spark/`, `warehouse/dbt/`, `orchestration/airflow/`, `infra/terraform/`,
  `docker/`, `bi/`, `docs/adr/`, `docs/architecture/`, `docs/runbooks/`) rather than
  inventing a different structure.

## Working with me on this repo

- I'm a student building this as a portfolio piece — optimize for things that are
  genuinely demoable and defensible in an interview, not just "more tools."
- GitHub Student Developer Pack credits: Azure ×2 $100 activated, but **this project only
  uses one of them** (user's deliberate choice — see the constraint above). Datadog 2yr
  free (already activated/logged in). **DigitalOcean's $200 credit is gone** —
  partnership ended, all credit expired 2026-08-01, not usable regardless of what the
  pack page still shows. Plus a 30-day Snowflake trial once Week 6 starts it. Flag
  anything that risks burning credits faster than `docs/PLAN.md` §10 budgets for.
- **Calendar reality check**: this project's analysis quality is gated by wall-clock time
  accrued since ingestion went live, not by hours worked. Don't suggest shortcuts that
  would effectively restart the accrual clock (e.g. re-architecting the bronze layout
  after Week 2) without flagging that cost explicitly.
