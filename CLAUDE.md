# sonic-lakehouse

A portfolio data engineering project: an incremental ETL/ELT platform that captures my
personal Spotify listening history, lands it in an Azure data lake, transforms it with
Spark, models it into a Kimball star schema in Snowflake via dbt, orchestrates it with
Airflow on a DigitalOcean droplet, and serves it through Power BI — with Terraform IaC,
GitHub Actions CI/CD, DVC data versioning, and Datadog/Sentry/OpenLineage observability.

**Full build plan, phase-by-phase deliverables, star schema design, CI/CD design, and
cost/credit budget: [`docs/PLAN.md`](docs/PLAN.md). Read it before proposing architecture
changes — most "why" questions are already answered there, including an Executive Design
Decisions table and per-phase justifications.**

**Status: pre-code, planning complete.** This repo currently has no application code.
When implementing, follow the monorepo layout and phase order in `docs/PLAN.md` §2.6/§3
— don't jump ahead to later phases (e.g. don't set up Snowflake before Phase 5; don't
build dbt models before the extractor has been running long enough to have real data).

## Tech stack

Python 3.11 · Apache Spark 3.5 (+ delta-spark 3.2) · Apache Airflow 3.x · dbt-core ·
Snowflake · DVC · Terraform (+ Terraform Cloud remote state) · Docker Compose ·
DigitalOcean (compute) · Azure (ADLS Gen2, Key Vault, PostgreSQL) · GitHub Actions ·
Datadog · Sentry · OpenLineage/Marquez · Power BI.

## Non-negotiable constraints

These are decisions from `docs/PLAN.md` that will cause real breakage or wasted spend if
ignored — don't silently "improve" around them without flagging it to me first:

- **Total spend must be $0. This is a hard requirement, not a preference.** I don't need
  anything to stay live after the build is done — see `docs/PLAN.md` §8.3 "Zero-cost
  mode" for the full guardrails. In particular: never add a payment method to Snowflake
  (no on-demand conversion, ever — flip to the DuckDB target instead), skip the Azure
  Sub B buffer subscription entirely, keep the GitHub repo public (unlimited free Actions
  minutes), and watch DigitalOcean specifically — it requires a card on file to redeem
  the student credit and *will* bill that card once the credit runs out, unlike Azure for
  Students which hard-stops with no card. Run the full final-teardown checklist in §8.3
  once the project is done rather than leaving anything running "just in case."
- **Spark runs in WSL2 or the devcontainer, never native Windows.** No `winutils.exe`
  workarounds — that's not a skill demonstration, it's a rabbit hole.
- **Spotify redirect URI is `http://127.0.0.1:8888/callback`** — the literal loopback IP,
  not `localhost`. Spotify rejects `localhost` now.
- **No audio-features / audio-analysis / recommendations endpoints.** Apps created after
  Nov 2024 don't have access. Dimensional richness comes from genres, popularity,
  followers, album metadata, and derived behavioural measures — not audio features.
- **Don't sign up for Snowflake until Phase 5.** The trial is 30 days of wall-clock time
  and does not pause. Signing up early burns runway for nothing.
- **The Spotify extractor must ship in Phase 2 and never stop running.** `recently-played`
  cannot be backfilled — every week of delay is permanently lost history.
- **Secrets never go into Terraform variables, `.env` files, or git.** They live in Azure
  Key Vault, written out-of-band by scripts. Terraform only creates the vault + access
  policies.
- **DAG/pipeline code ships baked into versioned Docker images, not `git-sync`'d from
  `main`.** This is what makes "which pipeline version produced this row" answerable.
- **A powered-off DigitalOcean droplet still bills.** The only way to stop the meter is
  `terraform destroy`; use `task teardown` / `task standup` once those exist.
- **No Kubernetes, no Airbyte/Fivetran, no Great Expectations** unless the reasoning in
  `docs/PLAN.md`'s ADRs changes — these were deliberate scope calls, not oversights.

## Conventions (once code exists)

- Package/env management: `uv`. Task running: `Taskfile.yml` (`Task`), not Make.
- Lint/format: `ruff` + `ruff-format` (Python), `sqlfluff` (dbt SQL), `terraform fmt` +
  `tflint` + `checkov` (IaC), `gitleaks` (secrets scanning) — wired via `pre-commit`.
- Every non-obvious architectural choice gets a short ADR in `docs/adr/`, not just a code
  comment — that's the artifact that actually gets read.
- Follow the monorepo layout in `docs/PLAN.md` §2.6 (`ingestion/`, `transform/spark/`,
  `warehouse/dbt/`, `orchestration/airflow/`, `infra/terraform/`, `bi/`, `docs/adr/`, etc.)
  rather than inventing a different structure.

## Working with me on this repo

- I'm a student building this as a portfolio piece — optimize for things that are
  genuinely demoable and defensible in an interview, not just "more tools."
- I'm on GitHub Student Developer Pack credits (Azure ×2 $100, DigitalOcean ~$200) plus a
  30-day Snowflake trial — flag anything that risks burning credits faster than
  `docs/PLAN.md` §8 (Cost & Credit Management) budgets for.
