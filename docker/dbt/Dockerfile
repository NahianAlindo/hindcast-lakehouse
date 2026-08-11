# syntax=docker/dockerfile:1
FROM python:3.11-slim

# Exact versions match this repo's own pyproject.toml/uv.lock resolution
# (dbt-core, dbt-databricks) so the containerized build matches whatever
# was last verified locally, not a moving target. azure-identity/
# azure-keyvault-secrets are for entrypoint-wrapper.sh's own secret fetch,
# not dbt itself. dbt-snowflake==1.12.0 matches dbt-core's version exactly
# (verified against PyPI's actual release list, not assumed lockstep --
# dbt-databricks above doesn't follow the same scheme). One image, two
# remote-warehouse adapters -- docs/PLAN.md Phase 8's "one dbt image, one
# PR gate," target picked at `docker run` time (docker/dbt/entrypoint-
# wrapper.sh), not a second image.
# elementary-data[snowflake] is Phase 7's DQ report generator (edr report,
# run against the snowflake target only -- docs/PLAN.md Phase 7: it needs
# the @hourly cadence/history the primary pipeline has, not the Databricks
# demo path's @daily one). It already depends on azure-storage-blob itself
# (verified against its own PyPI metadata), so entrypoint-wrapper.sh's ADLS
# upload step doesn't need a separate azure-storage-blob pin here.
# ratelimit and grapheme (elementary's/alive-progress's transitive deps)
# have never published a wheel for any version (verified against PyPI's own
# metadata) -- same `--no-binary` carve-out as docker/dvc/Dockerfile's
# antlr4-python3-runtime, same reasoning: verified pure-Python libraries (a
# rate-limit decorator, a unicode grapheme-cluster counter -- neither has a
# compiled extension), so building from sdist doesn't reopen the arbitrary-
# build-script risk `--only-binary :all:` exists to avoid. alive-progress is
# pinned explicitly too -- elementary only declares an upper bound
# (<=2.3.1, no lower bound), so left unpinned pip resolved all the way down
# to 1.6.2, which doesn't have the alive_it() function elementary's own
# code imports (hit this live: ImportError at `edr report` runtime, not at
# install time, since pip's resolver considered 1.6.2 a valid solution).
RUN pip install --no-cache-dir --only-binary :all: pip==25.2 \
    && pip install --no-cache-dir --only-binary :all: --no-binary grapheme,ratelimit \
       alive-progress==2.3.1 \
       azure-identity==1.25.3 \
       azure-keyvault-secrets==4.11.0 \
       dbt-core==1.12.0 \
       dbt-databricks==1.10.9 \
       dbt-snowflake==1.12.0 \
       "elementary-data[snowflake]==0.17.0"

WORKDIR /opt/dbt/hindcast
COPY warehouse/dbt/hindcast /opt/dbt/hindcast
# Not under models/macros/tests -- dbt only parses model-paths/macro-paths/
# test-paths (dbt_project.yml), so a stray .py file at the project root is
# invisible to it. Lives here (not a separate dir + PYTHONPATH entry) so
# entrypoint-wrapper.sh's post-build `python -c` -- run from this same
# WORKDIR -- can `from datadog_metrics import submit_count` via Python's
# own cwd-relative import resolution, no extra plumbing.
COPY observability/datadog_metrics.py /opt/dbt/hindcast/datadog_metrics.py

COPY docker/dbt/entrypoint-wrapper.sh /entrypoint-wrapper.sh

# Runs as a dedicated non-root user -- nothing here (fetching a secret over
# HTTPS, running dbt against a remote warehouse, exiting) needs root. `edr
# report` installs its own internal dbt package (elementary/monitor/
# dbt_project/dbt_packages) at runtime, under site-packages -- root-owned by
# default since pip installed it during the build, before USER switches to
# non-root. Without this chown, edr's own `dbt deps` call fails with
# PermissionError (hit this live) the first time elementary_report runs.
RUN chmod +x /entrypoint-wrapper.sh \
    && useradd --create-home --uid 1000 dbtuser \
    && chown -R dbtuser:dbtuser /opt/dbt \
    && chown -R dbtuser:dbtuser /usr/local/lib/python3.11/site-packages/elementary/monitor/dbt_project
USER dbtuser

# dbt_packages/ is gitignored (regenerable, like any lockfile-driven
# install) -- fetched fresh at build time from packages.yml/package-lock.yml
# rather than relying on whatever may or may not be present in the local
# build context, so the image is reproducible regardless of build-machine
# state.
RUN dbt deps --profiles-dir .

ENTRYPOINT ["/entrypoint-wrapper.sh"]
