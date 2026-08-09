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
RUN pip install --no-cache-dir --only-binary :all: pip==25.2 \
    && pip install --no-cache-dir --only-binary :all: \
       azure-identity==1.25.3 \
       azure-keyvault-secrets==4.11.0 \
       dbt-core==1.12.0 \
       dbt-databricks==1.10.9 \
       dbt-snowflake==1.12.0

WORKDIR /opt/dbt/hindcast
COPY warehouse/dbt/hindcast /opt/dbt/hindcast

COPY docker/dbt/entrypoint-wrapper.sh /entrypoint-wrapper.sh

# Runs as a dedicated non-root user -- nothing here (fetching a secret over
# HTTPS, running dbt against a remote warehouse, exiting) needs root.
RUN chmod +x /entrypoint-wrapper.sh \
    && useradd --create-home --uid 1000 dbtuser \
    && chown -R dbtuser:dbtuser /opt/dbt
USER dbtuser

# dbt_packages/ is gitignored (regenerable, like any lockfile-driven
# install) -- fetched fresh at build time from packages.yml/package-lock.yml
# rather than relying on whatever may or may not be present in the local
# build context, so the image is reproducible regardless of build-machine
# state.
RUN dbt deps --profiles-dir .

ENTRYPOINT ["/entrypoint-wrapper.sh"]
