# syntax=docker/dockerfile:1
FROM python:3.11-slim

# Unlike every other image in this repo, this one does NOT bake in
# warehouse/dbt/hindcast at build time -- entrypoint-wrapper.sh git-clones
# the actual repo fresh on every run instead. That's not a contradiction of
# "code ships baked into images, never git-synced" (docs/PLAN.md Phase 8):
# this container's own logic (the entrypoint script itself) IS image-pinned
# like everything else. What's different is this job's actual *purpose* --
# writing new commits/tags to the repo -- which requires a real git working
# tree to operate `dvc repro`/`git commit`/`git tag`/`git push` against. A
# fresh clone per run also means no persistent git state on the VM's disk,
# consistent with "the VM is cattle" (Phase 1): the clone lives only inside
# this one `docker run --rm` container and disappears when it exits.
# Versions match this repo's own pyproject.toml/uv.lock resolution
# (dbt-core, dbt-duckdb, dvc), same discipline as every other Dockerfile
# here. dvc-azure is the DVC remote adapter for Azure Blob (matches this
# repo's ADLS storage account); azure-identity/azure-keyvault-secrets are
# for entrypoint-wrapper.sh's own secret fetch. hydra-core/antlr4-python3-
# runtime are pinned explicitly (matching uv.lock's resolution) because
# left unpinned, pip's resolver picks hydra-core 1.1.0 -- which needs
# antlr4-python3-runtime==4.8, conflicting with dbt-core's own antlr4 pin --
# instead of the newer, compatible 1.3.5 uv resolves to for the exact same
# top-level requirements. hydra-core==1.3.5 in turn hard-pins
# antlr4-python3-runtime==4.9.* (verified against PyPI's own metadata), and
# no 4.9.x wheel exists at all -- only 4.11+ do. `--no-binary` carves out
# just this one package to build from its sdist: verified pure-Python, no
# compiled extension, so this doesn't reopen the arbitrary-build-script risk
# `--only-binary :all:` exists to avoid (contrast pyspark's wheelbuilder
# stage above, which exists precisely because that risk was real there).
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --only-binary :all: pip==25.2 \
    && pip install --no-cache-dir --only-binary :all: --no-binary antlr4-python3-runtime \
       antlr4-python3-runtime==4.9.3 \
       azure-identity==1.25.3 \
       azure-keyvault-secrets==4.11.0 \
       dbt-core==1.12.0 \
       dbt-duckdb==1.11.0 \
       dvc==3.67.1 \
       dvc-azure==3.1.1 \
       hydra-core==1.3.5

WORKDIR /workspace

COPY docker/dvc/entrypoint-wrapper.sh /entrypoint-wrapper.sh

# Runs as a dedicated non-root user -- fetching secrets, cloning a public
# repo over HTTPS, and running dbt/dvc against it needs no elevated
# privileges. Unlike the other images, this one can't chown a baked-in
# app directory at build time (there isn't one yet -- the clone happens at
# runtime), so ownership is set up once entrypoint-wrapper.sh creates
# /workspace/repo instead.
RUN chmod +x /entrypoint-wrapper.sh \
    && useradd --create-home --uid 1000 dvcuser \
    && chown -R dvcuser:dvcuser /workspace
USER dvcuser

ENTRYPOINT ["/entrypoint-wrapper.sh"]
