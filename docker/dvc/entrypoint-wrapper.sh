#!/bin/bash
# Weekly data-versioning snapshot (docs/PLAN.md Phase 6): fresh git clone ->
# `dvc repro` (dbt build --target duckdb, then export marts to Parquet) ->
# `dvc push` (uploads the new Parquet snapshot to the Azure remote) -> git
# commit + tag `data-vYYYY.WW` + push, only if this week's tag doesn't
# already exist (idempotent -- a re-run on the same ISO week is a no-op on
# the tag, though it still re-commits dvc.lock if the underlying data moved).
set -euo pipefail

KEY_VAULT_URL="https://kv-hindcastjlbpfz.vault.azure.net/"
REPO_URL_PATH="NahianAlindo/hindcast-lakehouse.git"
BRANCH="master"

fetch_secret() {
  local secret_name="$1"
  python -c "
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
client = SecretClient(vault_url='${KEY_VAULT_URL}', credential=DefaultAzureCredential())
print(client.get_secret('${secret_name}').value)
"
}

GITHUB_PAT="$(fetch_secret github-pat)"
export AZURE_STORAGE_CONNECTION_STRING
AZURE_STORAGE_CONNECTION_STRING="$(fetch_secret storage-connection-string)"

git clone --depth 1 --branch "$BRANCH" \
  "https://x-access-token:${GITHUB_PAT}@github.com/${REPO_URL_PATH}" \
  /workspace/repo
cd /workspace/repo

# A distinct bot identity, not the user's own -- these are automated data
# snapshots, not commits made on the user's behalf.
git config user.name "hindcast-dvc-bot"
git config user.email "dvc-bot@users.noreply.github.com"

# dbt_packages/ is gitignored (regenerable, like any lockfile-driven
# install -- same reasoning as docker/dbt/Dockerfile's build-time `dbt
# deps`), so a fresh clone never has it. `dbt build` doesn't install
# packages itself, so this has to run before dvc.yaml's dbt_build stage
# tries to resolve dbt_utils macros.
(cd warehouse/dbt/hindcast && dbt deps --profiles-dir .)

dvc repro
dvc push

TAG="data-v$(date -u +%G.%V)"

if git add dvc.lock && ! git diff --cached --quiet; then
  git commit -m "Weekly data snapshot: ${TAG}"
  if git ls-remote --exit-code --tags origin "refs/tags/${TAG}" >/dev/null 2>&1; then
    echo "Tag ${TAG} already exists on origin -- pushing the commit without re-tagging."
    git push origin "$BRANCH"
  else
    git tag "$TAG"
    git push origin "$BRANCH" "$TAG"
  fi
else
  echo "No data changes since the last snapshot -- nothing to commit."
fi
