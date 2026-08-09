#!/bin/bash
# Fetches the right credentials from Key Vault via the VM's Managed Identity
# (same pattern as docker/spark/entrypoint-wrapper.sh) before running `dbt
# build --target $1`. $1 must be "databricks" or "snowflake" -- everything
# after it is forwarded to dbt, e.g. `--full-refresh` or `--select <model>`.
# One image, two targets (docs/PLAN.md Phase 8: "one dbt image, one PR
# gate") -- the adapters for both are already installed, so which secrets
# to fetch is the only thing that needs to branch here.
set -euo pipefail

KEY_VAULT_URL="https://kv-hindcastjlbpfz.vault.azure.net/"
TARGET="${1:?usage: entrypoint-wrapper.sh <databricks|snowflake> [dbt args...]}"
shift

fetch_secret() {
  local secret_name="$1"
  python -c "
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
client = SecretClient(vault_url='${KEY_VAULT_URL}', credential=DefaultAzureCredential())
print(client.get_secret('${secret_name}').value)
"
}

case "$TARGET" in
  databricks)
    export DATABRICKS_HOST
    DATABRICKS_HOST="$(fetch_secret databricks-host)"
    export DATABRICKS_TOKEN
    DATABRICKS_TOKEN="$(fetch_secret databricks-token)"
    ;;
  snowflake)
    export SNOWFLAKE_ACCOUNT
    SNOWFLAKE_ACCOUNT="$(fetch_secret snowflake-account)"
    export SNOWFLAKE_USER
    SNOWFLAKE_USER="$(fetch_secret snowflake-user)"
    export SNOWFLAKE_PASSWORD
    SNOWFLAKE_PASSWORD="$(fetch_secret snowflake-password)"
    ;;
  *)
    echo "unknown target '$TARGET' -- expected databricks or snowflake" >&2
    exit 1
    ;;
esac

exec dbt build --profiles-dir . --target "$TARGET" "$@"
