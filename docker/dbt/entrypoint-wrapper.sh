#!/bin/bash
# Fetches Databricks credentials from Key Vault via the VM's Managed
# Identity (same pattern as docker/spark/entrypoint-wrapper.sh) before
# running `dbt build --target databricks`. Extra args passed to `docker
# run` are forwarded to dbt, e.g. `--full-refresh` or `--select <model>`.
set -euo pipefail

KEY_VAULT_URL="https://kv-hindcastjlbpfz.vault.azure.net/"

fetch_secret() {
  local secret_name="$1"
  python -c "
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
client = SecretClient(vault_url='${KEY_VAULT_URL}', credential=DefaultAzureCredential())
print(client.get_secret('${secret_name}').value)
"
}

export DATABRICKS_HOST
DATABRICKS_HOST="$(fetch_secret databricks-host)"
export DATABRICKS_TOKEN
DATABRICKS_TOKEN="$(fetch_secret databricks-token)"

exec dbt build --profiles-dir . --target databricks "$@"
