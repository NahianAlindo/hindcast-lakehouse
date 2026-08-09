#!/bin/bash
# Fetches the storage account key from Key Vault via the VM's Managed Identity
# (same pattern as docker/airflow/entrypoint-wrapper.sh) before running the
# job script passed as $1, e.g.:
#   docker run --rm hindcast-spark:local bronze_current_to_silver.py
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

export AZURE_STORAGE_ACCOUNT_KEY
AZURE_STORAGE_ACCOUNT_KEY="$(fetch_secret storage-account-key)"

exec python "/opt/transform/jobs/$1"
