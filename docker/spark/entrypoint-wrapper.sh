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

# Only load_into_databricks.py / load_into_snowflake.py need their
# respective warehouse credentials -- fetched conditionally so every other
# job here (the bronze->silver jobs, export_silver_snapshot.py) doesn't pay
# for a Key Vault round trip it has no use for.
if [[ "$1" = "load_into_databricks.py" ]]; then
  export DATABRICKS_HOST
  DATABRICKS_HOST="$(fetch_secret databricks-host)"
  export DATABRICKS_TOKEN
  DATABRICKS_TOKEN="$(fetch_secret databricks-token)"
elif [[ "$1" = "load_into_snowflake.py" ]]; then
  export SNOWFLAKE_ACCOUNT
  SNOWFLAKE_ACCOUNT="$(fetch_secret snowflake-account)"
  export SNOWFLAKE_USER
  SNOWFLAKE_USER="$(fetch_secret snowflake-user)"
  export SNOWFLAKE_PASSWORD
  SNOWFLAKE_PASSWORD="$(fetch_secret snowflake-password)"
fi

exec python "/opt/transform/jobs/$1"
