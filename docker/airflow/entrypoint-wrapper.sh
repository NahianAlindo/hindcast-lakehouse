#!/bin/bash
# Fetches secrets from Key Vault via the VM's Managed Identity (DefaultAzureCredential
# auto-detects it through Azure's Instance Metadata Service -- no client secret, no
# connection string typed into docker-compose) and exports them as plain env vars
# before handing off to Airflow's own entrypoint. config.py reads these the same way
# whether it's running here, standalone, or under GitHub Actions -- this is the only
# place that needs to know secrets come from Key Vault.
set -euo pipefail

KEY_VAULT_URL="https://kv-hindcastjlbpfz.vault.azure.net/"

fetch_secret() {
  python -c "
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
client = SecretClient(vault_url='${KEY_VAULT_URL}', credential=DefaultAzureCredential())
print(client.get_secret('$1').value)
"
}

export OWM_API_KEY
OWM_API_KEY="$(fetch_secret owm-api-key)"
export AZURE_STORAGE_CONNECTION_STRING
AZURE_STORAGE_CONNECTION_STRING="$(fetch_secret storage-connection-string)"
export AIRFLOW__CORE__FERNET_KEY
AIRFLOW__CORE__FERNET_KEY="$(fetch_secret airflow-fernet-key)"
export AIRFLOW__API_AUTH__JWT_SECRET
AIRFLOW__API_AUTH__JWT_SECRET="$(fetch_secret airflow-jwt-secret)"

exec /usr/bin/dumb-init -- /entrypoint "$@"
