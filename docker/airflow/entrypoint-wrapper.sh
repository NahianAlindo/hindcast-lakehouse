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
  local secret_name="$1"
  python -c "
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
client = SecretClient(vault_url='${KEY_VAULT_URL}', credential=DefaultAzureCredential())
print(client.get_secret('${secret_name}').value)
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
# SENTRY_DSN_INGEST: the extractor's own exceptions (ingestion/hindcast_
# extract/observability.py), reached via the ingest DAGs' direct TaskFlow
# import of fetch_and_land -- same container, same env, no separate fetch
# needed there. SENTRY_DSN_AIRFLOW: orchestration/airflow/config/
# airflow_local_settings.py's task_policy, every task's failure across
# every DAG.
export SENTRY_DSN_INGEST
SENTRY_DSN_INGEST="$(fetch_secret sentry-dsn-ingest)"
export SENTRY_DSN_AIRFLOW
SENTRY_DSN_AIRFLOW="$(fetch_secret sentry-dsn-airflow)"

exec /usr/bin/dumb-init -- /entrypoint "$@"
