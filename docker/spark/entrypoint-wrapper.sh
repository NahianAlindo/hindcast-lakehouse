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
export DATADOG_API_KEY
DATADOG_API_KEY="$(fetch_secret datadog-api-key)"

# Only load_into_databricks.py / load_into_snowflake.py / emit_pipeline_
# metrics.py need warehouse credentials -- fetched conditionally so every
# other job here (the bronze->silver jobs, export_silver_snapshot.py)
# doesn't pay for a Key Vault round trip it has no use for.
if [[ "$1" = "load_into_databricks.py" ]]; then
  export DATABRICKS_HOST
  DATABRICKS_HOST="$(fetch_secret databricks-host)"
  export DATABRICKS_TOKEN
  DATABRICKS_TOKEN="$(fetch_secret databricks-token)"
elif [[ "$1" = "load_into_snowflake.py" || "$1" = "emit_pipeline_metrics.py" ]]; then
  export SNOWFLAKE_ACCOUNT
  SNOWFLAKE_ACCOUNT="$(fetch_secret snowflake-account)"
  export SNOWFLAKE_USER
  SNOWFLAKE_USER="$(fetch_secret snowflake-user)"
  export SNOWFLAKE_PASSWORD
  SNOWFLAKE_PASSWORD="$(fetch_secret snowflake-password)"
fi

# hindcast.spark.job_duration_s (docs/PLAN.md Phase 7) only for genuine
# Spark/JVM jobs, not every script this image happens to also run --
# load_into_snowflake.py/load_into_databricks.py are lightweight non-Spark
# scripts that just reuse this image (see their own docstrings). Timed here
# at the entrypoint rather than inside each of the 4 job files individually
# (spark_session.py's build_spark_session() is shared, but each job's own
# main()/spark.stop() lifecycle isn't) -- one place to touch, and it
# measures the thing that's actually operationally relevant: total
# container runtime, not just time-inside-Spark.
SPARK_JOBS="bronze_current_to_silver.py bronze_air_quality_to_silver.py bronze_forecast_to_silver.py silver_forecast_milestones.py"
if [[ " $SPARK_JOBS " == *" $1 "* ]]; then
  start=$(date +%s.%N)
  set +e
  python "/opt/transform/jobs/$1"
  exit_code=$?
  set -e
  duration=$(python -c "print($(date +%s.%N) - $start)")
  python -c "
import sys
sys.path.insert(0, '/opt/transform/jobs')
from datadog_metrics import submit_gauge
submit_gauge('hindcast.spark.job_duration_s', $duration, tags=['job:$1'])
"
  exit "$exit_code"
else
  exec python "/opt/transform/jobs/$1"
fi
