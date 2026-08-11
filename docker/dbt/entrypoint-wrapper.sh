#!/bin/bash
# Fetches the right credentials from Key Vault via the VM's Managed Identity
# (same pattern as docker/spark/entrypoint-wrapper.sh), then either runs
# `dbt build --target $1` ($1 = "databricks" or "snowflake"; everything
# after it is forwarded to dbt, e.g. `--full-refresh` or `--select <model>`)
# or, for $1 = "elementary_report", generates Phase 7's DQ report and
# uploads it to ADLS. One image, three commands -- the tooling for all of
# them is already installed, so which secrets to fetch and which command to
# actually run is what branches here (docs/PLAN.md Phase 8: "one dbt image,
# one PR gate").
set -euo pipefail

KEY_VAULT_URL="https://kv-hindcastjlbpfz.vault.azure.net/"
TARGET="${1:?usage: entrypoint-wrapper.sh <databricks|snowflake|elementary_report> [args...]}"
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

fetch_snowflake_creds() {
  export SNOWFLAKE_ACCOUNT
  SNOWFLAKE_ACCOUNT="$(fetch_secret snowflake-account)"
  export SNOWFLAKE_USER
  SNOWFLAKE_USER="$(fetch_secret snowflake-user)"
  export SNOWFLAKE_PASSWORD
  SNOWFLAKE_PASSWORD="$(fetch_secret snowflake-password)"
}

# hindcast.dbt.test_failures (docs/PLAN.md Phase 7), tagged by target -- run
# `dbt build`, then parse the run_results.json artifact it always writes
# rather than trying to hook this from inside a macro (dbt's on-run-end
# Jinja context has the same `results` data, but emitting an HTTP metric
# from SQL/Jinja would mean a run_query() roundabout; parsing the JSON
# artifact dbt already produces is simpler and needs no dbt-side plumbing
# at all). Not `exec`'d, unlike the elementary_report branch below, because
# emitting the metric has to happen *after* the build finishes either way.
run_dbt_build_and_emit_test_failures() {
  local target="$1"
  shift
  set +e
  dbt build --profiles-dir . --target "$target" "$@"
  local exit_code=$?
  set -e
  python -c "
import json
import sys
from datadog_metrics import submit_count
try:
    with open('target/run_results.json') as f:
        results = json.load(f)['results']
except FileNotFoundError:
    sys.exit(0)  # dbt failed before writing any results at all
failures = sum(
    1 for r in results
    if r['unique_id'].startswith('test.') and r['status'] in ('fail', 'error')
)
submit_count('hindcast.dbt.test_failures', failures, tags=['target:${target}'])
print(f'hindcast.dbt.test_failures (target=${target}) = {failures}')
"
  exit "$exit_code"
}

case "$TARGET" in
  databricks)
    export DATABRICKS_HOST
    DATABRICKS_HOST="$(fetch_secret databricks-host)"
    export DATABRICKS_TOKEN
    DATABRICKS_TOKEN="$(fetch_secret databricks-token)"
    export DATADOG_API_KEY
    DATADOG_API_KEY="$(fetch_secret datadog-api-key)"
    run_dbt_build_and_emit_test_failures databricks "$@"
    ;;
  snowflake)
    fetch_snowflake_creds
    export DATADOG_API_KEY
    DATADOG_API_KEY="$(fetch_secret datadog-api-key)"
    run_dbt_build_and_emit_test_failures snowflake "$@"
    ;;
  elementary_report)
    # Reads elementary's tables from whatever dbt_build_snowflake's last run
    # already populated (its on-run-end hook, verified live) -- doesn't
    # re-run dbt build itself, just generates the report from existing data.
    fetch_snowflake_creds
    export AZURE_STORAGE_CONNECTION_STRING
    AZURE_STORAGE_CONNECTION_STRING="$(fetch_secret storage-connection-string)"
    edr report \
      --profiles-dir . --project-dir . --profile-target snowflake \
      --file-path /tmp/dq_report.html --open-browser false --disable-samples true
    python -c "
import os
from azure.storage.blob import BlobServiceClient
client = BlobServiceClient.from_connection_string(os.environ['AZURE_STORAGE_CONNECTION_STRING'])
with open('/tmp/dq_report.html', 'rb') as f:
    client.get_blob_client(container='exports', blob='dq_report.html').upload_blob(f, overwrite=True)
print('Uploaded dq_report.html to exports/dq_report.html')
"
    ;;
  *)
    echo "unknown target '$TARGET' -- expected databricks, snowflake, or elementary_report" >&2
    exit 1
    ;;
esac
