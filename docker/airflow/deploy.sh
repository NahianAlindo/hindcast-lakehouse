#!/bin/bash
# Syncs this repo's Airflow-relevant subtrees to the VM, materializes the
# VM-local .env (Postgres password + admin UI password, sourced from Key
# Vault -- never committed), and brings the stack up. Run from the repo root:
#   bash docker/airflow/deploy.sh
set -euo pipefail

# tr -d '\r': az.exe accessed through WSL2's Windows-interop PATH emits
# CRLF line endings -- bash's $(...) only strips the trailing \n, leaving a
# literal \r embedded in the IP string, which corrupts the hostname just
# enough for ssh to reject it outright ("hostname contains invalid
# characters") without a more specific error pointing at the real cause.
VM_HOST="hindcast@$(az vm show -d --resource-group rg-hindcast-compute --name vm-hindcast --query publicIps -o tsv | tr -d '\r')"
REMOTE_DIR="/opt/hindcast/repo"
KEY_VAULT="kv-hindcastjlbpfz"

echo "==> Syncing repo subtrees to ${VM_HOST}:${REMOTE_DIR}"
# /opt/hindcast is root-owned (created by cloud-init) -- hindcast can sudo
# but isn't in the docker group, so fix ownership once rather than sudo
# every scp/ssh call below. rsync isn't available in this dev environment
# (Windows/Git Bash), so scp -r -- fine at this repo's size, no incremental
# sync benefit but nothing here is large enough for that to matter.
ssh "${VM_HOST}" "sudo mkdir -p ${REMOTE_DIR}/docker ${REMOTE_DIR}/orchestration/airflow ${REMOTE_DIR}/ingestion && sudo chown -R hindcast:hindcast ${REMOTE_DIR}"
scp -rq docker/airflow "${VM_HOST}:${REMOTE_DIR}/docker/"
scp -rq orchestration/airflow/dags "${VM_HOST}:${REMOTE_DIR}/orchestration/airflow/"
scp -rq ingestion/hindcast_extract "${VM_HOST}:${REMOTE_DIR}/ingestion/"
scp -rq ingestion/config "${VM_HOST}:${REMOTE_DIR}/ingestion/"

echo "==> Materializing VM-local .env from Key Vault"
# vm-postgres-admin-password, NOT postgres-admin-password -- two separate
# secrets from two separate random_password resources (azure_compute's own,
# vs. an unused one in azure_data). The VM's live Postgres cluster was
# initialized via cloud-init using azure_compute's value; fetching the wrong
# one here caused a real, hard-to-spot "password authentication failed"
# failure on every Airflow service except airflow-init (whose migrate_db
# call swallows failures with `|| true`, so it looked like it had succeeded).
POSTGRES_PASSWORD=$(az keyvault secret show --vault-name "${KEY_VAULT}" --name vm-postgres-admin-password --query value -o tsv)
AIRFLOW_ADMIN_PASSWORD=$(az keyvault secret show --vault-name "${KEY_VAULT}" --name airflow-admin-password --query value -o tsv)
# The password is 24 random chars including symbols (random_password.postgres_admin,
# special=true) -- safe as a raw POSTGRES_PASSWORD env var, but not safe to
# interpolate directly into a connection URI (a literal `@`, `[`, `/` etc. in the
# password corrupts URL parsing -- hit this live as `ValueError: Invalid IPv6 URL`
# from Airflow's own config validator). Percent-encode it once here, build the
# full URI here, and hand Airflow the finished string instead of assembling it
# from separate pieces inside the compose file.
POSTGRES_PASSWORD_URLENC=$(uv run python -c "from urllib.parse import quote; import sys; print(quote(sys.argv[1], safe=''))" "${POSTGRES_PASSWORD}")
AIRFLOW_SQL_ALCHEMY_CONN="postgresql+psycopg2://hindcast_admin:${POSTGRES_PASSWORD_URLENC}@postgres/airflow"
ssh "${VM_HOST}" "cat > ${REMOTE_DIR}/docker/airflow/.env" <<EOF
POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
AIRFLOW_ADMIN_PASSWORD=${AIRFLOW_ADMIN_PASSWORD}
AIRFLOW_SQL_ALCHEMY_CONN=${AIRFLOW_SQL_ALCHEMY_CONN}
GIT_COMMIT=$(git rev-parse HEAD)
EOF
unset POSTGRES_PASSWORD POSTGRES_PASSWORD_URLENC AIRFLOW_ADMIN_PASSWORD AIRFLOW_SQL_ALCHEMY_CONN

echo "==> Bringing the stack up (build + up -d)"
ssh "${VM_HOST}" "cd ${REMOTE_DIR}/docker/airflow && sudo docker compose up -d --build"

echo "==> Ensuring the forecast DAG's serialization pool exists"
# 1 slot: serializes owm_forecast_ingest's mapped per-location tasks so they
# don't race the shared dedup-state blob (see run_forecast.fetch_and_land's
# docstring). Idempotent -- `pools set` just updates the pool if it already
# exists, safe to run on every deploy.
ssh "${VM_HOST}" "sudo docker exec hindcast-airflow-scheduler-1 airflow pools set forecast_serial 1 'Serializes owm_forecast_ingest per-location tasks to avoid racing the shared dedup-state blob'"

echo "==> Done. Tunnel the API server with:"
echo "    ssh -L 8080:localhost:8080 ${VM_HOST}"
