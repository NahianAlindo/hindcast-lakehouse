#!/bin/bash
# Syncs this repo's Airflow-relevant subtrees to the VM, materializes the
# VM-local .env (Postgres password + admin UI password, sourced from Key
# Vault -- never committed), and brings the stack up. Run from the repo root:
#   bash docker/airflow/deploy.sh
set -euo pipefail

VM_HOST="hindcast@$(az vm show -d --resource-group rg-hindcast-compute --name vm-hindcast --query publicIps -o tsv)"
REMOTE_DIR="/opt/hindcast/repo"
KEY_VAULT="kv-hindcastjlbpfz"

echo "==> Syncing repo subtrees to ${VM_HOST}:${REMOTE_DIR}"
# /opt/hindcast is root-owned (created by cloud-init) -- hindcast can sudo
# but isn't in the docker group, so fix ownership once rather than sudo
# every rsync/ssh call below.
ssh "${VM_HOST}" "sudo mkdir -p ${REMOTE_DIR} && sudo chown -R hindcast:hindcast ${REMOTE_DIR}"
rsync -az --delete \
  docker/airflow/ "${VM_HOST}:${REMOTE_DIR}/docker/airflow/"
rsync -az --delete \
  orchestration/airflow/dags/ "${VM_HOST}:${REMOTE_DIR}/orchestration/airflow/dags/"
rsync -az --delete \
  ingestion/hindcast_extract/ "${VM_HOST}:${REMOTE_DIR}/ingestion/hindcast_extract/"
rsync -az --delete \
  ingestion/config/ "${VM_HOST}:${REMOTE_DIR}/ingestion/config/"

echo "==> Materializing VM-local .env from Key Vault"
POSTGRES_PASSWORD=$(az keyvault secret show --vault-name "${KEY_VAULT}" --name postgres-admin-password --query value -o tsv)
AIRFLOW_ADMIN_PASSWORD=$(az keyvault secret show --vault-name "${KEY_VAULT}" --name airflow-admin-password --query value -o tsv)
ssh "${VM_HOST}" "cat > ${REMOTE_DIR}/docker/airflow/.env" <<EOF
POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
AIRFLOW_ADMIN_PASSWORD=${AIRFLOW_ADMIN_PASSWORD}
EOF
unset POSTGRES_PASSWORD AIRFLOW_ADMIN_PASSWORD

echo "==> Bringing the stack up (build + up -d)"
ssh "${VM_HOST}" "cd ${REMOTE_DIR}/docker/airflow && sudo docker compose up -d --build"

echo "==> Done. Tunnel the API server with:"
echo "    ssh -L 8080:localhost:8080 ${VM_HOST}"
