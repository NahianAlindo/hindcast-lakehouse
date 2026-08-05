#!/bin/bash
# Phase 1 scope only: Docker + the persistent disk + a Postgres container for
# Airflow's future metadata DB. Airflow itself (Phase 3) and Spark (Phase 4)
# add their own Compose services later -- not built here, per docs/PLAN.md's
# phase order.
set -euo pipefail

# --- Docker + Compose plugin ---
apt-get update
apt-get install -y ca-certificates curl gnupg
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
  > /etc/apt/sources.list.d/docker.list
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# --- Mount the persistent Managed Disk (owned by azure_data's workspace,
# just attached here) for Postgres's data directory. Idempotent: only
# formats if the disk has no filesystem yet, so re-running this script (or
# a VM rebuild that reattaches the same disk) never wipes existing data. ---
DISK_DEV="/dev/disk/azure/scsi1/lun0"
MOUNT_POINT="/mnt/postgres-data"
if ! blkid "$DISK_DEV" > /dev/null 2>&1; then
  mkfs.ext4 "$DISK_DEV"
fi
mkdir -p "$MOUNT_POINT"
mount "$DISK_DEV" "$MOUNT_POINT"
UUID=$(blkid -s UUID -o value "$DISK_DEV")
grep -q "$UUID" /etc/fstab || echo "UUID=$UUID $MOUNT_POINT ext4 defaults,nofail 0 2" >> /etc/fstab

mkdir -p /opt/hindcast /mnt/postgres-data/pgdata
cat > /opt/hindcast/docker-compose.yml <<COMPOSE
services:
  postgres:
    image: postgres:16
    restart: unless-stopped
    environment:
      POSTGRES_USER: hindcast_admin
      POSTGRES_PASSWORD: ${postgres_password}
      POSTGRES_DB: airflow
    volumes:
      # A subdirectory of the mount, not the mount point itself -- a fresh
      # ext4 filesystem always has a lost+found directory, and initdb refuses
      # to initialize a non-empty directory.
      - /mnt/postgres-data/pgdata:/var/lib/postgresql/data
    ports:
      - "127.0.0.1:5432:5432"
COMPOSE

cd /opt/hindcast && docker compose up -d
