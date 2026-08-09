resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

resource "azurerm_resource_group" "this" {
  name     = "rg-${var.project}"
  location = var.location
}

# --- ADLS Gen2: bronze / silver / exports / dvc ---

resource "azurerm_storage_account" "lake" {
  name                     = "st${var.project}${random_string.suffix.result}"
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # makes this ADLS Gen2, not plain blob storage

  # Not currently used for anything (we don't do customer-managed-key
  # encryption -- extractors and the VM authenticate *into* this account via
  # connection string / the VM's own Managed Identity, neither of which needs
  # this account to have an identity of its own). Added anyway: a
  # system-assigned identity costs nothing, doesn't replace the resource, and
  # clears terraform:S6378. Free to have on hand if CMK is ever adopted later.
  identity {
    type = "SystemAssigned"
  }

  blob_properties {
    delete_retention_policy {
      days = 7
    }
  }
}

resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "exports" {
  name                  = "exports"
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "dvc" {
  name                  = "dvc"
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

resource "azurerm_storage_management_policy" "lifecycle" {
  storage_account_id = azurerm_storage_account.lake.id

  rule {
    name    = "bronze-cool-after-30d"
    enabled = true
    filters {
      blob_types   = ["blockBlob"]
      prefix_match = ["bronze/"]
    }
    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than = 30
      }
    }
  }
}

# --- Key Vault ---

data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "this" {
  name                       = "kv-${var.project}${random_string.suffix.result}"
  resource_group_name        = azurerm_resource_group.this.name
  location                   = azurerm_resource_group.this.location
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  rbac_authorization_enabled = true
  # Left false through Phase 1 while this vault was repeatedly destroyed/
  # recreated during iteration (purge protection would've blocked reusing the
  # same name for 7 days after each destroy). Now that it's a stable,
  # long-lived resource holding real secrets (OWM key, Postgres password),
  # turned on -- this is a one-way flag (Azure never lets it go back to
  # false), which is exactly the point.
  purge_protection_enabled   = true
  soft_delete_retention_days = 7
}

resource "azurerm_role_assignment" "kv_admin_self" {
  scope                = azurerm_key_vault.this.id
  role_definition_name = "Key Vault Administrator"
  principal_id         = data.azurerm_client_config.current.object_id
}

# Placeholder value -- real key set out-of-band via `az keyvault secret set`
# straight after apply, read from local .env. Never touches Terraform state
# beyond this placeholder, per docs/PLAN.md's secrets-handling rule.
resource "azurerm_key_vault_secret" "owm_api_key" {
  name         = "owm-api-key"
  value        = "placeholder-set-via-az-cli"
  key_vault_id = azurerm_key_vault.this.id
  depends_on   = [azurerm_role_assignment.kv_admin_self]

  lifecycle {
    ignore_changes = [value]
  }
}

# Phase 3: the VM's Airflow containers fetch this at startup via the VM's
# Managed Identity (Key Vault Secrets User role, already granted in
# azure_compute) instead of a connection string typed into docker-compose.
# Real value set out-of-band via `az keyvault secret set`, same pattern as
# owm_api_key above.
resource "azurerm_key_vault_secret" "storage_connection_string" {
  name         = "storage-connection-string"
  value        = "placeholder-set-via-az-cli"
  key_vault_id = azurerm_key_vault.this.id
  depends_on   = [azurerm_role_assignment.kv_admin_self]

  lifecycle {
    ignore_changes = [value]
  }
}

# Encrypts connection passwords at rest in Airflow's own metadata DB -- not an
# extractor secret, but fetched by the same entrypoint-wrapper mechanism for
# the same reason (never typed into docker-compose).
resource "azurerm_key_vault_secret" "airflow_fernet_key" {
  name         = "airflow-fernet-key"
  value        = "placeholder-set-via-az-cli"
  key_vault_id = azurerm_key_vault.this.id
  depends_on   = [azurerm_role_assignment.kv_admin_self]

  lifecycle {
    ignore_changes = [value]
  }
}

# Airflow's web UI admin login. Unlike owm_api_key/storage_connection_string/
# airflow_fernet_key above (fetched at container runtime via Managed Identity),
# this is consumed at docker-compose render time via a VM-local .env file
# (git-ignored, materialized from this secret at deploy time) -- it only
# gates the `airflow-init` bootstrap step, not anything DAG/task code touches.
resource "azurerm_key_vault_secret" "airflow_admin_password" {
  name         = "airflow-admin-password"
  value        = "placeholder-set-via-az-cli"
  key_vault_id = azurerm_key_vault.this.id
  depends_on   = [azurerm_role_assignment.kv_admin_self]

  lifecycle {
    ignore_changes = [value]
  }
}

# --- Postgres for Airflow metadata ---
#
# No managed Postgres service here. After Azure PostgreSQL Flexible Server
# failed twice on region restrictions (eastus) and then CapacityNotAvailable
# (centralus, after a 17-minute provision attempt), Postgres moved to a Docker
# container on the compute VM (infra/terraform/azure_compute) instead. This
# disk -- not the VM's OS disk -- is what keeps that arrangement from losing
# Airflow's history every time the VM is torn down: it's managed here, in the
# data-plane workspace, specifically so a routine `terraform destroy` on the
# *compute* workspace can't touch it. azure_compute only attaches it.
resource "azurerm_managed_disk" "postgres_data" {
  name                = "disk-${var.project}-postgres-data"
  resource_group_name = azurerm_resource_group.this.name
  # Deliberately NOT azurerm_resource_group.this.location (eastus): the
  # compute VM ended up in westus2 after every Ubuntu VM size hit capacity
  # restrictions in eastus (see infra/terraform/azure_compute/main.tf), and a
  # Managed Disk can only attach to a VM in its own region. Storage
  # account/Key Vault stay in eastus -- only this disk needs to follow the VM.
  location             = "westus2"
  storage_account_type = "Standard_LRS"
  create_option        = "Empty"
  disk_size_gb         = 8

  # This disk is never exported/downloaded outside its VM attach (no
  # `az disk grant-access` SAS flow anywhere in this project) -- disabling
  # public network access has no functional cost and closes off that path
  # entirely.
  public_network_access_enabled = false
  network_access_policy         = "DenyAll"

  # No `disk_encryption_set_id`: that's for customer-managed keys. Azure
  # encrypts this disk at rest by default with platform-managed keys (SSE),
  # which is sufficient here -- there's no compliance requirement driving a
  # CMK setup, and standing one up would mean an extra Key Vault key + a
  # dedicated Disk Encryption Set resource for no real benefit to a $0
  # student portfolio project.

  lifecycle {
    prevent_destroy = true
  }
}
