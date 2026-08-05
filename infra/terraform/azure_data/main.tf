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
  purge_protection_enabled   = false
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

# --- Postgres for Airflow metadata ---
#
# NOT here. After Azure PostgreSQL Flexible Server failed twice on region
# restrictions (eastus) and then CapacityNotAvailable (centralus, after a
# 17-minute provision attempt), the pragmatic call was to drop the managed
# service entirely and run Postgres as a Docker container on the compute VM
# instead (infra/terraform/azure_compute). Traded away: the VM is no longer
# fully stateless -- destroying it loses Airflow's run history, not just its
# compute. Traded for: this resource stopped being the thing blocking Phase 1
# on Azure capacity/region availability outside our control. See
# docs/PLAN.md's updated Phase 1 section and ADR for the full reasoning.
