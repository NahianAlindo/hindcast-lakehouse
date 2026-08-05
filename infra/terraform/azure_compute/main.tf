data "terraform_remote_state" "data_plane" {
  backend = "remote"
  config = {
    organization = "NahianAlindo"
    workspaces = {
      name = "hindcast-azure-data"
    }
  }
}

# NOTE: this VM stack was created manually via the Azure Portal, not
# `terraform apply`, and then imported into this state. Every Ubuntu VM size
# (B2s, B2ms, B2ats_v2, B1s) hit SkuNotAvailable/CapacityRestrictions when
# created via Terraform/CLI in eastus; the Portal's own "See all sizes" view
# showed the exact same free-tier sizes as unavailable there too, but
# succeeded in westus2 once "No infrastructure redundancy required" +
# "Standard" security type were selected (zone-pinning + Trusted Launch look
# to have been excluding the free-tier B-series from availability). Resource
# names below match Azure's own auto-generated names from that Portal
# creation, not this project's usual naming convention, specifically so they
# import cleanly.

resource "azurerm_resource_group" "compute" {
  name = "rg-${var.project}-compute"
  # Hardcoded, deliberately NOT var.location: this resource group was first
  # created (by an earlier, since-abandoned Terraform apply attempt) with
  # location=eastus, before capacity restrictions forced the actual VM to
  # westus2. A resource group's own location is just metadata for where its
  # deployment history is stored -- it does NOT need to match where its
  # contained resources actually live, and changing it forces a destructive
  # replacement of the *entire* resource group (everything in it). Every
  # actual resource below sets its own location explicitly to var.location.
  location = "eastus"
}

# --- Networking (Portal-generated names, matched exactly for import) ---

resource "azurerm_virtual_network" "this" {
  name                = "vm-hindcast-vnet"
  address_space       = ["10.0.0.0/16"]
  location            = var.location
  resource_group_name = azurerm_resource_group.compute.name
}

resource "azurerm_subnet" "this" {
  name                 = "default"
  resource_group_name  = azurerm_resource_group.compute.name
  virtual_network_name = azurerm_virtual_network.this.name
  address_prefixes     = ["10.0.0.0/24"]
}

resource "azurerm_public_ip" "this" {
  name                = "vm-hindcast-ip"
  resource_group_name = azurerm_resource_group.compute.name
  location            = var.location
  allocation_method   = "Static"
  sku                 = "Standard"
}

# Static analysis flags any public IP as a hotspot to review (terraform:S6329).
# This one is load-bearing: it's how I SSH into the VM to do everything else
# in this project, and there's no free managed-jump-host alternative (Azure
# Bastion isn't free). The actual mitigation is the NSG above -- inbound is
# locked to `var.home_ip`/32 on port 22 only, nothing else reaches this IP.

resource "azurerm_network_security_group" "this" {
  name                = "vm-hindcast-nsg"
  location            = var.location
  resource_group_name = azurerm_resource_group.compute.name

  security_rule {
    name                       = "SSH"
    priority                   = 300
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefix      = "${var.home_ip}/32"
    destination_address_prefix = "*"
  }
}

resource "azurerm_network_interface" "this" {
  name                           = "vm-hindcast519"
  location                       = var.location
  resource_group_name            = azurerm_resource_group.compute.name
  accelerated_networking_enabled = true

  ip_configuration {
    name                          = "ipconfig1"
    subnet_id                     = azurerm_subnet.this.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.this.id
  }
}

resource "azurerm_network_interface_security_group_association" "this" {
  network_interface_id      = azurerm_network_interface.this.id
  network_security_group_id = azurerm_network_security_group.this.id
}

# --- The VM itself: cattle. Nothing on its OS disk persists across a
# destroy/apply -- only the attached Managed Disk (owned by azure_data) does. ---

resource "azurerm_linux_virtual_machine" "this" {
  name                  = "vm-hindcast"
  resource_group_name   = azurerm_resource_group.compute.name
  location              = var.location
  size                  = var.vm_size
  admin_username        = var.admin_username
  network_interface_ids = [azurerm_network_interface.this.id]

  admin_ssh_key {
    username   = var.admin_username
    public_key = var.ssh_public_key
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Premium_LRS"
  }

  additional_capabilities {
    hibernation_enabled = false
    ultra_ssd_enabled   = false
  }

  source_image_reference {
    publisher = "canonical"
    offer     = "ubuntu-24_04-lts"
    sku       = "server"
    version   = "latest"
  }

  # System-assigned Managed Identity: how the VM authenticates to ADLS/Key
  # Vault. Not a Service Principal -- this tenant blocks students from
  # creating those (see CLAUDE.md) -- but a Managed Identity only needs an
  # RBAC role assignment, which this account does have on its own subscription.
  identity {
    type = "SystemAssigned"
  }

  custom_data = base64encode(templatefile("${path.module}/cloud-init.sh.tpl", {
    postgres_password = random_password.postgres_admin.result
  }))

  lifecycle {
    # custom_data forces VM replacement on any diff, and this VM's actual
    # custom_data was pasted by hand into the Portal rather than applied by
    # Terraform -- don't let a whitespace/formatting mismatch trigger a
    # destructive VM recreation.
    ignore_changes = [custom_data]
  }
}

# Disk attachment: added once the persistent disk is moved to westus2 (it's
# currently in eastus with the rest of azure_data, and a Managed Disk can
# only attach to a VM in the same region -- see docs/PLAN.md's Phase 1 notes).
# Disk now lives in westus2 (moved from eastus in azure_data), matching this VM.
resource "azurerm_virtual_machine_data_disk_attachment" "postgres_data" {
  managed_disk_id    = data.terraform_remote_state.data_plane.outputs.postgres_disk_id
  virtual_machine_id = azurerm_linux_virtual_machine.this.id
  lun                = 0
  caching            = "ReadWrite"
}

# --- Postgres admin password: generated here, written back into the
# data-plane Key Vault as the durable copy (Terraform is the source of truth
# for it either way, since it's the thing generating it). ---

resource "random_password" "postgres_admin" {
  length  = 24
  special = true
}

resource "azurerm_key_vault_secret" "postgres_admin_password" {
  name         = "vm-postgres-admin-password"
  value        = random_password.postgres_admin.result
  key_vault_id = data.terraform_remote_state.data_plane.outputs.key_vault_id
}

# --- Managed Identity role assignments on the data-plane resources ---

resource "azurerm_role_assignment" "vm_storage" {
  scope                = data.terraform_remote_state.data_plane.outputs.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_linux_virtual_machine.this.identity[0].principal_id
}

resource "azurerm_role_assignment" "vm_keyvault" {
  scope                = data.terraform_remote_state.data_plane.outputs.key_vault_id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_linux_virtual_machine.this.identity[0].principal_id
}
