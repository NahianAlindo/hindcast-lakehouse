output "vm_public_ip" {
  value = azurerm_public_ip.this.ip_address
}

output "vm_name" {
  value = azurerm_linux_virtual_machine.this.name
}

output "vm_principal_id" {
  value = azurerm_linux_virtual_machine.this.identity[0].principal_id
}
