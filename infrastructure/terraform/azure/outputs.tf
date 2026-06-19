output "resource_group_name" {
  description = "Provisioned resource group name."
  value       = local.resource_group_name
}

output "location" {
  description = "Azure region."
  value       = local.location
}

output "storage_account_name" {
  description = "Storage account name."
  value       = local.storage_account_name
}

output "storage_container_name" {
  description = "ADLS Gen2 container/filesystem name."
  value       = length(var.storage_layers) > 0 ? var.storage_container_name : ""
}

output "storage_dfs_endpoint" {
  description = "Primary DFS endpoint for the storage account."
  value       = local.primary_dfs_endpoint
}

output "storage_account_key" {
  description = "Primary storage account access key."
  value       = local.storage_account_key
  sensitive   = true
}

output "storage_layers_created" {
  description = "Lakehouse layers created."
  value       = var.storage_layers
}

output "vm_id" {
  description = "Dedicated VM resource ID."
  value       = var.create_vm ? azurerm_linux_virtual_machine.quickelt[0].id : ""
}

output "vm_public_ip" {
  description = "Dedicated VM public IP address."
  value       = var.create_vm ? azurerm_public_ip.quickelt[0].ip_address : ""
}

output "vm_private_ip" {
  description = "Dedicated VM private IP address."
  value = var.create_vm ? one([
    for cfg in azurerm_network_interface.quickelt[0].ip_configuration : cfg.private_ip_address
  ]) : ""
}

output "vm_private_key_pem" {
  description = "Generated private SSH key for the dedicated VM."
  value       = var.create_vm ? tls_private_key.quickelt_vm[0].private_key_pem : ""
  sensitive   = true
}

output "postgres_fqdn" {
  description = "PostgreSQL Flexible Server FQDN."
  value       = var.create_postgres ? azurerm_postgresql_flexible_server.quickelt[0].fqdn : ""
}

output "postgres_port" {
  description = "PostgreSQL port."
  value       = var.create_postgres ? 5432 : 0
}

output "postgres_database_name" {
  description = "PostgreSQL database name."
  value       = var.create_postgres ? var.postgres_database_name : ""
}
