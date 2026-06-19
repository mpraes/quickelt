variable "resource_group_name" {
  description = "Azure resource group name."
  type        = string
  default     = "quickelt-rg"
}

variable "location" {
  description = "Azure region."
  type        = string
  default     = "eastus"
}

variable "storage_account_name" {
  description = "Storage account name for the data lake."
  type        = string
}

variable "storage_existing" {
  description = "When true, use an existing storage account instead of creating one."
  type        = bool
  default     = false
}

variable "resource_group_existing" {
  description = "When true, use an existing resource group instead of creating one."
  type        = bool
  default     = true
}

variable "storage_container_name" {
  description = "ADLS Gen2 filesystem/container name."
  type        = string
  default     = "quickelt-data"
}

variable "storage_layers" {
  description = "Lakehouse layer directories to create (e.g. bronze, silver, gold)."
  type        = list(string)
  default     = []
}

variable "storage_replication_type" {
  description = "Storage replication type (LRS, ZRS, GRS, RAGRS)."
  type        = string
  default     = "LRS"
}

variable "storage_soft_delete_days" {
  description = "Blob soft delete retention in days."
  type        = number
  default     = 7
}

variable "storage_versioning_enabled" {
  description = "Enable blob versioning."
  type        = bool
  default     = true
}

variable "create_vm" {
  description = "Create a dedicated compute VM."
  type        = bool
  default     = false
}

variable "vm_name" {
  description = "Linux VM name."
  type        = string
  default     = "quickelt-vm"
}

variable "vm_size" {
  description = "Azure VM SKU."
  type        = string
  default     = "Standard_D2s_v3"
}

variable "bootstrap_vm" {
  description = "Install Python/pip/git on the VM via cloud-init."
  type        = bool
  default     = true
}

variable "install_local_postgres" {
  description = "Install PostgreSQL on the VM via cloud-init."
  type        = bool
  default     = false
}

variable "local_pg_password" {
  description = "Password for local PostgreSQL user quickelt."
  type        = string
  default     = ""
  sensitive   = true
}

variable "create_postgres" {
  description = "Create Azure Database for PostgreSQL Flexible Server."
  type        = bool
  default     = false
}

variable "postgres_server_name" {
  description = "PostgreSQL Flexible Server name."
  type        = string
  default     = "quickelt-pg-server"
}

variable "postgres_database_name" {
  description = "PostgreSQL database name to create."
  type        = string
  default     = "quickelt_db"
}

variable "postgres_admin_username" {
  description = "PostgreSQL admin username."
  type        = string
  default     = "quickelt"
}

variable "postgres_admin_password" {
  description = "PostgreSQL admin password."
  type        = string
  default     = ""
  sensitive   = true
}

variable "postgres_sku_name" {
  description = "PostgreSQL Flexible Server SKU."
  type        = string
  default     = "B_Standard_B1ms"
}

variable "postgres_backup_retention_days" {
  description = "PostgreSQL backup retention days."
  type        = number
  default     = 7
}

variable "postgres_public_network_access_enabled" {
  description = "Enable public network access for PostgreSQL."
  type        = bool
  default     = true
}

variable "postgres_allowed_cidr" {
  description = "Allowed client CIDR for PostgreSQL firewall (e.g. 1.2.3.4/32)."
  type        = string
  default     = ""
}

variable "postgres_high_availability_enabled" {
  description = "Enable zone-redundant high availability for PostgreSQL."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Tags applied to managed resources."
  type        = map(string)
  default = {
    ManagedBy = "quickelt-setup"
    Project   = "quickelt"
  }
}

variable "enable_destroy_protection" {
  description = "When true, prevent Terraform from destroying critical resources."
  type        = bool
  default     = false
}
