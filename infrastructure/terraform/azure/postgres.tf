locals {
  postgres_firewall_cidr    = trimspace(var.postgres_allowed_cidr)
  postgres_firewall_has_cidr = local.postgres_firewall_cidr != ""
  postgres_firewall_is_cidr = local.postgres_firewall_has_cidr && can(cidrhost(local.postgres_firewall_cidr, 0))
  postgres_firewall_start_ip = local.postgres_firewall_is_cidr
    ? cidrhost(local.postgres_firewall_cidr, 0)
    : local.postgres_firewall_cidr
  postgres_firewall_end_ip = local.postgres_firewall_is_cidr
    ? cidrhost(
      local.postgres_firewall_cidr,
      pow(2, 32 - tonumber(split("/", local.postgres_firewall_cidr)[1])) - 1
    )
    : local.postgres_firewall_cidr
}

resource "azurerm_postgresql_flexible_server" "quickelt" {
  count                         = var.create_postgres ? 1 : 0
  name                          = var.postgres_server_name
  resource_group_name           = local.resource_group_name
  location                      = local.location
  administrator_login           = var.postgres_admin_username
  administrator_password        = var.postgres_admin_password
  sku_name                      = var.postgres_sku_name
  storage_mb                    = 32768
  version                       = "15"
  backup_retention_days         = var.postgres_backup_retention_days
  public_network_access_enabled = var.postgres_public_network_access_enabled

  dynamic "high_availability" {
    for_each = var.postgres_high_availability_enabled ? [1] : []
    content {
      mode = "ZoneRedundant"
    }
  }

  tags = var.tags
}

resource "azurerm_postgresql_flexible_server_database" "gold" {
  count     = var.create_postgres ? 1 : 0
  name      = var.postgres_database_name
  server_id = azurerm_postgresql_flexible_server.quickelt[0].id
  charset   = "UTF8"
  collation = "en_US.utf8"

}

resource "azurerm_management_lock" "postgres_server" {
  count      = var.create_postgres && var.enable_destroy_protection ? 1 : 0
  name       = "${var.postgres_server_name}-delete-lock"
  scope      = azurerm_postgresql_flexible_server.quickelt[0].id
  lock_level = "CanNotDelete"
  notes      = "Quickelt destroy protection lock"
}

resource "azurerm_management_lock" "postgres_database" {
  count      = var.create_postgres && var.enable_destroy_protection ? 1 : 0
  name       = "${var.postgres_database_name}-delete-lock"
  scope      = azurerm_postgresql_flexible_server_database.gold[0].id
  lock_level = "CanNotDelete"
  notes      = "Quickelt destroy protection lock"
}

resource "azurerm_postgresql_flexible_server_firewall_rule" "client_access" {
  count = (
    var.create_postgres
    && var.postgres_public_network_access_enabled
    && local.postgres_firewall_has_cidr
  ) ? 1 : 0

  name             = "quickelt-client-access"
  server_id        = azurerm_postgresql_flexible_server.quickelt[0].id
  start_ip_address = local.postgres_firewall_start_ip
  end_ip_address   = local.postgres_firewall_end_ip
}
