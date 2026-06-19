data "azurerm_resource_group" "existing" {
  count = var.resource_group_existing ? 1 : 0
  name  = var.resource_group_name
}

resource "azurerm_resource_group" "quickelt" {
  count    = var.resource_group_existing ? 0 : 1
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags
}

locals {
  resource_group_name = var.resource_group_existing ? data.azurerm_resource_group.existing[0].name : azurerm_resource_group.quickelt[0].name
  location            = var.resource_group_existing ? data.azurerm_resource_group.existing[0].location : azurerm_resource_group.quickelt[0].location
}

data "azurerm_storage_account" "existing" {
  count               = var.storage_existing ? 1 : 0
  name                = var.storage_account_name
  resource_group_name = local.resource_group_name
}

resource "azurerm_storage_account" "lake" {
  count                    = var.storage_existing ? 0 : 1
  name                     = var.storage_account_name
  resource_group_name      = local.resource_group_name
  location                 = local.location
  account_tier             = "Standard"
  account_replication_type = var.storage_replication_type
  account_kind             = "StorageV2"
  is_hns_enabled           = true
  blob_properties {
    versioning_enabled = var.storage_versioning_enabled
    delete_retention_policy {
      days = var.storage_soft_delete_days
    }
  }
  tags = var.tags
}

resource "azurerm_management_lock" "storage_account" {
  count      = var.enable_destroy_protection ? 1 : 0
  name       = "${var.storage_account_name}-delete-lock"
  scope      = azurerm_storage_account.lake[0].id
  lock_level = "CanNotDelete"
  notes      = "Quickelt destroy protection lock"
}

locals {
  storage_account_id   = var.storage_existing ? data.azurerm_storage_account.existing[0].id : azurerm_storage_account.lake[0].id
  storage_account_name = var.storage_account_name
  primary_dfs_endpoint = var.storage_existing ? data.azurerm_storage_account.existing[0].primary_dfs_endpoint : azurerm_storage_account.lake[0].primary_dfs_endpoint
  storage_account_key  = var.storage_existing ? data.azurerm_storage_account.existing[0].primary_access_key : azurerm_storage_account.lake[0].primary_access_key
}
