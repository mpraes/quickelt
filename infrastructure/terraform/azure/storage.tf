resource "azurerm_storage_data_lake_gen2_filesystem" "lake" {
  count              = length(var.storage_layers) > 0 ? 1 : 0
  name               = var.storage_container_name
  storage_account_id = local.storage_account_id
}

resource "azurerm_storage_data_lake_gen2_path" "layers" {
  for_each = length(var.storage_layers) > 0 ? toset(var.storage_layers) : toset([])

  path               = each.value
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.lake[0].name
  storage_account_id = local.storage_account_id
  resource           = "directory"

  depends_on = [azurerm_storage_data_lake_gen2_filesystem.lake]
}
