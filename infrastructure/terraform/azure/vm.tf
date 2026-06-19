locals {
  bootstrap_script = <<-EOT
    #!/bin/bash
    set -euo pipefail
    echo "[quickelt-bootstrap] Starting VM bootstrap..."
    sudo apt-get update -y
    sudo apt-get install -y python3-pip git
    echo "[quickelt-bootstrap] Bootstrap complete."
  EOT

  local_postgres_script = var.local_pg_password != "" ? templatefile("${path.module}/templates/local_postgres.sh.tpl", {
    local_pg_password = var.local_pg_password
  }) : ""

  vm_custom_data = var.create_vm ? (
    var.install_local_postgres && var.local_pg_password != "" ? local.local_postgres_script : (
      var.bootstrap_vm ? local.bootstrap_script : ""
    )
  ) : ""
}

resource "azurerm_virtual_network" "quickelt" {
  count               = var.create_vm ? 1 : 0
  name                = "quickelt-vnet"
  address_space       = ["10.0.0.0/16"]
  location            = local.location
  resource_group_name = local.resource_group_name
  tags                = var.tags
}

resource "azurerm_subnet" "quickelt" {
  count                = var.create_vm ? 1 : 0
  name                 = "quickelt-subnet"
  resource_group_name  = local.resource_group_name
  virtual_network_name = azurerm_virtual_network.quickelt[0].name
  address_prefixes     = ["10.0.1.0/24"]
}

resource "azurerm_public_ip" "quickelt" {
  count               = var.create_vm ? 1 : 0
  name                = "quickelt-vm-pip"
  location            = local.location
  resource_group_name = local.resource_group_name
  allocation_method   = "Static"
  sku                 = "Standard"
  tags                = var.tags
}

resource "azurerm_network_interface" "quickelt" {
  count               = var.create_vm ? 1 : 0
  name                = "quickelt-vm-nic"
  location            = local.location
  resource_group_name = local.resource_group_name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.quickelt[0].id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.quickelt[0].id
  }

  tags = var.tags
}

resource "tls_private_key" "quickelt_vm" {
  count     = var.create_vm ? 1 : 0
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "azurerm_linux_virtual_machine" "quickelt" {
  count               = var.create_vm ? 1 : 0
  name                = var.vm_name
  resource_group_name = local.resource_group_name
  location            = local.location
  size                = var.vm_size
  admin_username      = "quickelt"

  network_interface_ids = [
    azurerm_network_interface.quickelt[0].id,
  ]

  admin_ssh_key {
    username   = "quickelt"
    public_key = tls_private_key.quickelt_vm[0].public_key_openssh
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts-gen2"
    version   = "latest"
  }

  custom_data = local.vm_custom_data != "" ? base64encode(local.vm_custom_data) : null
  tags        = var.tags
}
