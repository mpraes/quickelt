#!/usr/bin/env python3
"""
Quickelt Azure Terraform Provisioner
====================================

Terraform-backed Azure provisioner used by the setup wizard.
Replaces imperative ``az`` CLI provisioning with declarative IaC.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from setup.cli_executor import CLIExecutor, ErrorCategory, Spinner
from setup.constants import DEFAULT_AZURE_LOCATION, DEFAULT_AZURE_RESOURCE_GROUP
from setup.env_writer import EnvWriter
from setup.provisioner import Provisioner
from setup.terraform_executor import TerraformExecutor


class AzureTerraformProvisioner(Provisioner):
  CLOUD_NAME = "Azure"
  _SUBSCRIPTION_ID_RE = re.compile(
      r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
  )

  def __init__(
      self,
      cli: CLIExecutor,
      env: EnvWriter,
      logger: logging.Logger | None = None,
      terraform: TerraformExecutor | None = None,
      setup_name: str | None = None,
  ):
    super().__init__(cli, env, logger)
    workspace_dir = None
    resolved_name = setup_name
    if not resolved_name:
      env_name = self.env.read_value("SETUP_NAME")
      if isinstance(env_name, str) and env_name:
        resolved_name = env_name
    if resolved_name:
      from setup.setup_registry import get_setup_terraform_workspace

      workspace_dir = get_setup_terraform_workspace(resolved_name)
    self.terraform = terraform or TerraformExecutor(logger=self.log, workspace_dir=workspace_dir)

  def _get_location(self) -> str:
    return self._detect_region(
        ["az", "account", "show", "--output", "json"],
        "AZURE_LOCATION",
        DEFAULT_AZURE_LOCATION,
        json_key="location",
    )

  def _ensure_subscription(self, subscription_id: str) -> dict[str, Any]:
    spinner = Spinner(f"Setting Azure subscription '{subscription_id}'...", logger=self.log).start()
    result = self.cli.execute(
        ["az", "account", "set", "--subscription", subscription_id],
        timeout=20,
    )
    if result["ok"]:
      spinner.succeed(f"Azure subscription set to '{subscription_id}'")
      return {"ok": True, "message": "set", "subscription_id": subscription_id}

    return self._handle_cli_error(
        result.get("error_category"),
        spinner,
        result,
        {"subscription_id": subscription_id},
        fail_label="Setting Azure subscription",
        unauthorized_log="Your Azure account lacks permission to use the selected subscription.",
        auth_expired_log="Your Azure authentication token has expired. Run 'az login' to re-authenticate.",
    )

  def _ensure_resource_group(self, resource_group: str, location: str) -> dict[str, Any]:
    spinner = Spinner(f"Checking resource group '{resource_group}'...", logger=self.log).start()
    exists_result = self.cli.execute(
        ["az", "group", "exists", "--name", resource_group, "--output", "tsv"],
        timeout=20,
    )

    if not exists_result["ok"]:
      return self._handle_cli_error(
          exists_result.get("error_category"),
          spinner,
          exists_result,
          {"resource_group": resource_group},
          fail_label="Checking resource group",
          unauthorized_log="Your Azure account lacks permission to read resource groups.",
          auth_expired_log="Your Azure authentication token has expired. Run 'az login' to re-authenticate.",
      )

    if exists_result["stdout"].strip().lower() == "true":
      spinner.succeed(f"Resource group '{resource_group}' already exists")
      return {"ok": True, "message": "already_exists", "resource_group": resource_group}

    spinner.clear()
    self.log.warning("Resource group '%s' not found. It will be created.", resource_group)

    create_spinner = Spinner(
        f"Creating resource group '{resource_group}' in {location}...",
        logger=self.log,
    ).start()
    create_result = self.cli.execute(
        [
            "az",
            "group",
            "create",
            "--name",
            resource_group,
            "--location",
            location,
            "--output",
            "json",
        ],
        timeout=90,
    )

    if create_result["ok"]:
      create_spinner.succeed(f"Resource group '{resource_group}' created in {location}")
      return {"ok": True, "message": "created", "resource_group": resource_group}

    return self._handle_cli_error(
        create_result.get("error_category", ErrorCategory.UNKNOWN),
        create_spinner,
        create_result,
        {"resource_group": resource_group},
        fail_label="Creating resource group",
        unauthorized_log="Your Azure account lacks permission to create resource groups.",
        auth_expired_log="Your Azure authentication token has expired. Run 'az login' to re-authenticate.",
    )

  def provision(self, storage: dict, compute: dict, dw: dict) -> dict[str, Any]:
    self._print_provision_banner()

    subscription_id = (self.env.read_value("AZURE_SUBSCRIPTION_ID") or "").strip()
    if subscription_id and self._SUBSCRIPTION_ID_RE.match(subscription_id):
      sub_result = self._ensure_subscription(subscription_id)
      if not sub_result.get("ok"):
        detail = sub_result.get("message", "unknown error")
        self.log.error("Azure subscription selection failed: %s", detail)
        return {"ok": False, "message": "subscription_failed", "detail": detail}

    resource_group = self.env.read_value("AZURE_RESOURCE_GROUP") or DEFAULT_AZURE_RESOURCE_GROUP
    location = self.env.read_value("AZURE_LOCATION") or self._get_location()

    rg_result = self._ensure_resource_group(resource_group, location)
    if not rg_result.get("ok"):
      detail = rg_result.get("message", "unknown error")
      self.log.error("Azure resource group check failed: %s", detail)
      return {"ok": False, "message": "resource_group_failed", "detail": detail, "resource_group": resource_group}

    if dw.get("gold_external_db") and dw.get("pg_strategy") == "managed_cloud":
      if dw.get("managed_cloud_choice") == "connect_existing":
        self.log.info("Connecting to existing managed PostgreSQL — skipping Terraform database provisioning.")

    result = self.terraform.provision(
        storage,
        compute,
        dw,
        resource_group=resource_group,
        location=location,
    )

    if not result.get("ok"):
      detail = result.get("detail", result.get("message", "unknown error"))
      self.log.error("Terraform provisioning failed: %s", detail)
      return {"ok": False, "message": result.get("message", "failed"), "terraform": result}

    outputs = result.get("outputs", {})
    metadata = self._outputs_to_env(outputs, storage["name"])
    if metadata:
      print()
      self.env.update_metadata(metadata)
      self.log.info(".env updated with Azure Terraform metadata (%d keys)", len(metadata))

    print()
    self.log.info("Azure Terraform provisioning completed successfully.")
    print()

    return {
        "ok": True,
        "message": "applied",
        "terraform": result,
        "outputs": outputs,
    }

  def destroy(self, storage: dict, compute: dict, dw: dict) -> dict[str, Any]:
    self._print_provision_banner()

    resource_group = self.env.read_value("AZURE_RESOURCE_GROUP") or DEFAULT_AZURE_RESOURCE_GROUP
    location = self.env.read_value("AZURE_LOCATION") or self._get_location()

    result = self.terraform.destroy(
        storage,
        compute,
        dw,
        resource_group=resource_group,
        location=location,
    )

    if not result.get("ok"):
      detail = result.get("detail", result.get("message", "unknown error"))
      self.log.error("Terraform destroy failed: %s", detail)
      return {"ok": False, "message": result.get("message", "failed"), "terraform": result}

    print()
    self.log.info("Azure Terraform destroy completed successfully.")
    print()

    return {
        "ok": True,
        "message": "destroyed",
        "terraform": result,
    }

  @staticmethod
  def _outputs_to_env(outputs: dict[str, Any], storage_name: str) -> dict[str, str]:
    metadata: dict[str, str] = {}

    if outputs.get("resource_group_name"):
      metadata["AZURE_RESOURCE_GROUP"] = str(outputs["resource_group_name"])
    if outputs.get("location"):
      metadata["AZURE_LOCATION"] = str(outputs["location"])

    metadata["AZURE_STORAGE_ACCOUNT"] = str(outputs.get("storage_account_name") or storage_name)

    if outputs.get("storage_container_name"):
      metadata["AZURE_STORAGE_CONTAINER"] = str(outputs["storage_container_name"])
    if outputs.get("storage_dfs_endpoint"):
      metadata["AZURE_STORAGE_DFS_ENDPOINT"] = str(outputs["storage_dfs_endpoint"])
    if outputs.get("storage_account_key"):
      metadata["AZURE_STORAGE_KEY"] = str(outputs["storage_account_key"])

    if outputs.get("vm_id"):
      metadata["AZURE_VM_ID"] = str(outputs["vm_id"])
    if outputs.get("vm_public_ip"):
      metadata["AZURE_VM_PUBLIC_IP"] = str(outputs["vm_public_ip"])
    if outputs.get("vm_private_ip"):
      metadata["AZURE_VM_PRIVATE_IP"] = str(outputs["vm_private_ip"])
    if outputs.get("vm_private_key_path"):
      metadata["AZURE_VM_SSH_PRIVATE_KEY_PATH"] = str(outputs["vm_private_key_path"])

    if outputs.get("postgres_fqdn"):
      metadata["DW_HOST"] = str(outputs["postgres_fqdn"])
      metadata["DW_PORT"] = str(outputs.get("postgres_port") or 5432)
    if outputs.get("postgres_database_name"):
      metadata["DW_DATABASE"] = str(outputs["postgres_database_name"])

    return metadata
