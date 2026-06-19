#!/usr/bin/env python3
"""
Quickelt Terraform Executor
===========================

Runs Terraform init/plan/apply/destroy/output for Azure infrastructure modules.
Translates wizard configuration into Terraform variables and parses outputs.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from setup.constants import (
    DEFAULT_AZURE_LOCATION,
    DEFAULT_AZURE_RESOURCE_GROUP,
    DEFAULT_AZURE_STORAGE_REPLICATION,
    DEFAULT_POSTGRES_BACKUP_DAYS,
    DEFAULT_POSTGRES_SKU,
)
from setup._style import (
    ACCENT,
    BRAND,
    BOLD,
    INDENT,
    PANEL_WIDTH,
    failure,
    panel,
    prompt_label,
    s,
    separator,
)

_DEFAULT_AZURE_MODULE = Path(__file__).resolve().parent.parent / "terraform" / "azure"
_WORKSPACE_DIR_NAME = ".quickelt-workspace"
_VAR_FILE_NAME = "quickelt.auto.tfvars.json"
_SKIP_SYNC = {
    ".terraform",
    ".gitignore",
    _WORKSPACE_DIR_NAME,
    "terraform.tfstate",
    "terraform.tfstate.backup",
}


class TerraformExecutor:
    """Wraps Terraform CLI operations for Quickelt setup."""

    def __init__(
        self,
        module_dir: Path | None = None,
        logger: logging.Logger | None = None,
        auto_approve: bool | None = None,
        workspace_dir: Path | None = None,
    ):
        self.module_dir = module_dir or _DEFAULT_AZURE_MODULE
        self.log = logger or logging.getLogger("quickelt.terraform")
        if auto_approve is None:
            auto_approve = os.getenv("QUICKELT_TERRAFORM_AUTO_APPROVE", "").lower() in ("1", "true", "yes")
        self.auto_approve = auto_approve
        self.workspace_dir = workspace_dir or (self.module_dir / _WORKSPACE_DIR_NAME)

    def ensure_terraform_installed(self) -> None:
        from setup.terraform_installer import ensure_terraform

        ensure_terraform(self.log, confirm=False)

    @staticmethod
    def normalize_storage_account_name(name: str) -> str:
        """Normalize arbitrary input into an Azure-valid storage account name."""
        normalized = re.sub(r"[^a-z0-9]", "", (name or "").lower())
        if len(normalized) < 3:
            raise ValueError(
                "Storage account name must contain at least 3 letters/numbers "
                "after removing unsupported characters."
            )
        if len(normalized) > 24:
            normalized = normalized[:24]
        return normalized

    @staticmethod
    def build_variables(
        storage: dict,
        compute: dict,
        dw: dict,
        *,
        resource_group: str = DEFAULT_AZURE_RESOURCE_GROUP,
        location: str = DEFAULT_AZURE_LOCATION,
    ) -> dict[str, Any]:
        """Map wizard dictionaries to Terraform input variables."""
        layers = storage.get("layers", []) or []
        create_vm = compute.get("compute") == "Dedicated VM"
        create_postgres = (
            dw.get("gold_external_db")
            and dw.get("pg_strategy") == "managed_cloud"
            and dw.get("managed_cloud_choice") == "provision_new"
        )
        storage_account_name = TerraformExecutor.normalize_storage_account_name(storage["name"])

        variables: dict[str, Any] = {
            "resource_group_name": resource_group,
            "location": location,
            # Resource group existence is handled before Terraform in the provisioner.
            "resource_group_existing": True,
            "storage_account_name": storage_account_name,
            "storage_existing": bool(storage.get("existing")),
            "storage_layers": layers,
            "storage_replication_type": storage.get("replication", DEFAULT_AZURE_STORAGE_REPLICATION),
            "storage_soft_delete_days": int(storage.get("soft_delete_days", 7) or 7),
            "storage_versioning_enabled": bool(storage.get("versioning_enabled", True)),
            "enable_destroy_protection": bool(storage.get("destroy_protection", False)),
            "create_vm": create_vm,
            "bootstrap_vm": bool(compute.get("bootstrap_vm", False)),
            "install_local_postgres": bool(dw.get("install_local_postgres", False)),
            "create_postgres": create_postgres,
            "postgres_admin_username": dw.get("dw_username", "quickelt"),
            "postgres_database_name": dw.get("dw_database", "quickelt_db"),
            "postgres_sku_name": dw.get("postgres_sku_name", DEFAULT_POSTGRES_SKU),
            "postgres_backup_retention_days": int(
                dw.get("postgres_backup_retention_days", DEFAULT_POSTGRES_BACKUP_DAYS) or DEFAULT_POSTGRES_BACKUP_DAYS
            ),
            "postgres_public_network_access_enabled": bool(
                dw.get("postgres_public_network_access_enabled", True)
            ),
            "postgres_allowed_cidr": dw.get("postgres_allowed_cidr", ""),
            "postgres_high_availability_enabled": bool(
                dw.get("postgres_high_availability_enabled", False)
            ),
        }

        tags = storage.get("tags", {})
        if isinstance(tags, dict) and tags:
            variables["tags"] = tags

        if dw.get("dw_password"):
            variables["local_pg_password"] = dw["dw_password"]
            if create_postgres:
                variables["postgres_admin_password"] = dw["dw_password"]

        return variables

    def provision(
        self,
        storage: dict,
        compute: dict,
        dw: dict,
        *,
        resource_group: str | None = None,
        location: str | None = None,
    ) -> dict[str, Any]:
        """Run the full Terraform workflow and return parsed outputs."""
        self.ensure_terraform_installed()

        rg = resource_group or DEFAULT_AZURE_RESOURCE_GROUP
        loc = location or DEFAULT_AZURE_LOCATION
        try:
            variables = self.build_variables(storage, compute, dw, resource_group=rg, location=loc)
        except ValueError as exc:
            return {"ok": False, "message": "invalid_storage_account_name", "detail": str(exc), "outputs": {}}
        normalized_name = variables["storage_account_name"]
        original_name = storage.get("name", "")
        if normalized_name != original_name:
            self.log.info(
                "Adjusted storage name '%s' to Azure account name '%s'.",
                original_name,
                normalized_name,
            )

        self._print_banner("Azure Terraform")
        self.log.info("Preparing Terraform workspace for Azure...")

        workspace = self._ensure_workspace()
        var_file = self._write_var_file(variables, workspace)

        init_result = self._run(["init", "-input=false"], cwd=workspace)
        if init_result["returncode"] != 0:
            return self._failure("terraform_init_failed", init_result)

        plan_result = self._run(
            ["plan", "-input=false", f"-var-file={var_file.name}", "-no-color"],
            cwd=workspace,
        )
        if plan_result["returncode"] != 0:
            return self._failure("terraform_plan_failed", plan_result)

        self._print_plan(plan_result["stdout"])

        if not self.auto_approve and not self._confirm(
            "Apply Terraform changes?",
            default_yes=True,
            require_phrase="APPLY",
        ):
            self.log.warning("Terraform apply cancelled by user.")
            return {"ok": False, "message": "cancelled", "outputs": {}}

        # The wizard already confirms intent before this call; force non-interactive apply.
        apply_cmd = ["apply", "-input=false", f"-var-file={var_file.name}", "-no-color", "-auto-approve"]

        apply_result = self._run(apply_cmd, cwd=workspace, timeout=1800)
        if apply_result["returncode"] != 0:
            return self._failure("terraform_apply_failed", apply_result)

        output_result = self._run(
            ["output", "-json", "-no-color"],
            cwd=workspace,
            timeout=120,
        )
        if output_result["returncode"] != 0:
            return self._failure("terraform_output_failed", output_result)

        outputs = self._parse_outputs(output_result["stdout"])
        self._persist_vm_private_key(outputs, workspace)
        self.log.info("Terraform apply completed successfully.")
        return {"ok": True, "message": "applied", "outputs": outputs}

    def destroy(
        self,
        storage: dict,
        compute: dict,
        dw: dict,
        *,
        resource_group: str | None = None,
        location: str | None = None,
    ) -> dict[str, Any]:
        """Destroy Azure infrastructure tracked by the persistent Terraform workspace."""
        self.ensure_terraform_installed()

        workspace = self._ensure_workspace()
        state_file = workspace / "terraform.tfstate"
        if not state_file.exists():
            return {
                "ok": False,
                "message": "no_state",
                "detail": (
                    f"No Terraform state found in {workspace}. "
                    "Nothing to destroy, or infrastructure was created before state persistence was enabled."
                ),
                "outputs": {},
            }

        variables = self._load_variables_for_destroy(storage, compute, dw, workspace, resource_group, location)
        var_file = self._write_var_file(variables, workspace)

        self._print_banner("Azure Terraform Destroy")
        self.log.warning("Preparing to destroy Azure infrastructure managed by Quickelt...")

        init_result = self._run(["init", "-input=false"], cwd=workspace)
        if init_result["returncode"] != 0:
            return self._failure("terraform_init_failed", init_result)

        plan_result = self._run(
            [
                "plan",
                "-destroy",
                "-input=false",
                f"-var-file={var_file.name}",
                "-no-color",
            ],
            cwd=workspace,
        )
        if plan_result["returncode"] != 0:
            return self._failure("terraform_plan_failed", plan_result)

        self._print_plan(plan_result["stdout"], title="Terraform Destroy Plan")

        if not self.auto_approve and not self._confirm("Destroy Azure infrastructure?"):
            self.log.warning("Terraform destroy cancelled by user.")
            return {"ok": False, "message": "cancelled", "outputs": {}}

        # Destroy confirmation is handled by the wizard; keep Terraform non-interactive.
        destroy_cmd = ["destroy", "-input=false", f"-var-file={var_file.name}", "-no-color", "-auto-approve"]

        destroy_result = self._run(destroy_cmd, cwd=workspace, timeout=1800)
        if destroy_result["returncode"] != 0:
            return self._failure("terraform_destroy_failed", destroy_result)

        self.log.info("Terraform destroy completed successfully.")
        return {"ok": True, "message": "destroyed", "outputs": {}}

    def _load_variables_for_destroy(
        self,
        storage: dict,
        compute: dict,
        dw: dict,
        workspace: Path,
        resource_group: str | None,
        location: str | None,
    ) -> dict[str, Any]:
        var_path = workspace / _VAR_FILE_NAME
        if var_path.exists():
            try:
                saved = json.loads(var_path.read_text(encoding="utf-8"))
                if isinstance(saved, dict) and saved.get("storage_account_name"):
                    self.log.debug("Using saved Terraform variables from %s", var_path.name)
                    return saved
            except json.JSONDecodeError:
                self.log.debug("Saved Terraform variables file is invalid; rebuilding from .env")

        rg = resource_group or DEFAULT_AZURE_RESOURCE_GROUP
        loc = location or DEFAULT_AZURE_LOCATION
        try:
            return self.build_variables(storage, compute, dw, resource_group=rg, location=loc)
        except ValueError as exc:
            self.log.error("Invalid storage account name in setup: %s", exc)
            raise

    def _ensure_workspace(self) -> Path:
        self.workspace_dir.mkdir(parents=True, exist_ok=True)
        self._sync_module_files(self.workspace_dir)
        return self.workspace_dir

    def _sync_module_files(self, destination: Path) -> None:
        if not self.module_dir.exists():
            raise FileNotFoundError(f"Terraform module not found: {self.module_dir}")

        for item in self.module_dir.iterdir():
            if item.name in _SKIP_SYNC:
                continue
            target = destination / item.name
            if item.is_dir():
                if target.exists():
                    shutil.rmtree(target)
                shutil.copytree(item, target)
            else:
                shutil.copy2(item, target)

    def _write_var_file(self, variables: dict[str, Any], workspace: Path) -> Path:
        var_path = workspace / _VAR_FILE_NAME
        var_path.write_text(json.dumps(variables, indent=2), encoding="utf-8")
        self.log.debug("Wrote Terraform variables to %s", var_path.name)
        return var_path

    def _run(
        self,
        args: list[str],
        *,
        cwd: Path,
        timeout: int = 600,
    ) -> dict[str, Any]:
        command = ["terraform", *args]
        cmd_str = " ".join(command)
        self.log.debug("Executing: %s (cwd=%s)", cmd_str, cwd)

        try:
            proc = subprocess.run(
                command,
                cwd=str(cwd),
                capture_output=True,
                text=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            self.log.error("Terraform command timed out: %s", cmd_str)
            return {"returncode": -1, "stdout": "", "stderr": "timeout"}

        if proc.stdout:
            for line in proc.stdout.splitlines():
                self.log.debug("[terraform:stdout] %s", line)
        if proc.stderr:
            for line in proc.stderr.splitlines():
                self.log.debug("[terraform:stderr] %s", line)

        return {
            "returncode": proc.returncode,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
        }

    @staticmethod
    def _parse_outputs(raw: str) -> dict[str, Any]:
        if not raw.strip():
            return {}
        data = json.loads(raw)
        parsed: dict[str, Any] = {}
        for key, wrapper in data.items():
            if isinstance(wrapper, dict) and "value" in wrapper:
                parsed[key] = wrapper["value"]
            else:
                parsed[key] = wrapper
        return parsed

    def _confirm(
        self,
        message: str,
        *,
        default_yes: bool = False,
        require_phrase: str | None = None,
    ) -> bool:
        if require_phrase:
            try:
                answer = input(
                    prompt_label(message)
                    + f" Type {require_phrase} to continue: "
                ).strip()
            except (KeyboardInterrupt, EOFError):
                return False
            return answer == require_phrase

        default_hint = "y" if default_yes else "n"
        while True:
            try:
                answer = input(
                    prompt_label(message) + f" (y/n) [{default_hint}]: "
                ).strip().lower()
            except (KeyboardInterrupt, EOFError):
                return False
            if not answer:
                return default_yes
            if answer in ("y", "yes"):
                return True
            if answer in ("n", "no"):
                return False
            print(f"  {failure('Enter y or n.')}")

    def _print_plan(self, stdout: str, *, title: str = "Terraform Plan") -> None:
        if not stdout.strip():
            return
        print()
        print(separator("─", ACCENT, PANEL_WIDTH))
        print(f"{INDENT}{s(title, BOLD, ACCENT)}")
        print(separator("─", ACCENT, PANEL_WIDTH))
        for line in stdout.splitlines()[-40:]:
            print(f"{INDENT}{line}")
        print(separator("─", ACCENT, PANEL_WIDTH))
        print()

    def _print_banner(self, title: str) -> None:
        print()
        print(panel(title, border_color=ACCENT, title_color=BRAND))
        print()

    def _persist_vm_private_key(self, outputs: dict[str, Any], workspace: Path) -> None:
        private_key = outputs.pop("vm_private_key_pem", None)
        if not private_key:
            return

        key_path = workspace / "quickelt-vm-ssh-key.pem"
        key_path.write_text(str(private_key), encoding="utf-8")
        key_path.chmod(0o600)
        outputs["vm_private_key_path"] = str(key_path)
        self.log.info("Saved VM SSH private key to %s", key_path)

    @staticmethod
    def _failure(message: str, result: dict[str, Any]) -> dict[str, Any]:
        stderr = (result.get("stderr") or "").strip()
        stdout = (result.get("stdout") or "").strip()
        detail = stderr or stdout or message
        return {"ok": False, "message": message, "detail": detail[:500], "outputs": {}}
