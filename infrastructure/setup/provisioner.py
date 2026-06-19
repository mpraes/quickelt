#!/usr/bin/env python3
"""
Quickelt Provisioner Base
=========================

Abstract base class for cloud-specific infrastructure provisioners.
Concrete implementations (AWS, Azure) receive shared dependencies
via constructor injection. Shared helpers eliminate duplicated logic.
"""

import abc
import json
import logging
import os
import secrets
from typing import Any

from setup.cli_executor import CLIExecutor, ErrorCategory, Spinner
from setup.env_writer import EnvWriter


class Provisioner(abc.ABC):
    CLOUD_NAME: str = ""

    BOOTSTRAP_SCRIPT = """#!/bin/bash
set -euo pipefail
echo "[quickelt-bootstrap] Starting VM bootstrap..."
sudo apt-get update -y
sudo apt-get install -y python3-pip git
echo "[quickelt-bootstrap] Bootstrap complete."
"""

    _LOCAL_PG_PASSWORD_PLACEHOLDER = "{LOCAL_PG_PASSWORD}"

    LOCAL_POSTGRES_SCRIPT = """#!/bin/bash
set -euo pipefail
echo "[quickelt-bootstrap] Starting VM bootstrap with local PostgreSQL..."
sudo apt-get update -y
sudo apt-get install -y python3-pip git
echo "[quickelt-bootstrap] Installing PostgreSQL..."
sudo apt-get install -y postgresql postgresql-contrib
sudo systemctl enable postgresql
sudo systemctl start postgresql
echo "[quickelt-bootstrap] Creating database and user..."
sudo -u postgres psql -c "CREATE USER quickelt WITH PASSWORD '{LOCAL_PG_PASSWORD}';"
sudo -u postgres psql -c "CREATE DATABASE quickelt_db OWNER quickelt;"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE quickelt_db TO quickelt;"
echo "[quickelt-bootstrap] PostgreSQL installation complete."
echo "[quickelt-bootstrap] Bootstrap complete."
"""

    _MAX_RETRY_NAME = 3

    def __init__(self, cli: CLIExecutor, env: EnvWriter, logger: logging.Logger | None = None):
        self.cli = cli
        self.env = env
        self.log = logger or logging.getLogger("quickelt.provisioner")
        self._retry_count = 0

    def _parse_json(self, raw: str) -> Any:
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            self.log.debug("Failed to parse JSON from CLI output (length=%d)", len(raw))
            return None

    def _detect_region(
        self,
        cli_cmd: list[str],
        env_var: str,
        default: str,
        *,
        json_key: str | None = None,
        timeout: int = 10,
    ) -> str:
        result = self.cli.execute(cli_cmd, timeout=timeout)
        if result["ok"] and result["stdout"].strip():
            if json_key:
                data = self._parse_json(result["stdout"])
                if isinstance(data, dict) and json_key in data:
                    return data[json_key]
            else:
                return result["stdout"].strip()
        return os.getenv(env_var, default)

    def _handle_already_exists(
        self,
        resource_label: str,
        current_name: str,
        spinner: Spinner,
    ) -> tuple[str, str | None]:
        spinner.clear()
        self.log.warning("%s '%s' already exists.", resource_label, current_name)

        if self._retry_count >= self._MAX_RETRY_NAME:
            self.log.error(
                "Maximum retry attempts (%d) reached for %s. Aborting.",
                self._MAX_RETRY_NAME, resource_label.lower(),
            )
            spinner.fail(f"Max retries reached — {resource_label.lower()} creation cancelled.")
            return "cancelled", None

        reuse_label = f"Reuse existing {resource_label.lower()}"
        choice = self.cli.prompt_choice(
            f"{resource_label} '{current_name}' already exists. What would you like to do?",
            [reuse_label, "Enter a new name"],
        )
        if choice == reuse_label or choice is None:
            self._retry_count = 0
            return "reuse", None
        new_name = self.cli.prompt_input(
            f"Enter a new {resource_label.lower()} name",
            default=f"{current_name}-v2",
        )
        if new_name:
            self._retry_count += 1
            return "retry", new_name
        self._retry_count = 0
        spinner.fail(f"No new name provided — {resource_label.lower()} creation cancelled.")
        return "cancelled", None

    def _handle_cli_error(
        self,
        category: ErrorCategory | None,
        spinner: Spinner,
        result: dict,
        error_dict: dict,
        *,
        fail_label: str = "Operation",
        unauthorized_log: str = "",
        auth_expired_log: str = "",
    ) -> dict:
        if category == ErrorCategory.UNAUTHORIZED:
            spinner.fail(f"{fail_label}: permission denied")
            self.log.error(unauthorized_log or result["remedy"])
            return {**error_dict, "ok": False, "message": "unauthorized"}
        if category == ErrorCategory.AUTH_EXPIRED:
            spinner.fail(f"{fail_label}: session expired")
            self.log.error(auth_expired_log or "Your session has expired. Re-authenticate and try again.")
            return {**error_dict, "ok": False, "message": "auth_expired"}
        if category == ErrorCategory.CLI_MISSING:
            spinner.fail(f"{fail_label}: CLI tool not found")
            self.log.error(result["remedy"])
            return {**error_dict, "ok": False, "message": "cli_missing"}
        if category == ErrorCategory.TIMEOUT:
            spinner.fail(f"{fail_label}: command timed out")
            self.log.error("CLI command timed out. Retry or check network connectivity.")
            return {**error_dict, "ok": False, "message": "timeout"}
        spinner.fail(f"{fail_label}: {result['stderr'][:200]}")
        self.log.error("%s", result["remedy"])
        return {**error_dict, "ok": False, "message": result["stderr"]}

    def _print_provision_banner(self) -> None:
        label = f"{self.CLOUD_NAME} Provisioner"
        padding = 42 - 7 - len(label)
        print()
        print("  ╔══════════════════════════════════════════╗")
        print(f"  ║       {label}{' ' * padding}║")
        print("  ╚══════════════════════════════════════════╝")
        print()
        self.log.debug("%s provisioner started", self.CLOUD_NAME)

    def _get_local_postgres_script(self, dw_password: str) -> str:
        password = dw_password if dw_password else secrets.token_urlsafe(32)
        return self.LOCAL_POSTGRES_SCRIPT.replace(self._LOCAL_PG_PASSWORD_PLACEHOLDER, password)

    def _empty_layers_result(self, **extra: Any) -> dict:
        self.log.info("No layers selected — skipping layer structuring.")
        return {"ok": True, "created": [], "failed": [], **extra}

    def _provision_layers(self, storage: dict, results: dict, layers_fn, **extra) -> None:
        layers = storage.get("layers", [])
        if layers:
            results["layers"] = layers_fn(storage["name"], layers)
        else:
            results["layers"] = self._empty_layers_result(**extra)

    @abc.abstractmethod
    def provision(self, storage: dict, compute: dict, dw: dict) -> dict[str, Any]: ...

    def _reset_retry_state(self) -> None:
        self._retry_count = 0
