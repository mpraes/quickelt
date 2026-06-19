#!/usr/bin/env python3
"""
Quickelt Interactive Prompts
=============================

Abstract base class and three concrete implementations for the setup
wizard prompts, supporting inquirer, questionary, and a pure builtin
fallback.  A factory function auto-detects the available backend.
"""

import abc
import importlib
import json
import getpass
import logging
import secrets
import string
import subprocess

from setup._backend_detect import detect_prompt_backend
from setup.constants import (
    DEFAULT_AZURE_LOCATION,
    DEFAULT_AZURE_RESOURCE_GROUP,
    DEFAULT_AZURE_STORAGE_REPLICATION,
    DEFAULT_POSTGRES_SKU,
)
from setup._style import (
    _NO_COLOR,
    ACCENT,
    ACCENT2,
    BRAND,
    BOLD,
    DIM,
    FAILURE,
    HIGHLIGHT,
    MUTED,
    SUCCESS,
    R,
    accent,
    accent2,
    brand,
    choice_line,
    failure,
    highlight,
    input_hint,
    muted,
    prompt_label,
    s,
    success,
    warn,
)

_AVAILABLE = detect_prompt_backend()

if _AVAILABLE == "inquirer":
    import inquirer
elif _AVAILABLE == "questionary":
    import questionary

_DEFAULT_LAYERS = ["bronze", "silver", "gold"]

_PG_STRATEGY_LOCAL_VM = "Local PostgreSQL inside the Compute VM (Cost-efficient / Dev environment)"
_PG_STRATEGY_MANAGED_CLOUD = "Managed Cloud Service (AWS Aurora PostgreSQL / Azure DB for PostgreSQL)"
_DEFAULT_AZURE_RESOURCE_GROUP = DEFAULT_AZURE_RESOURCE_GROUP
_DEFAULT_AZURE_LOCATION = DEFAULT_AZURE_LOCATION
_DEFAULT_AZURE_TAGS = "environment=dev,owner=quickelt,project=quickelt,cost_center=data"
_STORAGE_REPLICATION_CHOICES = ["LRS", "ZRS", "GRS", "RAGRS"]
_POSTGRES_SKU_CHOICES = [DEFAULT_POSTGRES_SKU, "GP_Standard_D2ds_v4", "MO_Standard_E2s_v3"]


def _questionary_module():
    return importlib.import_module("questionary")


def create_prompt_backend(logger: logging.Logger | None = None) -> "PromptBackend":
    if _AVAILABLE == "inquirer":
        return InquirerBackend(logger)
    if _AVAILABLE == "questionary":
        return QuestionaryBackend(logger)
    return BuiltinBackend(logger)


class PromptBackend(abc.ABC):
    def __init__(self, logger: logging.Logger | None = None):
        self.log = logger or logging.getLogger("quickelt.prompts")

    @abc.abstractmethod
    def ask_setup_name(self) -> str: ...

    @abc.abstractmethod
    def ask_cloud(self) -> str: ...

    @abc.abstractmethod
    def ask_storage(self) -> dict: ...

    @abc.abstractmethod
    def ask_gold_database(self) -> dict: ...

    @abc.abstractmethod
    def ask_compute(self) -> dict: ...

    @abc.abstractmethod
    def ask_dw(self, compute: dict, dw: dict | None = None) -> dict: ...

    @abc.abstractmethod
    def ask_azure_context(self) -> dict: ...

    @abc.abstractmethod
    def ask_azure_provisioning_options(self, storage: dict, dw: dict) -> dict: ...

    @staticmethod
    def _empty_dw() -> dict:
        return {
            "gold_external_db": False,
            "pg_strategy": None,
            "install_local_postgres": False,
            "managed_cloud_choice": None,
            "dw_host": None,
            "dw_port": "5432",
            "dw_database": "quickelt_db",
            "dw_username": "quickelt",
            "dw_password": None,
        }

    def _apply_local_vm_strategy(self, dw: dict, compute: dict) -> None:
        dw["pg_strategy"] = "local_vm"
        dw["dw_host"] = "localhost"
        dw["dw_password"] = self._generate_password()

        if compute["compute"] == "Dedicated VM":
            dw["install_local_postgres"] = True
        else:
            self.log.warning(
                "Local PostgreSQL inside a VM requires a Dedicated VM compute tier. "
                "INSTALL_LOCAL_POSTGRES will NOT be set. Install PostgreSQL manually."
            )

    @staticmethod
    def _generate_password(length: int = 24) -> str:
        alphabet = string.ascii_letters + string.digits
        return "".join(secrets.choice(alphabet) for _ in range(length))

    def _set_provision_new_dw(self, dw: dict) -> None:
        dw["managed_cloud_choice"] = "provision_new"
        dw["dw_password"] = self._generate_password()

    def _set_connect_existing_dw(self, dw: dict, host: str, port: str, username: str, password: str) -> None:
        dw["managed_cloud_choice"] = "connect_existing"
        dw["dw_host"] = host
        dw["dw_port"] = (port or "5432").strip()
        dw["dw_username"] = (username or "quickelt").strip()
        dw["dw_password"] = password

    def _get_active_azure_subscription(self) -> tuple[str, str]:
        try:
            result = subprocess.run(
                ["az", "account", "show", "--output", "json"],
                capture_output=True,
                text=True,
                timeout=10,
            )
        except Exception:
            return "", ""

        if result.returncode != 0:
            return "", ""

        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError:
            return "", ""

        if not isinstance(data, dict):
            return "", ""

        return str(data.get("id", "") or ""), str(data.get("name", "") or "")

    @staticmethod
    def _parse_tags(raw: str) -> dict[str, str]:
        tags: dict[str, str] = {}
        for pair in (raw or "").split(","):
            item = pair.strip()
            if not item or "=" not in item:
                continue
            key, value = item.split("=", 1)
            k = key.strip()
            v = value.strip()
            if k and v:
                tags[k] = v
        return tags

    @staticmethod
    def _safe_int(raw: str, default: int) -> int:
        try:
            return int((raw or str(default)).strip() or str(default))
        except (TypeError, ValueError):
            return default

    def _ask_gold_external_yes_no_inquirer(self) -> bool:
        gold_external = inquirer.prompt([
            inquirer.List(
                "gold_external",
                message="Will the Gold layer be hosted in an external database?",
                choices=["Yes", "No"],
            ),
        ])["gold_external"]
        return gold_external == "Yes"

    def _ask_gold_external_yes_no_questionary(self) -> bool:
        q = _questionary_module()
        gold_external = q.select(
            "Will the Gold layer be hosted in an external database?",
            choices=["Yes", "No"],
        ).ask()
        return gold_external == "Yes"

    def _ask_gold_external_yes_no_builtin(self) -> bool:
        print(f"\n  {s('Will the Gold layer be hosted in an external database?', BOLD, ACCENT)}")
        print(choice_line(1, "Yes", ACCENT2))
        print(choice_line(2, "No", MUTED))
        while True:
            choice = input(prompt_label("Enter choice") + " " + input_hint("(1/2)") + ": ").strip()
            if choice in ("1", "2"):
                return choice == "1"
            print(f"  {failure('Invalid choice. Enter 1 or 2.')}")


class InquirerBackend(PromptBackend):
    def ask_setup_name(self) -> str:
        from setup.setup_registry import get_setup_env_path, normalize_setup_name, list_setups

        existing = list_setups()
        if existing:
            self.log.info("Existing setups: %s", ", ".join(existing))

        while True:
            raw = inquirer.prompt([
                inquirer.Text(
                    "name",
                    message="Enter a name for this setup (project or environment)",
                ),
            ])["name"]
            try:
                name = normalize_setup_name(raw)
            except ValueError as exc:
                print(f"  {failure(str(exc))}")
                continue
            if get_setup_env_path(name).exists():
                print(f"  {failure(f'Setup \"{name}\" already exists. Choose another name or run destroy first.')}")
                continue
            return name

    def ask_cloud(self) -> str:
        questions = [
            inquirer.List(
                "cloud",
                message="Select your cloud provider",
                choices=["AWS", "Azure"],
            ),
        ]
        return inquirer.prompt(questions)["cloud"]

    def ask_storage(self) -> dict:
        has_existing = inquirer.prompt([
            inquirer.List(
                "has_existing",
                message="Do you have an existing Data Lake / Storage Account?",
                choices=["Yes", "No"],
            ),
        ])["has_existing"]

        if has_existing == "Yes":
            name = inquirer.prompt([
                inquirer.Text("name", message="Enter the existing bucket/container name"),
            ])["name"]
            return {"existing": True, "name": name, "layers": []}

        name = inquirer.prompt([
            inquirer.Text(
                "name",
                message="Enter a name for the new bucket/container",
                default="quickelt-data-lake",
            ),
        ])["name"]

        layers = inquirer.prompt([
            inquirer.Checkbox(
                "layers",
                message="Select layers to auto-create",
                choices=_DEFAULT_LAYERS,
                default=_DEFAULT_LAYERS,
            ),
        ])["layers"]

        return {"existing": False, "name": name, "layers": layers}

    def ask_gold_database(self) -> dict:
        dw = self._empty_dw()
        if self._ask_gold_external_yes_no_inquirer():
            dw["gold_external_db"] = True
        return dw

    def ask_compute(self) -> dict:
        compute = inquirer.prompt([
            inquirer.List(
                "compute",
                message="Where will the Quickelt code execute?",
                choices=["Local Machine", "Dedicated VM", "Serverless/PaaS"],
            ),
        ])["compute"]

        bootstrap = False
        if compute == "Dedicated VM":
            bootstrap = inquirer.prompt([
                inquirer.Confirm(
                    "bootstrap",
                    message="Auto-bootstrap the VM with Python/pip/git?",
                    default=True,
                ),
            ])["bootstrap"]

        return {"compute": compute, "bootstrap_vm": bootstrap}

    def ask_dw(self, compute: dict, dw: dict | None = None) -> dict:
        dw = dw if dw is not None else self._empty_dw()

        if not dw.get("gold_external_db"):
            return dw

        strategy = inquirer.prompt([
            inquirer.List(
                "strategy",
                message="Which PostgreSQL deployment strategy do you want to use?",
                choices=[_PG_STRATEGY_LOCAL_VM, _PG_STRATEGY_MANAGED_CLOUD],
            ),
        ])["strategy"]

        if strategy == _PG_STRATEGY_LOCAL_VM:
            self._apply_local_vm_strategy(dw, compute)
        else:
            dw["pg_strategy"] = "managed_cloud"
            managed = inquirer.prompt([
                inquirer.List(
                    "managed",
                    message="Provision a new managed cluster or connect to an existing one?",
                    choices=["Provision a new cluster", "Connect to an existing cluster"],
                ),
            ])["managed"]

            if managed == "Provision a new cluster":
                self._set_provision_new_dw(dw)
            else:
                host = inquirer.prompt([
                    inquirer.Text("host", message="Enter the PostgreSQL host endpoint"),
                ])["host"]

                port = inquirer.prompt([
                    inquirer.Text("port", message="Port", default="5432"),
                ])["port"]

                username = inquirer.prompt([
                    inquirer.Text("username", message="Username", default="quickelt"),
                ])["username"]

                password = inquirer.prompt([
                    inquirer.Password("password", message="Password"),
                ])["password"]
                self._set_connect_existing_dw(dw, host, port, username, password)

        return dw

    def ask_azure_context(self) -> dict:
        sub_id, sub_name = self._get_active_azure_subscription()
        if sub_name and sub_id:
            self.log.info("Active Azure subscription: %s (%s)", sub_name, sub_id)

        subscription_id = inquirer.prompt([
            inquirer.Text(
                "subscription_id",
                message="Azure subscription ID (leave blank to use current active)",
                default=sub_id,
            ),
        ])["subscription_id"]

        resource_group = inquirer.prompt([
            inquirer.Text(
                "resource_group",
                message="Azure resource group name",
                default=_DEFAULT_AZURE_RESOURCE_GROUP,
            ),
        ])["resource_group"]

        location = inquirer.prompt([
            inquirer.Text(
                "location",
                message="Azure location/region",
                default=_DEFAULT_AZURE_LOCATION,
            ),
        ])["location"]

        return {
            "subscription_id": (subscription_id or "").strip(),
            "resource_group": (resource_group or _DEFAULT_AZURE_RESOURCE_GROUP).strip(),
            "location": (location or _DEFAULT_AZURE_LOCATION).strip(),
        }

    def ask_azure_provisioning_options(self, storage: dict, dw: dict) -> dict:
        storage_replication = inquirer.prompt([
            inquirer.List(
                "storage_replication",
                message="Storage replication SKU",
                choices=_STORAGE_REPLICATION_CHOICES,
                default=DEFAULT_AZURE_STORAGE_REPLICATION,
            ),
        ])["storage_replication"]

        soft_delete_days_raw = inquirer.prompt([
            inquirer.Text(
                "soft_delete_days",
                message="Blob soft delete retention days",
                default="7",
            ),
        ])["soft_delete_days"]
        soft_delete_days = self._safe_int(soft_delete_days_raw or "7", 7)

        enable_versioning = inquirer.prompt([
            inquirer.Confirm(
                "enable_versioning",
                message="Enable blob versioning?",
                default=True,
            ),
        ])["enable_versioning"]

        tags_raw = inquirer.prompt([
            inquirer.Text(
                "tags",
                message="Resource tags (key=value comma-separated)",
                default=_DEFAULT_AZURE_TAGS,
            ),
        ])["tags"]
        destroy_protection = inquirer.prompt([
            inquirer.Confirm(
                "destroy_protection",
                message="Enable destroy protection for critical resources?",
                default=False,
            ),
        ])["destroy_protection"]

        options: dict = {
            "storage_replication": storage_replication,
            "storage_soft_delete_days": max(1, min(365, soft_delete_days)),
            "storage_versioning_enabled": bool(enable_versioning),
            "tags": self._parse_tags(tags_raw or ""),
            "enable_destroy_protection": bool(destroy_protection),
        }

        if dw.get("gold_external_db"):
            db_name = inquirer.prompt([
                inquirer.Text(
                    "db_name",
                    message="Gold database name",
                    default=dw.get("dw_database", "quickelt_db") or "quickelt_db",
                ),
            ])["db_name"]
            options["dw_database"] = (db_name or "quickelt_db").strip()

        if (
            dw.get("gold_external_db")
            and dw.get("pg_strategy") == "managed_cloud"
            and dw.get("managed_cloud_choice") == "provision_new"
        ):
            pg_sku = inquirer.prompt([
                inquirer.List(
                    "pg_sku",
                    message="PostgreSQL SKU",
                    choices=_POSTGRES_SKU_CHOICES,
                    default=DEFAULT_POSTGRES_SKU,
                ),
            ])["pg_sku"]

            backup_days_raw = inquirer.prompt([
                inquirer.Text(
                    "backup_days",
                    message="PostgreSQL backup retention days (7-35)",
                    default="7",
                ),
            ])["backup_days"]
            backup_days = self._safe_int(backup_days_raw or "7", 7)

            public_access = inquirer.prompt([
                inquirer.Confirm(
                    "public_access",
                    message="Enable public access for PostgreSQL?",
                    default=True,
                ),
            ])["public_access"]

            allowed_cidr = ""
            if public_access:
                allowed_cidr = inquirer.prompt([
                    inquirer.Text(
                        "allowed_cidr",
                        message="Allowed client CIDR for PostgreSQL firewall",
                        default="0.0.0.0/0",
                    ),
                ])["allowed_cidr"]

            enable_ha = inquirer.prompt([
                inquirer.Confirm(
                    "enable_ha",
                    message="Enable high availability (ZoneRedundant)?",
                    default=False,
                ),
            ])["enable_ha"]

            if enable_ha and pg_sku.startswith("B_"):
                self.log.warning("High availability is not supported on Burstable SKU. Disabling HA.")
                enable_ha = False

            options.update(
                {
                    "postgres_sku_name": pg_sku,
                    "postgres_backup_retention_days": max(7, min(35, backup_days)),
                    "postgres_public_network_access_enabled": bool(public_access),
                    "postgres_allowed_cidr": (allowed_cidr or "").strip(),
                    "postgres_high_availability_enabled": bool(enable_ha),
                }
            )

        return options


class QuestionaryBackend(PromptBackend):
    def ask_setup_name(self) -> str:
        from setup.setup_registry import get_setup_env_path, normalize_setup_name, list_setups
        q = _questionary_module()

        existing = list_setups()
        if existing:
            self.log.info("Existing setups: %s", ", ".join(existing))

        while True:
            raw = q.text(
                "Enter a name for this setup (project or environment):",
            ).ask()
            if not raw:
                print(f"  {failure('Setup name is required.')}")
                continue
            try:
                name = normalize_setup_name(raw)
            except ValueError as exc:
                print(f"  {failure(str(exc))}")
                continue
            if get_setup_env_path(name).exists():
                print(f"  {failure(f'Setup \"{name}\" already exists. Choose another name or run destroy first.')}")
                continue
            return name

    def ask_cloud(self) -> str:
        q = _questionary_module()
        return q.select(
            "Select your cloud provider:",
            choices=["AWS", "Azure"],
        ).ask()

    def ask_storage(self) -> dict:
        q = _questionary_module()
        has_existing = q.select(
            "Do you have an existing Data Lake / Storage Account?",
            choices=["Yes", "No"],
        ).ask()

        if has_existing == "Yes":
            name = q.text("Enter the existing bucket/container name:").ask()
            return {"existing": True, "name": name, "layers": []}

        name = q.text(
            "Enter a name for the new bucket/container:",
            default="quickelt-data-lake",
        ).ask()

        layers = q.checkbox(
            "Select layers to auto-create:",
            choices=_DEFAULT_LAYERS,
            default=_DEFAULT_LAYERS,
        ).ask()

        return {"existing": False, "name": name, "layers": layers or []}

    def ask_gold_database(self) -> dict:
        dw = self._empty_dw()
        if self._ask_gold_external_yes_no_questionary():
            dw["gold_external_db"] = True
        return dw

    def ask_compute(self) -> dict:
        q = _questionary_module()
        compute = q.select(
            "Where will the Quickelt code execute?",
            choices=["Local Machine", "Dedicated VM", "Serverless/PaaS"],
        ).ask()

        bootstrap = False
        if compute == "Dedicated VM":
            bootstrap = q.confirm(
                "Auto-bootstrap the VM with Python/pip/git?",
                default=True,
            ).ask()

        return {"compute": compute, "bootstrap_vm": bootstrap}

    def ask_dw(self, compute: dict, dw: dict | None = None) -> dict:
        dw = dw if dw is not None else self._empty_dw()
        q = _questionary_module()

        if not dw.get("gold_external_db"):
            return dw

        strategy = q.select(
            "Which PostgreSQL deployment strategy do you want to use?",
            choices=[_PG_STRATEGY_LOCAL_VM, _PG_STRATEGY_MANAGED_CLOUD],
        ).ask()

        if strategy == _PG_STRATEGY_LOCAL_VM or strategy is None:
            self._apply_local_vm_strategy(dw, compute)
        else:
            dw["pg_strategy"] = "managed_cloud"

            managed = q.select(
                "Provision a new managed cluster or connect to an existing one?",
                choices=["Provision a new cluster", "Connect to an existing cluster"],
            ).ask()

            if managed == "Provision a new cluster":
                self._set_provision_new_dw(dw)
            else:
                host = q.text("Enter the PostgreSQL host endpoint:").ask()
                port = q.text("Port:", default="5432").ask()
                username = q.text("Username:", default="quickelt").ask()
                password = q.password("Password:").ask()
                self._set_connect_existing_dw(dw, host or "", port, username, password or "")

        return dw

    def ask_azure_context(self) -> dict:
        q = _questionary_module()
        sub_id, sub_name = self._get_active_azure_subscription()
        if sub_name and sub_id:
            self.log.info("Active Azure subscription: %s (%s)", sub_name, sub_id)

        subscription_id = q.text(
            "Azure subscription ID (leave blank to use current active):",
            default=sub_id,
        ).ask()

        resource_group = q.text(
            "Azure resource group name:",
            default=_DEFAULT_AZURE_RESOURCE_GROUP,
        ).ask()

        location = q.text(
            "Azure location/region:",
            default=_DEFAULT_AZURE_LOCATION,
        ).ask()

        return {
            "subscription_id": (subscription_id or "").strip(),
            "resource_group": (resource_group or _DEFAULT_AZURE_RESOURCE_GROUP).strip(),
            "location": (location or _DEFAULT_AZURE_LOCATION).strip(),
        }

    def ask_azure_provisioning_options(self, storage: dict, dw: dict) -> dict:
        q = _questionary_module()
        storage_replication = q.select(
            "Storage replication SKU:",
            choices=_STORAGE_REPLICATION_CHOICES,
            default=DEFAULT_AZURE_STORAGE_REPLICATION,
        ).ask()

        soft_delete_days_raw = q.text(
            "Blob soft delete retention days:",
            default="7",
        ).ask()
        soft_delete_days = self._safe_int(soft_delete_days_raw or "7", 7)

        enable_versioning = q.confirm(
            "Enable blob versioning?",
            default=True,
        ).ask()

        tags_raw = q.text(
            "Resource tags (key=value comma-separated):",
            default=_DEFAULT_AZURE_TAGS,
        ).ask()
        destroy_protection = q.confirm(
            "Enable destroy protection for critical resources?",
            default=False,
        ).ask()

        options: dict = {
            "storage_replication": storage_replication or "LRS",
            "storage_soft_delete_days": max(1, min(365, soft_delete_days)),
            "storage_versioning_enabled": bool(enable_versioning),
            "tags": self._parse_tags(tags_raw or ""),
            "enable_destroy_protection": bool(destroy_protection),
        }

        if dw.get("gold_external_db"):
            db_name = q.text(
                "Gold database name:",
                default=dw.get("dw_database", "quickelt_db") or "quickelt_db",
            ).ask()
            options["dw_database"] = (db_name or "quickelt_db").strip()

        if (
            dw.get("gold_external_db")
            and dw.get("pg_strategy") == "managed_cloud"
            and dw.get("managed_cloud_choice") == "provision_new"
        ):
            pg_sku = q.select(
                "PostgreSQL SKU:",
                choices=_POSTGRES_SKU_CHOICES,
                default=DEFAULT_POSTGRES_SKU,
            ).ask()

            backup_days_raw = q.text(
                "PostgreSQL backup retention days (7-35):",
                default="7",
            ).ask()
            backup_days = self._safe_int(backup_days_raw or "7", 7)

            public_access = q.confirm(
                "Enable public access for PostgreSQL?",
                default=True,
            ).ask()

            allowed_cidr = ""
            if public_access:
                allowed_cidr = q.text(
                    "Allowed client CIDR for PostgreSQL firewall:",
                    default="0.0.0.0/0",
                ).ask() or ""

            enable_ha = q.confirm(
                "Enable high availability (ZoneRedundant)?",
                default=False,
            ).ask()

            if enable_ha and (pg_sku or "").startswith("B_"):
                self.log.warning("High availability is not supported on Burstable SKU. Disabling HA.")
                enable_ha = False

            options.update(
                {
                    "postgres_sku_name": pg_sku or DEFAULT_POSTGRES_SKU,
                    "postgres_backup_retention_days": max(7, min(35, backup_days)),
                    "postgres_public_network_access_enabled": bool(public_access),
                    "postgres_allowed_cidr": allowed_cidr.strip(),
                    "postgres_high_availability_enabled": bool(enable_ha),
                }
            )

        return options


class BuiltinBackend(PromptBackend):
    def ask_setup_name(self) -> str:
        from setup.setup_registry import get_setup_env_path, normalize_setup_name, list_setups

        existing = list_setups()
        if existing:
            print(f"  {muted('Existing setups: ' + ', '.join(existing))}")

        print(f"\n  {s('Each setup gets its own folder under infrastructure/setups/', DIM)}")
        while True:
            raw = input(
                prompt_label("Enter a name for this setup")
                + " "
                + input_hint("(e.g. acme-prod)")
                + ": ",
            ).strip()
            if not raw:
                print(f"  {failure('Setup name is required.')}")
                continue
            try:
                name = normalize_setup_name(raw)
            except ValueError as exc:
                print(f"  {failure(str(exc))}")
                continue
            if get_setup_env_path(name).exists():
                print(
                    f"  {failure(f'Setup \"{name}\" already exists. Choose another name or run destroy first.')}"
                )
                continue
            return name

    def ask_cloud(self) -> str:
        print(f"\n  {s('Select your cloud provider:', BOLD, ACCENT)}")
        print(choice_line(1, "AWS", ACCENT2))
        print(choice_line(2, "Azure", ACCENT2))
        while True:
            choice = input(prompt_label("Enter choice") + " " + input_hint("(1/2)") + ": ").strip()
            if choice == "1":
                return "AWS"
            if choice == "2":
                return "Azure"
            print(f"  {failure('Invalid choice. Enter 1 or 2.')}")

    def ask_storage(self) -> dict:
        print(f"\n  {s('Do you have an existing Data Lake / Storage Account?', BOLD, ACCENT)}")
        print(choice_line(1, "Yes", SUCCESS))
        print(choice_line(2, "No", MUTED))
        while True:
            choice = input(prompt_label("Enter choice") + " " + input_hint("(1/2)") + ": ").strip()
            if choice in ("1", "2"):
                break
            print(f"  {failure('Invalid choice. Enter 1 or 2.')}")

        if choice == "1":
            name = input(prompt_label("Enter the existing bucket/container name") + ": ").strip()
            return {"existing": True, "name": name, "layers": []}

        name = input(prompt_label("Enter a name for the new bucket/container") + " " + input_hint("[quickelt-data-lake]") + ": ").strip()
        if not name:
            name = "quickelt-data-lake"

        print(f"  {s('Select layers to auto-create (comma-separated):', BOLD, ACCENT)}")
        print(f"  {muted('Available: ' + ', '.join(_DEFAULT_LAYERS))}")
        raw = input(prompt_label("Layers") + " " + input_hint(f"[{','.join(_DEFAULT_LAYERS)}]") + ": ").strip()
        if not raw:
            layers = _DEFAULT_LAYERS[:]
        else:
            layers = [l.strip() for l in raw.split(",") if l.strip() in _DEFAULT_LAYERS]

        return {"existing": False, "name": name, "layers": layers}

    def ask_gold_database(self) -> dict:
        dw = self._empty_dw()
        if self._ask_gold_external_yes_no_builtin():
            dw["gold_external_db"] = True
        return dw

    def ask_compute(self) -> dict:
        print(f"\n  {s('Where will the Quickelt code execute?', BOLD, ACCENT)}")
        print(choice_line(1, "Local Machine", HIGHLIGHT))
        print(choice_line(2, "Dedicated VM", ACCENT2))
        print(choice_line(3, "Serverless / PaaS", ACCENT))
        while True:
            choice = input(prompt_label("Enter choice") + " " + input_hint("(1/2/3)") + ": ").strip()
            if choice in ("1", "2", "3"):
                break
            print(f"  {failure('Invalid choice. Enter 1, 2, or 3.')}")

        compute_map = {"1": "Local Machine", "2": "Dedicated VM", "3": "Serverless/PaaS"}
        compute = compute_map[choice]

        bootstrap = False
        if compute == "Dedicated VM":
            while True:
                ans = input(prompt_label("Auto-bootstrap the VM with Python/pip/git?") + " " + input_hint("(y/n) [y]") + ": ").strip().lower()
                if ans in ("y", "yes", ""):
                    bootstrap = True
                    break
                if ans in ("n", "no"):
                    break
                print(f"  {failure('Enter y or n.')}")

        return {"compute": compute, "bootstrap_vm": bootstrap}

    def ask_dw(self, compute: dict, dw: dict | None = None) -> dict:
        dw = dw if dw is not None else self._empty_dw()

        if not dw.get("gold_external_db"):
            return dw

        print(f"\n  {s('Which PostgreSQL deployment strategy do you want?', BOLD, ACCENT)}")
        print(choice_line(1, _PG_STRATEGY_LOCAL_VM))
        print(choice_line(2, _PG_STRATEGY_MANAGED_CLOUD))
        while True:
            strat = input(prompt_label("Enter choice") + " " + input_hint("(1/2)") + ": ").strip()
            if strat in ("1", "2"):
                break
            print(f"  {failure('Invalid choice. Enter 1 or 2.')}")

        if strat == "1":
            self._apply_local_vm_strategy(dw, compute)
        else:
            dw["pg_strategy"] = "managed_cloud"

            print(f"\n  {s('Provision a new managed cluster or connect to an existing one?', BOLD, ACCENT)}")
            print(choice_line(1, "Provision a new cluster", SUCCESS))
            print(choice_line(2, "Connect to an existing cluster", ACCENT2))
            while True:
                mc = input(prompt_label("Enter choice") + " " + input_hint("(1/2)") + ": ").strip()
                if mc in ("1", "2"):
                    break
                print(f"  {failure('Invalid choice. Enter 1 or 2.')}")

            if mc == "1":
                self._set_provision_new_dw(dw)
            else:
                host = input(prompt_label("Enter the PostgreSQL host endpoint") + ": ").strip()
                port = input(prompt_label("Port") + " " + input_hint("[5432]") + ": ").strip()
                username = input(prompt_label("Username") + " " + input_hint("[quickelt]") + ": ").strip()
                password = getpass.getpass(prompt_label("Password") + ": ")
                self._set_connect_existing_dw(dw, host, port, username, password)

        return dw

    def ask_azure_context(self) -> dict:
        sub_id, sub_name = self._get_active_azure_subscription()
        if sub_name and sub_id:
            print(f"\n  {s('Active Azure subscription:', BOLD, ACCENT)} {sub_name} ({sub_id})")

        subscription_id = input(
            prompt_label("Azure subscription ID")
            + " "
            + input_hint(f"[{sub_id or 'current active'}]")
            + ": "
        ).strip()
        if not subscription_id:
            subscription_id = sub_id

        resource_group = input(
            prompt_label("Azure resource group name")
            + " "
            + input_hint(f"[{_DEFAULT_AZURE_RESOURCE_GROUP}]")
            + ": "
        ).strip()
        if not resource_group:
            resource_group = _DEFAULT_AZURE_RESOURCE_GROUP

        location = input(
            prompt_label("Azure location/region")
            + " "
            + input_hint(f"[{_DEFAULT_AZURE_LOCATION}]")
            + ": "
        ).strip()
        if not location:
            location = _DEFAULT_AZURE_LOCATION

        return {
            "subscription_id": subscription_id,
            "resource_group": resource_group,
            "location": location,
        }

    def ask_azure_provisioning_options(self, storage: dict, dw: dict) -> dict:
        print(f"\n  {s('Azure Storage settings', BOLD, ACCENT)}")
        print(choice_line(1, "LRS (recommended for dev)", ACCENT2))
        print(choice_line(2, "ZRS", ACCENT2))
        print(choice_line(3, "GRS", ACCENT2))
        print(choice_line(4, "RAGRS", ACCENT2))
        sku_map = {"1": "LRS", "2": "ZRS", "3": "GRS", "4": "RAGRS"}
        while True:
            sku_choice = input(prompt_label("Storage replication SKU") + " " + input_hint("(1/2/3/4) [1]") + ": ").strip()
            if sku_choice == "":
                sku_choice = "1"
            if sku_choice in sku_map:
                break
            print(f"  {failure('Invalid choice. Enter 1, 2, 3, or 4.')}")

        soft_delete_raw = input(
            prompt_label("Blob soft delete retention days")
            + " "
            + input_hint("[7]")
            + ": "
        ).strip()
        soft_delete_days = self._safe_int(soft_delete_raw or "7", 7)

        while True:
            ans = input(prompt_label("Enable blob versioning?") + " " + input_hint("(y/n) [y]") + ": ").strip().lower()
            if ans in ("", "y", "yes"):
                versioning = True
                break
            if ans in ("n", "no"):
                versioning = False
                break
            print(f"  {failure('Enter y or n.')}")

        tags_raw = input(
            prompt_label("Resource tags (key=value comma-separated)")
            + " "
            + input_hint(f"[{_DEFAULT_AZURE_TAGS}]")
            + ": "
        ).strip()
        if not tags_raw:
            tags_raw = _DEFAULT_AZURE_TAGS

        while True:
            ans = input(
                prompt_label("Enable destroy protection for critical resources?")
                + " "
                + input_hint("(y/n) [n]")
                + ": "
            ).strip().lower()
            if ans in ("", "n", "no"):
                destroy_protection = False
                break
            if ans in ("y", "yes"):
                destroy_protection = True
                break
            print(f"  {failure('Enter y or n.')}")

        options: dict = {
            "storage_replication": sku_map[sku_choice],
            "storage_soft_delete_days": max(1, min(365, soft_delete_days)),
            "storage_versioning_enabled": versioning,
            "tags": self._parse_tags(tags_raw),
            "enable_destroy_protection": destroy_protection,
        }

        if dw.get("gold_external_db"):
            db_name = input(prompt_label("Gold database name") + " " + input_hint("[quickelt_db]") + ": ").strip()
            options["dw_database"] = db_name or "quickelt_db"

        if (
            dw.get("gold_external_db")
            and dw.get("pg_strategy") == "managed_cloud"
            and dw.get("managed_cloud_choice") == "provision_new"
        ):
            print(f"\n  {s('Azure PostgreSQL settings', BOLD, ACCENT)}")
            print(choice_line(1, "Burstable B1ms (dev/cost)", ACCENT2))
            print(choice_line(2, "General Purpose D2ds_v4", ACCENT2))
            print(choice_line(3, "Memory Optimized E2s_v3", ACCENT2))
            pg_sku_map = {"1": "B_Standard_B1ms", "2": "GP_Standard_D2ds_v4", "3": "MO_Standard_E2s_v3"}
            while True:
                pg_choice = input(prompt_label("PostgreSQL SKU") + " " + input_hint("(1/2/3) [1]") + ": ").strip()
                if pg_choice == "":
                    pg_choice = "1"
                if pg_choice in pg_sku_map:
                    break
                print(f"  {failure('Invalid choice. Enter 1, 2, or 3.')}")

            backup_raw = input(prompt_label("PostgreSQL backup retention days") + " " + input_hint("[7]") + ": ").strip()
            backup_days = self._safe_int(backup_raw or "7", 7)

            while True:
                ans = input(prompt_label("Enable public access for PostgreSQL?") + " " + input_hint("(y/n) [y]") + ": ").strip().lower()
                if ans in ("", "y", "yes"):
                    public_access = True
                    break
                if ans in ("n", "no"):
                    public_access = False
                    break
                print(f"  {failure('Enter y or n.')}")

            allowed_cidr = ""
            if public_access:
                allowed_cidr = input(
                    prompt_label("Allowed client CIDR for PostgreSQL firewall")
                    + " "
                    + input_hint("[0.0.0.0/0]")
                    + ": "
                ).strip()
                if not allowed_cidr:
                    allowed_cidr = "0.0.0.0/0"

            while True:
                ans = input(prompt_label("Enable high availability (ZoneRedundant)?") + " " + input_hint("(y/n) [n]") + ": ").strip().lower()
                if ans in ("", "n", "no"):
                    enable_ha = False
                    break
                if ans in ("y", "yes"):
                    enable_ha = True
                    break
                print(f"  {failure('Enter y or n.')}")

            pg_sku = pg_sku_map[pg_choice]
            if enable_ha and pg_sku.startswith("B_"):
                self.log.warning("High availability is not supported on Burstable SKU. Disabling HA.")
                enable_ha = False

            options.update(
                {
                    "postgres_sku_name": pg_sku,
                    "postgres_backup_retention_days": max(7, min(35, backup_days)),
                    "postgres_public_network_access_enabled": public_access,
                    "postgres_allowed_cidr": allowed_cidr,
                    "postgres_high_availability_enabled": enable_ha,
                }
            )

        return options
