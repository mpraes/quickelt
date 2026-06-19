#!/usr/bin/env python3
"""
Quickelt Infrastructure Setup Wizard
=====================================

Interactive terminal wizard that orchestrates the infrastructure pipeline
for the Quickelt framework. Composes PreflightChecker, PromptBackend,
EnvWriter, and cloud-specific Provisioners to guide the user through
cloud provider selection, storage, compute, and data warehouse setup.

Usage:
    python infrastructure/setup.py
    python infrastructure/setup.py --destroy
    python infrastructure/setup.py --destroy --setup-name my-project
"""

import logging
import os
import shutil
import sys
import traceback
from pathlib import Path

from setup.constants import DEFAULT_AZURE_LOCATION, DEFAULT_AZURE_RESOURCE_GROUP
from setup._style import (
    _NO_COLOR,
    ACCENT,
    ACCENT2,
    BRAND,
    BOLD,
    BRIGHT_CYAN,
    BRIGHT_GREEN,
    BRIGHT_MAGENTA,
    BRIGHT_RED,
    BRIGHT_YELLOW,
    CYAN,
    DIM,
    FAILURE,
    HIGHLIGHT,
    MUTED,
    SUCCESS,
    R,
    WARN_COLOR,
    WHITE,
    YELLOW,
    ICON_ARROW,
    ICON_BOLT,
    ICON_CHECK,
    ICON_CLOUD,
    ICON_CROSS,
    ICON_DB,
    ICON_DIAMOND,
    ICON_FOLDER,
    ICON_GEAR,
    ICON_INFO,
    ICON_LOCK,
    ICON_ROCKET,
    ICON_SERVER,
    ICON_SPARKLE,
    ICON_WARN,
    INDENT,
    PANEL_WIDTH,
    accent,
    accent2,
    brand,
    choice_line,
    completion,
    failure,
    goodbye,
    highlight,
    icon,
    input_hint,
    kv_line,
    muted,
    panel,
    prompt_label,
    s,
    separator,
    step_header,
    success,
    warn,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"
SETUP_DIR = Path(__file__).resolve().parent / "setup"
INFRA_LOGS_DIR = SETUP_DIR / "infra_logs"
LOG_FILE = INFRA_LOGS_DIR / ".quickelt_setup.log"

_USE_LEGACY_AZURE = os.getenv("QUICKELT_LEGACY_AZURE_PROVISIONER", "").lower() in ("1", "true", "yes")

_PROVISIONER_REGISTRY = {
    "AWS": "aws_provisioner",
    "Azure": "azure_provisioner" if _USE_LEGACY_AZURE else "azure_terraform_provisioner",
}

log = logging.getLogger("quickelt.setup")

_TOTAL_STEPS = 6


def _parse_args(argv: list[str]) -> tuple[bool, bool, str | None, bool]:
    destroy = "--destroy" in argv
    clean = "--clean" in argv
    force = "--yes" in argv
    setup_name: str | None = None

    if "--setup-name" in argv:
        idx = argv.index("--setup-name")
        if idx + 1 >= len(argv) or argv[idx + 1].startswith("-"):
            raise ValueError("Flag '--setup-name' requires a value.")
        setup_name = argv[idx + 1]

    if (destroy or clean) and setup_name is None:
        flag = "--destroy" if destroy else "--clean"
        idx = argv.index(flag)
        if idx + 1 < len(argv) and not argv[idx + 1].startswith("-"):
            setup_name = argv[idx + 1]

    if destroy and clean:
        raise ValueError("Use only one operation flag: --destroy or --clean.")

    return destroy, clean, setup_name, force


class _ConsoleFormatter(logging.Formatter):
    _STYLES = {
        logging.DEBUG: (DIM, ICON_GEAR, MUTED),
        logging.INFO: (SUCCESS, ICON_CHECK, SUCCESS),
        logging.WARNING: (WARN_COLOR, ICON_WARN, WARN_COLOR),
        logging.ERROR: (FAILURE, ICON_CROSS, FAILURE),
        logging.CRITICAL: (FAILURE, ICON_CROSS, FAILURE),
    }

    def format(self, record: logging.LogRecord) -> str:
        msg = record.getMessage()
        if _NO_COLOR:
            return f"[{record.levelname}] {msg}"
        icon_style, icon_char, msg_style = self._STYLES.get(
            record.levelno, (SUCCESS, ICON_CHECK, SUCCESS)
        )
        tag = s(icon_char, icon_style)
        return f"{INDENT}{tag} {msg_style}{msg}{R}"


class _FileFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ts = self.formatTime(record, self.datefmt)
        msg = record.getMessage()
        if record.exc_info and record.exc_info[1] is not None:
            msg += "\n" + self.formatException(record.exc_info)
        return f"{ts} [{record.levelname:>8}] [{record.name}] {msg}"


class SetupWizard:
    def __init__(self, setup_name: str | None = None):
        self.setup_name = setup_name
        self._configure_logging()
        sys.path.insert(0, str(Path(__file__).resolve().parent))

        from setup.cli_executor import CLIExecutor
        from setup.env_writer import EnvWriter
        from setup.preflight import PreflightChecker
        from setup.prompts import create_prompt_backend
        from setup.setup_registry import get_setup_env_path
        from setup.terraform_executor import TerraformExecutor

        self.cli = CLIExecutor()
        env_path = get_setup_env_path(setup_name) if setup_name else ENV_FILE
        self.env = EnvWriter(env_path, logger=log)
        self.preflight = PreflightChecker(logger=log)
        self.prompts = create_prompt_backend(logger=log)
        self._terraform_executor_cls = TerraformExecutor
        self._step = 0

    def run_destroy(self, setup_name: str | None = None) -> None:
        try:
            self._print_destroy_banner()

            resolved_name = self._resolve_setup_for_destroy(setup_name)
            self._bind_setup(resolved_name)

            if not self.env.env_path.exists():
                log.error("No .env file found for setup '%s' at %s", resolved_name, self.env.env_path)
                sys.exit(1)

            cloud, storage, compute, dw = self.env.load_setup_config()

            if cloud != "Azure":
                log.error("Destroy is only supported for Azure Terraform infrastructure.")
                log.error("Current CLOUD_PROVIDER=%r", cloud or "(not set)")
                sys.exit(1)

            if _USE_LEGACY_AZURE:
                log.error("Legacy Azure provisioner does not support automated destroy.")
                log.error("Remove resources manually in the Azure portal or use az CLI.")
                sys.exit(1)

            if not storage.get("name"):
                log.error("AZURE_STORAGE_ACCOUNT is missing from .env — cannot identify infrastructure.")
                sys.exit(1)

            print(separator("\u2550", ACCENT, PANEL_WIDTH))
            print(f"{INDENT}{s(ICON_WARN, WARN_COLOR)} {s('Destroy Target', BOLD, WARN_COLOR)}")
            print(separator("\u2550", ACCENT, PANEL_WIDTH))
            print(kv_line("Setup", resolved_name, val_color=ACCENT2))
            print(kv_line("Cloud", cloud, val_color=BRAND))
            print(kv_line("Storage", storage["name"]))
            if storage.get("layers"):
                print(kv_line("Layers", ", ".join(storage["layers"]), val_color=ACCENT2))
            print(kv_line("Compute", compute["compute"]))
            print(separator("\u2550", ACCENT, PANEL_WIDTH))
            print()

            self.preflight.check("Azure")
            self._ensure_terraform_cli()
            self._invoke_destroy(cloud, storage, compute, dw)

            print()
            print(separator("\u2550", ACCENT, PANEL_WIDTH))
            log.info("Azure infrastructure destroy finished.")
            print(separator("\u2550", ACCENT, PANEL_WIDTH))
            print()

        except KeyboardInterrupt:
            self._handle_keyboard_interrupt()

        except SystemExit:
            raise

        except Exception:
            log.debug("Unexpected exception traceback:\n%s", traceback.format_exc())
            log.error("Destroy failed. Check '%s' for full details.", LOG_FILE.name)
            sys.exit(1)

    def run_cleanup(self, setup_name: str | None = None, force: bool = False) -> None:
        from setup.env_writer import EnvWriter
        from setup.setup_registry import get_setup_dir, list_setups, normalize_setup_name

        try:
            print()
            print(panel("Quickelt", "Setup Cleanup", border_color=WARN_COLOR, title_color=BRAND))
            print()

            all_setups = list_setups()
            if not all_setups:
                log.warning("No setups found under infrastructure/setups/.")
                return

            targets: list[str]
            if setup_name:
                try:
                    resolved = normalize_setup_name(setup_name)
                except ValueError as exc:
                    log.error("%s", exc)
                    sys.exit(1)
                if resolved not in all_setups:
                    log.error("Setup '%s' not found under infrastructure/setups/.", resolved)
                    sys.exit(1)
                targets = [resolved]
            else:
                targets = [self._prompt_cleanup_setup(all_setups)]

            active_setup = None
            if ENV_FILE.exists():
                root_env = EnvWriter(ENV_FILE, logger=log)
                active_setup = root_env.read_value("QUICKELT_SETUP_NAME") or root_env.read_value("SETUP_NAME")

            print(separator("\u2550", ACCENT, PANEL_WIDTH))
            print(f"{INDENT}{s(ICON_WARN, WARN_COLOR)} {s('Cleanup Target', BOLD, WARN_COLOR)}")
            print(separator("\u2550", ACCENT, PANEL_WIDTH))
            print(kv_line("Setups", ", ".join(targets), val_color=ACCENT2))
            if active_setup in targets:
                print(kv_line("Active Setup", active_setup, val_color=WARN_COLOR))
                print(f"{INDENT}{warn('Active .env will be reset to another setup (or removed).')}")
            print(
                f"{INDENT}{warn('This only removes local setup files and Terraform state.')}\n"
                f"{INDENT}{warn('Cloud resources are NOT destroyed by this command.')}"
            )
            print(separator("\u2550", ACCENT, PANEL_WIDTH))
            print()

            if not force and not self._confirm_cleanup(targets):
                log.warning("Cleanup cancelled by user.")
                return

            removed: list[str] = []
            for target in targets:
                setup_dir = get_setup_dir(target)
                if setup_dir.exists():
                    shutil.rmtree(setup_dir)
                    removed.append(target)
                    log.info("Removed setup '%s' at %s", target, setup_dir)

            if not removed:
                log.warning("No setup files were removed.")
                return

            if active_setup in removed:
                remaining = list_setups()
                if remaining:
                    fallback = remaining[0]
                    fallback_env = get_setup_dir(fallback) / ".env"
                    shutil.copy2(fallback_env, ENV_FILE)
                    log.info("Active setup switched to '%s' at %s", fallback, ENV_FILE)
                elif ENV_FILE.exists():
                    ENV_FILE.unlink()
                    log.info("Removed active %s because no setups remain.", ENV_FILE)

            print()
            print(separator("\u2550", ACCENT, PANEL_WIDTH))
            log.info("Cleanup finished. Removed setup(s): %s", ", ".join(removed))
            print(separator("\u2550", ACCENT, PANEL_WIDTH))
            print()

        except KeyboardInterrupt:
            self._handle_keyboard_interrupt()
        except SystemExit:
            raise
        except Exception:
            log.debug("Unexpected cleanup exception traceback:\n%s", traceback.format_exc())
            log.error("Cleanup failed. Check '%s' for full details.", LOG_FILE.name)
            sys.exit(1)

    def run(self) -> None:
        try:
            self._print_banner()

            self._step = 1
            print(step_header(1, _TOTAL_STEPS, "Setup Name", ICON_ROCKET))
            setup_name = self.prompts.ask_setup_name()
            self._bind_setup(setup_name)
            log.info("Setup name selected: %s", setup_name)

            self._step = 2
            print(step_header(2, _TOTAL_STEPS, "Cloud Provider", ICON_CLOUD))
            cloud = self.prompts.ask_cloud()
            log.info("Cloud provider selected: %s", cloud)

            self._step = 3
            print(step_header(3, _TOTAL_STEPS, "Pre-flight Validation", ICON_BOLT))
            self.preflight.check(cloud)
            if cloud == "Azure" and not _USE_LEGACY_AZURE:
                self._ensure_terraform_cli()
            azure_context: dict = {}
            if cloud == "Azure":
                azure_context = self.prompts.ask_azure_context()
            else:
                azure_context = {}

            self._step = 4
            print(step_header(4, _TOTAL_STEPS, "Storage (Data Lakehouse)", ICON_FOLDER))
            while True:
                storage = self.prompts.ask_storage()
                if cloud == "Azure" and not _USE_LEGACY_AZURE:
                    try:
                        storage = self._normalize_azure_storage_name(storage)
                    except ValueError as exc:
                        log.error("%s", exc)
                        log.warning("Please enter a valid Azure storage account name and try again.")
                        continue
                break
            dw = self.prompts.ask_gold_database()

            self._step = 5
            print(step_header(5, _TOTAL_STEPS, "Compute & Data Warehouse", ICON_SERVER))
            compute = self.prompts.ask_compute()
            dw = self.prompts.ask_dw(compute, dw)
            if cloud == "Azure":
                azure_options = self.prompts.ask_azure_provisioning_options(storage, dw)
                if "dw_database" in azure_options:
                    dw["dw_database"] = azure_options["dw_database"]
                storage["replication"] = azure_options.get("storage_replication", "LRS")
                storage["soft_delete_days"] = azure_options.get("storage_soft_delete_days", 7)
                storage["versioning_enabled"] = azure_options.get("storage_versioning_enabled", True)
                storage["tags"] = azure_options.get("tags", {})
                storage["destroy_protection"] = azure_options.get("enable_destroy_protection", False)
                if "postgres_sku_name" in azure_options:
                    dw["postgres_sku_name"] = azure_options["postgres_sku_name"]
                if "postgres_backup_retention_days" in azure_options:
                    dw["postgres_backup_retention_days"] = azure_options["postgres_backup_retention_days"]
                if "postgres_public_network_access_enabled" in azure_options:
                    dw["postgres_public_network_access_enabled"] = azure_options[
                        "postgres_public_network_access_enabled"
                    ]
                if "postgres_allowed_cidr" in azure_options:
                    dw["postgres_allowed_cidr"] = azure_options["postgres_allowed_cidr"]
                if "postgres_high_availability_enabled" in azure_options:
                    dw["postgres_high_availability_enabled"] = azure_options[
                        "postgres_high_availability_enabled"
                    ]
                azure_context["tags"] = azure_options.get("tags", {})

            self._print_summary(setup_name, cloud, storage, compute, dw, azure_context)

            setup_dir = self._setup_dir_relative(setup_name)
            self.env.write(
                cloud,
                storage,
                compute,
                dw,
                setup_name=setup_name,
                setup_dir=setup_dir,
                azure=azure_context,
            )
            self._activate_setup(setup_name)

            self._step = 6
            self._invoke_provisioner(cloud, storage, compute, dw)

            print()
            print(separator("\u2550", ACCENT, PANEL_WIDTH))
            print(completion())
            print(separator("\u2550", ACCENT, PANEL_WIDTH))
            print()

        except KeyboardInterrupt:
            self._handle_keyboard_interrupt()

        except SystemExit:
            raise

        except Exception as exc:
            log.debug("Unexpected exception traceback:\n%s", traceback.format_exc())
            log.error("Unexpected error occurred. Check '%s' for full details.", LOG_FILE.name)
            sys.exit(1)

    def _invoke_provisioner(self, cloud: str, storage: dict, compute: dict, dw: dict) -> None:
        from setup.provisioner import Provisioner

        module_name = _PROVISIONER_REGISTRY[cloud]
        module_path = SETUP_DIR / f"{module_name}.py"

        if not module_path.exists():
            log.warning("Provisioner module not found: %s", module_path)
            log.warning("Skipping infrastructure provisioning.")
            return

        try:
            import importlib
            module = importlib.import_module(f"setup.{module_name}")
        except ImportError as exc:
            log.debug("Import traceback:\n%s", traceback.format_exc())
            log.error("Failed to import provisioner '%s': %s", module_name, exc)
            log.error("Infrastructure provisioning failed. Check '%s' for full details.", LOG_FILE.name)
            sys.exit(1)

        provisioner_cls = None
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if isinstance(attr, type) and issubclass(attr, Provisioner) and attr is not Provisioner:
                provisioner_cls = attr
                break

        if provisioner_cls is None:
            log.warning("Module '%s' has no Provisioner subclass. Skipping.", module_name)
            return

        if module_name == "azure_terraform_provisioner":
            from setup.azure_terraform_provisioner import AzureTerraformProvisioner

            provisioner = AzureTerraformProvisioner(
                cli=self.cli,
                env=self.env,
                logger=log,
                setup_name=self.setup_name,
            )
        else:
            provisioner = provisioner_cls(cli=self.cli, env=self.env, logger=log)
        log.info("Invoking %s provisioner...", cloud)

        try:
            result = provisioner.provision(storage, compute, dw)
            if not result.get("ok", False):
                log.warning("Provisioning completed with errors. See '%s' for details.", LOG_FILE.name)
        except SystemExit:
            raise
        except Exception as exc:
            log.debug("Provisioning exception traceback:\n%s", traceback.format_exc())
            log.error("Infrastructure provisioning failed. Check '%s' for full details.", LOG_FILE.name)
            sys.exit(1)

    def _invoke_destroy(self, cloud: str, storage: dict, compute: dict, dw: dict) -> None:
        from setup.azure_terraform_provisioner import AzureTerraformProvisioner

        provisioner = AzureTerraformProvisioner(
            cli=self.cli,
            env=self.env,
            logger=log,
            setup_name=self.setup_name,
        )
        log.info("Invoking %s destroy...", cloud)

        try:
            result = provisioner.destroy(storage, compute, dw)
            if not result.get("ok", False):
                log.warning("Destroy completed with errors. See '%s' for details.", LOG_FILE.name)
                sys.exit(1)
        except SystemExit:
            raise
        except Exception:
            log.debug("Destroy exception traceback:\n%s", traceback.format_exc())
            log.error("Infrastructure destroy failed. Check '%s' for full details.", LOG_FILE.name)
            sys.exit(1)

    @staticmethod
    def _ensure_terraform_cli() -> None:
        from setup.terraform_installer import ensure_terraform

        ensure_terraform(log)

    @staticmethod
    def _print_destroy_banner() -> None:
        print()
        print(panel("Quickelt", "Infrastructure Destroy", border_color=WARN_COLOR, title_color=BRAND))
        print()
        log.debug("Quickelt infrastructure destroy started")

    @staticmethod
    def _print_banner() -> None:
        print()
        print(panel("Quickelt", "Infrastructure Setup Wizard", border_color=ACCENT, title_color=BRAND))
        print()
        log.debug("Quickelt Infrastructure Setup started")

    def _bind_setup(self, setup_name: str) -> None:
        from setup.env_writer import EnvWriter
        from setup.setup_registry import ensure_setup_dir, get_setup_env_path

        ensure_setup_dir(setup_name)
        self.setup_name = setup_name
        self.env = EnvWriter(get_setup_env_path(setup_name), logger=log)

    def _setup_dir_relative(self, setup_name: str) -> str:
        from setup.setup_registry import get_setup_dir

        return str(get_setup_dir(setup_name).relative_to(PROJECT_ROOT)).replace("\\", "/")

    def _activate_setup(self, setup_name: str) -> None:
        """Mirror the setup .env to the project root as the active configuration."""
        setup_env = self.env.env_path
        if not setup_env.exists():
            return
        shutil.copy2(setup_env, ENV_FILE)
        log.info("Active setup '%s' linked at %s", setup_name, ENV_FILE)

    def _normalize_azure_storage_name(self, storage: dict) -> dict:
        normalized = self._terraform_executor_cls.normalize_storage_account_name(storage["name"])
        if storage.get("existing"):
            if normalized != storage["name"]:
                raise ValueError(
                    "Existing Azure storage account names must contain only lowercase letters "
                    "and numbers (3-24 chars)."
                )
            return storage
        if normalized != storage["name"]:
            log.info(
                "Adjusted storage name '%s' to Azure account name '%s'.",
                storage["name"],
                normalized,
            )
            storage = {**storage, "name": normalized}
        return storage

    def _resolve_setup_for_destroy(self, setup_name: str | None) -> str:
        from setup.env_writer import EnvWriter
        from setup.setup_registry import get_setup_env_path, list_setups, normalize_setup_name

        if setup_name:
            try:
                resolved = normalize_setup_name(setup_name)
            except ValueError as exc:
                log.error("%s", exc)
                sys.exit(1)
            if not get_setup_env_path(resolved).exists():
                log.error("Setup '%s' not found under infrastructure/setups/", resolved)
                sys.exit(1)
            return resolved

        if ENV_FILE.exists():
            root_env = EnvWriter(ENV_FILE, logger=log)
            root_name = root_env.read_value("QUICKELT_SETUP_NAME") or root_env.read_value("SETUP_NAME")
            if root_name and get_setup_env_path(root_name).exists():
                return root_name

        setups = list_setups()
        if not setups:
            log.error("No setups found under infrastructure/setups/")
            log.error("Run setup first or pass --setup-name <name>")
            sys.exit(1)
        if len(setups) == 1:
            return setups[0]
        return self._prompt_destroy_setup(setups)

    @staticmethod
    def _prompt_destroy_setup(setups: list[str]) -> str:
        print(f"\n  {s('Select a setup to destroy:', BOLD, WARN_COLOR)}")
        for idx, name in enumerate(setups, start=1):
            print(choice_line(idx, name, ACCENT2))
        while True:
            choice = input(prompt_label("Enter choice") + ": ").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(setups):
                return setups[int(choice) - 1]
            print(f"  {failure('Invalid choice.')}")

    @staticmethod
    def _prompt_cleanup_setup(setups: list[str]) -> str:
        print(f"\n  {s('Select a setup to delete:', BOLD, WARN_COLOR)}")
        for idx, name in enumerate(setups, start=1):
            print(choice_line(idx, name, ACCENT2))
        while True:
            choice = input(prompt_label("Enter choice") + ": ").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(setups):
                return setups[int(choice) - 1]
            print(f"  {failure('Invalid choice.')}")

    @staticmethod
    def _confirm_cleanup(targets: list[str]) -> bool:
        phrase = "DELETE"
        try:
            answer = input(
                prompt_label(
                    f"Type {phrase} to delete setup(s): {', '.join(targets)}"
                )
                + ": "
            ).strip()
        except (KeyboardInterrupt, EOFError):
            return False
        return answer == phrase

    @staticmethod
    def _print_summary(
        setup_name: str,
        cloud: str,
        storage: dict,
        compute: dict,
        dw: dict,
        azure: dict | None = None,
    ) -> None:
        print()
        print(separator("\u2550", ACCENT, PANEL_WIDTH))
        print(f"{INDENT}{s(ICON_DIAMOND, ACCENT2)} {s('Configuration Summary', BOLD, ACCENT)}")
        print(separator("\u2550", ACCENT, PANEL_WIDTH))

        print(kv_line("Setup", setup_name, val_color=ACCENT2))
        print(kv_line("Cloud", cloud, val_color=BRAND))
        if cloud == "Azure":
            azure = azure or {}
            print(kv_line("Subscription", azure.get("subscription_id", "(active)")))
            print(kv_line("Resource Group", azure.get("resource_group", DEFAULT_AZURE_RESOURCE_GROUP)))
            print(kv_line("Location", azure.get("location", DEFAULT_AZURE_LOCATION)))
            if azure.get("tags"):
                tag_preview = ", ".join(f"{k}={v}" for k, v in azure["tags"].items())
                print(kv_line("Tags", tag_preview))
        print(kv_line("Storage", storage["name"]))
        if cloud == "Azure":
            print(kv_line("Storage SKU", storage.get("replication", "LRS")))
            print(kv_line("Soft Delete", f"{storage.get('soft_delete_days', 7)}d"))
            print(kv_line("Versioning", "Enabled" if storage.get("versioning_enabled", True) else "Disabled"))
            print(kv_line("Destroy Protect", "Enabled" if storage.get("destroy_protection", False) else "Disabled"))
            print(kv_line("Est. Monthly", SetupWizard._estimate_azure_monthly_cost(storage, dw)))
        if storage["layers"]:
            print(kv_line("Layers", ", ".join(storage["layers"]), val_color=ACCENT2))
        print(kv_line("Existing", "Yes" if storage["existing"] else "No",
                       val_color=SUCCESS if storage["existing"] else MUTED))
        print(kv_line("Compute", compute["compute"]))
        if compute["compute"] == "Dedicated VM":
            print(kv_line("Bootstrap VM", "Yes" if compute["bootstrap_vm"] else "No",
                           val_color=SUCCESS if compute["bootstrap_vm"] else MUTED))
        if dw.get("gold_external_db"):
            print(kv_line("Gold DB", "Yes", val_color=ACCENT2))
            strategy_label = "Local VM" if dw["pg_strategy"] == "local_vm" else "Managed Cloud"
            print(kv_line("PG Strategy", strategy_label))
            if dw.get("install_local_postgres"):
                print(kv_line("Local PG", "Yes (auto-installed)", val_color=SUCCESS))
            if dw.get("managed_cloud_choice"):
                print(kv_line("Managed", dw["managed_cloud_choice"].replace("_", " ").title()))
            if dw.get("dw_database"):
                print(kv_line("DW Database", dw["dw_database"]))
            if dw.get("postgres_sku_name"):
                print(kv_line("PG SKU", dw["postgres_sku_name"]))
            if dw.get("postgres_backup_retention_days"):
                print(kv_line("PG Backup", f"{dw['postgres_backup_retention_days']}d"))
            if dw.get("postgres_public_network_access_enabled") is not None:
                print(
                    kv_line(
                        "PG Public Access",
                        "Yes" if dw.get("postgres_public_network_access_enabled") else "No",
                    )
                )
            if dw.get("postgres_allowed_cidr"):
                print(kv_line("PG Allowed CIDR", dw["postgres_allowed_cidr"]))
            if dw.get("postgres_high_availability_enabled") is not None:
                print(
                    kv_line(
                        "PG HA",
                        "Enabled" if dw.get("postgres_high_availability_enabled") else "Disabled",
                    )
                )
            if dw.get("dw_host"):
                print(kv_line("DW Host", dw["dw_host"]))

        print(separator("\u2550", ACCENT, PANEL_WIDTH))
        print()

        log.debug(
            "Summary: cloud=%s, storage=%s, existing=%s, layers=%s, compute=%s, bootstrap=%s, "
            "gold_external_db=%s, pg_strategy=%s, install_local_postgres=%s, managed_cloud_choice=%s",
            cloud, storage["name"], storage["existing"],
            ",".join(storage.get("layers", [])),
            compute["compute"], compute.get("bootstrap_vm", False),
            dw.get("gold_external_db", False), dw.get("pg_strategy"),
            dw.get("install_local_postgres", False), dw.get("managed_cloud_choice"),
        )

    @staticmethod
    def _estimate_azure_monthly_cost(storage: dict, dw: dict) -> str:
        storage_cost = {
            "LRS": 10,
            "ZRS": 16,
            "GRS": 20,
            "RAGRS": 24,
        }.get(storage.get("replication", "LRS"), 10)

        postgres_cost = 0
        if (
            dw.get("gold_external_db")
            and dw.get("pg_strategy") == "managed_cloud"
            and dw.get("managed_cloud_choice") == "provision_new"
        ):
            postgres_cost = {
                "B_Standard_B1ms": 30,
                "GP_Standard_D2ds_v4": 110,
                "MO_Standard_E2s_v3": 170,
            }.get(dw.get("postgres_sku_name", "B_Standard_B1ms"), 30)
            if dw.get("postgres_high_availability_enabled"):
                postgres_cost *= 2

        total = storage_cost + postgres_cost
        return f"~${total}-{int(total * 1.3)} USD"

    @staticmethod
    def _handle_keyboard_interrupt() -> None:
        try:
            import termios
            fd = sys.stdin.fileno()
            old = termios.tcgetattr(fd)
            termios.tcsetattr(fd, termios.TCSADRAIN, old)
        except Exception:
            pass
        print("\n")
        log.info("Setup cancelled by user (Ctrl+C)")
        print(goodbye())
        sys.exit(130)

    @staticmethod
    def _configure_logging() -> None:
        INFRA_LOGS_DIR.mkdir(parents=True, exist_ok=True)

        parent = logging.getLogger("quickelt")
        parent.setLevel(logging.DEBUG)
        parent.handlers.clear()
        parent.propagate = False

        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch.setFormatter(_ConsoleFormatter())
        parent.addHandler(ch)

        fh = logging.FileHandler(str(LOG_FILE), mode="w", encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(_FileFormatter(datefmt="%Y-%m-%d %H:%M:%S"))
        parent.addHandler(fh)


def main() -> None:
    try:
        destroy, clean, setup_name, force = _parse_args(sys.argv[1:])
    except ValueError as exc:
        print(f"Argument error: {exc}")
        sys.exit(2)

    wizard = SetupWizard(setup_name=setup_name if destroy else None)
    if destroy:
        wizard.run_destroy(setup_name)
    elif clean:
        wizard.run_cleanup(setup_name, force=force)
    else:
        wizard.run()


if __name__ == "__main__":
    main()
