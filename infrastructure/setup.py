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
"""

import logging
import os
import sys
import traceback
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"
SETUP_DIR = Path(__file__).resolve().parent / "setup"
LOG_FILE = PROJECT_ROOT / ".quickelt_setup.log"

_PROVISIONER_REGISTRY = {
    "AWS": "aws_provisioner",
    "Azure": "azure_provisioner",
}

log = logging.getLogger("quickelt.setup")

_RESET = "\033[0m"
_BOLD = "\033[1m"
_RED = "\033[31m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_BLUE = "\033[34m"
_DIM = "\033[2m"
_NO_COLOR = not sys.stdout.isatty() or bool(os.getenv("NO_COLOR"))


class _ConsoleFormatter(logging.Formatter):
    _STYLES = {
        logging.DEBUG: (_DIM, "[DEBUG]"),
        logging.INFO: (_GREEN, "[INFO]"),
        logging.WARNING: (_YELLOW, "[WARNING]"),
        logging.ERROR: (_RED + _BOLD, "[ERROR]"),
        logging.CRITICAL: (_RED + _BOLD, "[CRITICAL]"),
    }

    def format(self, record: logging.LogRecord) -> str:
        msg = record.getMessage()
        if _NO_COLOR:
            return f"[{record.levelname}] {msg}"
        style, tag = self._STYLES.get(record.levelno, ("", f"[{record.levelname}]"))
        return f"{style}{tag}{_RESET} {msg}"


class _FileFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ts = self.formatTime(record, self.datefmt)
        msg = record.getMessage()
        if record.exc_info and record.exc_info[1] is not None:
            msg += "\n" + self.formatException(record.exc_info)
        return f"{ts} [{record.levelname:>8}] [{record.name}] {msg}"


class SetupWizard:
    def __init__(self):
        self._configure_logging()
        sys.path.insert(0, str(Path(__file__).resolve().parent))

        from setup.cli_executor import CLIExecutor
        from setup.env_writer import EnvWriter
        from setup.preflight import PreflightChecker
        from setup.prompts import create_prompt_backend

        self.cli = CLIExecutor()
        self.env = EnvWriter(ENV_FILE, logger=log)
        self.preflight = PreflightChecker(logger=log)
        self.prompts = create_prompt_backend(logger=log)
        self._provisioner = None

    def run(self) -> None:
        try:
            self._print_banner()

            cloud = self.prompts.ask_cloud()
            log.info("Cloud provider selected: %s", cloud)

            self.preflight.check(cloud)

            storage = self.prompts.ask_storage()
            compute = self.prompts.ask_compute()
            dw = self.prompts.ask_dw(compute)

            self._print_summary(cloud, storage, compute, dw)

            self.env.write(cloud, storage, compute, dw)

            self._invoke_provisioner(cloud, storage, compute, dw)

            log.info("Setup complete.")

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

    @staticmethod
    def _print_banner() -> None:
        print()
        print("  ╔══════════════════════════════════════════╗")
        print("  ║       Quickelt Infrastructure Setup       ║")
        print("  ╚══════════════════════════════════════════╝")
        print()
        log.debug("Quickelt Infrastructure Setup started")

    @staticmethod
    def _print_summary(cloud: str, storage: dict, compute: dict, dw: dict) -> None:
        print()
        print("  ── Configuration Summary ──────────────────")
        print(f"  Cloud Provider  : {cloud}")
        print(f"  Storage         : {storage['name']}")
        if storage["layers"]:
            print(f"  Layers          : {', '.join(storage['layers'])}")
        print(f"  Existing        : {'Yes' if storage['existing'] else 'No'}")
        print(f"  Compute         : {compute['compute']}")
        if compute["compute"] == "Dedicated VM":
            print(f"  Bootstrap VM    : {'Yes' if compute['bootstrap_vm'] else 'No'}")
        if dw.get("gold_external_db"):
            print(f"  Gold External DB: Yes")
            strategy_label = "Local VM" if dw["pg_strategy"] == "local_vm" else "Managed Cloud"
            print(f"  PG Strategy     : {strategy_label}")
            if dw.get("install_local_postgres"):
                print(f"  Local Postgres  : Yes (auto-installed in VM)")
            if dw.get("managed_cloud_choice"):
                print(f"  Managed Choice  : {dw['managed_cloud_choice'].replace('_', ' ').title()}")
            if dw.get("dw_host"):
                print(f"  DW Host         : {dw['dw_host']}")
        print("  ──────────────────────────────────────────")
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
        print("  Setup cancelled. Goodbye!")
        sys.exit(130)

    @staticmethod
    def _configure_logging() -> None:
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
    wizard = SetupWizard()
    wizard.run()


if __name__ == "__main__":
    main()
