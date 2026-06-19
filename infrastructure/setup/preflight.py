#!/usr/bin/env python3
"""
Quickelt Pre-flight Checker
============================

Validates that the required cloud CLI tools are installed and authenticated
before the setup wizard proceeds with provisioning.  When a CLI is missing,
the user is prompted to auto-install it.  For Azure, the subscription is
also validated and the user can select the correct one.
"""

import json
import logging
import platform
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from setup._style import (
    ACCENT,
    ACCENT2,
    BRAND,
    BOLD,
    FAILURE,
    HIGHLIGHT,
    MUTED,
    SUCCESS,
    WARN_COLOR,
    ICON_CHECK,
    ICON_CROSS,
    ICON_WARN,
    ICON_GEAR,
    INDENT,
    accent,
    accent2,
    brand,
    choice_line,
    failure,
    highlight,
    icon,
    input_hint,
    muted,
    prompt_label,
    s,
    success,
    warn,
)


def _install_aws_linux(tmpdir: str, log: logging.Logger) -> None:
    zip_path = Path(tmpdir) / "awscliv2.zip"
    log.info("  Downloading AWS CLI v2...")
    _run(["curl", "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip", "-o", str(zip_path)], tmpdir, log)
    log.info("  Extracting...")
    _run(["unzip", str(zip_path)], tmpdir, log)
    log.info("  Installing AWS CLI (sudo required)...")
    _run(["sudo", str(Path(tmpdir) / "aws" / "install")], None, log)


def _install_aws_macos(tmpdir: str, log: logging.Logger) -> None:
    pkg_path = Path(tmpdir) / "AWSCLIV2.pkg"
    log.info("  Downloading AWS CLI v2...")
    _run(["curl", "https://awscli.amazonaws.com/AWSCLIV2.pkg", "-o", str(pkg_path)], tmpdir, log)
    log.info("  Installing AWS CLI (sudo required)...")
    _run(["sudo", "installer", "-pkg", str(pkg_path), "-target", "/"], None, log)


def _install_azure_linux(tmpdir: str, log: logging.Logger) -> None:
    script_path = Path(tmpdir) / "install_az_cli.sh"
    log.info("  Downloading Azure CLI install script...")
    result = subprocess.run(
        ["curl", "-sL", "https://aka.ms/InstallAzureCLIDeb"],
        capture_output=True,
        text=True,
        timeout=300,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Download failed: {result.stderr.strip()[:200]}")
    script_path.write_text(result.stdout)
    log.info("  Installing Azure CLI (sudo required)...")
    _run(["sudo", "bash", str(script_path)], None, log)


def _install_azure_macos(tmpdir: str, log: logging.Logger) -> None:
    log.info("  Installing Azure CLI via Homebrew...")
    _run(["brew", "install", "azure-cli"], None, log)


_INSTALLERS = {
    ("AWS", "linux"): _install_aws_linux,
    ("AWS", "darwin"): _install_aws_macos,
    ("Azure", "linux"): _install_azure_linux,
    ("Azure", "darwin"): _install_azure_macos,
}


def _run(cmd: list[str], cwd: str | None, log: logging.Logger) -> None:
    log.debug("  Executing: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600, cwd=cwd)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed (rc={result.returncode}): {result.stderr.strip()[:200]}")


def _exec(cmd: list[str], timeout: int = 15) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


class PreflightChecker:
    _PREFLIGHT = {
        "AWS": {
            "command": ["aws", "sts", "get-caller-identity"],
            "remedy": "aws configure",
        },
        "Azure": {
            "command": ["az", "account", "show"],
            "remedy": "az login",
        },
    }

    def __init__(self, logger: logging.Logger | None = None):
        self.log = logger or logging.getLogger("quickelt.preflight")

    def check(self, cloud: str) -> None:
        cfg = self._PREFLIGHT[cloud]
        cmd_str = " ".join(cfg["command"])
        self.log.info("Running pre-flight check for %s...", cloud)
        self.log.debug("Executing: %s", cmd_str)

        try:
            result = _exec(cfg["command"])
            self.log.debug("CLI exit code: %d", result.returncode)
            self.log.debug("CLI stdout: %s", result.stdout.strip())
            self.log.debug("CLI stderr: %s", result.stderr.strip())
            if result.returncode != 0:
                if cloud == "Azure" and "SubscriptionNotFound" in result.stderr:
                    self._azure_fix_subscription(result.stderr.strip())
                    return
                self._fail(cloud, cfg["remedy"], result.stderr.strip())
            else:
                if cloud == "Azure":
                    self._azure_validate_subscription(current_result=result)
                else:
                    self.log.info("Pre-flight check passed for %s", cloud)
        except FileNotFoundError:
            self._try_install(cloud, cfg["remedy"])
        except subprocess.TimeoutExpired:
            self._fail(cloud, cfg["remedy"], "Pre-flight check timed out.")
        except OSError as exc:
            self._fail(cloud, cfg["remedy"], str(exc))

    def _azure_validate_subscription(self, current_result: subprocess.CompletedProcess | None = None) -> None:
        self.log.debug("Validating Azure subscription access...")
        result = current_result or _exec(["az", "account", "show", "--output", "json"], timeout=15)

        if result.returncode != 0:
            if "SubscriptionNotFound" in result.stderr:
                self._azure_fix_subscription(result.stderr.strip())
            else:
                self._fail("Azure", "az login", result.stderr.strip())
            return

        current = self._parse_json(result.stdout)
        if not isinstance(current, dict):
            self.log.info("Pre-flight check passed for Azure")
            return

        if not current.get("id"):
            self.log.info("Pre-flight check passed for Azure")
            return

        sub_id = current.get("id", "")
        sub_name = current.get("name", "")
        state = current.get("state", "")
        self.log.info("Azure subscription: %s (%s) — State: %s", sub_name, sub_id, state)

        if state not in ("Enabled", "Warned", "PastDue", "Enabled"):
            self.log.warning("Subscription state is '%s'. It may not be usable for deployments.", state)
            self._azure_ensure_subscription()
            return

        verify = _exec(["az", "group", "list", "--query", "[0].name", "--output", "tsv"], timeout=15)
        if verify.returncode != 0 and "SubscriptionNotFound" in verify.stderr:
            self._azure_fix_subscription(verify.stderr.strip())
            return

        self.log.info("Pre-flight check passed for Azure")

    def _azure_fix_subscription(self, error_detail: str) -> None:
        self.log.error("Current Azure subscription is not accessible: %s", error_detail[:200])
        self.log.info("Checking available subscriptions...")
        self._azure_ensure_subscription()

    def _azure_ensure_subscription(self) -> None:
        result = _exec(["az", "account", "list", "--all", "--output", "json", "--query",
                        "[?state=='Enabled'].{id:id, name:name, state:state}"], timeout=30)

        if result.returncode != 0:
            self._fail("Azure", "az login",
                       f"Could not list subscriptions: {result.stderr.strip()[:200]}")

        subs = self._parse_json(result.stdout)
        if not isinstance(subs, list):
            self.log.info("Could not parse Azure subscription list; keeping current subscription.")
            self.log.info("Pre-flight check passed for Azure")
            return
        if not subs:
            self._fail(
                "Azure", "az login",
                "No active Azure subscriptions found. "
                "Create a subscription at https://azure.microsoft.com/free before running setup.",
            )

        if len(subs) == 1:
            sub = subs[0]
            self._azure_set_subscription(sub)
            return

        self.log.info("Multiple Azure subscriptions found:")
        for i, sub in enumerate(subs, 1):
            self.log.info("  %s", choice_line(i, f"{sub['name']}  ({sub['id']})", ACCENT2))

        while True:
            ans = input(prompt_label("Select a subscription") + " " + input_hint(f"(1-{len(subs)})") + ": ").strip()
            if ans.isdigit() and 1 <= int(ans) <= len(subs):
                self._azure_set_subscription(subs[int(ans) - 1])
                return
            print(f"  {failure(f'Enter a number between 1 and {len(subs)}.')}")

    def _azure_set_subscription(self, sub: dict) -> None:
        sub_id = sub["id"]
        sub_name = sub["name"]
        self.log.info("Setting active subscription to: %s (%s)", sub_name, sub_id)

        result = _exec(["az", "account", "set", "--subscription", sub_id], timeout=15)
        if result.returncode != 0:
            self._fail("Azure", "az login",
                       f"Failed to set subscription '{sub_name}': {result.stderr.strip()[:200]}")

        verify = _exec(["az", "account", "show", "--output", "json"], timeout=15)
        if verify.returncode != 0:
            self._fail("Azure", "az login",
                       f"Subscription verification failed: {verify.stderr.strip()[:200]}")

        self.log.info("Pre-flight check passed for Azure (subscription: %s)", sub_name)

    @staticmethod
    def _parse_json(raw: str):
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None

    def _try_install(self, cloud: str, remedy: str) -> None:
        cli_name = self._PREFLIGHT[cloud]["command"][0]
        self.log.warning("CLI tool '%s' not found.", cli_name)

        if not sys.stdin.isatty():
            self._fail(
                cloud,
                remedy,
                (
                    f"CLI tool '{cli_name}' is missing and setup is running in a non-interactive shell. "
                    f"Install '{cli_name}' manually and re-run setup."
                ),
            )

        os_key = platform.system().lower()
        installer = _INSTALLERS.get((cloud, os_key))

        if installer is None:
            self._fail(
                cloud,
                remedy,
                f"Auto-install not available for {cloud} on {platform.system()}. "
                f"Install '{cli_name}' manually and re-run setup.",
            )

        if not self._confirm_install(cloud, cli_name):
            self._fail(
                cloud,
                remedy,
                f"CLI tool '{cli_name}' is required. Install it manually and re-run setup.",
            )

        self.log.info("Installing %s CLI...", cloud)
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                installer(tmpdir, self.log)
        except Exception as exc:
            self._fail(cloud, remedy, f"Installation failed: {exc}")

        if not shutil.which(cli_name):
            self._fail(
                cloud,
                remedy,
                f"'{cli_name}' still not found after installation. "
                f"Ensure it is on your PATH and re-run setup.",
            )

        self.log.info("%s CLI installed successfully. Re-running pre-flight check...", cloud)
        self.check(cloud)

    def _confirm_install(self, cloud: str, cli_name: str) -> bool:
        self.log.info("Auto-install available for %s CLI (%s)", cloud, platform.system())
        while True:
            ans = input(prompt_label(f"Install {brand(cli_name)} automatically?") + " " + input_hint("(y/n) [y]") + ": ").strip().lower()
            if ans in ("y", "yes", ""):
                return True
            if ans in ("n", "no"):
                return False
            print(f"  {failure('Enter y or n.')}")

    def _fail(self, cloud: str, remedy: str, detail: str) -> None:
        self.log.error("Pre-flight check FAILED for %s", cloud)
        if detail:
            self.log.error("  %s", detail)
        self.log.error("Authenticate first by running: %s", remedy)
        sys.exit(1)
