#!/usr/bin/env python3
"""
Quickelt Terraform Installer
============================

Auto-installs the Terraform CLI when missing, following the same interactive
pattern used for AWS/Azure CLI installation in preflight.
"""

from __future__ import annotations

import logging
import os
import platform
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from setup._style import failure, input_hint, prompt_label

_TERRAFORM_VERSION = "1.9.8"


def _terraform_platform_slug() -> str:
    system = platform.system().lower()
    machine = platform.machine().lower()
    arch = "arm64" if machine in ("aarch64", "arm64") else "amd64"

    if system == "linux":
        return f"linux_{arch}"
    if system == "darwin":
        return f"darwin_{arch}"
    raise RuntimeError(
        f"Auto-install for Terraform is not supported on {platform.system()}. "
        "Install Terraform >= 1.5 manually and re-run setup."
    )


def _prepend_local_bin_to_path() -> Path:
    local_bin = Path.home() / ".local" / "bin"
    local_bin.mkdir(parents=True, exist_ok=True)
    path = os.environ.get("PATH", "")
    local_bin_str = str(local_bin)
    if local_bin_str not in path.split(os.pathsep):
        os.environ["PATH"] = f"{local_bin_str}{os.pathsep}{path}"
    return local_bin


def _run(cmd: list[str], cwd: str | None, log: logging.Logger) -> None:
    log.debug("  Executing: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600, cwd=cwd)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()[:200]
        raise RuntimeError(f"Command failed (rc={result.returncode}): {detail}")


def _install_terraform(tmpdir: str, log: logging.Logger) -> None:
    slug = _terraform_platform_slug()
    version = _TERRAFORM_VERSION
    url = f"https://releases.hashicorp.com/terraform/{version}/terraform_{version}_{slug}.zip"
    zip_path = Path(tmpdir) / "terraform.zip"

    log.info("  Downloading Terraform %s...", version)
    _run(["curl", "-fsSL", url, "-o", str(zip_path)], tmpdir, log)
    log.info("  Extracting...")
    _run(["unzip", "-o", str(zip_path), "-d", tmpdir], tmpdir, log)

    binary = Path(tmpdir) / "terraform"
    if not binary.exists():
        raise RuntimeError("Terraform binary not found after extraction.")

    dest_dir = _prepend_local_bin_to_path()
    dest = dest_dir / "terraform"
    shutil.copy2(binary, dest)
    dest.chmod(0o755)


def _confirm_install(log: logging.Logger) -> bool:
    log.info("Auto-install available for Terraform (%s)", platform.system())
    while True:
        try:
            ans = input(
                prompt_label("Install terraform automatically?")
                + " "
                + input_hint("(y/n) [y]")
                + ": "
            ).strip().lower()
        except (KeyboardInterrupt, EOFError):
            return False
        if ans in ("y", "yes", ""):
            return True
        if ans in ("n", "no"):
            return False
        print(f"  {failure('Enter y or n.')}")


def ensure_terraform(logger: logging.Logger | None = None, *, confirm: bool = True) -> None:
    """Ensure ``terraform`` is available, optionally installing it interactively."""
    log = logger or logging.getLogger("quickelt.terraform")

    if shutil.which("terraform"):
        return

    log.warning("Terraform CLI not found on PATH.")

    if platform.system().lower() not in ("linux", "darwin"):
        log.error(
            "Auto-install for Terraform is not available on %s.",
            platform.system(),
        )
        log.error("Install Terraform >= 1.5 manually or set QUICKELT_LEGACY_AZURE_PROVISIONER=1.")
        sys.exit(1)

    skip_confirm = os.getenv("QUICKELT_TERRAFORM_AUTO_INSTALL", "").lower() in ("1", "true", "yes")
    if confirm and not skip_confirm and not _confirm_install(log):
        log.error("Terraform is required for Azure provisioning.")
        log.error("Install Terraform >= 1.5 or set QUICKELT_LEGACY_AZURE_PROVISIONER=1.")
        sys.exit(1)

    log.info("Installing Terraform %s...", _TERRAFORM_VERSION)
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            _install_terraform(tmpdir, log)
    except Exception as exc:
        log.error("Terraform installation failed: %s", exc)
        log.error("Install Terraform >= 1.5 manually or set QUICKELT_LEGACY_AZURE_PROVISIONER=1.")
        sys.exit(1)

    if not shutil.which("terraform"):
        log.error(
            "Terraform was installed to ~/.local/bin but is still not on PATH. "
            "Add ~/.local/bin to your PATH and re-run setup."
        )
        sys.exit(1)

    log.info("Terraform installed successfully.")
