#!/usr/bin/env python3
"""
Quickelt Setup Registry
=======================

Named setup workspaces under infrastructure/setups/<name>/.
Each setup keeps its own .env and Terraform state.
"""

from __future__ import annotations

import re
from pathlib import Path

_SETUP_NAME_RE = re.compile(r"^[a-z][a-z0-9-]{1,62}$")

INFRASTRUCTURE_DIR = Path(__file__).resolve().parent.parent
SETUPS_ROOT = INFRASTRUCTURE_DIR / "setups"


def normalize_setup_name(raw: str) -> str:
    """Convert user input into a safe setup directory name."""
    slug = raw.strip().lower().replace("_", "-").replace(" ", "-")
    slug = re.sub(r"[^a-z0-9-]", "", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    if not slug or not _SETUP_NAME_RE.match(slug):
        raise ValueError(
            "Setup name must be 2-63 characters, start with a letter, "
            "and contain only lowercase letters, digits, and hyphens."
        )
    return slug


def get_setup_dir(setup_name: str) -> Path:
    return SETUPS_ROOT / normalize_setup_name(setup_name)


def get_setup_env_path(setup_name: str) -> Path:
    return get_setup_dir(setup_name) / ".env"


def get_setup_terraform_workspace(setup_name: str) -> Path:
    return get_setup_dir(setup_name) / "terraform"


def list_setups() -> list[str]:
    if not SETUPS_ROOT.exists():
        return []
    names: list[str] = []
    for entry in sorted(SETUPS_ROOT.iterdir()):
        if entry.is_dir() and (entry / ".env").exists():
            names.append(entry.name)
    return names


def ensure_setup_dir(setup_name: str) -> Path:
    setup_dir = get_setup_dir(setup_name)
    setup_dir.mkdir(parents=True, exist_ok=True)
    get_setup_terraform_workspace(setup_name).mkdir(parents=True, exist_ok=True)
    return setup_dir
