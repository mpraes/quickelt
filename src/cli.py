#!/usr/bin/env python3
"""
Quickelt CLI
============

Entry point for the ``quickelt`` console script.

Usage:
    quickelt setup              — Run the infrastructure setup wizard
    quickelt setup --destroy    — Destroy Azure infrastructure (Terraform)
    quickelt setup --destroy --setup-name my-project
    quickelt cleanup            — Delete a local setup workspace
    quickelt cleanup --setup-name my-project --yes
"""

import sys


def main() -> None:
    args = sys.argv[1:]

    if not args or args[0] in ("--help", "-h"):
        _print_usage()
        sys.exit(0)

    command = args[0]

    if command == "setup":
        _run_setup(args[1:])
    elif command == "cleanup":
        _run_setup(["--clean", *args[1:]])
    else:
        print(f"Unknown command: {command}")
        _print_usage()
        sys.exit(1)


def _print_usage() -> None:
    print("Usage: quickelt <command>")
    print()
    print("Commands:")
    print("  setup              Run the infrastructure setup wizard")
    print("  setup --destroy    Destroy Azure infrastructure created by setup")
    print("                     Use --setup-name <name> to target a specific setup")
    print("  cleanup            Delete local setup files (does not destroy cloud resources)")
    print("                     Use --setup-name <name> and optional --yes for no prompt")
    print()
    print("Options:")
    print("  -h, --help         Show this help message")
    print("  --destroy          Tear down Azure Terraform resources (with setup)")
    print("  --clean            Delete local setup folder under infrastructure/setups/")
    print("  --setup-name NAME  Named setup under infrastructure/setups/")
    print("  --yes              Skip DELETE confirmation for cleanup")


def _run_setup(extra_args: list[str]) -> None:
    from pathlib import Path

    project_root = Path(__file__).resolve().parent.parent
    setup_script = project_root / "infrastructure" / "setup.py"

    if not setup_script.exists():
        print(f"Setup script not found: {setup_script}")
        sys.exit(1)

    import subprocess

    cmd = [sys.executable, str(setup_script), *extra_args]
    result = subprocess.run(cmd)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
