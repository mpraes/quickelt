#!/usr/bin/env python3
"""
Quickelt Interactive Prompts
=============================

Abstract base class and three concrete implementations for the setup
wizard prompts, supporting inquirer, questionary, and a pure builtin
fallback.  A factory function auto-detects the available backend.
"""

import abc
import getpass
import logging
import secrets
import string

from setup._backend_detect import detect_prompt_backend

_AVAILABLE = detect_prompt_backend()

_DEFAULT_LAYERS = ["bronze", "silver", "gold"]

_PG_STRATEGY_LOCAL_VM = "Local PostgreSQL inside the Compute VM (Cost-efficient / Dev environment)"
_PG_STRATEGY_MANAGED_CLOUD = "Managed Cloud Service (AWS Aurora PostgreSQL / Azure DB for PostgreSQL)"


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
    def ask_cloud(self) -> str: ...

    @abc.abstractmethod
    def ask_storage(self) -> dict: ...

    @abc.abstractmethod
    def ask_compute(self) -> dict: ...

    @abc.abstractmethod
    def ask_dw(self, compute: dict) -> dict: ...

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


class InquirerBackend(PromptBackend):
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

    def ask_dw(self, compute: dict) -> dict:
        dw = self._empty_dw()

        gold_external = inquirer.prompt([
            inquirer.List(
                "gold_external",
                message="Will the Gold layer be hosted in an external database?",
                choices=["Yes", "No"],
            ),
        ])["gold_external"]

        if gold_external == "No":
            return dw

        dw["gold_external_db"] = True

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


class QuestionaryBackend(PromptBackend):
    def ask_cloud(self) -> str:
        return questionary.select(
            "Select your cloud provider:",
            choices=["AWS", "Azure"],
        ).ask()

    def ask_storage(self) -> dict:
        has_existing = questionary.select(
            "Do you have an existing Data Lake / Storage Account?",
            choices=["Yes", "No"],
        ).ask()

        if has_existing == "Yes":
            name = questionary.text("Enter the existing bucket/container name:").ask()
            return {"existing": True, "name": name, "layers": []}

        name = questionary.text(
            "Enter a name for the new bucket/container:",
            default="quickelt-data-lake",
        ).ask()

        layers = questionary.checkbox(
            "Select layers to auto-create:",
            choices=_DEFAULT_LAYERS,
            default=_DEFAULT_LAYERS,
        ).ask()

        return {"existing": False, "name": name, "layers": layers or []}

    def ask_compute(self) -> dict:
        compute = questionary.select(
            "Where will the Quickelt code execute?",
            choices=["Local Machine", "Dedicated VM", "Serverless/PaaS"],
        ).ask()

        bootstrap = False
        if compute == "Dedicated VM":
            bootstrap = questionary.confirm(
                "Auto-bootstrap the VM with Python/pip/git?",
                default=True,
            ).ask()

        return {"compute": compute, "bootstrap_vm": bootstrap}

    def ask_dw(self, compute: dict) -> dict:
        dw = self._empty_dw()

        gold_external = questionary.select(
            "Will the Gold layer be hosted in an external database?",
            choices=["Yes", "No"],
        ).ask()

        if gold_external == "No" or gold_external is None:
            return dw

        dw["gold_external_db"] = True

        strategy = questionary.select(
            "Which PostgreSQL deployment strategy do you want to use?",
            choices=[_PG_STRATEGY_LOCAL_VM, _PG_STRATEGY_MANAGED_CLOUD],
        ).ask()

        if strategy == _PG_STRATEGY_LOCAL_VM or strategy is None:
            self._apply_local_vm_strategy(dw, compute)
        else:
            dw["pg_strategy"] = "managed_cloud"

            managed = questionary.select(
                "Provision a new managed cluster or connect to an existing one?",
                choices=["Provision a new cluster", "Connect to an existing cluster"],
            ).ask()

            if managed == "Provision a new cluster":
                self._set_provision_new_dw(dw)
            else:
                host = questionary.text("Enter the PostgreSQL host endpoint:").ask()
                port = questionary.text("Port:", default="5432").ask()
                username = questionary.text("Username:", default="quickelt").ask()
                password = questionary.password("Password:").ask()
                self._set_connect_existing_dw(dw, host or "", port, username, password or "")

        return dw


class BuiltinBackend(PromptBackend):
    def ask_cloud(self) -> str:
        print("\n  Select your cloud provider:")
        print("  [1] AWS")
        print("  [2] Azure")
        while True:
            choice = input("  Enter choice (1/2): ").strip()
            if choice == "1":
                return "AWS"
            if choice == "2":
                return "Azure"
            print("  Invalid choice. Enter 1 or 2.")

    def ask_storage(self) -> dict:
        print("\n  Do you have an existing Data Lake / Storage Account?")
        print("  [1] Yes")
        print("  [2] No")
        while True:
            choice = input("  Enter choice (1/2): ").strip()
            if choice in ("1", "2"):
                break
            print("  Invalid choice. Enter 1 or 2.")

        if choice == "1":
            name = input("  Enter the existing bucket/container name: ").strip()
            return {"existing": True, "name": name, "layers": []}

        name = input("  Enter a name for the new bucket/container [quickelt-data-lake]: ").strip()
        if not name:
            name = "quickelt-data-lake"

        print("  Select layers to auto-create (comma-separated):")
        print(f"  Available: {', '.join(_DEFAULT_LAYERS)}")
        raw = input(f"  Layers [{','.join(_DEFAULT_LAYERS)}]: ").strip()
        if not raw:
            layers = _DEFAULT_LAYERS[:]
        else:
            layers = [l.strip() for l in raw.split(",") if l.strip() in _DEFAULT_LAYERS]

        return {"existing": False, "name": name, "layers": layers}

    def ask_compute(self) -> dict:
        print("\n  Where will the Quickelt code execute?")
        print("  [1] Local Machine")
        print("  [2] Dedicated VM")
        print("  [3] Serverless/PaaS")
        while True:
            choice = input("  Enter choice (1/2/3): ").strip()
            if choice in ("1", "2", "3"):
                break
            print("  Invalid choice. Enter 1, 2, or 3.")

        compute_map = {"1": "Local Machine", "2": "Dedicated VM", "3": "Serverless/PaaS"}
        compute = compute_map[choice]

        bootstrap = False
        if compute == "Dedicated VM":
            while True:
                ans = input("  Auto-bootstrap the VM with Python/pip/git? (y/n) [y]: ").strip().lower()
                if ans in ("y", "yes", ""):
                    bootstrap = True
                    break
                if ans in ("n", "no"):
                    break
                print("  Enter y or n.")

        return {"compute": compute, "bootstrap_vm": bootstrap}

    def ask_dw(self, compute: dict) -> dict:
        dw = self._empty_dw()

        print("\n  Will the Gold layer be hosted in an external database?")
        print("  [1] Yes")
        print("  [2] No")
        while True:
            choice = input("  Enter choice (1/2): ").strip()
            if choice in ("1", "2"):
                break
            print("  Invalid choice. Enter 1 or 2.")

        if choice == "2":
            return dw

        dw["gold_external_db"] = True

        print("\n  Which PostgreSQL deployment strategy do you want to use?")
        print(f"  [1] {_PG_STRATEGY_LOCAL_VM}")
        print(f"  [2] {_PG_STRATEGY_MANAGED_CLOUD}")
        while True:
            strat = input("  Enter choice (1/2): ").strip()
            if strat in ("1", "2"):
                break
            print("  Invalid choice. Enter 1 or 2.")

        if strat == "1":
            self._apply_local_vm_strategy(dw, compute)
        else:
            dw["pg_strategy"] = "managed_cloud"

            print("\n  Provision a new managed cluster or connect to an existing one?")
            print("  [1] Provision a new cluster")
            print("  [2] Connect to an existing cluster")
            while True:
                mc = input("  Enter choice (1/2): ").strip()
                if mc in ("1", "2"):
                    break
                print("  Invalid choice. Enter 1 or 2.")

            if mc == "1":
                self._set_provision_new_dw(dw)
            else:
                host = input("  Enter the PostgreSQL host endpoint: ").strip()
                port = input("  Port [5432]: ").strip()
                username = input("  Username [quickelt]: ").strip()
                password = getpass.getpass("  Password: ")
                self._set_connect_existing_dw(dw, host, port, username, password)

        return dw
