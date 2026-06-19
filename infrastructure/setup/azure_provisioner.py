#!/usr/bin/env python3
"""
Quickelt Azure Provisioner
=========================

Provisions Azure infrastructure resources using the native ``az`` CLI via
subprocess. No Azure SDK dependency — all operations shell out to the
``az`` binary.
"""

import base64
import json
import logging
import os
import re
from typing import Any

from setup.cli_executor import CLIExecutor, ErrorCategory, Spinner
from setup.constants import (
    DEFAULT_AZURE_LOCATION,
    DEFAULT_AZURE_RESOURCE_GROUP,
    DEFAULT_CONTAINER_NAME,
    DEFAULT_VM_NAME,
    DEFAULT_VM_SIZE,
)
from setup.env_writer import EnvWriter
from setup.provisioner import Provisioner


class AzureProvisioner(Provisioner):
    CLOUD_NAME = "Azure"
    _DEFAULT_VM_IMAGE = "Ubuntu2204"
    _DEFAULT_VM_SIZE = DEFAULT_VM_SIZE
    _DEFAULT_VM_NAME = DEFAULT_VM_NAME
    _DEFAULT_CONTAINER_NAME = DEFAULT_CONTAINER_NAME
    _SUBSCRIPTION_ID_RE = re.compile(
        r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    )

    def _ensure_subscription(self, subscription_id: str) -> dict[str, Any]:
        spinner = Spinner(f"Setting Azure subscription '{subscription_id}'...", logger=self.log).start()
        result = self.cli.execute(
            ["az", "account", "set", "--subscription", subscription_id],
            timeout=20,
        )
        if result["ok"]:
            spinner.succeed(f"Azure subscription set to '{subscription_id}'")
            return {"ok": True, "message": "set", "subscription_id": subscription_id}

        return self._handle_cli_error(
            result.get("error_category"),
            spinner,
            result,
            {"subscription_id": subscription_id},
            fail_label="Setting Azure subscription",
            unauthorized_log="Your Azure account lacks permission to use the selected subscription.",
            auth_expired_log="Your Azure authentication token has expired. Run 'az login' to re-authenticate.",
        )

    def _get_subscription_location(self) -> str:
        return self._detect_region(
            ["az", "account", "show", "--output", "json"],
            "AZURE_LOCATION",
            DEFAULT_AZURE_LOCATION,
            json_key="location",
        )

    def _ensure_resource_group(self, resource_group: str, location: str) -> dict[str, Any]:
        spinner = Spinner(f"Checking resource group '{resource_group}'...", logger=self.log).start()

        result = self.cli.execute(
            ["az", "group", "exists", "--name", resource_group, "--output", "json"],
            timeout=15,
        )

        if not result["ok"]:
            category = result.get("error_category")

            if category == ErrorCategory.NOT_FOUND:
                spinner.clear()
                self.log.warning("Resource group '%s' not found. It will be created.", resource_group)
            else:
                base = {"resource_group": resource_group}
                return self._handle_cli_error(
                    category, spinner, result, base,
                    fail_label="Checking resource group",
                    unauthorized_log="Your Azure account lacks permission. Request 'Reader' role on the subscription.",
                    auth_expired_log="Your Azure authentication token has expired. Run 'az login' to re-authenticate.",
                )
        else:
            exists = result["stdout"].strip().lower() == "true"
            if exists:
                spinner.succeed(f"Resource group '{resource_group}' already exists")
                return {"ok": True, "message": "already_exists", "resource_group": resource_group}

        spinner2 = Spinner(f"Creating resource group '{resource_group}' in {location}...", logger=self.log).start()
        create_result = self.cli.execute([
            "az", "group", "create",
            "--name", resource_group,
            "--location", location,
            "--output", "json",
        ], timeout=60)

        if create_result["ok"]:
            spinner2.succeed(f"Resource group '{resource_group}' created in {location}")
            return {"ok": True, "message": "created", "resource_group": resource_group}

        category = create_result.get("error_category")
        base = {"resource_group": resource_group}
        return self._handle_cli_error(
            category, spinner2, create_result, base,
            fail_label="Creating resource group",
            unauthorized_log="Your Azure account lacks permission to create resource groups.",
            auth_expired_log="Your Azure authentication token has expired. Run 'az login' to re-authenticate.",
        )

    def create_azure_lake(
        self,
        account_name: str,
        resource_group: str | None = None,
    ) -> dict[str, Any]:
        if resource_group is None:
            resource_group = DEFAULT_AZURE_RESOURCE_GROUP

        location = self._get_subscription_location()

        rg_result = self._ensure_resource_group(resource_group, location)
        if not rg_result["ok"]:
            return {
                "ok": False, "message": rg_result["message"],
                "account_name": account_name, "resource_group": resource_group,
                "location": location, "primary_endpoint": None, "account_key": None,
            }

        spinner = Spinner(f"Creating storage account '{account_name}' (ADLS Gen2)...", logger=self.log).start()

        result = self.cli.execute([
            "az", "storage", "account", "create",
            "--name", account_name,
            "--resource-group", resource_group,
            "--location", location,
            "--sku", "Standard_LRS",
            "--kind", "StorageV2",
            "--enable-hierarchical-namespace", "true",
            "--output", "json",
        ], timeout=120)

        if result["ok"]:
            spinner.succeed(f"Storage account '{account_name}' created (ADLS Gen2)")
            endpoint, key = self._query_storage_account(account_name, resource_group)
            return {
                "ok": True, "message": "created",
                "account_name": account_name, "resource_group": resource_group,
                "location": location, "primary_endpoint": endpoint, "account_key": key,
            }

        category = result.get("error_category")

        if category == ErrorCategory.ALREADY_EXISTS:
            action, new_name = self._handle_already_exists("Storage account", account_name, spinner)
            if action == "reuse":
                self.log.info("Reusing existing storage account '%s'", account_name)
                endpoint, key = self._query_storage_account(account_name, resource_group)
                return {
                    "ok": True, "message": "already_exists",
                    "account_name": account_name, "resource_group": resource_group,
                    "location": location, "primary_endpoint": endpoint, "account_key": key,
                }
            if action == "retry" and new_name:
                return self.create_azure_lake(new_name, resource_group)
            return {
                "ok": False, "message": "cancelled",
                "account_name": account_name, "resource_group": resource_group,
                "location": location, "primary_endpoint": None, "account_key": None,
            }

        if category == ErrorCategory.INVALID_NAME:
            spinner.fail(f"Invalid storage account name: '{account_name}'")
            self.log.error("%s", result["remedy"])
            return {
                "ok": False, "message": result["stderr"],
                "account_name": account_name, "resource_group": resource_group,
                "location": location, "primary_endpoint": None, "account_key": None,
            }

        base = {
            "account_name": account_name, "resource_group": resource_group,
            "location": location, "primary_endpoint": None, "account_key": None,
        }
        return self._handle_cli_error(
            category, spinner, result, base,
            fail_label=f"Creating storage account '{account_name}'",
            unauthorized_log="Your Azure account lacks permission to create storage accounts.",
            auth_expired_log="Your Azure authentication token has expired. Run 'az login' to re-authenticate.",
        )

    def _query_storage_account(self, account_name: str, resource_group: str) -> tuple[str | None, str | None]:
        self.log.debug("Querying storage account metadata for '%s'", account_name)

        show_result = self.cli.execute([
            "az", "storage", "account", "show",
            "--name", account_name,
            "--resource-group", resource_group,
            "--output", "json",
        ], timeout=30)

        endpoint = None
        if show_result["ok"]:
            data = self._parse_json(show_result["stdout"])
            if data:
                endpoints = data.get("primaryEndpoints", {})
                dfs = endpoints.get("dfs", None) if isinstance(endpoints, dict) else None
                blob = endpoints.get("blob", None) if isinstance(endpoints, dict) else None
                endpoint = dfs or blob
                self.log.debug("Storage endpoint: %s", endpoint)

        key_result = self.cli.execute([
            "az", "storage", "account", "keys", "list",
            "--account-name", account_name,
            "--resource-group", resource_group,
            "--query", "[0].value",
            "--output", "json",
        ], timeout=30)

        key = None
        if key_result["ok"] and key_result["stdout"]:
            key = key_result["stdout"].strip().strip('"')

        return endpoint, key

    def structure_azure_layers(
        self,
        account_name: str,
        layers_list: list[str],
    ) -> dict[str, Any]:
        container_name = self._DEFAULT_CONTAINER_NAME

        spinner = Spinner(f"Creating container '{container_name}' in '{account_name}'...", logger=self.log).start()
        result = self.cli.execute([
            "az", "storage", "container", "create",
            "--name", container_name,
            "--account-name", account_name,
            "--output", "json",
        ], timeout=30)

        if result["ok"]:
            spinner.succeed(f"Container '{container_name}' created")
        else:
            stderr_lower = (result["stderr"] or "").lower()
            if "already exists" in stderr_lower:
                spinner.succeed(f"Container '{container_name}' already exists")
            else:
                category = result.get("error_category")
                base = {"container": container_name, "created": [], "failed": layers_list}
                self._handle_cli_error(
                    category, spinner, result, base,
                    fail_label="Container creation",
                    unauthorized_log="Your Azure account lacks 'Storage Blob Data Contributor' role.",
                )
                return {"ok": False, "container": container_name, "created": [], "failed": layers_list}

        created: list[str] = []
        failed: list[str] = []

        for layer in layers_list:
            directory = f"{layer}/"
            spinner = Spinner(f"Creating directory '{directory}' in '{container_name}'...", logger=self.log).start()

            dir_result = self.cli.execute([
                "az", "storage", "fs", "directory", "create",
                "--name", directory,
                "--filesystem", container_name,
                "--account-name", account_name,
                "--output", "json",
            ], timeout=30)

            if dir_result["ok"]:
                spinner.succeed(f"Directory '{directory}' created")
                created.append(layer)
            else:
                stderr_lower = (dir_result["stderr"] or "").lower()
                if "already exists" in stderr_lower:
                    spinner.succeed(f"Directory '{directory}' already exists")
                    created.append(layer)
                else:
                    self.log.debug("HNS directory create failed, falling back to blob upload for '%s'", directory)
                    fallback_result = self.cli.execute([
                        "az", "storage", "blob", "upload",
                        "--container-name", container_name,
                        "--account-name", account_name,
                        "--name", f"{layer}/.keep",
                        "--data", "",
                        "--overwrite",
                        "--output", "json",
                    ], timeout=30)

                    if fallback_result["ok"]:
                        spinner.succeed(f"Directory '{directory}' created (via blob upload)")
                        created.append(layer)
                    else:
                        category = fallback_result.get("error_category")
                        if category == ErrorCategory.UNAUTHORIZED:
                            spinner.fail(f"Directory '{directory}' failed: permission denied")
                            self.log.error("Lacks 'Storage Blob Data Contributor' role for container '%s'.", container_name)
                        else:
                            spinner.fail(f"Directory '{directory}' failed: {fallback_result['stderr'][:120]}")
                        failed.append(layer)

        overall_ok = len(failed) == 0
        if overall_ok:
            self.log.info("All %d layer(s) created successfully.", len(created))
        else:
            self.log.warning("%d succeeded, %d failed: %s", len(created), len(failed), ", ".join(failed))

        return {"ok": overall_ok, "container": container_name, "created": created, "failed": failed}

    def provision_compute_vm(
        self,
        resource_group: str,
        bootstrap: bool = True,
        install_local_postgres: bool = False,
        dw_password: str = "",
    ) -> dict[str, Any]:
        spinner = Spinner(f"Launching VM '{self._DEFAULT_VM_NAME}' ({self._DEFAULT_VM_SIZE})...", logger=self.log).start()

        result = self.cli.execute([
            "az", "vm", "create",
            "--resource-group", resource_group,
            "--name", self._DEFAULT_VM_NAME,
            "--image", self._DEFAULT_VM_IMAGE,
            "--size", self._DEFAULT_VM_SIZE,
            "--admin-username", "quickelt",
            "--generate-ssh-keys",
            "--tags", "ManagedBy=quickelt-setup",
            "--output", "json",
        ], timeout=300)

        if not result["ok"]:
            category = result.get("error_category")

            if category == ErrorCategory.NOT_FOUND:
                spinner.fail("VM creation failed: resource not found")
                self.log.error("A required resource was not found. Check the resource group and image name.")
                self.log.error("%s", result["remedy"])
                return {
                    "ok": False, "message": result["stderr"],
                    "vm_name": self._DEFAULT_VM_NAME, "resource_group": resource_group,
                    "vm_id": None, "public_ip": None, "private_ip": None,
                }

            base = {
                "vm_name": self._DEFAULT_VM_NAME, "resource_group": resource_group,
                "vm_id": None, "public_ip": None, "private_ip": None,
            }
            return self._handle_cli_error(
                category, spinner, result, base,
                fail_label="VM creation",
                unauthorized_log="Your Azure account lacks permission to create virtual machines.",
                auth_expired_log="Your Azure authentication token has expired. Run 'az login' to re-authenticate.",
            )

        vm_id = None
        public_ip = None
        private_ip = None

        data = self._parse_json(result["stdout"])
        if data:
            vm_id = data.get("vmId") or data.get("id", "").split("/")[-1] if data.get("id") else None
            public_ips = data.get("publicIps", "")
            if isinstance(public_ips, str) and public_ips:
                public_ip = public_ips
            elif isinstance(public_ips, list) and public_ips:
                public_ip = public_ips[0]

            private_ips = data.get("privateIps", "")
            if isinstance(private_ips, str) and private_ips:
                private_ip = private_ips
            elif isinstance(private_ips, list) and private_ips:
                private_ip = private_ips[0]

        if public_ip is None and vm_id is not None:
            spinner2 = Spinner("Querying public IP address...", logger=self.log).start()
            ip_result = self.cli.execute([
                "az", "vm", "list-ip-addresses",
                "--resource-group", resource_group,
                "--name", self._DEFAULT_VM_NAME,
                "--query", "[0].virtualMachine.network.publicIpAddresses[0].ipAddress",
                "--output", "json",
            ], timeout=15)
            if ip_result["ok"] and ip_result["stdout"]:
                public_ip = ip_result["stdout"].strip().strip('"')
            spinner2.succeed(f"Public IP: {public_ip or 'N/A'}")

        label = "with bootstrap"
        if install_local_postgres:
            label = "with bootstrap + local PostgreSQL"
        elif not bootstrap:
            label = "without bootstrap"
        spinner.succeed(f"VM '{self._DEFAULT_VM_NAME}' provisioned ({label})")

        if bootstrap or install_local_postgres:
            if install_local_postgres:
                ext_label = "bootstrap + local PostgreSQL"
                script = self._get_local_postgres_script(dw_password or "")
            else:
                ext_label = "bootstrap (python3-pip, git)"
                script = self.BOOTSTRAP_SCRIPT

            ext_spinner = Spinner(f"Applying {ext_label} extension...", logger=self.log).start()
            encoded = base64.b64encode(script.encode()).decode()

            ext_result = self.cli.execute([
                "az", "vm", "extension", "set",
                "--resource-group", resource_group,
                "--vm-name", self._DEFAULT_VM_NAME,
                "--name", "quickelt-bootstrap",
                "--publisher", "Microsoft.Azure.Extensions",
                "--extension-type", "CustomScript",
                "--type-handler-version", "2.1",
                "--settings", json.dumps({"script": encoded}),
                "--output", "json",
            ], timeout=180)

            if ext_result["ok"]:
                ext_spinner.succeed("Bootstrap extension applied successfully")
            else:
                stderr_lower = (ext_result["stderr"] or "").lower()
                if "already exists" in stderr_lower:
                    ext_spinner.succeed("Bootstrap extension already exists")
                else:
                    category = ext_result.get("error_category")
                    if category == ErrorCategory.UNAUTHORIZED:
                        ext_spinner.fail("Bootstrap extension failed: permission denied")
                        self.log.error("Lacks permission to set VM extensions. Request 'Virtual Machine Contributor' role.")
                    else:
                        ext_spinner.fail(f"Bootstrap extension failed: {ext_result['stderr'][:150]}")

        self.log.debug("VM provisioned: id=%s, public_ip=%s, private_ip=%s", vm_id, public_ip, private_ip)

        return {
            "ok": True, "message": "launched",
            "vm_name": self._DEFAULT_VM_NAME, "resource_group": resource_group,
            "vm_id": vm_id, "public_ip": public_ip, "private_ip": private_ip,
        }

    def provision_azure_postgres(
        self,
        resource_group: str,
        server_name: str = "quickelt-pg-server",
        admin_username: str = "quickelt",
        admin_password: str = "",
        location: str | None = None,
    ) -> dict[str, Any]:
        if not admin_password:
            self.log.error("An admin password is required for Azure DB for PostgreSQL creation.")
            return {
                "ok": False, "message": "missing_password",
                "server_name": server_name, "resource_group": resource_group,
                "location": location or "", "fqdn": None, "port": None,
            }

        if location is None:
            location = self._get_subscription_location()

        spinner = Spinner(
            f"Creating Azure DB for PostgreSQL '{server_name}' in {location}...",
            logger=self.log,
        ).start()

        result = self.cli.execute([
            "az", "postgres", "flexible-server", "create",
            "--name", server_name,
            "--resource-group", resource_group,
            "--location", location,
            "--admin-user", admin_username,
            "--admin-password", admin_password,
            "--sku-name", "Standard_B1ms",
            "--tier", "Burstable",
            "--version", "15",
            "--yes",
            "--output", "json",
        ], timeout=600)

        if not result["ok"]:
            category = result.get("error_category")

            if category == ErrorCategory.ALREADY_EXISTS:
                action, new_name = self._handle_already_exists("PostgreSQL server", server_name, spinner)
                if action == "reuse":
                    return self._describe_azure_postgres(server_name, resource_group)
                if action == "retry" and new_name:
                    return self.provision_azure_postgres(resource_group, new_name, admin_username, admin_password, location)
                return {
                    "ok": False, "message": "cancelled",
                    "server_name": server_name, "resource_group": resource_group,
                    "location": location, "fqdn": None, "port": None,
                }

            base = {
                "server_name": server_name, "resource_group": resource_group,
                "location": location, "fqdn": None, "port": None,
            }
            return self._handle_cli_error(
                category, spinner, result, base,
                fail_label="PostgreSQL server creation",
                unauthorized_log="Your Azure account lacks permission to create PostgreSQL servers.",
                auth_expired_log="Your Azure authentication token has expired. Run 'az login' to re-authenticate.",
            )

        fqdn = None
        port = 5432
        data = self._parse_json(result["stdout"])
        if data:
            fqdn = data.get("fullyQualifiedDomainName") or data.get("fqdn")
            port = data.get("port", 5432)

        if fqdn is None:
            fqdn = self._describe_azure_postgres(server_name, resource_group).get("fqdn")

        spinner.succeed(f"Azure DB for PostgreSQL '{server_name}' created (FQDN: {fqdn or 'pending'})")

        return {
            "ok": True, "message": "created",
            "server_name": server_name, "resource_group": resource_group,
            "location": location, "fqdn": fqdn, "port": port,
        }

    def _describe_azure_postgres(self, server_name: str, resource_group: str) -> dict[str, Any]:
        result = self.cli.execute([
            "az", "postgres", "flexible-server", "show",
            "--name", server_name,
            "--resource-group", resource_group,
            "--output", "json",
        ], timeout=30)

        if result["ok"]:
            data = self._parse_json(result["stdout"])
            if data:
                fqdn = data.get("fullyQualifiedDomainName") or data.get("fqdn")
                port = data.get("port", 5432)
                self.log.info("Reusing existing PostgreSQL server: fqdn=%s, port=%s", fqdn, port)
                return {
                    "ok": True, "message": "already_exists",
                    "server_name": server_name, "resource_group": resource_group,
                    "location": data.get("location", ""), "fqdn": fqdn, "port": port,
                }

        self.log.warning("Could not describe existing PostgreSQL server '%s'", server_name)
        return {
            "ok": False, "message": "describe_failed",
            "server_name": server_name, "resource_group": resource_group,
            "location": "", "fqdn": None, "port": None,
        }

    def provision(self, storage: dict, compute: dict, dw: dict) -> dict[str, Any]:
        self._reset_retry_state()
        self._print_provision_banner()

        results: dict[str, Any] = {}

        subscription_id = (self.env.read_value("AZURE_SUBSCRIPTION_ID") or "").strip()
        if subscription_id and self._SUBSCRIPTION_ID_RE.match(subscription_id):
            sub_result = self._ensure_subscription(subscription_id)
            if not sub_result.get("ok"):
                detail = sub_result.get("message", "unknown error")
                self.log.error("Azure subscription selection failed: %s", detail)
                return {"ok": False, "message": "subscription_failed", "detail": detail}

        resource_group = self.env.read_value("AZURE_RESOURCE_GROUP") or DEFAULT_AZURE_RESOURCE_GROUP
        account_name = storage["name"]
        self.log.debug("Resource group: %s, Storage account: %s", resource_group, account_name)
        if storage["existing"]:
            location = self._get_subscription_location()
            rg_result = self._ensure_resource_group(resource_group, location)
            if not rg_result["ok"]:
                return {
                    "ok": False,
                    "message": rg_result["message"],
                    "resource_group": resource_group,
                }

        if not storage["existing"]:
            lake_result = self.create_azure_lake(account_name, resource_group)
            results["lake"] = lake_result

            if lake_result.get("ok") and lake_result.get("resource_group"):
                resource_group = lake_result["resource_group"]

            if lake_result.get("ok") and lake_result.get("message") == "already_exists":
                results["lake"]["reused"] = True

            self._provision_layers(storage, results, self.structure_azure_layers, container="")
        else:
            self.log.info("Using existing storage account: '%s' — skipping creation.", account_name)
            endpoint, key = self._query_storage_account(account_name, resource_group)
            results["lake"] = {
                "ok": True, "message": "existing",
                "account_name": account_name, "resource_group": resource_group,
                "location": location,
                "primary_endpoint": endpoint, "account_key": key,
            }

            self._provision_layers(storage, results, self.structure_azure_layers, container="")

        env_metadata: dict[str, str] = {}
        lake = results.get("lake", {})
        if lake.get("ok"):
            if lake.get("resource_group"):
                env_metadata["AZURE_RESOURCE_GROUP"] = lake["resource_group"]
            if lake.get("location"):
                env_metadata["AZURE_LOCATION"] = lake["location"]
            if lake.get("primary_endpoint"):
                env_metadata["AZURE_STORAGE_DFS_ENDPOINT"] = lake["primary_endpoint"]
            if lake.get("account_key"):
                env_metadata["AZURE_STORAGE_KEY"] = lake["account_key"]
            env_metadata["AZURE_STORAGE_ACCOUNT"] = account_name

        layers_result = results.get("layers", {})
        if layers_result.get("container"):
            env_metadata["AZURE_STORAGE_CONTAINER"] = layers_result["container"]

        if compute["compute"] == "Dedicated VM":
            vm_result = self.provision_compute_vm(
                resource_group=resource_group,
                bootstrap=compute.get("bootstrap_vm", False),
                install_local_postgres=dw.get("install_local_postgres", False),
                dw_password=dw.get("dw_password", ""),
            )
            results["vm"] = vm_result

            if vm_result.get("ok"):
                if vm_result.get("vm_id"):
                    env_metadata["AZURE_VM_ID"] = vm_result["vm_id"]
                if vm_result.get("public_ip"):
                    env_metadata["AZURE_VM_PUBLIC_IP"] = vm_result["public_ip"]
                if vm_result.get("private_ip"):
                    env_metadata["AZURE_VM_PRIVATE_IP"] = vm_result["private_ip"]
        else:
            self.log.info("Compute type: %s — skipping VM provisioning.", compute["compute"])
            results["vm"] = {"ok": True, "message": "skipped"}

        if dw.get("gold_external_db") and dw.get("pg_strategy") == "managed_cloud":
            if dw.get("managed_cloud_choice") == "provision_new":
                pg_result = self.provision_azure_postgres(
                    resource_group=resource_group,
                    admin_username=dw.get("dw_username", "quickelt"),
                    admin_password=dw.get("dw_password", ""),
                    location=results.get("lake", {}).get("location") or self.env.read_value("AZURE_LOCATION"),
                )
                results["postgres"] = pg_result

                if pg_result.get("ok") and pg_result.get("fqdn"):
                    env_metadata["DW_HOST"] = pg_result["fqdn"]
                    env_metadata["DW_PORT"] = str(pg_result.get("port", 5432))
            else:
                self.log.info("Connecting to existing managed PostgreSQL server — skipping provisioning.")

        if env_metadata:
            print()
            self.env.update_metadata(env_metadata)
            self.log.info(".env updated with Azure metadata (%d keys)", len(env_metadata))

        all_ok = (
            results.get("lake", {}).get("ok", False)
            and results.get("layers", {}).get("ok", False)
            and results.get("vm", {}).get("ok", False)
        )

        if dw.get("gold_external_db") and dw.get("pg_strategy") == "managed_cloud":
            all_ok = all_ok and results.get("postgres", {}).get("ok", False)

        print()
        if all_ok:
            self.log.info("Azure provisioning completed successfully.")
        else:
            self.log.warning("Azure provisioning completed with errors. Review output above.")
        print()

        results["ok"] = all_ok
        return results
