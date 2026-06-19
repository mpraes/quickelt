#!/usr/bin/env python3
"""
Quickelt AWS Provisioner
=======================

Provisions AWS infrastructure resources using the native AWS CLI via subprocess.
No boto3 dependency — all operations shell out to the ``aws`` binary.
"""

import base64
import json
import os
from typing import Any

from setup.cli_executor import CLIExecutor, ErrorCategory, Spinner
from setup.env_writer import EnvWriter
from setup.provisioner import Provisioner


class AWSProvisioner(Provisioner):
    CLOUD_NAME = "AWS"
    _DEFAULT_AMI = "ami-0c02fb55956c7d316"
    _DEFAULT_INSTANCE_TYPE = "t3.medium"

    def _get_region(self) -> str:
        return self._detect_region(
            ["aws", "configure", "get", "region"],
            "AWS_REGION",
            "us-east-1",
        )

    def create_s3_lake(self, bucket_name: str, region: str | None = None) -> dict[str, Any]:
        if region is None:
            region = self._get_region()

        spinner = Spinner(f"Creating S3 bucket '{bucket_name}' in {region}...", logger=self.log).start()

        cmd = [
            "aws", "s3api", "create-bucket",
            "--bucket", bucket_name,
            "--region", region,
        ]

        if region != "us-east-1":
            constraint = {"LocationConstraint": region}
            cmd += ["--create-bucket-configuration", json.dumps(constraint)]

        result = self.cli.execute(cmd)

        if result["ok"]:
            spinner.succeed(f"Bucket '{bucket_name}' created in {region}")
            return {"ok": True, "message": "created", "bucket": bucket_name, "region": region}

        category = result.get("error_category")

        if category == ErrorCategory.ALREADY_EXISTS:
            action, new_name = self._handle_already_exists("S3 bucket", bucket_name, spinner)
            if action == "reuse":
                self.log.info("Reusing existing bucket '%s'", bucket_name)
                return {"ok": True, "message": "already_exists", "bucket": bucket_name, "region": region}
            if action == "retry" and new_name:
                return self.create_s3_lake(new_name, region)
            return {"ok": False, "message": "cancelled", "bucket": bucket_name, "region": region}

        if category == ErrorCategory.INVALID_NAME:
            spinner.fail(f"Invalid bucket name: '{bucket_name}'")
            self.log.error("%s", result["remedy"])
            return {"ok": False, "message": result["stderr"], "bucket": bucket_name, "region": region}

        return self._handle_cli_error(
            category, spinner, result,
            {"bucket": bucket_name, "region": region},
            fail_label=f"Creating bucket '{bucket_name}'",
            unauthorized_log="Your AWS IAM User lacks permission to create S3 buckets.",
            auth_expired_log="Your AWS session has expired. Run 'aws configure' or 'aws sts get-session-token'.",
        )

    def structure_lake_layers(self, bucket_name: str, layers_list: list[str]) -> dict[str, Any]:
        created: list[str] = []
        failed: list[str] = []

        for layer in layers_list:
            key = f"{layer}/"
            spinner = Spinner(f"Creating layer '{key}' in '{bucket_name}'...", logger=self.log).start()

            result = self.cli.execute([
                "aws", "s3api", "put-object",
                "--bucket", bucket_name,
                "--key", key,
            ])

            if result["ok"]:
                spinner.succeed(f"Layer '{key}' created")
                created.append(layer)
            else:
                category = result.get("error_category")
                if category == ErrorCategory.UNAUTHORIZED:
                    spinner.fail(f"Layer '{key}' failed: IAM permission missing")
                    self.log.error("Your AWS IAM User lacks permission for 's3:PutObject' on bucket '%s'.", bucket_name)
                else:
                    spinner.fail(f"Layer '{key}' failed: {result['stderr'][:120]}")
                failed.append(layer)

        overall_ok = len(failed) == 0
        if overall_ok:
            self.log.info("All %d layer(s) created successfully.", len(created))
        else:
            self.log.warning("%d succeeded, %d failed: %s", len(created), len(failed), ", ".join(failed))

        return {"ok": overall_ok, "created": created, "failed": failed}

    def provision_compute_vm(
        self, bootstrap: bool = True, install_local_postgres: bool = False, dw_password: str = ""
    ) -> dict[str, Any]:
        spinner = Spinner("Launching EC2 instance (t3.medium, Ubuntu)...", logger=self.log).start()

        cmd = [
            "aws", "ec2", "run-instances",
            "--image-id", self._DEFAULT_AMI,
            "--instance-type", self._DEFAULT_INSTANCE_TYPE,
            "--tag-specifications",
            "ResourceType=instance,Tags=[{Key=Name,Value=quickelt-vm},{Key=ManagedBy,Value=quickelt-setup}]",
        ]

        if install_local_postgres:
            script = self._get_local_postgres_script(dw_password or "")
            encoded = base64.b64encode(script.encode()).decode()
            cmd += ["--user-data", encoded]
        elif bootstrap:
            encoded = base64.b64encode(self.BOOTSTRAP_SCRIPT.encode()).decode()
            cmd += ["--user-data", encoded]

        result = self.cli.execute(cmd, timeout=120)

        if not result["ok"]:
            category = result.get("error_category")

            if category == ErrorCategory.INVALID_NAME:
                spinner.fail(f"EC2 launch failed: invalid parameter")
                self.log.error("%s", result["remedy"])
                return {"ok": False, "message": result["stderr"], "instance_id": None}

            return self._handle_cli_error(
                category, spinner, result,
                {"instance_id": None},
                fail_label="EC2 launch",
                unauthorized_log="Your AWS IAM User lacks permission to run EC2 instances.",
                auth_expired_log="Your AWS session has expired. Re-authenticate and try again.",
            )

        instance_id = None
        try:
            data = json.loads(result["stdout"])
            instances = data.get("Instances", [])
            if instances:
                instance_id = instances[0].get("InstanceId")
        except (json.JSONDecodeError, KeyError, IndexError):
            self.log.debug("Could not parse InstanceId from CLI output")

        if instance_id:
            if install_local_postgres:
                label = "with bootstrap + local PostgreSQL"
            elif bootstrap:
                label = "with bootstrap"
            else:
                label = "without bootstrap"
            spinner.succeed(f"Instance {instance_id} launched ({label})")
        else:
            spinner.succeed("Instance launched (could not parse InstanceId)")

        return {"ok": True, "message": "launched", "instance_id": instance_id}

    def provision_aurora_postgres(
        self,
        cluster_identifier: str = "quickelt-aurora-cluster",
        master_username: str = "quickelt",
        master_password: str = "",
        region: str | None = None,
    ) -> dict[str, Any]:
        if not master_password:
            self.log.error("A master password is required for Aurora cluster creation.")
            return {
                "ok": False, "message": "missing_password",
                "cluster_id": None, "endpoint": None, "port": None, "region": region or "",
            }

        if region is None:
            region = self._get_region()

        spinner = Spinner(f"Creating Aurora PostgreSQL cluster '{cluster_identifier}' in {region}...", logger=self.log).start()

        cmd = [
            "aws", "rds", "create-db-cluster",
            "--db-cluster-identifier", cluster_identifier,
            "--engine", "aurora-postgresql",
            "--engine-version", "15.4",
            "--master-username", master_username,
            "--master-user-password", master_password,
            "--region", region,
            "--output", "json",
        ]

        result = self.cli.execute(cmd, timeout=180)

        if not result["ok"]:
            category = result.get("error_category")

            if category == ErrorCategory.ALREADY_EXISTS:
                action, new_name = self._handle_already_exists("Aurora cluster", cluster_identifier, spinner)
                if action == "reuse":
                    return self._describe_aurora_cluster(cluster_identifier, region)
                if action == "retry" and new_name:
                    return self.provision_aurora_postgres(new_name, master_username, master_password, region)
                return {
                    "ok": False, "message": "cancelled",
                    "cluster_id": cluster_identifier, "endpoint": None, "port": None, "region": region,
                }

            base = {"cluster_id": None, "endpoint": None, "port": None, "region": region}
            return self._handle_cli_error(
                category, spinner, result, base,
                fail_label="Aurora creation",
                unauthorized_log="Your AWS IAM User lacks permission to create RDS clusters.",
                auth_expired_log="Your AWS session has expired. Re-authenticate and try again.",
            )

        cluster_data = None
        try:
            data = json.loads(result["stdout"])
            cluster_data = data.get("DBCluster", data)
        except (json.JSONDecodeError, KeyError):
            self.log.debug("Could not parse Aurora cluster details from CLI output")

        endpoint = None
        port = None
        if cluster_data:
            endpoint = cluster_data.get("Endpoint")
            port = cluster_data.get("Port", 5432)

        spinner2 = Spinner(f"Creating primary instance for cluster '{cluster_identifier}'...", logger=self.log).start()

        inst_result = self.cli.execute([
            "aws", "rds", "create-db-instance",
            "--db-instance-identifier", f"{cluster_identifier}-primary",
            "--db-cluster-identifier", cluster_identifier,
            "--engine", "aurora-postgresql",
            "--db-instance-class", "db.r5.large",
            "--region", region,
            "--output", "json",
        ], timeout=120)

        if inst_result["ok"]:
            spinner2.succeed(f"Primary instance created for '{cluster_identifier}'")
        else:
            stderr_lower = (inst_result["stderr"] or "").lower()
            if "already exists" in stderr_lower:
                spinner2.succeed(f"Primary instance already exists for '{cluster_identifier}'")
            else:
                spinner2.fail(f"Primary instance creation failed: {inst_result['stderr'][:150]}")
                self.log.warning("Cluster created but primary instance failed. You may need to create it manually.")

        spinner.succeed(f"Aurora cluster '{cluster_identifier}' created")

        return {
            "ok": True, "message": "created",
            "cluster_id": cluster_identifier, "endpoint": endpoint, "port": port, "region": region,
        }

    def _describe_aurora_cluster(self, cluster_identifier: str, region: str) -> dict[str, Any]:
        result = self.cli.execute([
            "aws", "rds", "describe-db-clusters",
            "--db-cluster-identifier", cluster_identifier,
            "--region", region,
            "--output", "json",
        ], timeout=30)

        if result["ok"]:
            data = json.loads(result["stdout"])
            clusters = data.get("DBClusters", [])
            if clusters:
                cluster = clusters[0]
                endpoint = cluster.get("Endpoint")
                port = cluster.get("Port", 5432)
                self.log.info("Reusing existing Aurora cluster: endpoint=%s, port=%s", endpoint, port)
                return {
                    "ok": True, "message": "already_exists",
                    "cluster_id": cluster_identifier, "endpoint": endpoint, "port": port, "region": region,
                }

        self.log.warning("Could not describe existing Aurora cluster '%s'", cluster_identifier)
        return {
            "ok": False, "message": "describe_failed",
            "cluster_id": cluster_identifier, "endpoint": None, "port": None, "region": region,
        }

    def provision(self, storage: dict, compute: dict, dw: dict) -> dict[str, Any]:
        self._reset_retry_state()
        self._print_provision_banner()

        results: dict[str, Any] = {}
        region = self.env.read_value("AWS_REGION") or self._get_region()
        results["region"] = region
        self.log.debug("Using region: %s", region)

        bucket_name = storage["name"]

        if not storage["existing"]:
            bucket_result = self.create_s3_lake(bucket_name, region)
            results["bucket"] = bucket_result

            if bucket_result.get("ok") and bucket_result.get("message") == "already_exists":
                results["bucket"]["reused"] = True

            self._provision_layers(storage, results, self.structure_lake_layers)
        else:
            self.log.info("Using existing bucket: '%s' — skipping bucket creation.", bucket_name)
            results["bucket"] = {"ok": True, "message": "existing", "bucket": bucket_name, "region": region}

            self._provision_layers(storage, results, self.structure_lake_layers)

        if compute["compute"] == "Dedicated VM":
            results["vm"] = self.provision_compute_vm(
                bootstrap=compute.get("bootstrap_vm", False),
                install_local_postgres=dw.get("install_local_postgres", False),
                dw_password=dw.get("dw_password", ""),
            )
        else:
            self.log.info("Compute type: %s — skipping VM provisioning.", compute["compute"])
            results["vm"] = {"ok": True, "message": "skipped", "instance_id": None}

        if dw.get("gold_external_db") and dw.get("pg_strategy") == "managed_cloud":
            if dw.get("managed_cloud_choice") == "provision_new":
                aurora_result = self.provision_aurora_postgres(
                    master_username=dw.get("dw_username", "quickelt"),
                    master_password=dw.get("dw_password", ""),
                    region=region,
                )
                results["aurora"] = aurora_result

                if aurora_result.get("ok") and aurora_result.get("endpoint"):
                    self.env.update_metadata({
                        "DW_HOST": aurora_result["endpoint"],
                        "DW_PORT": str(aurora_result.get("port", 5432)),
                    })
            else:
                self.log.info("Connecting to existing managed PostgreSQL cluster — skipping provisioning.")

        all_ok = (
            results.get("bucket", {}).get("ok", False)
            and results.get("layers", {}).get("ok", False)
            and results.get("vm", {}).get("ok", False)
        )

        if dw.get("gold_external_db") and dw.get("pg_strategy") == "managed_cloud":
            all_ok = all_ok and results.get("aurora", {}).get("ok", False)

        print()
        if all_ok:
            self.log.info("AWS provisioning completed successfully.")
        else:
            self.log.warning("AWS provisioning completed with errors. Review output above.")
        print()

        results["ok"] = all_ok
        return results
