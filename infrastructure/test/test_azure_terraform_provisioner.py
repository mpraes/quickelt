import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from setup.azure_terraform_provisioner import AzureTerraformProvisioner
from setup.terraform_executor import TerraformExecutor

_TEST_LOGGER = logging.getLogger("test.azure_terraform_provisioner")


class TestOutputsToEnv:
    def test_maps_all_outputs(self):
        outputs = {
            "resource_group_name": "quickelt-rg",
            "location": "eastus",
            "storage_account_name": "quickeltdatalake",
            "storage_container_name": "quickelt-data",
            "storage_dfs_endpoint": "https://quickeltdatalake.dfs.core.windows.net/",
            "storage_account_key": "secret-key",
            "vm_id": "/subscriptions/.../quickelt-vm",
            "vm_public_ip": "20.0.0.1",
            "vm_private_ip": "10.0.1.4",
            "postgres_fqdn": "quickelt-pg-server.postgres.database.azure.com",
            "postgres_port": 5432,
        }

        metadata = AzureTerraformProvisioner._outputs_to_env(outputs, "fallback-name")

        assert metadata["AZURE_RESOURCE_GROUP"] == "quickelt-rg"
        assert metadata["AZURE_LOCATION"] == "eastus"
        assert metadata["AZURE_STORAGE_ACCOUNT"] == "quickeltdatalake"
        assert metadata["AZURE_STORAGE_CONTAINER"] == "quickelt-data"
        assert metadata["AZURE_STORAGE_DFS_ENDPOINT"].startswith("https://")
        assert metadata["AZURE_STORAGE_KEY"] == "secret-key"
        assert metadata["AZURE_VM_ID"].endswith("quickelt-vm")
        assert metadata["AZURE_VM_PUBLIC_IP"] == "20.0.0.1"
        assert metadata["AZURE_VM_PRIVATE_IP"] == "10.0.1.4"
        assert metadata["DW_HOST"] == "quickelt-pg-server.postgres.database.azure.com"
        assert metadata["DW_PORT"] == "5432"

    def test_uses_storage_name_fallback(self):
        metadata = AzureTerraformProvisioner._outputs_to_env({}, "my-storage")
        assert metadata["AZURE_STORAGE_ACCOUNT"] == "my-storage"


@pytest.fixture
def terraform_provisioner(mock_cli, mock_env_writer):
    terraform = MagicMock()
    return AzureTerraformProvisioner(mock_cli, mock_env_writer, logger=_TEST_LOGGER, terraform=terraform)


class TestSetupWorkspace:
    def test_setup_name_uses_named_workspace(self, tmp_path, mock_cli, mock_env_writer, monkeypatch):
        monkeypatch.setattr("setup.setup_registry.SETUPS_ROOT", tmp_path / "setups")
        provisioner = AzureTerraformProvisioner(
            mock_cli,
            mock_env_writer,
            logger=_TEST_LOGGER,
            setup_name="acme-dev",
        )
        assert provisioner.terraform.workspace_dir == tmp_path / "setups" / "acme-dev" / "terraform"


class TestProvision:
  def test_success_updates_env(
      self,
      terraform_provisioner,
      mock_env_writer,
      sample_storage_new,
      sample_compute_local,
      sample_dw_empty,
  ):
      terraform_provisioner.terraform.provision.return_value = {
          "ok": True,
          "message": "applied",
          "outputs": {
              "resource_group_name": "quickelt-rg",
              "location": "eastus",
              "storage_account_name": "new-bucket",
          },
      }
      mock_env_writer.read_value.return_value = None
      terraform_provisioner.cli.execute.return_value = {
          "ok": True,
          "stdout": json.dumps({"location": "eastus"}),
      }

      result = terraform_provisioner.provision(sample_storage_new, sample_compute_local, sample_dw_empty)

      assert result["ok"] is True
      mock_env_writer.update_metadata.assert_called_once()
      metadata = mock_env_writer.update_metadata.call_args[0][0]
      assert metadata["AZURE_RESOURCE_GROUP"] == "quickelt-rg"
      assert metadata["AZURE_STORAGE_ACCOUNT"] == "new-bucket"

  def test_failure_returns_error(
      self,
      terraform_provisioner,
      sample_storage_new,
      sample_compute_local,
      sample_dw_empty,
  ):
      terraform_provisioner.terraform.provision.return_value = {
          "ok": False,
          "message": "terraform_plan_failed",
          "detail": "plan error",
      }
      terraform_provisioner.env.read_value.return_value = "quickelt-rg"

      result = terraform_provisioner.provision(sample_storage_new, sample_compute_local, sample_dw_empty)

      assert result["ok"] is False
      assert result["message"] == "terraform_plan_failed"

  def test_connect_existing_skips_postgres_log_only(
      self,
      terraform_provisioner,
      sample_storage_existing,
      sample_compute_local,
      sample_dw_managed_provision,
      caplog,
  ):
      sample_dw_managed_provision["managed_cloud_choice"] = "connect_existing"
      sample_dw_managed_provision["dw_host"] = "existing.postgres.database.azure.com"
      terraform_provisioner.terraform.provision.return_value = {
          "ok": True,
          "message": "applied",
          "outputs": {"storage_account_name": "my-bucket"},
      }
      terraform_provisioner.env.read_value.return_value = "quickelt-rg"

      with caplog.at_level(logging.INFO):
          result = terraform_provisioner.provision(
              sample_storage_existing,
              sample_compute_local,
              sample_dw_managed_provision,
          )

      assert result["ok"] is True
      assert any("existing managed PostgreSQL" in record.message for record in caplog.records)


class TestDestroy:
    def test_destroy_success(self, terraform_provisioner, mock_env_writer, sample_storage_new, sample_compute_local, sample_dw_empty):
        terraform_provisioner.terraform.destroy.return_value = {"ok": True, "message": "destroyed"}
        mock_env_writer.read_value.return_value = "quickelt-rg"

        result = terraform_provisioner.destroy(sample_storage_new, sample_compute_local, sample_dw_empty)

        assert result["ok"] is True
        assert result["message"] == "destroyed"
        terraform_provisioner.terraform.destroy.assert_called_once()

    def test_destroy_failure(self, terraform_provisioner, sample_storage_new, sample_compute_local, sample_dw_empty):
        terraform_provisioner.terraform.destroy.return_value = {
            "ok": False,
            "message": "no_state",
            "detail": "missing state",
        }
        terraform_provisioner.env.read_value.return_value = "quickelt-rg"

        result = terraform_provisioner.destroy(sample_storage_new, sample_compute_local, sample_dw_empty)

        assert result["ok"] is False
        assert result["message"] == "no_state"
