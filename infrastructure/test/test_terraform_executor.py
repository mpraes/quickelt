import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from setup.terraform_executor import TerraformExecutor

_TEST_LOGGER = logging.getLogger("test.terraform_executor")
_MODULE_DIR = Path(__file__).resolve().parent.parent / "terraform" / "azure"


@pytest.fixture
def executor(tmp_path):
    return TerraformExecutor(
        module_dir=_MODULE_DIR,
        logger=_TEST_LOGGER,
        auto_approve=True,
        workspace_dir=tmp_path / "workspace",
    )


class TestBuildVariables:
    def test_new_storage_local_compute(self, sample_storage_new, sample_compute_local, sample_dw_empty):
        variables = TerraformExecutor.build_variables(
            sample_storage_new,
            sample_compute_local,
            sample_dw_empty,
        )

        assert variables["storage_account_name"] == "newbucket"
        assert variables["storage_existing"] is False
        assert variables["storage_layers"] == ["bronze", "silver", "gold"]
        assert variables["create_vm"] is False
        assert variables["create_postgres"] is False

    def test_dedicated_vm_with_managed_postgres(
        self,
        sample_storage_new,
        sample_compute_vm,
        sample_dw_managed_provision,
    ):
        variables = TerraformExecutor.build_variables(
            sample_storage_new,
            sample_compute_vm,
            sample_dw_managed_provision,
        )

        assert variables["create_vm"] is True
        assert variables["bootstrap_vm"] is True
        assert variables["create_postgres"] is True
        assert variables["postgres_admin_password"] == "ManagedPassword456!"
        assert variables["postgres_admin_username"] == "quickelt"

    def test_existing_storage(self, sample_storage_existing, sample_compute_local, sample_dw_empty):
        variables = TerraformExecutor.build_variables(
            sample_storage_existing,
            sample_compute_local,
            sample_dw_empty,
        )

        assert variables["storage_account_name"] == "mybucket"
        assert variables["storage_existing"] is True
        assert variables["storage_layers"] == []

    def test_storage_name_short_after_normalization_raises(self, sample_compute_local, sample_dw_empty):
        with pytest.raises(ValueError):
            TerraformExecutor.build_variables(
                {"existing": False, "name": "--", "layers": []},
                sample_compute_local,
                sample_dw_empty,
            )


class TestParseOutputs:
    def test_parses_wrapped_values(self):
        raw = json.dumps({
            "resource_group_name": {"value": "quickelt-rg", "type": "string"},
            "postgres_port": {"value": 5432, "type": "number"},
        })
        parsed = TerraformExecutor._parse_outputs(raw)
        assert parsed["resource_group_name"] == "quickelt-rg"
        assert parsed["postgres_port"] == 5432


class TestProvision:
    @patch("setup.terraform_installer.ensure_terraform")
    @patch("setup.terraform_executor.subprocess.run")
    def test_happy_path(
        self,
        mock_run,
        mock_ensure,
        executor,
        sample_storage_new,
        sample_compute_local,
        sample_dw_empty,
    ):
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="init ok", stderr=""),
            MagicMock(returncode=0, stdout="Plan: 3 to add", stderr=""),
            MagicMock(returncode=0, stdout="Apply complete", stderr=""),
            MagicMock(
                returncode=0,
                stdout=json.dumps({
                    "resource_group_name": {"value": "quickelt-rg"},
                    "storage_account_name": {"value": "new-bucket"},
                    "vm_private_key_pem": {"value": "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----"},
                }),
                stderr="",
            ),
        ]

        result = executor.provision(sample_storage_new, sample_compute_local, sample_dw_empty)

        assert result["ok"] is True
        assert result["outputs"]["resource_group_name"] == "quickelt-rg"
        assert "vm_private_key_pem" not in result["outputs"]
        assert result["outputs"]["vm_private_key_path"].endswith("quickelt-vm-ssh-key.pem")
        assert Path(result["outputs"]["vm_private_key_path"]).exists()
        assert mock_run.call_count == 4
        apply_args = mock_run.call_args_list[2][0][0]
        assert "apply" in apply_args
        assert "-auto-approve" in apply_args

    @patch("setup.terraform_installer.ensure_terraform", side_effect=SystemExit(1))
    def test_missing_terraform_exits(self, mock_ensure, executor, sample_storage_new, sample_compute_local, sample_dw_empty):
        with pytest.raises(SystemExit) as exc_info:
            executor.provision(sample_storage_new, sample_compute_local, sample_dw_empty)
        assert exc_info.value.code == 1

    @patch("setup.terraform_installer.ensure_terraform")
    @patch("setup.terraform_executor.subprocess.run")
    def test_plan_failure(self, mock_run, mock_ensure, executor, sample_storage_new, sample_compute_local, sample_dw_empty):
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="", stderr=""),
            MagicMock(returncode=1, stdout="", stderr="plan failed"),
        ]

        result = executor.provision(sample_storage_new, sample_compute_local, sample_dw_empty)

        assert result["ok"] is False
        assert result["message"] == "terraform_plan_failed"

    @patch("setup.terraform_installer.ensure_terraform")
    @patch("setup.terraform_executor.subprocess.run")
    def test_user_cancels_apply(
        self,
        mock_run,
        mock_ensure,
        sample_storage_new,
        sample_compute_local,
        sample_dw_empty,
    ):
        executor = TerraformExecutor(module_dir=_MODULE_DIR, logger=_TEST_LOGGER, auto_approve=False)
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="", stderr=""),
            MagicMock(returncode=0, stdout="Plan: 1 to add", stderr=""),
        ]

        with patch("builtins.input", return_value="n"):
            result = executor.provision(sample_storage_new, sample_compute_local, sample_dw_empty)

        assert result["ok"] is False
        assert result["message"] == "cancelled"
        assert mock_run.call_count == 2


class TestDestroy:
    @patch("setup.terraform_installer.ensure_terraform")
    def test_no_state_returns_error(self, mock_ensure, executor, sample_storage_new, sample_compute_local, sample_dw_empty):
        result = executor.destroy(sample_storage_new, sample_compute_local, sample_dw_empty)
        assert result["ok"] is False
        assert result["message"] == "no_state"

    @patch("setup.terraform_installer.ensure_terraform")
    @patch("setup.terraform_executor.subprocess.run")
    def test_destroy_happy_path(
        self,
        mock_run,
        mock_ensure,
        executor,
        sample_storage_new,
        sample_compute_local,
        sample_dw_empty,
    ):
        executor.workspace_dir.mkdir(parents=True, exist_ok=True)
        (executor.workspace_dir / "terraform.tfstate").write_text('{"version": 4}', encoding="utf-8")

        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="", stderr=""),
            MagicMock(returncode=0, stdout="Plan: 3 to destroy", stderr=""),
            MagicMock(returncode=0, stdout="Destroy complete", stderr=""),
        ]

        result = executor.destroy(sample_storage_new, sample_compute_local, sample_dw_empty)

        assert result["ok"] is True
        assert result["message"] == "destroyed"
        destroy_args = mock_run.call_args_list[2][0][0]
        assert "destroy" in destroy_args
        assert "-auto-approve" in destroy_args

    @patch("setup.terraform_installer.ensure_terraform")
    @patch("setup.terraform_executor.subprocess.run")
    def test_destroy_user_cancels(
        self,
        mock_run,
        mock_ensure,
        sample_storage_new,
        sample_compute_local,
        sample_dw_empty,
        tmp_path,
    ):
        executor = TerraformExecutor(
            module_dir=_MODULE_DIR,
            logger=_TEST_LOGGER,
            auto_approve=False,
            workspace_dir=tmp_path / "workspace",
        )
        executor.workspace_dir.mkdir(parents=True, exist_ok=True)
        (executor.workspace_dir / "terraform.tfstate").write_text('{"version": 4}', encoding="utf-8")

        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="", stderr=""),
            MagicMock(returncode=0, stdout="Plan: 1 to destroy", stderr=""),
        ]

        with patch("builtins.input", return_value="n"):
            result = executor.destroy(sample_storage_new, sample_compute_local, sample_dw_empty)

        assert result["ok"] is False
        assert result["message"] == "cancelled"
