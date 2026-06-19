import importlib.util
import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from setup.prompts import BuiltinBackend

_TEST_LOGGER = logging.getLogger("test.setup_wizard")

_SETUP_PY_PATH = Path(__file__).resolve().parent.parent / "setup.py"


@pytest.fixture
def setup_module():
    spec = importlib.util.spec_from_file_location("quickelt_setup", str(_SETUP_PY_PATH))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestSetupWizardInit:
    def test_init_creates_components(self, setup_module):
        SetupWizard = setup_module.SetupWizard

        with patch.object(SetupWizard, '__init__', lambda self: None):
            wiz = SetupWizard()

        assert hasattr(SetupWizard, 'run')
        assert hasattr(SetupWizard, '_invoke_provisioner')
        assert hasattr(SetupWizard, '_print_banner')
        assert hasattr(SetupWizard, '_print_summary')
        assert hasattr(SetupWizard, '_handle_keyboard_interrupt')
        assert hasattr(SetupWizard, '_configure_logging')


class TestSetupWizardRun:
    def test_run_happy_path(self, setup_module):
        SetupWizard = setup_module.SetupWizard

        with patch.object(SetupWizard, '__init__', lambda self: None):
            wiz = SetupWizard()
            wiz.cli = MagicMock()
            wiz.env = MagicMock()
            wiz.preflight = MagicMock()
            wiz.prompts = MagicMock(spec=BuiltinBackend)
            wiz._provisioner = None
            wiz.setup_name = "test-setup"

            wiz.prompts.ask_setup_name.return_value = "test-setup"
            wiz.prompts.ask_cloud.return_value = "AWS"
            wiz.prompts.ask_storage.return_value = {"existing": True, "name": "b", "layers": []}
            wiz.prompts.ask_gold_database.return_value = {"gold_external_db": False}
            wiz.prompts.ask_compute.return_value = {"compute": "Local Machine", "bootstrap_vm": False}
            wiz.prompts.ask_dw.return_value = {"gold_external_db": False}

            with patch.object(wiz, '_print_banner'):
                with patch.object(wiz, '_bind_setup'):
                    with patch.object(wiz, '_print_summary'):
                        with patch.object(wiz, '_activate_setup'):
                            with patch.object(wiz, '_invoke_provisioner'):
                                wiz.run()

        wiz.prompts.ask_setup_name.assert_called_once()
        wiz.prompts.ask_cloud.assert_called_once()
        wiz.preflight.check.assert_called_once_with("AWS")
        wiz.prompts.ask_storage.assert_called_once()
        wiz.prompts.ask_gold_database.assert_called_once()
        wiz.prompts.ask_compute.assert_called_once()
        wiz.prompts.ask_dw.assert_called_once()
        wiz.env.write.assert_called_once()

    def test_run_keyboard_interrupt(self, setup_module):
        SetupWizard = setup_module.SetupWizard

        with patch.object(SetupWizard, '__init__', lambda self: None):
            wiz = SetupWizard()
            wiz.prompts = MagicMock()
            wiz.prompts.ask_setup_name.side_effect = KeyboardInterrupt()

            with patch.object(wiz, '_print_banner'):
                with pytest.raises(SystemExit) as exc_info:
                    wiz.run()
                assert exc_info.value.code == 130

    def test_run_system_exit_propagates(self, setup_module):
        SetupWizard = setup_module.SetupWizard

        with patch.object(SetupWizard, '__init__', lambda self: None):
            wiz = SetupWizard()
            wiz.prompts = MagicMock()
            wiz.prompts.ask_cloud.return_value = "AWS"
            wiz.preflight = MagicMock()
            wiz.preflight.check.side_effect = SystemExit(1)

            with patch.object(wiz, '_print_banner'):
                with pytest.raises(SystemExit) as exc_info:
                    wiz.run()
                assert exc_info.value.code == 1


class TestInvokeProvisioner:
    def test_invoke_provisioner_missing_module(self, setup_module):
        SetupWizard = setup_module.SetupWizard

        with patch.object(SetupWizard, '__init__', lambda self: None):
            wiz = SetupWizard()
            wiz.cli = MagicMock()
            wiz.env = MagicMock()

            with patch.object(setup_module, "SETUP_DIR", Path("/nonexistent")):
                with patch.object(setup_module, "_PROVISIONER_REGISTRY", {"AWS": "aws_provisioner"}):
                    wiz._invoke_provisioner(
                        "AWS",
                        {"existing": True, "name": "b", "layers": []},
                        {"compute": "Local Machine", "bootstrap_vm": False},
                        {"gold_external_db": False},
                    )


class TestPrintSummary:
    def test_summary_output(self, setup_module, capsys):
        SetupWizard = setup_module.SetupWizard
        cloud = "AWS"
        storage = {"existing": True, "name": "my-bucket", "layers": ["bronze", "silver"]}
        compute = {"compute": "Dedicated VM", "bootstrap_vm": True}
        dw = {"gold_external_db": True, "pg_strategy": "local_vm", "install_local_postgres": True, "dw_host": "localhost"}

        SetupWizard._print_summary("acme-prod", cloud, storage, compute, dw)

        captured = capsys.readouterr()
        assert "acme-prod" in captured.out
        assert "AWS" in captured.out
        assert "my-bucket" in captured.out
        assert "bronze, silver" in captured.out
        assert "Dedicated VM" in captured.out
        assert "Local VM" in captured.out
        assert "localhost" in captured.out


class TestHandleKeyboardInterrupt:
    def test_keyboard_interrupt_exits_130(self, setup_module):
        SetupWizard = setup_module.SetupWizard
        with pytest.raises(SystemExit) as exc_info:
            SetupWizard._handle_keyboard_interrupt()
        assert exc_info.value.code == 130


class TestParseArgs:
    def test_destroy_without_setup_name(self, setup_module):
        destroy, clean, setup_name, force = setup_module._parse_args(["--destroy"])
        assert destroy is True
        assert clean is False
        assert setup_name is None
        assert force is False

    def test_destroy_with_setup_name_flag(self, setup_module):
        destroy, clean, setup_name, force = setup_module._parse_args(
            ["--destroy", "--setup-name", "acme-prod"]
        )
        assert destroy is True
        assert clean is False
        assert setup_name == "acme-prod"
        assert force is False

    def test_destroy_with_positional_setup_name(self, setup_module):
        destroy, clean, setup_name, force = setup_module._parse_args(["--destroy", "acme-prod"])
        assert destroy is True
        assert clean is False
        assert setup_name == "acme-prod"
        assert force is False

    def test_cleanup_with_setup_name_and_force(self, setup_module):
        destroy, clean, setup_name, force = setup_module._parse_args(
            ["--clean", "--setup-name", "acme-prod", "--yes"]
        )
        assert destroy is False
        assert clean is True
        assert setup_name == "acme-prod"
        assert force is True

    def test_cleanup_with_positional_setup_name(self, setup_module):
        destroy, clean, setup_name, force = setup_module._parse_args(["--clean", "acme-prod"])
        assert destroy is False
        assert clean is True
        assert setup_name == "acme-prod"
        assert force is False

    def test_rejects_destroy_and_clean_together(self, setup_module):
        with pytest.raises(ValueError):
            setup_module._parse_args(["--destroy", "--clean"])

    def test_setup_name_flag_requires_value(self, setup_module):
        with pytest.raises(ValueError):
            setup_module._parse_args(["--destroy", "--setup-name"])


class TestConfigureLogging:
    def test_configure_logging_creates_handlers(self, setup_module):
        SetupWizard = setup_module.SetupWizard
        with patch.object(setup_module, "LOG_FILE", Path("/tmp/.quickelt_setup_test.log")):
            SetupWizard._configure_logging()

        parent = logging.getLogger("quickelt")
        assert parent.level == logging.DEBUG
        assert len(parent.handlers) == 2


class TestAzureStorageNormalization:
    def test_new_storage_name_is_normalized(self, setup_module):
        SetupWizard = setup_module.SetupWizard
        with patch.object(SetupWizard, "__init__", lambda self: None):
            wiz = SetupWizard()
            from setup.terraform_executor import TerraformExecutor

            wiz._terraform_executor_cls = TerraformExecutor
            normalized = wiz._normalize_azure_storage_name(
                {"existing": False, "name": "quickelt-data-lake-001", "layers": ["bronze"]}
            )
        assert normalized["name"] == "quickeltdatalake001"

    def test_existing_invalid_name_raises(self, setup_module):
        SetupWizard = setup_module.SetupWizard
        with patch.object(SetupWizard, "__init__", lambda self: None):
            wiz = SetupWizard()
            from setup.terraform_executor import TerraformExecutor

            wiz._terraform_executor_cls = TerraformExecutor
            with pytest.raises(ValueError):
                wiz._normalize_azure_storage_name(
                    {"existing": True, "name": "my-existing-bucket", "layers": []}
                )


class TestSetupWizardDestroyCleanupFlows:
    def test_run_destroy_happy_path_invokes_destroy(self, setup_module):
        SetupWizard = setup_module.SetupWizard
        with patch.object(SetupWizard, "__init__", lambda self: None):
            wiz = SetupWizard()
            wiz.env = MagicMock()
            wiz.preflight = MagicMock()
            wiz.setup_name = "acme-dev"
            wiz.env.env_path = MagicMock()
            wiz.env.env_path.exists.return_value = True
            wiz.env.load_setup_config.return_value = (
                "Azure",
                {"name": "quickeltlake", "layers": ["bronze"], "existing": False},
                {"compute": "Local Machine", "bootstrap_vm": False},
                {"gold_external_db": False},
            )

            with patch.object(wiz, "_print_destroy_banner"), \
                 patch.object(wiz, "_resolve_setup_for_destroy", return_value="acme-dev"), \
                 patch.object(wiz, "_bind_setup"), \
                 patch.object(wiz, "_ensure_terraform_cli"), \
                 patch.object(wiz, "_invoke_destroy") as mock_invoke_destroy:
                wiz.run_destroy("acme-dev")

            wiz.preflight.check.assert_called_once_with("Azure")
            mock_invoke_destroy.assert_called_once()

    def test_run_cleanup_removes_active_and_switches_fallback(self, setup_module, tmp_path):
        SetupWizard = setup_module.SetupWizard
        setups_root = tmp_path / "setups"
        alpha = setups_root / "alpha"
        beta = setups_root / "beta"
        alpha.mkdir(parents=True)
        beta.mkdir(parents=True)
        (alpha / ".env").write_text("SETUP_NAME=alpha\n", encoding="utf-8")
        (beta / ".env").write_text("SETUP_NAME=beta\n", encoding="utf-8")
        root_env = tmp_path / ".env"
        root_env.write_text("QUICKELT_SETUP_NAME=alpha\n", encoding="utf-8")

        with patch.object(SetupWizard, "__init__", lambda self: None), \
             patch.object(setup_module, "ENV_FILE", root_env), \
             patch("setup.setup_registry.SETUPS_ROOT", setups_root):
            wiz = SetupWizard()
            wiz.run_cleanup("alpha", force=True)

        assert not alpha.exists()
        assert beta.exists()
        assert "SETUP_NAME=beta" in root_env.read_text(encoding="utf-8")
