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

            wiz.prompts.ask_cloud.return_value = "AWS"
            wiz.prompts.ask_storage.return_value = {"existing": True, "name": "b", "layers": []}
            wiz.prompts.ask_compute.return_value = {"compute": "Local Machine", "bootstrap_vm": False}
            wiz.prompts.ask_dw.return_value = {"gold_external_db": False}

            with patch.object(wiz, '_print_banner'):
                with patch.object(wiz, '_print_summary'):
                    with patch.object(wiz, '_invoke_provisioner'):
                        wiz.run()

        wiz.prompts.ask_cloud.assert_called_once()
        wiz.preflight.check.assert_called_once_with("AWS")
        wiz.prompts.ask_storage.assert_called_once()
        wiz.prompts.ask_compute.assert_called_once()
        wiz.prompts.ask_dw.assert_called_once()
        wiz.env.write.assert_called_once()

    def test_run_keyboard_interrupt(self, setup_module):
        SetupWizard = setup_module.SetupWizard

        with patch.object(SetupWizard, '__init__', lambda self: None):
            wiz = SetupWizard()
            wiz.prompts = MagicMock()
            wiz.prompts.ask_cloud.side_effect = KeyboardInterrupt()

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

        SetupWizard._print_summary(cloud, storage, compute, dw)

        captured = capsys.readouterr()
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


class TestConfigureLogging:
    def test_configure_logging_creates_handlers(self, setup_module):
        SetupWizard = setup_module.SetupWizard
        with patch.object(setup_module, "LOG_FILE", Path("/tmp/.quickelt_setup_test.log")):
            SetupWizard._configure_logging()

        parent = logging.getLogger("quickelt")
        assert parent.level == logging.DEBUG
        assert len(parent.handlers) == 2
