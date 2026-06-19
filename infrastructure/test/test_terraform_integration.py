import importlib.util
import os
from pathlib import Path
from unittest.mock import patch

import pytest

_SETUP_PY_PATH = Path(__file__).resolve().parent.parent / "setup.py"


@pytest.fixture
def setup_module():
    spec = importlib.util.spec_from_file_location("quickelt_setup", str(_SETUP_PY_PATH))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestProvisionerRegistry:
    def test_azure_defaults_to_terraform(self, setup_module):
        with patch.dict(os.environ, {}, clear=True):
            if "QUICKELT_LEGACY_AZURE_PROVISIONER" in os.environ:
                del os.environ["QUICKELT_LEGACY_AZURE_PROVISIONER"]
            spec = importlib.util.spec_from_file_location("quickelt_setup_reload", str(_SETUP_PY_PATH))
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            assert mod._PROVISIONER_REGISTRY["Azure"] == "azure_terraform_provisioner"

    def test_azure_legacy_flag(self, setup_module):
        with patch.dict(os.environ, {"QUICKELT_LEGACY_AZURE_PROVISIONER": "1"}):
            spec = importlib.util.spec_from_file_location("quickelt_setup_legacy", str(_SETUP_PY_PATH))
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            assert mod._PROVISIONER_REGISTRY["Azure"] == "azure_provisioner"

    def test_terraform_module_exists(self):
        module_path = Path(__file__).resolve().parent.parent / "setup" / "azure_terraform_provisioner.py"
        assert module_path.exists()

    def test_terraform_azure_module_files_exist(self):
        tf_dir = Path(__file__).resolve().parent.parent / "terraform" / "azure"
        required = ["main.tf", "variables.tf", "outputs.tf", "versions.tf", "storage.tf", "vm.tf", "postgres.tf"]
        for name in required:
            assert (tf_dir / name).exists(), f"Missing {name}"
