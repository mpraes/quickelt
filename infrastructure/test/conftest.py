import logging
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from setup.cli_executor import CLIExecutor, ErrorCategory
from setup.env_writer import EnvWriter


@pytest.fixture
def mock_cli() -> MagicMock:
    cli = MagicMock(spec=CLIExecutor)
    cli.log = logging.getLogger("test.cli")
    cli._backend = "builtin"
    return cli


@pytest.fixture
def mock_env_writer() -> MagicMock:
    env = MagicMock(spec=EnvWriter)
    env.log = logging.getLogger("test.env")
    return env


@pytest.fixture
def mock_env(tmp_path: Path) -> EnvWriter:
    env_path = tmp_path / ".env"
    env_path.touch()
    return EnvWriter(env_path, logger=logging.getLogger("test.env"))


@pytest.fixture
def env_path(tmp_path: Path) -> Path:
    return tmp_path / ".env"


@pytest.fixture
def mock_spinner() -> MagicMock:
    spinner = MagicMock()
    spinner.start.return_value = spinner
    return spinner


@pytest.fixture
def sample_storage_existing() -> dict:
    return {"existing": True, "name": "my-bucket", "layers": []}


@pytest.fixture
def sample_storage_new() -> dict:
    return {"existing": False, "name": "new-bucket", "layers": ["bronze", "silver", "gold"]}


@pytest.fixture
def sample_compute_vm() -> dict:
    return {"compute": "Dedicated VM", "bootstrap_vm": True}


@pytest.fixture
def sample_compute_local() -> dict:
    return {"compute": "Local Machine", "bootstrap_vm": False}


@pytest.fixture
def sample_dw_empty() -> dict:
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


@pytest.fixture
def sample_dw_managed_provision() -> dict:
    return {
        "gold_external_db": True,
        "pg_strategy": "managed_cloud",
        "install_local_postgres": False,
        "managed_cloud_choice": "provision_new",
        "dw_host": None,
        "dw_port": "5432",
        "dw_database": "quickelt_db",
        "dw_username": "quickelt",
        "dw_password": "ManagedPassword456!",
    }
