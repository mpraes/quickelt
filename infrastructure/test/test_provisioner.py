import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from setup.provisioner import Provisioner
from setup.cli_executor import CLIExecutor, ErrorCategory, Spinner

_TEST_LOGGER = logging.getLogger("test.provisioner")


class ConcreteProvisioner(Provisioner):
    CLOUD_NAME = "Test"

    def provision(self, storage, compute, dw):
        return {"ok": True}


@pytest.fixture
def provisioner(mock_cli, mock_env_writer):
    return ConcreteProvisioner(cli=mock_cli, env=mock_env_writer, logger=_TEST_LOGGER)


class TestBootstrapScripts:
    def test_bootstrap_script_in_base_class(self):
        assert "quickelt-bootstrap" in Provisioner.BOOTSTRAP_SCRIPT
        assert "python3-pip git" in Provisioner.BOOTSTRAP_SCRIPT

    def test_local_postgres_script_in_base_class(self):
        assert Provisioner._LOCAL_PG_PASSWORD_PLACEHOLDER in Provisioner.LOCAL_POSTGRES_SCRIPT
        assert "postgresql" in Provisioner.LOCAL_POSTGRES_SCRIPT.lower()


class TestParseJson:
    def test_valid_json(self, provisioner):
        assert provisioner._parse_json('{"key": "val"}') == {"key": "val"}

    def test_invalid_json(self, provisioner):
        assert provisioner._parse_json("not json") is None

    def test_empty_string(self, provisioner):
        assert provisioner._parse_json("") is None

    def test_none_input(self, provisioner):
        assert provisioner._parse_json(None) is None


class TestDetectRegion:
    def test_raw_stdout(self, provisioner, mock_cli):
        mock_cli.execute.return_value = {"ok": True, "stdout": "  us-west-2  \n", "stderr": ""}

        result = provisioner._detect_region(
            ["aws", "configure", "get", "region"], "AWS_REGION", "us-east-1"
        )
        assert result == "us-west-2"

    def test_json_key(self, provisioner, mock_cli):
        mock_cli.execute.return_value = {
            "ok": True,
            "stdout": json.dumps({"location": "westeurope"}),
            "stderr": "",
        }

        result = provisioner._detect_region(
            ["az", "account", "show"], "AZURE_LOCATION", "eastus", json_key="location"
        )
        assert result == "westeurope"

    def test_env_fallback(self, provisioner, mock_cli):
        mock_cli.execute.return_value = {"ok": False, "stdout": "", "stderr": "err"}

        with patch.dict("os.environ", {"MY_REGION": "ap-south-1"}):
            result = provisioner._detect_region(["cmd"], "MY_REGION", "us-east-1")
            assert result == "ap-south-1"

    def test_default(self, provisioner, mock_cli):
        mock_cli.execute.return_value = {"ok": False, "stdout": "", "stderr": "err"}

        result = provisioner._detect_region(["cmd"], "NO_VAR", "us-east-1")
        assert result == "us-east-1"


class TestHandleAlreadyExists:
    def test_reuse(self, provisioner, mock_cli):
        spinner = MagicMock()
        mock_cli.prompt_choice.return_value = "Reuse existing s3 bucket"

        action, new_name = provisioner._handle_already_exists("S3 bucket", "my-bucket", spinner)

        assert action == "reuse"
        assert new_name is None
        spinner.clear.assert_called_once()

    def test_reuse_on_none_choice(self, provisioner, mock_cli):
        spinner = MagicMock()
        mock_cli.prompt_choice.return_value = None

        action, new_name = provisioner._handle_already_exists("S3 bucket", "my-bucket", spinner)

        assert action == "reuse"

    def test_retry_with_new_name(self, provisioner, mock_cli):
        spinner = MagicMock()
        mock_cli.prompt_choice.return_value = "Enter a new name"
        mock_cli.prompt_input.return_value = "my-bucket-v2"

        action, new_name = provisioner._handle_already_exists("S3 bucket", "my-bucket", spinner)

        assert action == "retry"
        assert new_name == "my-bucket-v2"

    def test_cancelled_no_name(self, provisioner, mock_cli):
        spinner = MagicMock()
        mock_cli.prompt_choice.return_value = "Enter a new name"
        mock_cli.prompt_input.return_value = ""

        action, new_name = provisioner._handle_already_exists("S3 bucket", "my-bucket", spinner)

        assert action == "cancelled"
        assert new_name is None
        spinner.fail.assert_called_once()


class TestHandleCliError:
    def test_unauthorized(self, provisioner):
        spinner = MagicMock()
        result = {"stderr": "AccessDenied", "remedy": "add IAM policy"}
        error_dict = {"bucket": "b", "region": "r"}

        out = provisioner._handle_cli_error(
            ErrorCategory.UNAUTHORIZED, spinner, result, error_dict,
            fail_label="Creating bucket", unauthorized_log="Lacks S3 permission.",
        )

        assert out["ok"] is False
        assert out["message"] == "unauthorized"
        assert out["bucket"] == "b"
        spinner.fail.assert_called_once()

    def test_auth_expired(self, provisioner):
        spinner = MagicMock()
        result = {"stderr": "", "remedy": "re-auth"}

        out = provisioner._handle_cli_error(
            ErrorCategory.AUTH_EXPIRED, spinner, result, {},
            fail_label="Op",
        )

        assert out["ok"] is False
        assert out["message"] == "auth_expired"

    def test_cli_missing(self, provisioner):
        spinner = MagicMock()
        result = {"stderr": "", "remedy": "Install AWS CLI"}

        out = provisioner._handle_cli_error(
            ErrorCategory.CLI_MISSING, spinner, result, {},
            fail_label="Op",
        )

        assert out["ok"] is False
        assert out["message"] == "cli_missing"
        spinner.fail.assert_called_once()

    def test_timeout(self, provisioner):
        spinner = MagicMock()
        result = {"stderr": "", "remedy": ""}

        out = provisioner._handle_cli_error(
            ErrorCategory.TIMEOUT, spinner, result, {},
            fail_label="Op",
        )

        assert out["ok"] is False
        assert out["message"] == "timeout"

    def test_generic_fallback(self, provisioner):
        spinner = MagicMock()
        result = {"stderr": "SomeError details here", "remedy": "check logs"}

        out = provisioner._handle_cli_error(
            ErrorCategory.UNKNOWN, spinner, result, {"bucket": "b"},
            fail_label="Op",
        )

        assert out["ok"] is False
        assert out["message"] == "SomeError details here"
        assert out["bucket"] == "b"

    def test_none_category_falls_to_generic(self, provisioner):
        spinner = MagicMock()
        result = {"stderr": "unknown error", "remedy": ""}

        out = provisioner._handle_cli_error(
            None, spinner, result, {},
            fail_label="Op",
        )

        assert out["ok"] is False


class TestPrintProvisionBanner:
    def test_banner_output(self, provisioner, capsys):
        provisioner._print_provision_banner()
        captured = capsys.readouterr()
        assert "Test Provisioner" in captured.out
        assert "╔" in captured.out


class TestEmptyLayersResult:
    def test_basic(self, provisioner):
        result = provisioner._empty_layers_result()
        assert result["ok"] is True
        assert result["created"] == []
        assert result["failed"] == []

    def test_with_extra(self, provisioner):
        result = provisioner._empty_layers_result(container="my-container")
        assert result["container"] == "my-container"


class TestProvisionLayers:
    def test_with_layers(self, provisioner, mock_cli):
        results = {}
        storage = {"name": "b", "layers": ["bronze", "silver"]}
        mock_cli.execute.return_value = {"ok": True, "stdout": "", "stderr": "", "error_category": None, "remedy": ""}

        def fake_layers_fn(name, layers):
            return {"ok": True, "created": layers, "failed": []}

        provisioner._provision_layers(storage, results, fake_layers_fn)

        assert results["layers"]["ok"] is True
        assert results["layers"]["created"] == ["bronze", "silver"]

    def test_without_layers(self, provisioner, mock_cli):
        results = {}
        storage = {"name": "b", "layers": []}

        def fake_layers_fn(name, layers):
            return {"ok": True, "created": layers, "failed": []}

        provisioner._provision_layers(storage, results, fake_layers_fn)

        assert results["layers"]["ok"] is True
        assert results["layers"]["created"] == []

    def test_with_extra_kwargs(self, provisioner, mock_cli):
        results = {}
        storage = {"name": "b", "layers": []}

        def fake_layers_fn(name, layers):
            return {"ok": True, "created": layers, "failed": [], "container": "c"}

        provisioner._provision_layers(storage, results, fake_layers_fn, container="")

        assert results["layers"]["container"] == ""


class TestCLoudName:
    def test_cloud_name_on_subclass(self):
        assert ConcreteProvisioner.CLOUD_NAME == "Test"


class TestGetLocalPostgresScript:
    def test_password_substitution(self, provisioner):
        script = provisioner._get_local_postgres_script("MyStr0ngP@ss!")
        assert "MyStr0ngP@ss!" in script
        assert Provisioner._LOCAL_PG_PASSWORD_PLACEHOLDER not in script

    def test_empty_password_generates_random(self, provisioner):
        script = provisioner._get_local_postgres_script("")
        assert Provisioner._LOCAL_PG_PASSWORD_PLACEHOLDER not in script
        assert "WITH PASSWORD '" in script

    def test_none_password_generates_random(self, provisioner):
        script = provisioner._get_local_postgres_script(None)
        assert Provisioner._LOCAL_PG_PASSWORD_PLACEHOLDER not in script
        assert "WITH PASSWORD '" in script

    def test_generated_password_is_unique(self, provisioner):
        script1 = provisioner._get_local_postgres_script("")
        script2 = provisioner._get_local_postgres_script("")
        pw1 = script1.split("WITH PASSWORD '")[1].split("'")[0]
        pw2 = script2.split("WITH PASSWORD '")[1].split("'")[0]
        assert pw1 != pw2


class TestHandleAlreadyExistsMaxRetry:
    def test_max_retry_cancels(self, provisioner, mock_cli):
        spinner = MagicMock()
        provisioner._retry_count = provisioner._MAX_RETRY_NAME

        action, new_name = provisioner._handle_already_exists("S3 bucket", "my-bucket", spinner)

        assert action == "cancelled"
        assert new_name is None
        spinner.fail.assert_called_once()

    def test_retry_increments_counter(self, provisioner, mock_cli):
        spinner = MagicMock()
        mock_cli.prompt_choice.return_value = "Enter a new name"
        mock_cli.prompt_input.return_value = "new-bucket"

        provisioner._handle_already_exists("S3 bucket", "my-bucket", spinner)

        assert provisioner._retry_count == 1

    def test_reuse_resets_counter(self, provisioner, mock_cli):
        spinner = MagicMock()
        provisioner._retry_count = 2
        mock_cli.prompt_choice.return_value = "Reuse existing s3 bucket"

        provisioner._handle_already_exists("S3 bucket", "my-bucket", spinner)

        assert provisioner._retry_count == 0


class TestRetryCountResetsOnProvision:
    def test_reset_retry_state(self, provisioner):
        provisioner._retry_count = 3
        provisioner._reset_retry_state()
        assert provisioner._retry_count == 0
