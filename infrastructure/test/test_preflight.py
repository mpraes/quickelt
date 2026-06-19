import logging
import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest

from setup.preflight import PreflightChecker

_TEST_LOGGER = logging.getLogger("test.preflight")


class TestPreflightCheck:
    @patch("setup.preflight.subprocess.run")
    def test_check_aws_pass(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout='{"Account": "123"}', stderr="")

        checker = PreflightChecker(logger=_TEST_LOGGER)
        checker.check("AWS")

        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert cmd == ["aws", "sts", "get-caller-identity"]

    @patch("setup.preflight.subprocess.run")
    def test_check_azure_pass(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="{}", stderr="")

        checker = PreflightChecker(logger=_TEST_LOGGER)
        checker.check("Azure")

        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert cmd == ["az", "account", "show"]

    @patch("setup.preflight.subprocess.run")
    def test_check_aws_fail_nonzero_exit(self, mock_run):
        mock_run.return_value = MagicMock(returncode=255, stdout="", stderr="Unable to locate credentials")

        checker = PreflightChecker(logger=_TEST_LOGGER)
        with pytest.raises(SystemExit) as exc_info:
            checker.check("AWS")
        assert exc_info.value.code == 1

    @patch("setup.preflight.subprocess.run")
    def test_check_azure_fail_nonzero_exit(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="Please run az login")

        checker = PreflightChecker(logger=_TEST_LOGGER)
        with pytest.raises(SystemExit) as exc_info:
            checker.check("Azure")
        assert exc_info.value.code == 1

    @patch("setup.preflight.subprocess.run", side_effect=FileNotFoundError)
    def test_check_cli_missing(self, mock_run):
        checker = PreflightChecker(logger=_TEST_LOGGER)
        with pytest.raises(SystemExit) as exc_info:
            checker.check("AWS")
        assert exc_info.value.code == 1

    @patch("setup.preflight.subprocess.run", side_effect=subprocess.TimeoutExpired(cmd="aws", timeout=15))
    def test_check_timeout(self, mock_run):
        checker = PreflightChecker(logger=_TEST_LOGGER)
        with pytest.raises(SystemExit) as exc_info:
            checker.check("AWS")
        assert exc_info.value.code == 1

    @patch("setup.preflight.subprocess.run", side_effect=OSError("broken pipe"))
    def test_check_os_error(self, mock_run):
        checker = PreflightChecker(logger=_TEST_LOGGER)
        with pytest.raises(SystemExit) as exc_info:
            checker.check("Azure")
        assert exc_info.value.code == 1

    @patch("setup.preflight.subprocess.run")
    def test_check_passes_with_empty_stderr(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="{}", stderr="")

        checker = PreflightChecker(logger=_TEST_LOGGER)
        checker.check("AWS")

    def test_preflight_commands_defined(self):
        assert "AWS" in PreflightChecker._PREFLIGHT
        assert "Azure" in PreflightChecker._PREFLIGHT
        assert PreflightChecker._PREFLIGHT["AWS"]["command"] == ["aws", "sts", "get-caller-identity"]
        assert PreflightChecker._PREFLIGHT["Azure"]["command"] == ["az", "account", "show"]

    @patch("setup.preflight.subprocess.run")
    def test_check_timeout_param(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")

        checker = PreflightChecker(logger=_TEST_LOGGER)
        checker.check("AWS")

        assert mock_run.call_args[1]["timeout"] == 15
        assert mock_run.call_args[1]["capture_output"] is True
        assert mock_run.call_args[1]["text"] is True
