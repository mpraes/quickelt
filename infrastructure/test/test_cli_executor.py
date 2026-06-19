import logging
import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest

from setup.cli_executor import CLIExecutor, ErrorCategory, Spinner

_TEST_LOGGER = logging.getLogger("test.cli_executor")


class TestErrorCategory:
    def test_enum_values(self):
        assert ErrorCategory.ALREADY_EXISTS.value == "already_exists"
        assert ErrorCategory.UNAUTHORIZED.value == "unauthorized"
        assert ErrorCategory.AUTH_EXPIRED.value == "auth_expired"
        assert ErrorCategory.NOT_FOUND.value == "not_found"
        assert ErrorCategory.INVALID_NAME.value == "invalid_name"
        assert ErrorCategory.CLI_MISSING.value == "cli_missing"
        assert ErrorCategory.TIMEOUT.value == "timeout"
        assert ErrorCategory.UNKNOWN.value == "unknown"


class TestDetectCloud:
    @pytest.mark.parametrize(
        "command,expected",
        [
            (["aws", "s3", "ls"], "AWS"),
            (["aws", "ec2", "describe-instances"], "AWS"),
            (["az", "group", "create"], "Azure"),
            (["az", "vm", "create"], "Azure"),
            (["kubectl", "get", "pods"], "unknown"),
            ([], "unknown"),
        ],
    )
    def test_detect_cloud(self, command, expected):
        assert CLIExecutor._detect_cloud(command) == expected


class TestCLIExecutorExecute:
    @patch("setup.cli_executor.subprocess.Popen")
    def test_execute_success(self, mock_popen):
        proc = MagicMock()
        proc.stdout = iter(["line1\n", "line2\n"])
        proc.stderr = iter([])
        proc.wait.return_value = 0
        mock_popen.return_value = proc

        executor = CLIExecutor(logger=_TEST_LOGGER)
        result = executor.execute(["aws", "s3", "ls"])

        assert result["ok"] is True
        assert result["returncode"] == 0
        assert "line1" in result["stdout"]
        assert result["error_category"] is None
        assert result["cloud"] == "AWS"

    @patch("setup.cli_executor.subprocess.Popen")
    def test_execute_file_not_found(self, mock_popen):
        mock_popen.side_effect = FileNotFoundError

        executor = CLIExecutor(logger=_TEST_LOGGER)
        result = executor.execute(["nonexistent-cli", "test"])

        assert result["ok"] is False
        assert result["error_category"] == ErrorCategory.CLI_MISSING
        assert "nonexistent-cli" in result["remedy"]

    @patch("setup.cli_executor.subprocess.Popen")
    def test_execute_os_error(self, mock_popen):
        mock_popen.side_effect = OSError("something broke")

        executor = CLIExecutor(logger=_TEST_LOGGER)
        result = executor.execute(["aws", "test"])

        assert result["ok"] is False
        assert result["error_category"] == ErrorCategory.UNKNOWN

    @patch("setup.cli_executor.subprocess.Popen")
    def test_execute_timeout(self, mock_popen):
        proc = MagicMock()
        proc.stdout = iter(["partial\n"])
        proc.stderr = iter([])
        proc.wait.side_effect = [subprocess.TimeoutExpired(cmd="aws", timeout=60), 0]
        proc.kill.return_value = None
        mock_popen.return_value = proc

        executor = CLIExecutor(logger=_TEST_LOGGER)
        result = executor.execute(["aws", "slow-command"], timeout=60)

        assert result["ok"] is False
        assert result["error_category"] == ErrorCategory.TIMEOUT
        assert result["returncode"] == -9

    @patch("setup.cli_executor.subprocess.Popen")
    def test_execute_nonzero_with_aws_already_exists(self, mock_popen):
        proc = MagicMock()
        proc.stdout = iter([])
        proc.stderr = iter(["BucketAlreadyOwnedByYou\n"])
        proc.wait.return_value = 1
        mock_popen.return_value = proc

        executor = CLIExecutor(logger=_TEST_LOGGER)
        result = executor.execute(["aws", "s3api", "create-bucket"])

        assert result["ok"] is False
        assert result["error_category"] == ErrorCategory.ALREADY_EXISTS

    @patch("setup.cli_executor.subprocess.Popen")
    def test_execute_nonzero_with_aws_unauthorized(self, mock_popen):
        proc = MagicMock()
        proc.stdout = iter([])
        proc.stderr = iter(["UnauthorizedOperation\n"])
        proc.wait.return_value = 1
        mock_popen.return_value = proc

        executor = CLIExecutor(logger=_TEST_LOGGER)
        result = executor.execute(["aws", "s3api", "create-bucket"])

        assert result["ok"] is False
        assert result["error_category"] == ErrorCategory.UNAUTHORIZED

    @patch("setup.cli_executor.subprocess.Popen")
    def test_execute_nonzero_with_aws_auth_expired(self, mock_popen):
        proc = MagicMock()
        proc.stdout = iter([])
        proc.stderr = iter(["RequestExpired token is stale\n"])
        proc.wait.return_value = 1
        mock_popen.return_value = proc

        executor = CLIExecutor(logger=_TEST_LOGGER)
        result = executor.execute(["aws", "sts", "get-session-token"])

        assert result["ok"] is False
        assert result["error_category"] == ErrorCategory.AUTH_EXPIRED

    @patch("setup.cli_executor.subprocess.Popen")
    def test_execute_nonzero_with_azure_auth_expired(self, mock_popen):
        proc = MagicMock()
        proc.stdout = iter([])
        proc.stderr = iter(["ExpiredAuthenticationToken: token expired\n"])
        proc.wait.return_value = 1
        mock_popen.return_value = proc

        executor = CLIExecutor(logger=_TEST_LOGGER)
        result = executor.execute(["az", "group", "list"])

        assert result["ok"] is False
        assert result["error_category"] == ErrorCategory.AUTH_EXPIRED
        assert result["cloud"] == "Azure"

    @patch("setup.cli_executor.subprocess.Popen")
    def test_execute_nonzero_with_azure_already_exists(self, mock_popen):
        proc = MagicMock()
        proc.stdout = iter([])
        proc.stderr = iter(["StorageAccountAlreadyExists\n"])
        proc.wait.return_value = 1
        mock_popen.return_value = proc

        executor = CLIExecutor(logger=_TEST_LOGGER)
        result = executor.execute(["az", "storage", "account", "create"])

        assert result["ok"] is False
        assert result["error_category"] == ErrorCategory.ALREADY_EXISTS

    @patch("setup.cli_executor.subprocess.Popen")
    def test_execute_nonzero_unknown_error(self, mock_popen):
        proc = MagicMock()
        proc.stdout = iter([])
        proc.stderr = iter(["SomeRandomError message\n"])
        proc.wait.return_value = 42
        mock_popen.return_value = proc

        executor = CLIExecutor(logger=_TEST_LOGGER)
        result = executor.execute(["aws", "something"])

        assert result["ok"] is False
        assert result["error_category"] == ErrorCategory.UNKNOWN
        assert result["returncode"] == 42


class TestParseError:
    @pytest.mark.parametrize(
        "stderr,cloud,expected_category",
        [
            ("BucketAlreadyOwnedByYou", "AWS", ErrorCategory.ALREADY_EXISTS),
            ("AccessDenied", "AWS", ErrorCategory.UNAUTHORIZED),
            ("InvalidBucketName", "AWS", ErrorCategory.INVALID_NAME),
            ("AuthorizationFailed", "Azure", ErrorCategory.UNAUTHORIZED),
            ("ResourceGroupNotFound", "Azure", ErrorCategory.NOT_FOUND),
            ("OperationNotAllowed quota exceeded", "Azure", ErrorCategory.UNAUTHORIZED),
        ],
    )
    def test_parse_error_patterns(self, stderr, cloud, expected_category):
        executor = CLIExecutor(logger=_TEST_LOGGER)
        category, remedy = executor._parse_error(stderr, cloud)
        assert category == expected_category
        assert isinstance(remedy, str)
        assert len(remedy) > 0

    def test_parse_error_unknown_cloud(self):
        executor = CLIExecutor(logger=_TEST_LOGGER)
        category, remedy = executor._parse_error("some error", "GCP")
        assert category == ErrorCategory.UNKNOWN


class TestPromptChoiceBuiltin:
    @patch("setup.cli_executor._AVAILABLE_BACKEND", "builtin")
    @patch("builtins.input", return_value="1")
    def test_prompt_choice_valid(self, mock_input):
        executor = CLIExecutor(logger=_TEST_LOGGER)
        executor._backend = "builtin"
        result = executor.prompt_choice("Pick one", ["A", "B", "C"])
        assert result == "A"

    @patch("setup.cli_executor._AVAILABLE_BACKEND", "builtin")
    @patch("builtins.input", return_value="q")
    def test_prompt_choice_quit(self, mock_input):
        executor = CLIExecutor(logger=_TEST_LOGGER)
        executor._backend = "builtin"
        result = executor.prompt_choice("Pick one", ["A", "B"])
        assert result is None

    @patch("setup.cli_executor._AVAILABLE_BACKEND", "builtin")
    @patch("builtins.input", side_effect=KeyboardInterrupt)
    def test_prompt_choice_keyboard_interrupt(self, mock_input):
        executor = CLIExecutor(logger=_TEST_LOGGER)
        executor._backend = "builtin"
        result = executor.prompt_choice("Pick one", ["A", "B"])
        assert result is None


class TestPromptInputBuiltin:
    @patch("setup.cli_executor._AVAILABLE_BACKEND", "builtin")
    @patch("builtins.input", return_value="my-value")
    def test_prompt_input_with_value(self, mock_input):
        executor = CLIExecutor(logger=_TEST_LOGGER)
        executor._backend = "builtin"
        result = executor.prompt_input("Enter value", default="default")
        assert result == "my-value"

    @patch("setup.cli_executor._AVAILABLE_BACKEND", "builtin")
    @patch("builtins.input", return_value="")
    def test_prompt_input_empty_uses_default(self, mock_input):
        executor = CLIExecutor(logger=_TEST_LOGGER)
        executor._backend = "builtin"
        result = executor.prompt_input("Enter value", default="fallback")
        assert result == "fallback"

    @patch("setup.cli_executor._AVAILABLE_BACKEND", "builtin")
    @patch("builtins.input", side_effect=EOFError)
    def test_prompt_input_eof(self, mock_input):
        executor = CLIExecutor(logger=_TEST_LOGGER)
        executor._backend = "builtin"
        result = executor.prompt_input("Enter value")
        assert result is None


class TestSpinner:
    def test_spinner_start_stop(self):
        spinner = Spinner("test message", delay=0.01)
        spinner.start()
        spinner.stop("done")
        assert spinner._stop.is_set()

    def test_spinner_succeed(self):
        spinner = Spinner("test", delay=0.01, logger=_TEST_LOGGER)
        spinner.start()
        spinner.succeed("all good")
        assert spinner._stop.is_set()

    def test_spinner_fail(self):
        spinner = Spinner("test", delay=0.01, logger=_TEST_LOGGER)
        spinner.start()
        spinner.fail("bad")
        assert spinner._stop.is_set()

    def test_spinner_clear(self):
        spinner = Spinner("test", delay=0.01)
        spinner.start()
        spinner.clear()
        assert spinner._stop.is_set()

    def test_spinner_context_cleanup(self):
        spinner = Spinner("test", delay=0.01)
        spinner.start()
        assert spinner._thread is not None
        spinner.stop()
        assert not spinner._thread.is_alive()


class TestCloseStreams:
    def test_close_streams_with_none(self):
        proc = MagicMock()
        proc.stdout = None
        proc.stderr = None
        CLIExecutor._close_streams(proc)

    def test_close_streams_with_open_streams(self):
        proc = MagicMock()
        proc.stdout = MagicMock()
        proc.stderr = MagicMock()
        CLIExecutor._close_streams(proc)
        proc.stdout.close.assert_called_once()
        proc.stderr.close.assert_called_once()

    def test_close_streams_handles_exception(self):
        proc = MagicMock()
        proc.stdout = MagicMock()
        proc.stdout.close.side_effect = OSError("broken")
        proc.stderr = None
        CLIExecutor._close_streams(proc)
