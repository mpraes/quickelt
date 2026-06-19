#!/usr/bin/env python3
"""
Quickelt CLI Executor
=====================

Secure, robust subprocess execution layer for cloud CLI tools.

Provides:
    - CLIExecutor: Class-based command execution with real-time log streaming,
      error classification, and interactive recovery prompts.
    - ErrorCategory: Typed cloud-error classification enum.
    - Spinner: Terminal progress indicator.
"""

import enum
import logging
import re
import subprocess
import sys
import threading
from typing import Any

from setup._backend_detect import detect_prompt_backend

_AVAILABLE_BACKEND = detect_prompt_backend()

_SPINNER_FRAMES = ("⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏")


class ErrorCategory(enum.Enum):
    ALREADY_EXISTS = "already_exists"
    UNAUTHORIZED = "unauthorized"
    AUTH_EXPIRED = "auth_expired"
    NOT_FOUND = "not_found"
    INVALID_NAME = "invalid_name"
    CLI_MISSING = "cli_missing"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


_AWS_PATTERNS: list[tuple[re.Pattern, ErrorCategory, str]] = [
    (
        re.compile(r"BucketAlreadyOwnedByYou", re.IGNORECASE),
        ErrorCategory.ALREADY_EXISTS,
        "The S3 bucket already exists and is owned by your AWS account. You can reuse it or pick a new name.",
    ),
    (
        re.compile(r"BucketAlreadyExists", re.IGNORECASE),
        ErrorCategory.ALREADY_EXISTS,
        "The S3 bucket name is already taken by another AWS account. Choose a different name.",
    ),
    (
        re.compile(r"UnauthorizedOperation", re.IGNORECASE),
        ErrorCategory.UNAUTHORIZED,
        "Your AWS IAM User lacks permission to run this operation. Attach the required IAM policy and retry.",
    ),
    (
        re.compile(r"AccessDenied", re.IGNORECASE),
        ErrorCategory.UNAUTHORIZED,
        "Access denied. Your AWS credentials lack the required permissions for this operation.",
    ),
    (
        re.compile(r"InvalidBucketName", re.IGNORECASE),
        ErrorCategory.INVALID_NAME,
        "The S3 bucket name is invalid. Must be 3-63 chars, lowercase, no underscores, start with letter/digit.",
    ),
    (
        re.compile(r"RequestExpired|TokenExpired", re.IGNORECASE),
        ErrorCategory.AUTH_EXPIRED,
        "Your AWS session has expired. Run 'aws sts get-session-token' or re-configure with 'aws configure'.",
    ),
    (
        re.compile(r"InvalidParameterValue|InvalidAMIid", re.IGNORECASE),
        ErrorCategory.INVALID_NAME,
        "An invalid parameter value was supplied. Check the command arguments and retry.",
    ),
]

_AZURE_PATTERNS: list[tuple[re.Pattern, ErrorCategory, str]] = [
    (
        re.compile(r"ResourceGroupNotFound", re.IGNORECASE),
        ErrorCategory.NOT_FOUND,
        "Resource group not found. Create it first with: az group create --name <name> --location <loc>",
    ),
    (
        re.compile(r"AuthorizationFailed|InsufficientPrivileges|Forbidden", re.IGNORECASE),
        ErrorCategory.UNAUTHORIZED,
        "Your Azure account lacks permission for this operation. Request the required role assignment from your admin.",
    ),
    (
        re.compile(r"ExpiredAuthenticationToken|AADSTS700082|TokenExpired", re.IGNORECASE),
        ErrorCategory.AUTH_EXPIRED,
        "Your Azure authentication token has expired. Run 'az login' to re-authenticate.",
    ),
    (
        re.compile(r"StorageAccountAlreadyExists", re.IGNORECASE),
        ErrorCategory.ALREADY_EXISTS,
        "A storage account with this name already exists. You can reuse it or choose a different name.",
    ),
    (
        re.compile(r"not a valid storage account name|InvalidStorageAccountName", re.IGNORECASE),
        ErrorCategory.INVALID_NAME,
        "Storage account name must be 3-24 characters, lowercase letters and digits only.",
    ),
    (
        re.compile(r"SubscriptionNotFound|ResourceNotFound", re.IGNORECASE),
        ErrorCategory.NOT_FOUND,
        "The specified Azure resource was not found. Verify the name and your subscription.",
    ),
    (
        re.compile(r"OperationNotAllowed|QuotaExceeded", re.IGNORECASE),
        ErrorCategory.UNAUTHORIZED,
        "This Azure operation is not allowed. You may have hit a quota limit or policy restriction.",
    ),
]


class Spinner:
    def __init__(self, message: str, delay: float = 0.1, logger: logging.Logger | None = None):
        self._message = message
        self._delay = delay
        self._log = logger
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> "Spinner":
        self._stop.clear()
        self._thread = threading.Thread(target=self._spin, daemon=True)
        self._thread.start()
        return self

    def _spin(self) -> None:
        idx = 0
        while not self._stop.is_set():
            frame = _SPINNER_FRAMES[idx % len(_SPINNER_FRAMES)]
            sys.stdout.write(f"\r  {frame} {self._message}")
            sys.stdout.flush()
            idx += 1
            self._stop.wait(self._delay)

    def stop(self, final: str = "", level: int = logging.INFO) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        sys.stdout.write("\r" + " " * (len(self._message) + 6) + "\r")
        sys.stdout.flush()
        if final:
            if self._log:
                self._log.log(level, final)
            else:
                print(f"  {final}")

    def succeed(self, msg: str) -> None:
        self.stop(f"✔ {msg}", level=logging.INFO)

    def fail(self, msg: str) -> None:
        self.stop(f"✖ {msg}", level=logging.ERROR)

    def clear(self) -> None:
        self.stop()


class CLIExecutor:
    """Subprocess execution, error classification, and interactive prompts."""

    def __init__(self, logger: logging.Logger | None = None):
        self.log = logger or logging.getLogger("quickelt.executor")
        self._backend = _AVAILABLE_BACKEND

    def execute(self, command_list: list[str], timeout: int = 60) -> dict[str, Any]:
        cloud = self._detect_cloud(command_list)
        cmd_str = " ".join(command_list)
        self.log.debug("Executing: %s", cmd_str)

        try:
            proc = subprocess.Popen(
                command_list,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
        except FileNotFoundError:
            self.log.debug("CLI binary not found: %s", command_list[0])
            return {
                "ok": False, "returncode": -1, "stdout": "", "stderr": "",
                "error_category": ErrorCategory.CLI_MISSING,
                "remedy": f"{command_list[0]} CLI not found. Install it and ensure it is on your PATH.",
                "cloud": cloud,
            }
        except OSError as exc:
            self.log.debug("Popen OSError: %s", exc)
            return {
                "ok": False, "returncode": -1, "stdout": "", "stderr": "",
                "error_category": ErrorCategory.UNKNOWN,
                "remedy": str(exc),
                "cloud": cloud,
            }

        stdout_chunks: list[str] = []
        stderr_chunks: list[str] = []

        def _drain(stream, chunks, tag):
            try:
                for line in stream:
                    stripped = line.rstrip("\n\r")
                    chunks.append(stripped)
                    self.log.debug("[%s] %s", tag, stripped)
            except ValueError:
                pass

        t_out = threading.Thread(target=_drain, args=(proc.stdout, stdout_chunks, "stdout"), daemon=True)
        t_err = threading.Thread(target=_drain, args=(proc.stderr, stderr_chunks, "stderr"), daemon=True)
        t_out.start()
        t_err.start()

        try:
            returncode = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.log.debug("Process timed out after %ds, killing", timeout)
            proc.kill()
            proc.wait()
            t_out.join(timeout=3)
            t_err.join(timeout=3)
            self._close_streams(proc)
            return {
                "ok": False, "returncode": -9, "stdout": "\n".join(stdout_chunks),
                "stderr": "\n".join(stderr_chunks),
                "error_category": ErrorCategory.TIMEOUT,
                "remedy": f"Command timed out after {timeout}s: {cmd_str}",
                "cloud": cloud,
            }

        t_out.join(timeout=5)
        t_err.join(timeout=5)
        self._close_streams(proc)

        stdout_text = "\n".join(stdout_chunks)
        stderr_text = "\n".join(stderr_chunks)

        self.log.debug("Exit code: %d", returncode)

        if returncode != 0:
            self.log.debug("Command failed (rc=%d). stderr=%s", returncode, stderr_text[:300])
            category, remedy = self._parse_error(stderr_text, cloud)
            return {
                "ok": False, "returncode": returncode,
                "stdout": stdout_text, "stderr": stderr_text,
                "error_category": category, "remedy": remedy,
                "cloud": cloud,
            }

        return {
            "ok": True, "returncode": returncode,
            "stdout": stdout_text, "stderr": stderr_text,
            "error_category": None, "remedy": "",
            "cloud": cloud,
        }

    def prompt_choice(self, message: str, choices: list[str]) -> str | None:
        try:
            if self._backend == "inquirer":
                result = _inquirer.prompt([
                    _inquirer.List("choice", message=message, choices=choices),
                ])
                return result["choice"] if result else None

            if self._backend == "questionary":
                return _questionary.select(message, choices=choices).ask()

            print(f"\n  {message}")
            for i, c in enumerate(choices, 1):
                print(f"  [{i}] {c}")
            while True:
                ans = input(f"  Enter choice ({'/'.join(str(i) for i in range(1, len(choices) + 1))}): ").strip()
                if ans.isdigit() and 1 <= int(ans) <= len(choices):
                    return choices[int(ans) - 1]
                if ans.lower() in ("q", "quit", "cancel"):
                    return None
                print("  Invalid choice.")
        except (KeyboardInterrupt, EOFError):
            return None

    def prompt_input(self, message: str, default: str = "") -> str | None:
        try:
            if self._backend == "inquirer":
                result = _inquirer.prompt([
                    _inquirer.Text("value", message=message, default=default),
                ])
                return result["value"] if result else None

            if self._backend == "questionary":
                return _questionary.text(message, default=default).ask()

            hint = f" [{default}]" if default else ""
            ans = input(f"  {message}{hint}: ").strip()
            return ans if ans else default
        except (KeyboardInterrupt, EOFError):
            return None

    @staticmethod
    def _detect_cloud(command: list[str]) -> str:
        if not command:
            return "unknown"
        binary = command[0].lower()
        if binary == "aws":
            return "AWS"
        if binary == "az":
            return "Azure"
        return "unknown"

    def _parse_error(self, stderr: str, cloud: str) -> tuple[ErrorCategory, str]:
        patterns = _AWS_PATTERNS if cloud == "AWS" else _AZURE_PATTERNS if cloud == "Azure" else []
        for pattern, category, remedy in patterns:
            if pattern.search(stderr):
                return category, remedy
        return ErrorCategory.UNKNOWN, stderr[:500]

    @staticmethod
    def _close_streams(proc: subprocess.Popen) -> None:
        for stream in (proc.stdout, proc.stderr):
            if stream:
                try:
                    stream.close()
                except Exception:
                    pass
