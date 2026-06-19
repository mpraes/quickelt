#!/usr/bin/env python3
"""
Quickelt Pre-flight Checker
============================

Validates that the required cloud CLI tools are installed and authenticated
before the setup wizard proceeds with provisioning.
"""

import logging
import subprocess
import sys


class PreflightChecker:
    _PREFLIGHT = {
        "AWS": {
            "command": ["aws", "sts", "get-caller-identity"],
            "remedy": "aws configure",
        },
        "Azure": {
            "command": ["az", "account", "show"],
            "remedy": "az login",
        },
    }

    def __init__(self, logger: logging.Logger | None = None):
        self.log = logger or logging.getLogger("quickelt.preflight")

    def check(self, cloud: str) -> None:
        cfg = self._PREFLIGHT[cloud]
        cmd_str = " ".join(cfg["command"])
        self.log.info("Running pre-flight check for %s...", cloud)
        self.log.debug("Executing: %s", cmd_str)

        try:
            result = subprocess.run(
                cfg["command"],
                capture_output=True,
                text=True,
                timeout=15,
            )
            self.log.debug("CLI exit code: %d", result.returncode)
            self.log.debug("CLI stdout: %s", result.stdout.strip())
            self.log.debug("CLI stderr: %s", result.stderr.strip())
            if result.returncode != 0:
                self._fail(cloud, cfg["remedy"], result.stderr.strip())
            else:
                self.log.info("Pre-flight check passed for %s", cloud)
        except FileNotFoundError:
            self._fail(
                cloud,
                cfg["remedy"],
                f"CLI tool '{cfg['command'][0]}' not found. Install it and try again.",
            )
        except subprocess.TimeoutExpired:
            self._fail(cloud, cfg["remedy"], "Pre-flight check timed out.")
        except OSError as exc:
            self._fail(cloud, cfg["remedy"], str(exc))

    def _fail(self, cloud: str, remedy: str, detail: str) -> None:
        self.log.error("Pre-flight check FAILED for %s", cloud)
        if detail:
            self.log.error("  %s", detail)
        self.log.error("Authenticate first by running: %s", remedy)
        sys.exit(1)
