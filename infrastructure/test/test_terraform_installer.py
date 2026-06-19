import logging
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from setup.terraform_installer import (
    _install_terraform,
    _terraform_platform_slug,
    ensure_terraform,
)

_TEST_LOGGER = logging.getLogger("test.terraform_installer")


class TestTerraformPlatformSlug:
    @patch("setup.terraform_installer.platform.system", return_value="Linux")
    @patch("setup.terraform_installer.platform.machine", return_value="x86_64")
    def test_linux_amd64(self, mock_machine, mock_system):
        assert _terraform_platform_slug() == "linux_amd64"

    @patch("setup.terraform_installer.platform.system", return_value="Darwin")
    @patch("setup.terraform_installer.platform.machine", return_value="arm64")
    def test_darwin_arm64(self, mock_machine, mock_system):
        assert _terraform_platform_slug() == "darwin_arm64"

    @patch("setup.terraform_installer.platform.system", return_value="Windows")
    @patch("setup.terraform_installer.platform.machine", return_value="AMD64")
    def test_unsupported_os(self, mock_machine, mock_system):
        with pytest.raises(RuntimeError, match="not supported"):
            _terraform_platform_slug()


class TestEnsureTerraform:
    @patch("setup.terraform_installer.shutil.which", return_value="/usr/bin/terraform")
    def test_already_installed(self, mock_which):
        ensure_terraform(_TEST_LOGGER)

    @patch("setup.terraform_installer.shutil.which", return_value=None)
    @patch("setup.terraform_installer.platform.system", return_value="Windows")
    def test_unsupported_platform_exits(self, mock_system, mock_which):
        with pytest.raises(SystemExit) as exc_info:
            ensure_terraform(_TEST_LOGGER)
        assert exc_info.value.code == 1

    @patch("setup.terraform_installer.shutil.which", side_effect=[None, "/home/user/.local/bin/terraform"])
    @patch("setup.terraform_installer._install_terraform")
    @patch("setup.terraform_installer._confirm_install", return_value=True)
    @patch("setup.terraform_installer.platform.system", return_value="Linux")
    def test_install_on_confirm(self, mock_system, mock_confirm, mock_install, mock_which):
        ensure_terraform(_TEST_LOGGER)
        mock_install.assert_called_once()

    @patch("setup.terraform_installer.shutil.which", return_value=None)
    @patch("setup.terraform_installer._confirm_install", return_value=False)
    @patch("setup.terraform_installer.platform.system", return_value="Linux")
    def test_user_declines_install(self, mock_system, mock_confirm, mock_which):
        with pytest.raises(SystemExit) as exc_info:
            ensure_terraform(_TEST_LOGGER)
        assert exc_info.value.code == 1

    @patch("setup.terraform_installer.shutil.which", side_effect=[None, "/home/user/.local/bin/terraform"])
    @patch("setup.terraform_installer._install_terraform")
    @patch("setup.terraform_installer.platform.system", return_value="Linux")
    def test_auto_install_env_skips_confirm(self, mock_system, mock_install, mock_which):
        with patch.dict("os.environ", {"QUICKELT_TERRAFORM_AUTO_INSTALL": "1"}):
            ensure_terraform(_TEST_LOGGER, confirm=True)
        mock_install.assert_called_once()

    @patch("setup.terraform_installer.shutil.which", side_effect=[None, "/usr/bin/terraform"])
    @patch("setup.terraform_installer._install_terraform")
    @patch("setup.terraform_installer._confirm_install")
    @patch("setup.terraform_installer.platform.system", return_value="Linux")
    def test_confirm_false_still_installs(self, mock_system, mock_confirm, mock_install, mock_which):
        ensure_terraform(_TEST_LOGGER, confirm=False)
        mock_install.assert_called_once()
        mock_confirm.assert_not_called()


class TestInstallTerraform:
    @patch("setup.terraform_installer._run")
    @patch("setup.terraform_installer._terraform_platform_slug", return_value="linux_amd64")
    def test_installs_to_local_bin(self, mock_slug, mock_run, tmp_path):
        binary = tmp_path / "terraform"

        def fake_run(cmd, cwd, log):
            if cmd[0] == "unzip":
                binary.write_text("bin", encoding="utf-8")

        mock_run.side_effect = fake_run
        home = tmp_path / "home"

        with patch("setup.terraform_installer.Path.home", return_value=home):
            _install_terraform(str(tmp_path), _TEST_LOGGER)

        dest = home / ".local" / "bin" / "terraform"
        assert dest.exists()
        assert dest.read_text(encoding="utf-8") == "bin"
