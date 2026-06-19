import pytest

from setup.setup_registry import (
    get_setup_dir,
    get_setup_env_path,
    get_setup_terraform_workspace,
    list_setups,
    normalize_setup_name,
)


class TestNormalizeSetupName:
    def test_basic_slug(self):
        assert normalize_setup_name("My Company Prod") == "my-company-prod"

    def test_underscores(self):
        assert normalize_setup_name("skco_data_lake") == "skco-data-lake"

    def test_invalid_starts_with_digit(self):
        with pytest.raises(ValueError):
            normalize_setup_name("1bad-name")

    def test_invalid_too_short(self):
        with pytest.raises(ValueError):
            normalize_setup_name("a")


class TestSetupPaths:
    def test_paths_under_setups_root(self, tmp_path, monkeypatch):
        monkeypatch.setattr("setup.setup_registry.SETUPS_ROOT", tmp_path / "setups")
        name = "acme-dev"
        assert get_setup_dir(name) == tmp_path / "setups" / "acme-dev"
        assert get_setup_env_path(name) == tmp_path / "setups" / "acme-dev" / ".env"
        assert get_setup_terraform_workspace(name) == tmp_path / "setups" / "acme-dev" / "terraform"


class TestListSetups:
    def test_lists_only_dirs_with_env(self, tmp_path, monkeypatch):
        monkeypatch.setattr("setup.setup_registry.SETUPS_ROOT", tmp_path)
        (tmp_path / "alpha").mkdir()
        (tmp_path / "alpha" / ".env").write_text("SETUP_NAME=alpha\n", encoding="utf-8")
        (tmp_path / "beta").mkdir()

        assert list_setups() == ["alpha"]
