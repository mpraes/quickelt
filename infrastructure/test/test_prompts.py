import getpass
import logging
import string
from unittest.mock import MagicMock, patch

import pytest

from setup.prompts import (
    BuiltinBackend,
    InquirerBackend,
    PromptBackend,
    QuestionaryBackend,
    create_prompt_backend,
    _DEFAULT_LAYERS,
    _PG_STRATEGY_LOCAL_VM,
    _PG_STRATEGY_MANAGED_CLOUD,
)

_TEST_LOGGER = logging.getLogger("test.prompts")


class TestEmptyDw:
    def test_empty_dw_structure(self):
        dw = PromptBackend._empty_dw()
        assert dw["gold_external_db"] is False
        assert dw["pg_strategy"] is None
        assert dw["install_local_postgres"] is False
        assert dw["managed_cloud_choice"] is None
        assert dw["dw_host"] is None
        assert dw["dw_port"] == "5432"
        assert dw["dw_database"] == "quickelt_db"
        assert dw["dw_username"] == "quickelt"
        assert dw["dw_password"] is None


class TestGeneratePassword:
    def test_default_length(self):
        pw = PromptBackend._generate_password()
        assert len(pw) == 24

    def test_custom_length(self):
        pw = PromptBackend._generate_password(32)
        assert len(pw) == 32

    def test_characters_from_alphabet(self):
        alphabet = set(string.ascii_letters + string.digits)
        pw = PromptBackend._generate_password(100)
        assert all(c in alphabet for c in pw)

    def test_uniqueness(self):
        pws = {PromptBackend._generate_password() for _ in range(20)}
        assert len(pws) == 20


class TestApplyLocalVmStrategy:
    def _make_backend(self):
        return BuiltinBackend(logger=_TEST_LOGGER)

    def test_dedicated_vm_sets_install_local_postgres(self):
        backend = self._make_backend()
        dw = PromptBackend._empty_dw()
        compute = {"compute": "Dedicated VM"}

        backend._apply_local_vm_strategy(dw, compute)

        assert dw["pg_strategy"] == "local_vm"
        assert dw["dw_host"] == "localhost"
        assert dw["install_local_postgres"] is True
        assert dw["dw_password"] is not None
        assert len(dw["dw_password"]) == 24

    def test_non_dedicated_vm_warns(self):
        backend = self._make_backend()
        dw = PromptBackend._empty_dw()
        compute = {"compute": "Local Machine"}

        backend._apply_local_vm_strategy(dw, compute)

        assert dw["pg_strategy"] == "local_vm"
        assert dw["dw_host"] == "localhost"
        assert dw["install_local_postgres"] is False
        assert dw["dw_password"] is not None

    def test_serverless_compute_no_install(self):
        backend = self._make_backend()
        dw = PromptBackend._empty_dw()
        compute = {"compute": "Serverless/PaaS"}

        backend._apply_local_vm_strategy(dw, compute)

        assert dw["install_local_postgres"] is False


class TestBuiltinBackendAskCloud:
    @patch("builtins.input", return_value="1")
    def test_select_aws(self, mock_input):
        backend = BuiltinBackend(logger=_TEST_LOGGER)
        assert backend.ask_cloud() == "AWS"

    @patch("builtins.input", return_value="2")
    def test_select_azure(self, mock_input):
        backend = BuiltinBackend(logger=_TEST_LOGGER)
        assert backend.ask_cloud() == "Azure"

    @patch("builtins.input", side_effect=["9", "1"])
    def test_invalid_then_valid(self, mock_input):
        backend = BuiltinBackend(logger=_TEST_LOGGER)
        assert backend.ask_cloud() == "AWS"


class TestBuiltinBackendAskStorage:
    @patch("builtins.input", side_effect=["1", "existing-bucket"])
    def test_existing_storage(self, mock_input):
        backend = BuiltinBackend(logger=_TEST_LOGGER)
        result = backend.ask_storage()

        assert result["existing"] is True
        assert result["name"] == "existing-bucket"
        assert result["layers"] == []

    @patch("builtins.input", side_effect=["2", "new-bucket", ""])
    def test_new_storage_default_layers(self, mock_input):
        backend = BuiltinBackend(logger=_TEST_LOGGER)
        result = backend.ask_storage()

        assert result["existing"] is False
        assert result["name"] == "new-bucket"
        assert result["layers"] == _DEFAULT_LAYERS

    @patch("builtins.input", side_effect=["2", "", "bronze,silver"])
    def test_new_storage_default_name_custom_layers(self, mock_input):
        backend = BuiltinBackend(logger=_TEST_LOGGER)
        result = backend.ask_storage()

        assert result["existing"] is False
        assert result["name"] == "quickelt-data-lake"
        assert result["layers"] == ["bronze", "silver"]


class TestBuiltinBackendAskCompute:
    @patch("builtins.input", return_value="1")
    def test_local_machine(self, mock_input):
        backend = BuiltinBackend(logger=_TEST_LOGGER)
        result = backend.ask_compute()
        assert result["compute"] == "Local Machine"
        assert result["bootstrap_vm"] is False

    @patch("builtins.input", side_effect=["2", "y"])
    def test_dedicated_vm_bootstrap_yes(self, mock_input):
        backend = BuiltinBackend(logger=_TEST_LOGGER)
        result = backend.ask_compute()
        assert result["compute"] == "Dedicated VM"
        assert result["bootstrap_vm"] is True

    @patch("builtins.input", side_effect=["2", "n"])
    def test_dedicated_vm_bootstrap_no(self, mock_input):
        backend = BuiltinBackend(logger=_TEST_LOGGER)
        result = backend.ask_compute()
        assert result["compute"] == "Dedicated VM"
        assert result["bootstrap_vm"] is False

    @patch("builtins.input", side_effect=["2", ""])
    def test_dedicated_vm_bootstrap_default_yes(self, mock_input):
        backend = BuiltinBackend(logger=_TEST_LOGGER)
        result = backend.ask_compute()
        assert result["bootstrap_vm"] is True

    @patch("builtins.input", return_value="3")
    def test_serverless(self, mock_input):
        backend = BuiltinBackend(logger=_TEST_LOGGER)
        result = backend.ask_compute()
        assert result["compute"] == "Serverless/PaaS"


class TestBuiltinBackendAskDw:
    @patch("builtins.input", return_value="2")
    def test_no_external_db(self, mock_input):
        backend = BuiltinBackend(logger=_TEST_LOGGER)
        result = backend.ask_dw({"compute": "Local Machine"})

        assert result["gold_external_db"] is False

    @patch("builtins.input", side_effect=["1", "1"])
    def test_local_vm_strategy(self, mock_input):
        backend = BuiltinBackend(logger=_TEST_LOGGER)
        result = backend.ask_dw({"compute": "Dedicated VM"})

        assert result["gold_external_db"] is True
        assert result["pg_strategy"] == "local_vm"
        assert result["dw_host"] == "localhost"
        assert result["install_local_postgres"] is True

    @patch("builtins.input", side_effect=["1", "2", "1"])
    def test_managed_cloud_provision_new(self, mock_input):
        backend = BuiltinBackend(logger=_TEST_LOGGER)
        result = backend.ask_dw({"compute": "Dedicated VM"})

        assert result["gold_external_db"] is True
        assert result["pg_strategy"] == "managed_cloud"
        assert result["managed_cloud_choice"] == "provision_new"
        assert result["dw_password"] is not None
        assert len(result["dw_password"]) == 24

    @patch("builtins.input", side_effect=["1", "2", "2", "myhost.example.com", "5433", "admin", "s3cret"])
    @patch("setup.prompts.getpass.getpass", return_value="s3cret")
    def test_managed_cloud_connect_existing(self, mock_getpass, mock_input):
        backend = BuiltinBackend(logger=_TEST_LOGGER)
        result = backend.ask_dw({"compute": "Local Machine"})

        assert result["gold_external_db"] is True
        assert result["pg_strategy"] == "managed_cloud"
        assert result["managed_cloud_choice"] == "connect_existing"
        assert result["dw_host"] == "myhost.example.com"
        assert result["dw_port"] == "5433"
        assert result["dw_username"] == "admin"
        assert result["dw_password"] == "s3cret"


class TestCreatePromptBackend:
    @patch("setup.prompts._AVAILABLE", "builtin")
    def test_factory_builtin(self):
        backend = create_prompt_backend()
        assert isinstance(backend, BuiltinBackend)

    @patch("setup.prompts._AVAILABLE", "inquirer")
    def test_factory_inquirer(self):
        backend = create_prompt_backend()
        assert isinstance(backend, InquirerBackend)

    @patch("setup.prompts._AVAILABLE", "questionary")
    def test_factory_questionary(self):
        backend = create_prompt_backend()
        assert isinstance(backend, QuestionaryBackend)

    def test_factory_with_logger(self):
        import logging

        backend = create_prompt_backend(logger=logging.getLogger("test"))
        assert backend.log.name == "test"


class TestInquirerBackendStructure:
    def test_ask_cloud_returns_string(self):
        backend = InquirerBackend(logger=_TEST_LOGGER)
        assert hasattr(backend, "ask_cloud")
        assert hasattr(backend, "ask_storage")
        assert hasattr(backend, "ask_compute")
        assert hasattr(backend, "ask_dw")

    def test_ask_cloud_mocked(self):
        import importlib
        try:
            inquirer_mod = importlib.import_module("inquirer")
        except ImportError:
            pytest.skip("inquirer not installed")

        backend = InquirerBackend(logger=_TEST_LOGGER)
        with patch.object(inquirer_mod, "prompt", return_value={"cloud": "AWS"}):
            result = backend.ask_cloud()
        assert result == "AWS"

    def test_ask_storage_new_mocked(self):
        import importlib
        try:
            inquirer_mod = importlib.import_module("inquirer")
        except ImportError:
            pytest.skip("inquirer not installed")

        backend = InquirerBackend(logger=_TEST_LOGGER)
        with patch.object(inquirer_mod, "prompt", side_effect=[
            {"has_existing": "No"},
            {"name": "test-lake"},
            {"layers": ["bronze", "gold"]},
        ]):
            result = backend.ask_storage()
        assert result["existing"] is False
        assert result["name"] == "test-lake"
        assert "bronze" in result["layers"]

    def test_ask_storage_existing_mocked(self):
        import importlib
        try:
            inquirer_mod = importlib.import_module("inquirer")
        except ImportError:
            pytest.skip("inquirer not installed")

        backend = InquirerBackend(logger=_TEST_LOGGER)
        with patch.object(inquirer_mod, "prompt", side_effect=[
            {"has_existing": "Yes"},
            {"name": "existing-bucket"},
        ]):
            result = backend.ask_storage()
        assert result["existing"] is True
        assert result["name"] == "existing-bucket"

    def test_ask_compute_local_mocked(self):
        import importlib
        try:
            inquirer_mod = importlib.import_module("inquirer")
        except ImportError:
            pytest.skip("inquirer not installed")

        backend = InquirerBackend(logger=_TEST_LOGGER)
        with patch.object(inquirer_mod, "prompt", side_effect=[
            {"compute": "Local Machine"},
        ]):
            result = backend.ask_compute()
        assert result["compute"] == "Local Machine"
        assert result["bootstrap_vm"] is False

    def test_ask_dw_no_external_mocked(self):
        import importlib
        try:
            inquirer_mod = importlib.import_module("inquirer")
        except ImportError:
            pytest.skip("inquirer not installed")

        backend = InquirerBackend(logger=_TEST_LOGGER)
        with patch.object(inquirer_mod, "prompt", side_effect=[
            {"gold_external": "No"},
        ]):
            result = backend.ask_dw({"compute": "Local Machine"})
        assert result["gold_external_db"] is False


class TestQuestionaryBackendStructure:
    def test_ask_cloud_returns_string(self):
        backend = QuestionaryBackend(logger=_TEST_LOGGER)
        assert hasattr(backend, "ask_cloud")
        assert hasattr(backend, "ask_storage")
        assert hasattr(backend, "ask_compute")
        assert hasattr(backend, "ask_dw")

    def test_ask_cloud_mocked(self):
        import importlib
        try:
            questionary_mod = importlib.import_module("questionary")
        except ImportError:
            pytest.skip("questionary not installed")

        backend = QuestionaryBackend(logger=_TEST_LOGGER)
        mock_select = MagicMock()
        mock_select.ask.return_value = "Azure"
        with patch.object(questionary_mod, "select", return_value=mock_select):
            result = backend.ask_cloud()
        assert result == "Azure"

    def test_ask_storage_existing_mocked(self):
        import importlib
        try:
            questionary_mod = importlib.import_module("questionary")
        except ImportError:
            pytest.skip("questionary not installed")

        backend = QuestionaryBackend(logger=_TEST_LOGGER)
        mock_existing = MagicMock()
        mock_existing.ask.return_value = "Yes"
        mock_name = MagicMock()
        mock_name.ask.return_value = "my-existing-bucket"

        with patch.object(questionary_mod, "select", return_value=mock_existing), \
             patch.object(questionary_mod, "text", return_value=mock_name):
            result = backend.ask_storage()
        assert result["existing"] is True
        assert result["name"] == "my-existing-bucket"

    def test_ask_storage_new_mocked(self):
        import importlib
        try:
            questionary_mod = importlib.import_module("questionary")
        except ImportError:
            pytest.skip("questionary not installed")

        backend = QuestionaryBackend(logger=_TEST_LOGGER)
        mock_existing_q = MagicMock()
        mock_existing_q.ask.return_value = "No"
        mock_name_q = MagicMock()
        mock_name_q.ask.return_value = "new-lake"
        mock_layers_q = MagicMock()
        mock_layers_q.ask.return_value = ["bronze", "silver"]

        with patch.object(questionary_mod, "select", return_value=mock_existing_q), \
             patch.object(questionary_mod, "text", return_value=mock_name_q), \
             patch.object(questionary_mod, "checkbox", return_value=mock_layers_q):
            result = backend.ask_storage()
        assert result["existing"] is False
        assert result["name"] == "new-lake"
