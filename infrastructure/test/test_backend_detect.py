from unittest.mock import patch

from setup._backend_detect import detect_prompt_backend


class TestDetectPromptBackend:
    def test_always_returns_valid_string(self):
        result = detect_prompt_backend()
        assert result in ("inquirer", "questionary", "builtin")

    def test_builtin_fallback(self):
        with patch("builtins.__import__", side_effect=ImportError("nope")):
            assert detect_prompt_backend() == "builtin"

    def test_inquirer_detected(self):
        import importlib
        try:
            importlib.import_module("inquirer")
        except ImportError:
            return
        assert detect_prompt_backend() == "inquirer"
