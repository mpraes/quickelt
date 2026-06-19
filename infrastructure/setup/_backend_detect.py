def detect_prompt_backend() -> str:
    try:
        import inquirer

        return "inquirer"
    except ImportError:
        pass

    try:
        import questionary

        return "questionary"
    except ImportError:
        pass

    return "builtin"
