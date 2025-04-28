import logging

def setup_logger(name: str = "pipeline_logger") -> logging.Logger:
    """
    Configura e retorna um logger padrão.

    Args:
        name (str): Nome do logger.

    Returns:
        logging.Logger: Logger configurado.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger
