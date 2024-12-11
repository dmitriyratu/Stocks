import logging
import sys

from tqdm.contrib.logging import logging_redirect_tqdm


def setup_logger(name, log_file, level=logging.INFO):
    """
    Set up a logger with the specified name and integrate it with tqdm automatically.

    Args:
        name (str): Name of the logger.
        log_file (Path): Path object for the log file.
        level (int): Logging level (e.g., logging.INFO, logging.DEBUG).

    Returns:
        logging.Logger: Configured logger instance.
    """
    # Create a custom logger
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(level)

        # Formatter for log messages
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        # File handler to write logs to a file
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Tqdm handler
        tqdm_handler = logging.StreamHandler(stream=sys.stdout)
        tqdm_handler.setFormatter(formatter)
        logger.addHandler(tqdm_handler)

        # Apply logging redirection automatically
        logging_redirect_tqdm(logger)

    # Prevent logs from propagating to the root logger
    logger.propagate = False

    return logger
