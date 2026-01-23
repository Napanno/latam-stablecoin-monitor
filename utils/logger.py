"""
Centralized logging configuration for LATAM Stablecoin Pipeline
"""

import logging
import os
from pathlib import Path
from datetime import datetime


def setup_logging(log_dir='./logs', log_level='INFO'):
    """
    Setup centralized logging configuration

    Args:
        log_dir (str): Directory for log files
        log_level (str): Logging level (DEBUG, INFO, WARNING, ERROR)

    Returns:
        logging.Logger: Configured root logger
    """
    # Create logs directory if it doesn't exist
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    # Generate log filename with today's date
    log_filename = log_path / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))

    # Clear any existing handlers
    root_logger.handlers.clear()

    # File handler (DEBUG level - everything)
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)

    # Console handler (WARNING level - only warnings and errors)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_formatter = logging.Formatter(
        '%(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_formatter)

    # Add handlers to root logger
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    return root_logger


def get_logger(name):
    """
    Get a logger instance for a specific module

    Args:
        name (str): Name of the logger (typically __name__)

    Returns:
        logging.Logger: Logger instance

    Example:
        from utils.logger import get_logger
        logger = get_logger(__name__)
        logger.info("Processing started")
    """
    return logging.getLogger(name)


def set_console_level(level='INFO'):
    """
    Change console logging level at runtime

    Args:
        level (str): New logging level (DEBUG, INFO, WARNING, ERROR)
    """
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
            handler.setLevel(getattr(logging, level.upper()))


# Initialize logging when module is imported
# Can be overridden by calling setup_logging() with custom parameters
if not logging.getLogger().handlers:
    setup_logging()
