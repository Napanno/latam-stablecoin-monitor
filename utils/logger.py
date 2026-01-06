"""
Centralized logging configuration
"""

import logging
from pathlib import Path
from datetime import datetime
import yaml
import sys


def setup_logger(name: str, config_path: str = 'config.yaml') -> logging.Logger:
    """
    Setup logger with configuration from config.yaml

    Args:
        name: Logger name (usually __name__)
        config_path: Path to config.yaml

    Returns:
        Configured logger instance
    """
    # Load config
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    log_config = config['logging']
    log_dir = Path(config['paths']['logs'])
    log_dir.mkdir(parents=True, exist_ok=True)

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_config['level']))

    # Remove existing handlers
    logger.handlers.clear()

    # Console handler - ONLY WARNINGS AND ERRORS
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.WARNING)  # ← CHANGED: Was logging.INFO
    console_formatter = logging.Formatter(log_config['format'])
    console_handler.setFormatter(console_formatter)

    # Force UTF-8 for Windows
    if sys.platform == 'win32':
        try:
            sys.stdout.reconfigure(encoding='utf-8')
        except AttributeError:
            pass

    logger.addHandler(console_handler)

    # File handler - Keep DEBUG for detailed logs
    log_file = log_dir / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(log_config['format'])
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    return logger
