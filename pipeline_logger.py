"""
Pipeline Logging Framework.

Provides a unified logger that outputs formatted messages to both
the console and persistent daily log files in the 'logs/' directory.
"""

import sys
import logging
from pathlib import Path
from datetime import datetime

current_dir = Path(__file__).resolve().parent


def setup_logger(script_name: str) -> logging.Logger:
    """Configures and returns a logger instance for a given pipeline script."""
    logs_dir = current_dir / "logs"
    logs_dir.mkdir(exist_ok=True)

    today_str = datetime.now().strftime("%Y-%m-%d")
    log_file_path = logs_dir / f"pipeline_{today_str}.log"

    logger = logging.getLogger(script_name)
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        return logger

    formatter = logging.Formatter(
        fmt="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger