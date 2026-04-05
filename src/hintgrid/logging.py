"""Centralized logging configuration for HintGrid."""

from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hintgrid.config import HintGridSettings

# ANSI color codes for terminal output
COLORS = {
    "DEBUG": "\033[36m",  # Cyan
    "INFO": "\033[32m",  # Green
    "WARNING": "\033[33m",  # Yellow
    "ERROR": "\033[31m",  # Red
    "CRITICAL": "\033[35m",  # Magenta
    "RESET": "\033[0m",
}


class ColoredFormatter(logging.Formatter):
    """Formatter that adds colors to log levels for terminal output."""

    def __init__(self, fmt: str, datefmt: str | None = None, use_colors: bool = True) -> None:
        super().__init__(fmt, datefmt)
        self.use_colors = use_colors and sys.stderr.isatty()

    def format(self, record: logging.LogRecord) -> str:
        if self.use_colors:
            color = COLORS.get(record.levelname, COLORS["RESET"])
            reset = COLORS["RESET"]
            record.levelname = f"{color}{record.levelname}{reset}"
        return super().format(record)


def setup_logging(settings: HintGridSettings) -> None:
    """Configure logging based on settings.

    Sets up file handler with full format and console handler with compact format.
    Console output uses colors when writing to a terminal.

    Console handler shows only WARNING and above by default to avoid mixing
    with Rich progress bars. Use --verbose flag to see INFO/DEBUG on console.
    """
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)

    # File formatter - full format with timestamp and module name
    file_formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console formatter - compact format with colors
    console_formatter = ColoredFormatter(
        fmt="%(levelname)s: %(message)s",
        use_colors=True,
    )

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # File handler with full format - captures all logs at configured level
    file_handler = logging.FileHandler(settings.log_file)
    file_handler.setLevel(log_level)
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)

    # Console handler with compact colored format
    # Default to WARNING to avoid mixing with Rich progress bars
    # In verbose mode (DEBUG level), show all messages on console
    console_handler = logging.StreamHandler()
    console_level = log_level if log_level <= logging.DEBUG else logging.WARNING
    console_handler.setLevel(console_level)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # Suppress noisy third-party loggers
    logging.getLogger("neo4j").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("gensim").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the given name.

    Convenience function for consistent logger creation across modules.
    """
    return logging.getLogger(name)
