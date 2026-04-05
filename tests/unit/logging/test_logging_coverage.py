"""Unit tests for logging configuration.

Tests ColoredFormatter with and without color support.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from hintgrid.config import HintGridSettings
from hintgrid.logging import ColoredFormatter, setup_logging

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


class TestColoredFormatter:
    """Tests for ColoredFormatter class."""

    def test_format_with_colors_disabled(self) -> None:
        """Test formatting when use_colors is explicitly False."""
        formatter = ColoredFormatter(
            fmt="%(levelname)s: %(message)s",
            use_colors=False,
        )

        record = logging.LogRecord(
            name="test",
            level=logging.WARNING,
            pathname="test.py",
            lineno=1,
            msg="Test warning message",
            args=(),
            exc_info=None,
        )

        result = formatter.format(record)
        # Should NOT contain ANSI color codes
        assert "\033[" not in result
        assert "WARNING" in result
        assert "Test warning message" in result

    def test_format_with_colors_enabled_but_not_tty(self) -> None:
        """Test that colors are disabled when stderr is not a TTY.

        In test environment, stderr is typically not a TTY,
        so use_colors=True should still result in no colors.
        """
        formatter = ColoredFormatter(
            fmt="%(levelname)s: %(message)s",
            use_colors=True,
        )

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test info message",
            args=(),
            exc_info=None,
        )

        result = formatter.format(record)
        # In test (non-TTY), should not have colors even with use_colors=True
        assert "Test info message" in result

    def test_format_with_colors_on_tty(self) -> None:
        """Test that colors ARE applied when stderr IS a TTY."""
        from unittest.mock import patch

        # Force isatty() to return True so colors are applied
        with patch("sys.stderr") as mock_stderr:
            mock_stderr.isatty.return_value = True
            formatter = ColoredFormatter(
                fmt="%(levelname)s: %(message)s",
                use_colors=True,
            )

        assert formatter.use_colors is True

        record = logging.LogRecord(
            name="test",
            level=logging.WARNING,
            pathname="test.py",
            lineno=1,
            msg="Colored warning",
            args=(),
            exc_info=None,
        )

        result = formatter.format(record)
        # Should contain ANSI color codes
        assert "\033[" in result
        assert "Colored warning" in result

    def test_format_preserves_message(self) -> None:
        """Test that formatting preserves the message content."""
        formatter = ColoredFormatter(
            fmt="%(levelname)s: %(message)s",
            use_colors=False,
        )

        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="test.py",
            lineno=1,
            msg="Something went wrong: %s",
            args=("details",),
            exc_info=None,
        )

        result = formatter.format(record)
        assert "ERROR" in result
        assert "Something went wrong: details" in result


def _console_stream_handlers(root: logging.Logger) -> list[logging.Handler]:
    """Handlers that are StreamHandler but not FileHandler (console only)."""
    return [
        h
        for h in root.handlers
        if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
    ]


class TestSetupLogging:
    """Tests for setup_logging console handler level."""

    def test_console_handler_level_matches_info(self, tmp_path: Path) -> None:
        """Console StreamHandler uses INFO when log_level is INFO."""
        log_path = tmp_path / "app.log"
        settings = HintGridSettings.model_validate({"log_file": str(log_path), "log_level": "INFO"})
        setup_logging(settings)
        root = logging.getLogger()
        consoles = _console_stream_handlers(root)
        assert len(consoles) == 1
        assert consoles[0].level == logging.INFO

    def test_console_handler_level_matches_debug(self, tmp_path: Path) -> None:
        """Console StreamHandler uses DEBUG when log_level is DEBUG."""
        log_path = tmp_path / "app.log"
        settings = HintGridSettings.model_validate({"log_file": str(log_path), "log_level": "DEBUG"})
        setup_logging(settings)
        root = logging.getLogger()
        consoles = _console_stream_handlers(root)
        assert len(consoles) == 1
        assert consoles[0].level == logging.DEBUG

    def test_hintgrid_progress_info_reaches_stderr(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        """Plain progress logger INFO appears on stderr when log_level is INFO."""
        settings = HintGridSettings.model_validate({"log_file": str(tmp_path / "app.log"), "log_level": "INFO"})
        setup_logging(settings)
        logging.getLogger("hintgrid.progress").info("progress 50% (1/2) task")
        err = capsys.readouterr().err
        assert "progress 50%" in err

    def test_hintgrid_progress_info_hidden_when_log_level_warning(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """INFO progress lines follow global log_level (hidden when WARNING)."""
        settings = HintGridSettings.model_validate({"log_file": str(tmp_path / "app.log"), "log_level": "WARNING"})
        setup_logging(settings)
        logging.getLogger("hintgrid.progress").info("should not appear on console")
        err = capsys.readouterr().err
        assert "should not appear on console" not in err
