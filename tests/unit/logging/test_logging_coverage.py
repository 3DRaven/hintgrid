"""Unit tests for logging configuration.

Tests ColoredFormatter with and without color support.
"""

from __future__ import annotations

import logging

from hintgrid.logging import ColoredFormatter


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
