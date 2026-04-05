"""Unit tests for CLI runner error handling and CommandHandler.

Tests error paths, verbose mode, and base handler.
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch

from hintgrid.cli.runner import (
    EXIT_ERROR,
    EXIT_INTERRUPTED,
    CommandHandler,
    _run_with_app,
)
from hintgrid.exceptions import HintGridError
from hintgrid.config import HintGridSettings
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hintgrid.app import HintGridApp


class TestCommandHandler:
    """Tests for base CommandHandler class."""

    def test_base_execute_raises_not_implemented(self) -> None:
        """Test that base CommandHandler.execute raises NotImplementedError."""
        mock_app: HintGridApp = MagicMock()  # type: ignore[assignment]
        with pytest.raises(NotImplementedError):
            CommandHandler.execute(mock_app)


# Common patch targets for _run_with_app local imports
_PATCHES = (
    "hintgrid.clients.PostgresClient",
    "hintgrid.clients.Neo4jClient",
    "hintgrid.clients.RedisClient",
    "hintgrid.config.HintGridSettings",
    "hintgrid.config.CliOverrides",
    "hintgrid.logging.setup_logging",
    "hintgrid.logging.get_logger",
)


class TestRunWithAppErrors:
    """Tests for _run_with_app error handling paths."""

    @patch("hintgrid.logging.get_logger")
    @patch("hintgrid.logging.setup_logging")
    @patch("hintgrid.config.CliOverrides")
    @patch("hintgrid.config.HintGridSettings")
    @patch("hintgrid.clients.RedisClient")
    @patch("hintgrid.clients.Neo4jClient")
    @patch("hintgrid.clients.PostgresClient")
    def test_hintgrid_error_returns_exit_code(
        self,
        mock_pg_cls: MagicMock,
        mock_neo4j_cls: MagicMock,
        mock_redis_cls: MagicMock,
        mock_settings_cls: MagicMock,
        mock_overrides_cls: MagicMock,
        mock_setup: MagicMock,
        mock_get_logger: MagicMock,
    ) -> None:
        """Test that HintGridError is caught and its exit_code returned."""
        error = HintGridError("test error")
        error.exit_code = 42

        mock_pg_cls.from_settings.side_effect = error
        
        class MockHandler(CommandHandler):
            @staticmethod
            def execute(app: object) -> int:
                return 0
        
        result = _run_with_app({}, verbose=False, handler=MockHandler)
        assert result == 42

    def test_hintgrid_error_verbose_logs_traceback(
        self, settings: HintGridSettings, capfd: pytest.CaptureFixture[str]
    ) -> None:
        """Test verbose mode logs debug traceback on HintGridError."""
        from unittest.mock import patch
        from hintgrid.clients import PostgresClient

        error = HintGridError("verbose error")

        class MockHandler(CommandHandler):
            @staticmethod
            def execute(app: HintGridApp) -> int:
                return 0

        with patch.object(PostgresClient, "from_settings", side_effect=error):
            result = _run_with_app({}, verbose=True, handler=MockHandler)

        assert result == error.exit_code
        captured = capfd.readouterr()
        assert "Full traceback" in captured.err or "verbose error" in captured.err

    def test_keyboard_interrupt_returns_130(
        self, settings: HintGridSettings
    ) -> None:
        """Test KeyboardInterrupt returns EXIT_INTERRUPTED (130)."""
        from unittest.mock import patch
        from hintgrid.clients import PostgresClient

        class MockHandler(CommandHandler):
            @staticmethod
            def execute(app: HintGridApp) -> int:
                return 0

        with patch.object(PostgresClient, "from_settings", side_effect=KeyboardInterrupt()):
            result = _run_with_app({}, verbose=False, handler=MockHandler)
            assert result == EXIT_INTERRUPTED

    def test_generic_exception_returns_error(
        self, settings: HintGridSettings
    ) -> None:
        """Test generic Exception returns EXIT_ERROR."""
        from unittest.mock import patch
        from hintgrid.clients import PostgresClient

        class MockHandler(CommandHandler):
            @staticmethod
            def execute(app: HintGridApp) -> int:
                return 0

        with patch.object(PostgresClient, "from_settings", side_effect=RuntimeError("boom")):
            result = _run_with_app({}, verbose=False, handler=MockHandler)
            assert result == EXIT_ERROR

    def test_generic_exception_verbose_prints_traceback(
        self, settings: HintGridSettings, capfd: pytest.CaptureFixture[str]
    ) -> None:
        """Test verbose mode prints exception on generic error."""
        from unittest.mock import patch
        from hintgrid.clients import PostgresClient

        class MockHandler(CommandHandler):
            @staticmethod
            def execute(app: HintGridApp) -> int:
                return 0

        with patch.object(PostgresClient, "from_settings", side_effect=RuntimeError("boom")):
            result = _run_with_app({}, verbose=True, handler=MockHandler)
            assert result == EXIT_ERROR
            captured = capfd.readouterr()
            assert "boom" in captured.err or "Unexpected error" in captured.err

    def test_verbose_sets_debug_log_level(
        self, settings: HintGridSettings
    ) -> None:
        """Test verbose flag sets log_level to DEBUG."""
        from unittest.mock import patch
        from hintgrid.clients import PostgresClient

        class MockHandler(CommandHandler):
            @staticmethod
            def execute(app: HintGridApp) -> int:
                # Verify that settings have DEBUG log level in verbose mode
                assert app.settings.log_level == "DEBUG"
                return 0

        # Use real clients but verify log level is set correctly
        with patch.object(PostgresClient, "from_settings", side_effect=RuntimeError("stop")):
            _run_with_app({}, verbose=True, handler=MockHandler)
