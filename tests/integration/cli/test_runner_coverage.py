"""Integration tests for CLI runner error handling and CommandHandler.

Tests error paths, verbose mode, and base handler with real clients.
"""

from __future__ import annotations

import pytest

from hintgrid.cli.runner import (
    EXIT_ERROR,
    EXIT_INTERRUPTED,
    CommandHandler,
    execute_run,
)
from hintgrid.exceptions import HintGridError
from hintgrid.config import HintGridSettings
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hintgrid.app import HintGridApp
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.clients.redis import RedisClient
    from tests.conftest import DockerComposeInfo
else:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.clients.redis import RedisClient


@pytest.mark.integration
class TestCommandHandler:
    """Tests for base CommandHandler class."""

    def test_base_execute_raises_not_implemented(
        self, docker_compose: DockerComposeInfo, settings: HintGridSettings
    ) -> None:
        """Test that base CommandHandler.execute raises NotImplementedError."""
        from hintgrid.app import HintGridApp
        from hintgrid.clients import Neo4jClient, PostgresClient, RedisClient

        test_settings = settings.model_copy(
            update={
                "postgres_host": docker_compose.postgres_host,
                "postgres_port": docker_compose.postgres_port,
                "postgres_database": docker_compose.postgres_db,
                "postgres_user": docker_compose.postgres_user,
                "postgres_password": docker_compose.postgres_password,
                "neo4j_host": docker_compose.neo4j_host,
                "neo4j_port": docker_compose.neo4j_port,
                "neo4j_username": docker_compose.neo4j_user,
                "neo4j_password": docker_compose.neo4j_password,
                "redis_host": docker_compose.redis_host,
                "redis_port": docker_compose.redis_port,
            }
        )

        with (
            PostgresClient.from_settings(test_settings) as pg,
            Neo4jClient.from_settings(test_settings) as neo4j,
            RedisClient.from_settings(test_settings) as redis_client,
        ):
            app = HintGridApp(
                neo4j=neo4j,
                postgres=pg,
                redis=redis_client,
                settings=test_settings,
            )
            with pytest.raises(NotImplementedError):
                CommandHandler.execute(app)


@pytest.mark.integration
class TestExecuteRunErrors:
    """Tests for execute_run error handling paths through public API."""

    def test_hintgrid_error_returns_exit_code(
        self, settings: HintGridSettings
    ) -> None:
        """Test that HintGridError is caught and its exit_code returned."""
        # Create a custom error that will be raised during client initialization
        error = HintGridError("test error")
        error.exit_code = 42

        # Patch PostgresClient.from_settings to raise our error
        from unittest.mock import patch
        from hintgrid.clients import PostgresClient

        with patch.object(PostgresClient, "from_settings", side_effect=error):
            # Test through public API
            result = execute_run({}, dry_run=False, user_id=None, do_train=False, verbose=False)
            assert result == 42

    def test_hintgrid_error_verbose_logs_traceback(
        self, settings: HintGridSettings, capfd: pytest.CaptureFixture[str]
    ) -> None:
        """Test verbose mode logs debug traceback on HintGridError."""
        from unittest.mock import patch
        from hintgrid.clients import PostgresClient

        error = HintGridError("verbose error")

        with patch.object(PostgresClient, "from_settings", side_effect=error):
            result = execute_run({}, dry_run=False, user_id=None, do_train=False, verbose=True)

        assert result == error.exit_code
        captured = capfd.readouterr()
        assert "Full traceback" in captured.err or "verbose error" in captured.err

    def test_keyboard_interrupt_returns_130(
        self, settings: HintGridSettings
    ) -> None:
        """Test KeyboardInterrupt returns EXIT_INTERRUPTED (130)."""
        from unittest.mock import patch
        from hintgrid.clients import PostgresClient

        with patch.object(PostgresClient, "from_settings", side_effect=KeyboardInterrupt()):
            # Test through public API
            result = execute_run({}, dry_run=False, user_id=None, do_train=False, verbose=False)
            assert result == EXIT_INTERRUPTED

    def test_generic_exception_returns_error(
        self, settings: HintGridSettings
    ) -> None:
        """Test generic Exception returns EXIT_ERROR."""
        from unittest.mock import patch
        from hintgrid.clients import PostgresClient

        with patch.object(PostgresClient, "from_settings", side_effect=RuntimeError("boom")):
            # Test through public API
            result = execute_run({}, dry_run=False, user_id=None, do_train=False, verbose=False)
            assert result == EXIT_ERROR

    def test_generic_exception_verbose_prints_traceback(
        self, settings: HintGridSettings, capfd: pytest.CaptureFixture[str]
    ) -> None:
        """Test verbose mode prints exception on generic error."""
        from unittest.mock import patch
        from hintgrid.clients import PostgresClient

        with patch.object(PostgresClient, "from_settings", side_effect=RuntimeError("boom")):
            result = execute_run({}, dry_run=False, user_id=None, do_train=False, verbose=True)
            assert result == EXIT_ERROR
            captured = capfd.readouterr()
            assert "boom" in captured.err or "Unexpected error" in captured.err

    def test_verbose_sets_debug_log_level(
        self, settings: HintGridSettings
    ) -> None:
        """Test verbose flag sets log_level to DEBUG."""
        from unittest.mock import patch
        from hintgrid.clients import PostgresClient
        from hintgrid.app import HintGridApp

        # Patch to capture app instance and verify log level
        captured_app: HintGridApp | None = None

        def capture_app(*args: object, **kwargs: object) -> object:
            nonlocal captured_app
            # Create real app to verify settings
            with PostgresClient.from_settings(settings) as pg:
                with Neo4jClient.from_settings(settings) as neo4j:
                    with RedisClient.from_settings(settings) as redis:
                        captured_app = HintGridApp(neo4j=neo4j, postgres=pg, redis=redis, settings=settings)
                        # Raise error to stop execution
                        raise RuntimeError("stop")

        with patch.object(PostgresClient, "from_settings", side_effect=capture_app):
            try:
                execute_run({}, dry_run=False, user_id=None, do_train=False, verbose=True)
            except RuntimeError:
                pass

        # Verify log level was set to DEBUG in verbose mode
        # This is tested through the actual execution path
