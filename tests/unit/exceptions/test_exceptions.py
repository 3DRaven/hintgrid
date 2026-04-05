"""Unit tests for HintGrid custom exceptions.

Tests verify exception messages, hints, and exit codes
without requiring any external dependencies.
"""

from hintgrid.exceptions import (
    ConfigurationError,
    ConnectionError,
    GDSNotAvailableError,
    HintGridError,
    Neo4jConnectionError,
    PipelineError,
    PostgresConnectionError,
    RedisConnectionError,
)


class TestHintGridError:
    """Tests for base HintGridError exception."""

    def test_message_only(self) -> None:
        """Test exception with message only."""
        exc = HintGridError("Something went wrong")

        assert exc.message == "Something went wrong"
        assert exc.hint is None
        assert str(exc) == "Something went wrong"
        assert exc.exit_code == 1

    def test_message_with_hint(self) -> None:
        """Test exception with message and hint."""
        exc = HintGridError("Failed to process", hint="Try again later")

        assert exc.message == "Failed to process"
        assert exc.hint == "Try again later"
        assert str(exc) == "Failed to process\nHint: Try again later"

    def test_default_exit_code(self) -> None:
        """Test that default exit code is 1."""
        exc = HintGridError("Error")
        assert exc.exit_code == 1


class TestConnectionError:
    """Tests for ConnectionError base class."""

    def test_exit_code(self) -> None:
        """Test that ConnectionError has exit code 2."""
        exc = ConnectionError("Connection failed")
        assert exc.exit_code == 2


class TestNeo4jConnectionError:
    """Tests for Neo4jConnectionError exception."""

    def test_basic_message(self) -> None:
        """Test Neo4j error with host and port only."""
        exc = Neo4jConnectionError(host="localhost", port=7687)

        assert "Cannot connect to Neo4j at localhost:7687" in str(exc)
        assert exc.host == "localhost"
        assert exc.port == 7687
        assert exc.original_error is None
        assert exc.exit_code == 2

    def test_with_original_error(self) -> None:
        """Test Neo4j error with original exception."""
        original = Exception("Connection refused")
        exc = Neo4jConnectionError(
            host="neo4j.example.com",
            port=7687,
            original_error=original,
        )

        assert "Cannot connect to Neo4j at neo4j.example.com:7687" in str(exc)
        assert "Connection refused" in str(exc)
        assert exc.original_error is original

    def test_hint_content(self) -> None:
        """Test that hint mentions configuration settings."""
        exc = Neo4jConnectionError(host="localhost", port=7687)

        assert exc.hint is not None
        assert "HINTGRID_NEO4J_HOST" in exc.hint
        assert "HINTGRID_NEO4J_PORT" in exc.hint


class TestPostgresConnectionError:
    """Tests for PostgresConnectionError exception."""

    def test_basic_message(self) -> None:
        """Test Postgres error with host, port, and database."""
        exc = PostgresConnectionError(
            host="localhost",
            port=5432,
            database="mastodon",
        )

        assert "Cannot connect to PostgreSQL at localhost:5432/mastodon" in str(exc)
        assert exc.host == "localhost"
        assert exc.port == 5432
        assert exc.database == "mastodon"
        assert exc.original_error is None
        assert exc.exit_code == 2

    def test_with_password_error(self) -> None:
        """Test Postgres error with password-related original error."""
        original = Exception("password authentication failed for user 'test'")
        exc = PostgresConnectionError(
            host="localhost",
            port=5432,
            database="test_db",
            original_error=original,
        )

        assert exc.hint is not None
        assert "HINTGRID_POSTGRES_USER" in exc.hint
        assert "HINTGRID_POSTGRES_PASSWORD" in exc.hint

    def test_with_connection_refused_error(self) -> None:
        """Test Postgres error with connection refused."""
        original = Exception("could not connect to server: connection refused")
        exc = PostgresConnectionError(
            host="localhost",
            port=5432,
            database="test_db",
            original_error=original,
        )

        assert exc.hint is not None
        assert "HINTGRID_POSTGRES_HOST" in exc.hint or "HINTGRID_POSTGRES_PORT" in exc.hint

    def test_with_database_not_exists_error(self) -> None:
        """Test Postgres error when database does not exist."""
        original = Exception('database "nonexistent" does not exist')
        exc = PostgresConnectionError(
            host="localhost",
            port=5432,
            database="nonexistent",
            original_error=original,
        )

        assert exc.hint is not None
        assert "HINTGRID_POSTGRES_DATABASE" in exc.hint

    def test_with_authentication_error(self) -> None:
        """Test Postgres error with authentication failure."""
        original = Exception("authentication failed")
        exc = PostgresConnectionError(
            host="localhost",
            port=5432,
            database="test_db",
            original_error=original,
        )

        assert exc.hint is not None
        assert "Authentication failed" in exc.hint

    def test_with_generic_error_no_pattern_match(self) -> None:
        """Test Postgres error with generic error not matching any hint pattern."""
        original = Exception("timeout after 30 seconds")
        exc = PostgresConnectionError(
            host="localhost",
            port=5432,
            database="test_db",
            original_error=original,
        )

        assert "timeout after 30 seconds" in str(exc)
        assert exc.hint is not None
        assert "Check that PostgreSQL is running" in exc.hint


class TestRedisConnectionError:
    """Tests for RedisConnectionError exception."""

    def test_basic_message(self) -> None:
        """Test Redis error with host and port only."""
        exc = RedisConnectionError(host="localhost", port=6379)

        assert "Cannot connect to Redis at localhost:6379" in str(exc)
        assert exc.host == "localhost"
        assert exc.port == 6379
        assert exc.original_error is None
        assert exc.exit_code == 2

    def test_with_original_error(self) -> None:
        """Test Redis error with original exception."""
        original = Exception("Connection timed out")
        exc = RedisConnectionError(
            host="redis.example.com",
            port=6379,
            original_error=original,
        )

        assert "Cannot connect to Redis at redis.example.com:6379" in str(exc)
        assert "Connection timed out" in str(exc)
        assert exc.original_error is original

    def test_hint_content(self) -> None:
        """Test that hint mentions configuration settings."""
        exc = RedisConnectionError(host="localhost", port=6379)

        assert exc.hint is not None
        assert "HINTGRID_REDIS_HOST" in exc.hint
        assert "HINTGRID_REDIS_PORT" in exc.hint
        assert "HINTGRID_REDIS_PASSWORD" in exc.hint


class TestConfigurationError:
    """Tests for ConfigurationError exception."""

    def test_message_only(self) -> None:
        """Test configuration error with message only."""
        exc = ConfigurationError("Invalid batch size")

        assert exc.message == "Invalid batch size"
        assert exc.parameter is None
        assert exc.hint is None
        assert exc.exit_code == 3

    def test_with_parameter(self) -> None:
        """Test configuration error with parameter name."""
        exc = ConfigurationError(
            "Value must be positive",
            parameter="HINTGRID_BATCH_SIZE",
        )

        assert exc.message == "Value must be positive"
        assert exc.parameter == "HINTGRID_BATCH_SIZE"
        assert exc.hint is not None
        assert "HINTGRID_BATCH_SIZE" in exc.hint


class TestPipelineError:
    """Tests for PipelineError exception."""

    def test_exit_code(self) -> None:
        """Test that PipelineError has exit code 4."""
        exc = PipelineError("Pipeline stage failed")
        assert exc.exit_code == 4

    def test_inherits_from_hintgrid_error(self) -> None:
        """Test that PipelineError is a HintGridError."""
        exc = PipelineError("Error")
        assert isinstance(exc, HintGridError)


class TestGDSNotAvailableError:
    """Tests for GDSNotAvailableError exception."""

    def test_basic_message(self) -> None:
        """Test GDS error without original exception."""
        exc = GDSNotAvailableError()

        assert "Neo4j GDS" in str(exc)
        assert "Graph Data Science" in str(exc)
        assert exc.original_error is None
        assert exc.exit_code == 5

    def test_with_original_error(self) -> None:
        """Test GDS error with original exception."""
        original = Exception("Unknown procedure gds.alpha")
        exc = GDSNotAvailableError(original_error=original)

        assert "Neo4j GDS" in str(exc)
        assert "Unknown procedure gds.alpha" in str(exc)
        assert exc.original_error is original

    def test_hint_content(self) -> None:
        """Test that hint explains how to install GDS."""
        exc = GDSNotAvailableError()

        assert exc.hint is not None
        assert "NEO4J_PLUGINS" in exc.hint
        assert "graph-data-science" in exc.hint
