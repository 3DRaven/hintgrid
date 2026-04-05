"""Custom exceptions for HintGrid with user-friendly messages."""

from __future__ import annotations


class HintGridError(Exception):
    """Base exception for all HintGrid errors.

    Provides user-friendly error messages without stack traces.
    """

    exit_code: int = 1

    def __init__(self, message: str, hint: str | None = None) -> None:
        """Initialize exception with message and optional hint.

        Args:
            message: User-friendly error description.
            hint: Optional suggestion for fixing the issue.
        """
        self.message = message
        self.hint = hint
        super().__init__(message)

    def __str__(self) -> str:
        if self.hint:
            return f"{self.message}\nHint: {self.hint}"
        return self.message


class ConnectionError(HintGridError):
    """Base exception for database/service connection failures."""

    exit_code: int = 2


class Neo4jConnectionError(ConnectionError):
    """Neo4j database is unavailable or authentication failed."""

    def __init__(
        self,
        host: str,
        port: int,
        original_error: Exception | None = None,
    ) -> None:
        message = f"Cannot connect to Neo4j at {host}:{port}"
        hint = (
            "Check that Neo4j is running and accessible. "
            "Verify HINTGRID_NEO4J_HOST and HINTGRID_NEO4J_PORT settings."
        )
        if original_error:
            message = f"{message}: {original_error}"
        super().__init__(message, hint)
        self.host = host
        self.port = port
        self.original_error = original_error


class PostgresConnectionError(ConnectionError):
    """PostgreSQL database is unavailable or authentication failed."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        original_error: Exception | None = None,
    ) -> None:
        message = f"Cannot connect to PostgreSQL at {host}:{port}/{database}"

        # Detect common error patterns for better hints
        hint = "Check that PostgreSQL is running and accessible."
        if original_error:
            error_str = str(original_error).lower()
            if "password" in error_str or "authentication" in error_str:
                hint = (
                    "Authentication failed. Verify HINTGRID_POSTGRES_USER and "
                    "HINTGRID_POSTGRES_PASSWORD settings."
                )
            elif "connection refused" in error_str:
                hint = (
                    "Connection refused. Check that PostgreSQL is running "
                    "and HINTGRID_POSTGRES_HOST/PORT are correct."
                )
            elif "does not exist" in error_str:
                hint = (
                    "Database not found. Verify HINTGRID_POSTGRES_DATABASE setting."
                )
            message = f"{message}: {original_error}"

        super().__init__(message, hint)
        self.host = host
        self.port = port
        self.database = database
        self.original_error = original_error


class RedisConnectionError(ConnectionError):
    """Redis is unavailable or authentication failed."""

    def __init__(
        self,
        host: str,
        port: int,
        original_error: Exception | None = None,
    ) -> None:
        message = f"Cannot connect to Redis at {host}:{port}"
        hint = (
            "Check that Redis is running and accessible. "
            "Verify HINTGRID_REDIS_HOST, HINTGRID_REDIS_PORT, "
            "and HINTGRID_REDIS_PASSWORD settings."
        )
        if original_error:
            message = f"{message}: {original_error}"
        super().__init__(message, hint)
        self.host = host
        self.port = port
        self.original_error = original_error


class ConfigurationError(HintGridError):
    """Invalid or missing configuration."""

    exit_code: int = 3

    def __init__(self, message: str, parameter: str | None = None) -> None:
        hint = None
        if parameter:
            hint = f"Check the value of {parameter} in environment or CLI arguments."
        super().__init__(message, hint)
        self.parameter = parameter


class PipelineError(HintGridError):
    """Pipeline execution failure."""

    exit_code: int = 4


class GDSNotAvailableError(HintGridError):
    """Neo4j GDS plugin is not installed or not accessible."""

    exit_code: int = 5

    def __init__(self, original_error: Exception | None = None) -> None:
        message = "Neo4j GDS (Graph Data Science) plugin is not available"
        hint = (
            "HintGrid requires Neo4j with GDS plugin installed. "
            "For Docker, use NEO4J_PLUGINS='[\"graph-data-science\"]' environment variable."
        )
        if original_error:
            message = f"{message}: {original_error}"
        super().__init__(message, hint)
        self.original_error = original_error
