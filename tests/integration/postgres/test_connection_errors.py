"""Integration tests for PostgreSQL connection error handling.

Tests verify that PostgresConnectionError is raised correctly
when connection fails.
"""

from __future__ import annotations

import pytest

from hintgrid.clients.postgres import PostgresClient
from hintgrid.config import HintGridSettings
from hintgrid.exceptions import PostgresConnectionError


@pytest.mark.integration
def test_postgres_client_connection_error() -> None:
    """Test PostgresConnectionError is raised when connection fails."""
    # Create settings with invalid connection parameters
    settings = HintGridSettings(
        postgres_host="invalid-host-that-does-not-exist",
        postgres_port=5432,
        postgres_database="invalid_db",
        postgres_user="invalid_user",
        postgres_password="invalid_password",
    )

    # Attempt to create client - should raise PostgresConnectionError
    with pytest.raises(PostgresConnectionError) as exc_info:
        PostgresClient.from_settings(settings)

    # Verify error details
    assert exc_info.value.host == "invalid-host-that-does-not-exist"
    assert exc_info.value.port == 5432
    assert exc_info.value.database == "invalid_db"
    assert exc_info.value.original_error is not None
