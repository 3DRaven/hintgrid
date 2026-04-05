"""Integration tests for connection error handling.

Covers:
- PostgreSQL connection errors during pipeline execution
- Neo4j connection errors during operations
- Redis connection errors during feed writes
- Error propagation through application layers
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.clients.neo4j import Neo4jClient
from hintgrid.clients.postgres import PostgresClient
from hintgrid.clients.redis import RedisClient
from hintgrid.config import HintGridSettings

if TYPE_CHECKING:
    from tests.conftest import DockerComposeInfo


# ---------------------------------------------------------------------------
# Tests: PostgreSQL connection errors
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_postgres_client_invalid_host() -> None:
    """PostgresClient should raise ConnectionError for invalid host."""
    invalid_settings = HintGridSettings(
        postgres_host="invalid_host_that_does_not_exist",
        postgres_port=5432,
        postgres_database="test",
        postgres_user="test",
        postgres_password="test",
    )

    with pytest.raises(Exception):  # ConnectionError or psycopg.OperationalError
        PostgresClient.from_settings(invalid_settings)


@pytest.mark.integration
def test_postgres_client_invalid_port() -> None:
    """PostgresClient should raise ConnectionError for invalid port."""
    invalid_settings = HintGridSettings(
        postgres_host="localhost",
        postgres_port=99999,  # Invalid port
        postgres_database="test",
        postgres_user="test",
        postgres_password="test",
    )

    with pytest.raises(Exception):  # ConnectionError or psycopg.OperationalError
        PostgresClient.from_settings(invalid_settings)


@pytest.mark.integration
def test_postgres_client_invalid_credentials() -> None:
    """PostgresClient should raise ConnectionError for invalid credentials."""
    invalid_settings = HintGridSettings(
        postgres_host="localhost",
        postgres_port=5432,
        postgres_database="test",
        postgres_user="invalid_user",
        postgres_password="invalid_password",
    )

    with pytest.raises(Exception):  # ConnectionError or psycopg.OperationalError
        PostgresClient.from_settings(invalid_settings)


# ---------------------------------------------------------------------------
# Tests: Neo4j connection errors
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_neo4j_client_invalid_host() -> None:
    """Neo4jClient should raise ConnectionError for invalid host."""
    invalid_settings = HintGridSettings(
        neo4j_host="invalid_host_that_does_not_exist",
        neo4j_port=7687,
        neo4j_username="neo4j",
        neo4j_password="test",
    )

    with pytest.raises(Exception):  # ConnectionError or neo4j.exceptions.ServiceUnavailable
        Neo4jClient.from_settings(invalid_settings)


@pytest.mark.integration
def test_neo4j_client_invalid_port() -> None:
    """Neo4jClient should raise ConnectionError for invalid port."""
    invalid_settings = HintGridSettings(
        neo4j_host="localhost",
        neo4j_port=99999,  # Invalid port
        neo4j_username="neo4j",
        neo4j_password="test",
    )

    with pytest.raises(Exception):  # ConnectionError or neo4j.exceptions.ServiceUnavailable
        Neo4jClient.from_settings(invalid_settings)


@pytest.mark.integration
def test_neo4j_client_invalid_credentials(
    docker_compose: DockerComposeInfo,
) -> None:
    """Neo4jClient should raise ConnectionError for invalid credentials."""
    invalid_settings = HintGridSettings(
        neo4j_host=docker_compose.neo4j_host,
        neo4j_port=docker_compose.neo4j_port,
        neo4j_username="invalid_user",
        neo4j_password="invalid_password",
    )

    with pytest.raises(Exception):  # ConnectionError or neo4j.exceptions.AuthError
        Neo4jClient.from_settings(invalid_settings)


@pytest.mark.integration
def test_neo4j_execute_after_connection_lost(
    neo4j: Neo4jClient,
) -> None:
    """Neo4j operations should handle connection loss gracefully."""
    # Close the connection
    neo4j.close()

    # Operations should raise exception
    with pytest.raises(Exception):  # ConnectionError or neo4j.exceptions.ServiceUnavailable
        neo4j.execute("RETURN 1")


# ---------------------------------------------------------------------------
# Tests: Redis connection errors
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_redis_client_invalid_host() -> None:
    """RedisClient should raise ConnectionError for invalid host."""
    invalid_settings = HintGridSettings(
        redis_host="invalid_host_that_does_not_exist",
        redis_port=6379,
    )

    with pytest.raises(Exception):  # ConnectionError or redis.ConnectionError
        RedisClient.from_settings(invalid_settings)


@pytest.mark.integration
def test_redis_client_invalid_port() -> None:
    """RedisClient should raise ConnectionError for invalid port."""
    invalid_settings = HintGridSettings(
        redis_host="localhost",
        redis_port=99999,  # Invalid port
    )

    with pytest.raises(Exception):  # ConnectionError or redis.ConnectionError
        RedisClient.from_settings(invalid_settings)


@pytest.mark.integration
def test_redis_operations_after_connection_lost(
    docker_compose: DockerComposeInfo,
) -> None:
    """Redis operations should handle connection loss gracefully.

    Simulates connection loss by pointing the client at an unreachable port
    after verifying the real connection works.
    """
    import redis as redis_lib

    # Verify the real server is up
    probe = redis_lib.Redis(
        host=docker_compose.redis_host,
        port=docker_compose.redis_port,
        db=0,
        decode_responses=False,
        socket_connect_timeout=2,
    )
    assert probe.ping()
    probe.close()

    # Simulate connection loss: point to an unreachable port
    unreachable = redis_lib.Redis(
        host=docker_compose.redis_host,
        port=1,  # privileged port, nothing listening
        decode_responses=False,
        socket_connect_timeout=1,
        socket_timeout=1,
    )
    client = RedisClient(unreachable, host=docker_compose.redis_host, port=1)

    with pytest.raises(redis_lib.ConnectionError):
        client.ping()


# ---------------------------------------------------------------------------
# Tests: Error propagation
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_connection_error_propagates_to_app(
    docker_compose: DockerComposeInfo,
) -> None:
    """Connection errors should propagate through HintGridApp initialization."""
    from hintgrid.app import HintGridApp
    from hintgrid.clients.neo4j import Neo4jClient as Neo4jClientType
    from hintgrid.clients.postgres import PostgresClient as PostgresClientType
    from hintgrid.clients.redis import RedisClient as RedisClientType

    # Use invalid PostgreSQL settings
    invalid_settings = HintGridSettings(
        postgres_host="invalid_host",
        postgres_port=5432,
        postgres_database="test",
        postgres_user="test",
        postgres_password="test",
        neo4j_host=docker_compose.neo4j_host,
        neo4j_port=docker_compose.neo4j_port,
        neo4j_username=docker_compose.neo4j_user,
        neo4j_password=docker_compose.neo4j_password,
        redis_host=docker_compose.redis_host,
        redis_port=docker_compose.redis_port,
    )

    # App initialization should raise connection error
    with pytest.raises(Exception):  # ConnectionError or similar
        with (
            PostgresClientType.from_settings(invalid_settings) as pg,
            Neo4jClientType.from_settings(invalid_settings) as neo4j,
            RedisClientType.from_settings(invalid_settings) as redis_client,
        ):
            _ = HintGridApp(
                neo4j=neo4j,
                postgres=pg,
                redis=redis_client,
                settings=invalid_settings,
            )
