"""Integration tests for HintGrid application initialization.

Tests basic app initialization. Full pipeline tests are covered by CLI end-to-end tests
which ensure proper initialization flow (setup, load_data, run_analytics, etc.).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

import pytest

if TYPE_CHECKING:
    from collections.abc import Generator
    from psycopg import Connection
    from psycopg.rows import TupleRow

    import redis

    from hintgrid.clients.neo4j import Neo4jClient
    from tests.conftest import DockerComposeInfo, EmbeddingServiceConfig
else:
    import redis

from hintgrid.app import HintGridApp
from hintgrid.clients.postgres import PostgresClient
from hintgrid.clients.redis import RedisClient
from hintgrid.config import HintGridSettings


class SampleData(TypedDict):
    status_ids: list[int]
    favourite_ids: list[int]
    follow_ids: list[int]
    user_ids: list[int]


class SampleBlocksMutes(TypedDict):
    last_block_id: int
    last_mute_id: int


class SampleReblogsReplies(TypedDict):
    last_reblog_id: int
    last_reply_id: int
    reblog_count: int
    reply_count: int


@pytest.fixture
def settings_with_tfidf(
    settings: HintGridSettings, fasttext_embedding_service: EmbeddingServiceConfig
) -> HintGridSettings:
    """HintGrid settings configured to use TF-IDF embedding service."""
    # Explicit runtime use of HintGridSettings
    assert isinstance(settings, HintGridSettings)
    return settings.model_copy(
        update={
            "llm_base_url": fasttext_embedding_service["api_base"],
            "llm_model": fasttext_embedding_service["model"],
            "llm_provider": "openai",
            "interests_min_favourites": 1,  # Lower threshold for tests
            "feed_days": 365,  # Accept posts from last year for tests
        }
    )


@pytest.fixture
def postgres_client_for_app(
    docker_compose: DockerComposeInfo,
    worker_schema: str,
    settings: HintGridSettings,
) -> Generator[PostgresClient, None, None]:
    """PostgreSQL client fixture for HintGridApp."""
    # Explicit runtime use of HintGridSettings
    assert isinstance(settings, HintGridSettings)
    test_settings = settings.model_copy(
        update={
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": worker_schema,
        }
    )
    client = PostgresClient.from_settings(test_settings)
    try:
        yield client
    finally:
        client.close()


@pytest.fixture
def redis_client_for_app(redis_client_from_pool: redis.Redis) -> RedisClient:
    # Explicit runtime use of redis
    assert isinstance(redis_client_from_pool, redis.Redis)
    """Redis client fixture for HintGridApp."""
    return RedisClient(redis_client_from_pool)


@pytest.fixture
def sample_data(
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> SampleData:
    """Insert sample data into PostgreSQL.
    
    Neo4j label-based isolation ensures each worker has separate constraints,
    so the same IDs can be used across workers without conflict.
    """
    with postgres_conn.cursor() as cur:
        # Create test statuses
        cur.execute("""
            INSERT INTO statuses (id, account_id, text, language, visibility, reblog_of_id)
            VALUES 
                (1, 101, 'Hello Fediverse! #introduction', 'en', 0, NULL),
                (2, 102, 'I love Python programming #python', 'en', 0, NULL),
                (3, 101, 'Second post about #technology', 'en', 0, NULL),
                (4, 103, 'Deleted post', 'en', 0, NULL),
                (5, 102, 'More Python content #python #coding', 'en', 0, NULL),
                (6, 103, 'GraphDB is awesome #neo4j #graphs', 'en', 0, NULL)
            RETURNING id;
        """)
        status_ids = [row[0] for row in cur.fetchall()]

        # Mark one status as deleted
        cur.execute("UPDATE statuses SET deleted_at = NOW() WHERE id = 4;")

        # Create test favourites
        cur.execute("""
            INSERT INTO favourites (id, account_id, status_id)
            VALUES 
                (1, 102, 1),
                (2, 103, 1),
                (3, 101, 2),
                (4, 102, 5),
                (5, 103, 6)
            RETURNING id;
        """)
        favourite_ids = [row[0] for row in cur.fetchall()]

        # Create test follows
        cur.execute("""
            INSERT INTO follows (id, account_id, target_account_id)
            VALUES 
                (1, 101, 102),
                (2, 102, 103),
                (3, 103, 101)
            RETURNING id;
        """)
        follow_ids = [row[0] for row in cur.fetchall()]

        postgres_conn.commit()

    return {
        "status_ids": status_ids,
        "favourite_ids": favourite_ids,
        "follow_ids": follow_ids,
        "user_ids": [101, 102, 103],
    }


@pytest.fixture
def sample_blocks_mutes(
    postgres_conn: Connection[TupleRow],
    sample_data: SampleData,
) -> SampleBlocksMutes:
    """Insert sample blocks and mutes for incremental state tests."""
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO blocks (id, account_id, target_account_id)
            VALUES
                (10, 101, 102),
                (20, 102, 103)
            RETURNING id;
            """
        )
        block_ids = [row[0] for row in cur.fetchall()]
        cur.execute(
            """
            INSERT INTO mutes (id, account_id, target_account_id)
            VALUES
                (11, 101, 103),
                (21, 103, 101)
            RETURNING id;
            """
        )
        mute_ids = [row[0] for row in cur.fetchall()]
        postgres_conn.commit()

    return {"last_block_id": max(block_ids), "last_mute_id": max(mute_ids)}


@pytest.fixture
def sample_reblogs_replies(
    postgres_conn: Connection[TupleRow],
    sample_data: SampleData,
) -> SampleReblogsReplies:
    """Insert sample reblogs and replies for REBLOGGED/REPLIED relationship tests."""
    with postgres_conn.cursor() as cur:
        # Create reblog statuses (status with reblog_of_id pointing to original post)
        # User 102 reblogs post 1 (written by user 101)
        # User 103 reblogs post 2 (written by user 102)
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, visibility, reblog_of_id)
            VALUES
                (100, 102, '', 0, 1),
                (101, 103, '', 0, 2)
            RETURNING id;
            """
        )
        reblog_ids = [row[0] for row in cur.fetchall()]

        # Create reply statuses (status with in_reply_to_id pointing to original post)
        # User 101 replies to post 2 (written by user 102)
        # User 103 replies to post 3 (written by user 101)
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, visibility, in_reply_to_id)
            VALUES
                (200, 101, 'Great post!', 0, 2),
                (201, 103, 'I agree!', 0, 3)
            RETURNING id;
            """
        )
        reply_ids = [row[0] for row in cur.fetchall()]
        postgres_conn.commit()

    return {
        "last_reblog_id": max(reblog_ids),
        "last_reply_id": max(reply_ids),
        "reblog_count": len(reblog_ids),
        "reply_count": len(reply_ids),
    }


@pytest.mark.smoke
@pytest.mark.integration
def test_app_initialization(
    neo4j: Neo4jClient,
    postgres_client_for_app: PostgresClient,
    redis_client_for_app: RedisClient,
    settings_with_tfidf: HintGridSettings,
    fasttext_embedding_service: EmbeddingServiceConfig,
) -> None:
    """Test that HintGridApp initializes correctly."""
    app = HintGridApp(
        neo4j=neo4j,
        postgres=postgres_client_for_app,
        redis=redis_client_for_app,
        settings=settings_with_tfidf,
    )

    # Verify state store is initialized
    assert app.state_store is not None

    # Verify initial state
    state = app.state_store.load()
    assert state.last_status_id == 0
    assert state.last_favourite_id == 0
    assert state.last_block_id == 0
    assert state.last_mute_id == 0
    assert state.last_reblog_id == 0
    assert state.last_reply_id == 0
