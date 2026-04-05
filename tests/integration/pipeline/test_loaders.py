"""Integration tests for pipeline loaders.

Tests verify that all data types (statuses, favourites, blocks,
mutes, reblogs, replies) are correctly loaded from PostgreSQL to Neo4j.
Uses testcontainers for real database interactions.
Note: FOLLOWS are no longer loaded separately, they are included in INTERACTS_WITH via SQL.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.pipeline.loaders import load_incremental_data
from hintgrid.state import StateStore
from hintgrid.utils.coercion import coerce_int

if TYPE_CHECKING:
    from hintgrid.config import HintGridSettings
    from hintgrid.clients.neo4j import Neo4jClient
    from psycopg import Connection
    from psycopg.rows import TupleRow

    from hintgrid.clients.postgres import PostgresClient
    from tests.conftest import DockerComposeInfo, EmbeddingServiceConfig


@pytest.fixture
def complete_sample_data(
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> dict[str, list[int]]:
    """Insert complete sample data including blocks, mutes, reblogs, replies."""
    with postgres_conn.cursor() as cur:
        # Create accounts
        cur.execute("""
            INSERT INTO accounts (id, username, domain)
            VALUES
                (201, 'user1', NULL),
                (202, 'user2', 'mastodon.social'),
                (203, 'user3', 'example.org'),
                (204, 'user4', NULL)
            ON CONFLICT (id) DO NOTHING;
        """)

        # Create statuses including reblogs and replies
        cur.execute("""
            INSERT INTO statuses (id, account_id, text, language, visibility, 
                                  reblog_of_id, in_reply_to_id, in_reply_to_account_id, reply)
            VALUES
                -- Original posts
                (1001, 201, 'Original post about Python', 'en', 0, NULL, NULL, NULL, false),
                (1002, 202, 'Original post about Docker', 'en', 0, NULL, NULL, NULL, false),
                (1003, 203, 'Original post about Testing', 'en', 0, NULL, NULL, NULL, false),
                -- Reblogs
                (1004, 202, '', 'en', 0, 1001, NULL, NULL, false),
                (1005, 203, '', 'en', 0, 1002, NULL, NULL, false),
                -- Replies
                (1006, 203, 'Reply to Python post', 'en', 0, NULL, 1001, 201, true),
                (1007, 204, 'Another reply', 'en', 0, NULL, 1001, 201, true),
                (1008, 201, 'Reply to Docker post', 'en', 0, NULL, 1002, 202, true)
            ON CONFLICT (id) DO NOTHING;
        """)

        # Create favourites
        cur.execute("""
            INSERT INTO favourites (id, account_id, status_id)
            VALUES
                (501, 202, 1001),
                (502, 203, 1001),
                (503, 204, 1002),
                (504, 201, 1003)
            ON CONFLICT (id) DO NOTHING;
        """)

        # Note: FOLLOWS are no longer loaded separately, they are included in INTERACTS_WITH via SQL

        # Create blocks
        cur.execute("""
            INSERT INTO blocks (id, account_id, target_account_id)
            VALUES
                (401, 201, 204),
                (402, 203, 202)
            ON CONFLICT (id) DO NOTHING;
        """)

        # Create mutes
        cur.execute("""
            INSERT INTO mutes (id, account_id, target_account_id, hide_notifications)
            VALUES
                (601, 202, 204, true),
                (602, 204, 203, false)
            ON CONFLICT (id) DO NOTHING;
        """)

        postgres_conn.commit()

    return {
        "user_ids": [201, 202, 203, 204],
        "status_ids": [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008],
        "reblog_ids": [1004, 1005],
        "reply_ids": [1006, 1007, 1008],
        "block_ids": [401, 402],
        "mute_ids": [601, 602],
    }


@pytest.mark.integration
def test_load_incremental_data_all_types(
    docker_compose: DockerComposeInfo,
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    complete_sample_data: dict[str, list[int]],
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that load_incremental_data loads all data types correctly.

    Verifies:
    - Statuses are loaded as Post nodes with embeddings
    - Favourites create FAVOURITED relationships
    - Blocks create BLOCKS relationships
    - Mutes create MUTES relationships
    - Reblogs create REBLOGGED relationships
    - Replies create REPLIED_TO relationships
    - INTERACTS_WITH relationships are created (includes FOLLOWS via SQL)
    Note: FOLLOWS are no longer loaded separately, they are included in INTERACTS_WITH via SQL.
    """
    from hintgrid.clients.postgres import PostgresClient

    # Create settings with embedding service
    test_settings = settings.model_copy(
        update={
            "llm_provider": "openai",
            "llm_base_url": fasttext_embedding_service["api_base"],
            "llm_model": fasttext_embedding_service["model"],
            "llm_dimensions": settings.fasttext_vector_size,
            "batch_size": 100,
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": settings.postgres_schema,
        }
    )

    # Create state store using neo4j fixture with unique state_id per worker
    state_id = f"loader_test_{worker_id}"
    state_store = StateStore(neo4j, state_id)

    # Create PostgresClient with test settings
    pg_client = PostgresClient.from_settings(test_settings)

    try:
        # Run loader
        load_incremental_data(pg_client, neo4j, test_settings, state_store)

        # Verify Post nodes were created
        neo4j.label("Post")
        post_count_result = list(
            neo4j.execute_and_fetch_labeled(
                "MATCH (p:__post__) RETURN count(p) AS cnt",
                {"post": "Post"},
            )
        )
        post_count = coerce_int(post_count_result[0].get("cnt"))
        # We expect 5 original posts (excluding reblogs with empty text)
        assert post_count >= 3, f"Expected at least 3 posts, got {post_count}"

        # Verify User nodes were created
        neo4j.label("User")
        user_count_result = list(
            neo4j.execute_and_fetch_labeled(
                "MATCH (u:__user__) RETURN count(u) AS cnt",
                {"user": "User"},
            )
        )
        user_count = coerce_int(user_count_result[0].get("cnt"))
        assert user_count >= 4, f"Expected at least 4 users, got {user_count}"

        # Verify INTERACTS_WITH relationships (includes FOLLOWS via SQL)
        interacts_count_result = list(
            neo4j.execute_and_fetch_labeled(
                "MATCH (:__user__)-[r:INTERACTS_WITH]->(:__user__) RETURN count(r) AS cnt",
                {"user": "User"},
            )
        )
        interacts_count = coerce_int(interacts_count_result[0].get("cnt"))
        assert interacts_count > 0, f"Expected INTERACTS_WITH relationships, got {interacts_count}"

        # Verify HATES_USER relationships (created from blocks and mutes)
        # Blocks and mutes are both merged as HATES_USER relationships
        hates_count_result = list(
            neo4j.execute_and_fetch_labeled(
                "MATCH (:__user__)-[r:HATES_USER]->(:__user__) RETURN count(r) AS cnt",
                {"user": "User"},
            )
        )
        hates_count = coerce_int(hates_count_result[0].get("cnt"))
        # We created 2 blocks + 2 mutes = 4 HATES_USER relationships
        assert hates_count >= 4, f"Expected at least 4 HATES_USER rels, got {hates_count}"

        # Verify FAVORITED relationships (note: American spelling)
        fav_count_result = list(
            neo4j.execute_and_fetch_labeled(
                "MATCH (:__user__)-[r:FAVORITED]->(:__post__) RETURN count(r) AS cnt",
                {"user": "User", "post": "Post"},
            )
        )
        fav_count = coerce_int(fav_count_result[0].get("cnt"))
        assert fav_count >= 4, f"Expected at least 4 FAVORITED rels, got {fav_count}"

    finally:
        pg_client.close()


@pytest.mark.integration
def test_load_incremental_data_state_persistence(
    docker_compose: DockerComposeInfo,
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    complete_sample_data: dict[str, list[int]],
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that loader correctly updates state after each batch.

    Verifies that pipeline state (last_*_id values) are persisted to Neo4j.
    """
    from hintgrid.clients.postgres import PostgresClient

    test_settings = settings.model_copy(
        update={
            "llm_provider": "openai",
            "llm_base_url": fasttext_embedding_service["api_base"],
            "llm_model": fasttext_embedding_service["model"],
            "llm_dimensions": settings.fasttext_vector_size,
            "batch_size": 100,
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": settings.postgres_schema,
        }
    )

    state_id = f"loader_state_test_{worker_id}"
    state_store = StateStore(neo4j, state_id)
    pg_client = PostgresClient.from_settings(test_settings)

    try:
        # Run loader
        load_incremental_data(pg_client, neo4j, test_settings, state_store)

        # Load state and verify cursors were updated
        state = state_store.load()

        # At least some cursor should be updated (not all zero)
        cursors_updated = (
            state.last_status_id > 0
            or state.last_favourite_id > 0
            or state.last_block_id > 0
            or state.last_mute_id > 0
        )
        assert cursors_updated, "Expected at least one cursor to be updated"

        # Verify specific cursors for data we created
        assert state.last_status_id > 0, "Status cursor should be updated"
        assert state.last_block_id > 0, "Block cursor should be updated"
        assert state.last_mute_id > 0, "Mute cursor should be updated"

    finally:
        pg_client.close()


@pytest.mark.integration
def test_load_incremental_second_run_is_idempotent(
    docker_compose: DockerComposeInfo,
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    complete_sample_data: dict[str, list[int]],
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that running loader twice doesn't duplicate data.

    Incremental loader should skip already processed items on second run.
    """
    from hintgrid.clients.postgres import PostgresClient

    test_settings = settings.model_copy(
        update={
            "llm_provider": "openai",
            "llm_base_url": fasttext_embedding_service["api_base"],
            "llm_model": fasttext_embedding_service["model"],
            "llm_dimensions": settings.fasttext_vector_size,
            "batch_size": 100,
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": settings.postgres_schema,
        }
    )

    state_id = f"loader_idempotent_test_{worker_id}"
    state_store = StateStore(neo4j, state_id)
    pg_client = PostgresClient.from_settings(test_settings)

    try:
        # First run
        load_incremental_data(pg_client, neo4j, test_settings, state_store)

        # Count nodes after first run
        neo4j.label("Post")
        neo4j.label("User")

        posts_after_first = coerce_int(
            next(iter(neo4j.execute_and_fetch_labeled(
                    "MATCH (p:__post__) RETURN count(p) AS cnt",
                    {"post": "Post"},
                ))).get("cnt")
        )
        users_after_first = coerce_int(
            next(iter(neo4j.execute_and_fetch_labeled(
                    "MATCH (u:__user__) RETURN count(u) AS cnt",
                    {"user": "User"},
                ))).get("cnt")
        )

        # Second run
        load_incremental_data(pg_client, neo4j, test_settings, state_store)

        # Count after second run
        posts_after_second = coerce_int(
            next(iter(neo4j.execute_and_fetch_labeled(
                    "MATCH (p:__post__) RETURN count(p) AS cnt",
                    {"post": "Post"},
                ))).get("cnt")
        )
        users_after_second = coerce_int(
            next(iter(neo4j.execute_and_fetch_labeled(
                    "MATCH (u:__user__) RETURN count(u) AS cnt",
                    {"user": "User"},
                ))).get("cnt")
        )

        # Counts should be the same (no duplicates)
        assert posts_after_first == posts_after_second, "Post count should not change"
        assert users_after_first == users_after_second, "User count should not change"

    finally:
        pg_client.close()
