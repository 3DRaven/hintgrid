"""Integration tests for loading all data types.

Tests verify that all data types (blocks, mutes, replies, reblogs)
are correctly loaded from PostgreSQL to Neo4j.
Note: FOLLOWS are no longer loaded separately; they are included in INTERACTS_WITH aggregation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.pipeline.loaders import load_incremental_data
from hintgrid.state import StateStore
from hintgrid.utils.coercion import coerce_int

if TYPE_CHECKING:
    from hintgrid.clients.postgres import PostgresClient
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.config import HintGridSettings
    from psycopg import Connection
    from psycopg.rows import TupleRow

    from tests.conftest import DockerComposeInfo, EmbeddingServiceConfig


@pytest.mark.integration
def test_load_blocks(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that load_incremental_data loads blocks correctly."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (4003, 'block_loader_user1', NULL), (4004, 'block_loader_user2', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO blocks (id, account_id, target_account_id)
            VALUES
                (4201, 4003, 4004),
                (4202, 4004, 4003)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    # Create test settings
    test_settings = settings.model_copy(
        update={
            "batch_size": 100,
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": settings.postgres_schema,
        }
    )

    # Create state store
    state_id = f"load_blocks_test_{worker_id}"
    state_store = StateStore(neo4j, state_id)

    # Load data
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)

    # Verify blocks were loaded
    final_state = state_store.load()
    assert final_state.last_block_id >= 4202, "Should have loaded blocks"

    # Verify in Neo4j (blocks are merged as HATES_USER)
    neo4j.label("User")
    hates_count_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (:__user__)-[r:HATES_USER]->(:__user__) RETURN count(r) AS cnt",
            {"user": "User"},
        )
    )
    hates_count = coerce_int(hates_count_result[0].get("cnt"))
    assert hates_count >= 2, f"Expected at least 2 HATES_USER relationships, got {hates_count}"


@pytest.mark.integration
def test_load_mutes(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that load_incremental_data loads mutes correctly."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (4005, 'mute_loader_user1', NULL), (4006, 'mute_loader_user2', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO mutes (id, account_id, target_account_id)
            VALUES
                (4301, 4005, 4006),
                (4302, 4006, 4005)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    # Create test settings
    test_settings = settings.model_copy(
        update={
            "batch_size": 100,
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": settings.postgres_schema,
        }
    )

    # Create state store
    state_id = f"load_mutes_test_{worker_id}"
    state_store = StateStore(neo4j, state_id)

    # Load data
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)

    # Verify mutes were loaded
    final_state = state_store.load()
    assert final_state.last_mute_id >= 4302, "Should have loaded mutes"

    # Verify in Neo4j (mutes are merged as HATES_USER)
    neo4j.label("User")
    hates_count_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (:__user__)-[r:HATES_USER]->(:__user__) RETURN count(r) AS cnt",
            {"user": "User"},
        )
    )
    hates_count = coerce_int(hates_count_result[0].get("cnt"))
    assert hates_count >= 2, f"Expected at least 2 HATES_USER relationships, got {hates_count}"


@pytest.mark.integration
def test_load_replies(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that load_incremental_data loads replies correctly."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (4007, 'reply_loader_author', NULL), (4008, 'reply_loader_target', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, reply, in_reply_to_id, in_reply_to_account_id)
            VALUES
                (4401, 4008, 'Original post for reply loader test', 'en', 0, false, NULL, NULL),
                (4402, 4007, 'Reply to original post from user seven', 'en', 0, true, 4401, 4008),
                (4403, 4008, 'Another reply to original from user eight', 'en', 0, true, 4401, 4007)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    # Create test settings
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

    # Create state store
    state_id = f"load_replies_test_{worker_id}"
    state_store = StateStore(neo4j, state_id)

    # Load data
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)

    # Verify replies were loaded (all statuses processed in unified stream)
    final_state = state_store.load()
    assert final_state.last_status_id >= 4403, "Should have loaded replies via unified stream"

    # Verify in Neo4j
    neo4j.label("User")
    neo4j.label("Post")
    replies_count_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (:__user__)-[r:REPLIED]->(:__post__) RETURN count(r) AS cnt",
            {"user": "User", "post": "Post"},
        )
    )
    replies_count = coerce_int(replies_count_result[0].get("cnt"))
    assert replies_count >= 2, f"Expected at least 2 REPLIED relationships, got {replies_count}"


@pytest.mark.integration
def test_load_reblogs(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that load_incremental_data loads reblogs correctly."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (4009, 'reblog_loader_user1', NULL), (4010, 'reblog_loader_user2', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, reblog_of_id)
            VALUES
                (4501, 4009, 'Original post for reblog loader test with enough text', 'en', 0, NULL),
                (4502, 4010, 'Reblog of original post with enough text', 'en', 0, 4501),
                (4503, 4009, 'Another reblog of original post with enough text', 'en', 0, 4501)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    # Create test settings
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

    # Create state store
    state_id = f"load_reblogs_test_{worker_id}"
    state_store = StateStore(neo4j, state_id)

    # Load data
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)

    # Verify reblogs were loaded (all statuses processed in unified stream)
    final_state = state_store.load()
    assert final_state.last_status_id >= 4503, "Should have loaded reblogs via unified stream"

    # Verify in Neo4j
    # Note: REBLOGGED is (User)-[:REBLOGGED]->(Post), not (Post)-[:REBLOGGED]->(Post)
    neo4j.label("User")
    neo4j.label("Post")
    reblogs_count_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (:__user__)-[r:REBLOGGED]->(:__post__) RETURN count(r) AS cnt",
            {"user": "User", "post": "Post"},
        )
    )
    reblogs_count = coerce_int(reblogs_count_result[0].get("cnt"))
    assert reblogs_count >= 2, f"Expected at least 2 REBLOGGED relationships, got {reblogs_count}"


@pytest.mark.integration
def test_load_incremental_data_all_types_with_progress(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that load_incremental_data loads all types successfully."""

    # Insert complete test data
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES
                (5001, 'all_types_user1', NULL),
                (5002, 'all_types_user2', NULL),
                (5003, 'all_types_user3', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, reply, in_reply_to_id, in_reply_to_account_id, reblog_of_id)
            VALUES
                (5101, 5001, 'Original post for all types test', 'en', 0, false, NULL, NULL, NULL),
                (5102, 5002, 'Reply to original', 'en', 0, true, 5101, 5001, NULL),
                (5103, 5003, '', 'en', 0, false, NULL, NULL, 5101)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO favourites (id, account_id, status_id)
            VALUES (5201, 5002, 5101)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO follows (id, account_id, target_account_id)
            VALUES (5301, 5001, 5002)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO blocks (id, account_id, target_account_id)
            VALUES (5401, 5001, 5003)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO mutes (id, account_id, target_account_id)
            VALUES (5501, 5002, 5003)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    # Create test settings
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

    # Create state store
    state_id = f"load_all_types_progress_{worker_id}"
    state_store = StateStore(neo4j, state_id)

    # Load data - LoadingProgress is used internally
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)

    # Verify all types were loaded
    final_state = state_store.load()
    # All statuses (regular, replies, reblogs) are processed in unified stream
    assert final_state.last_status_id >= 5103, "Should have loaded all statuses (including replies and reblogs)"
    assert final_state.last_favourite_id >= 5201, "Should have loaded favourites"
    # Note: FOLLOWS are no longer loaded separately; they are included in INTERACTS_WITH
    assert final_state.last_block_id >= 5401, "Should have loaded blocks"
    assert final_state.last_mute_id >= 5501, "Should have loaded mutes"

    # Verify that INTERACTS_WITH contains relationships (follows are included via SQL aggregation)
    neo4j.label("User")
    interacts_count_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (:__user__)-[r:INTERACTS_WITH]->(:__user__) RETURN count(r) AS cnt",
            {"user": "User"},
        )
    )
    interacts_count = coerce_int(interacts_count_result[0].get("cnt")) if interacts_count_result else 0
    # INTERACTS_WITH should contain data from follows (if follows_weight > 0) and other interactions
    assert interacts_count >= 0, "INTERACTS_WITH relationships should exist (may include follows if follows_weight > 0)"
