"""Integration tests for loaders with progress tracking.

Tests verify that load_incremental_data successfully loads data
and updates state, which implies LoadingProgress is working correctly.
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
def test_load_statuses_with_progress(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that load_incremental_data loads statuses successfully."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (1101, 'progress_user1', NULL), (1102, 'progress_user2', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility)
            VALUES
                (1201, 1101, 'First status for progress testing with enough text', 'en', 0),
                (1202, 1102, 'Second status for progress testing with enough text', 'en', 0),
                (1203, 1101, 'Third status for progress testing with enough text', 'en', 0)
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
            "batch_size": 2,
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": settings.postgres_schema,
        }
    )

    # Create state store
    state_id = f"progress_test_{worker_id}"
    state_store = StateStore(neo4j, state_id)

    # Load data - LoadingProgress is used internally
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)

    # Verify data was loaded successfully
    final_state = state_store.load()
    assert final_state.last_status_id >= 1201, "Should have loaded at least some statuses"

    # Verify nodes exist in Neo4j
    user_count = coerce_int(
        next(iter(neo4j.execute_and_fetch("MATCH (u:User) RETURN count(u) AS count"))).get("count")
    )
    post_count = coerce_int(
        next(iter(neo4j.execute_and_fetch("MATCH (p:Post) RETURN count(p) AS count"))).get("count")
    )
    assert user_count >= 2, "Should have created user nodes"
    assert post_count >= 2, "Should have created at least some post nodes (some may be filtered)"


@pytest.mark.integration
def test_load_favourites_with_progress(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that load_incremental_data loads favourites successfully."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (1103, 'fav_user1', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility)
            VALUES (1204, 1103, 'Status for favourite testing', 'en', 0)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO favourites (id, account_id, status_id)
            VALUES
                (1301, 1103, 1204),
                (1302, 1103, 1204),
                (1303, 1103, 1204)
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
            "batch_size": 2,
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": settings.postgres_schema,
        }
    )

    # Create state store
    state_id = f"progress_fav_test_{worker_id}"
    state_store = StateStore(neo4j, state_id)

    # Load data - LoadingProgress is used internally
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)

    # Verify data was loaded successfully
    final_state = state_store.load()
    # Status must be loaded first for favourites to be processed
    assert final_state.last_status_id >= 1204, "Should have loaded status first"
    assert final_state.last_favourite_id >= 1301, "Should have loaded at least some favourites"


@pytest.mark.integration
def test_load_blocks_with_progress(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that load_incremental_data loads blocks successfully."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (1106, 'block_user1', NULL), (1107, 'block_user2', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO blocks (id, account_id, target_account_id)
            VALUES (1501, 1106, 1107)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    # Create test settings
    test_settings = settings.model_copy(
        update={
            "batch_size": 10,
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": settings.postgres_schema,
        }
    )

    # Create state store
    state_id = f"progress_blocks_test_{worker_id}"
    state_store = StateStore(neo4j, state_id)

    # Load data - LoadingProgress is used internally
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)

    # Verify data was loaded successfully
    final_state = state_store.load()
    assert final_state.last_block_id >= 1501, "Should have loaded blocks"


@pytest.mark.integration
def test_load_mutes_with_progress(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that load_incremental_data loads mutes successfully."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (1108, 'mute_user1', NULL), (1109, 'mute_user2', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO mutes (id, account_id, target_account_id)
            VALUES (1601, 1108, 1109)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    # Create test settings
    test_settings = settings.model_copy(
        update={
            "batch_size": 10,
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": settings.postgres_schema,
        }
    )

    # Create state store
    state_id = f"progress_mutes_test_{worker_id}"
    state_store = StateStore(neo4j, state_id)

    # Load data - LoadingProgress is used internally
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)

    # Verify data was loaded successfully
    final_state = state_store.load()
    assert final_state.last_mute_id >= 1601, "Should have loaded mutes"


@pytest.mark.integration
def test_load_replies_with_progress(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that load_incremental_data loads replies successfully."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (1110, 'reply_author', NULL), (1111, 'reply_target', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, reply, in_reply_to_id, in_reply_to_account_id)
            VALUES
                (1205, 1111, 'Original post for reply testing', 'en', 0, false, NULL, NULL),
                (1206, 1110, 'Reply to original post', 'en', 0, true, 1205, 1111)
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
            "batch_size": 10,
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": settings.postgres_schema,
        }
    )

    # Create state store
    state_id = f"progress_replies_test_{worker_id}"
    state_store = StateStore(neo4j, state_id)

    # Load data - LoadingProgress is used internally
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)

    # Verify data was loaded successfully (unified stream uses last_status_id)
    final_state = state_store.load()
    assert final_state.last_status_id >= 1206, "Should have loaded replies via unified stream"


@pytest.mark.integration
def test_load_reblogs_with_progress(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that load_incremental_data loads reblogs successfully."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (1112, 'reblog_user1', NULL), (1113, 'reblog_user2', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, reblog_of_id)
            VALUES
                (1207, 1112, 'Original post for reblog testing', 'en', 0, NULL),
                (1208, 1113, '', 'en', 0, 1207)
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
            "batch_size": 10,
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": settings.postgres_schema,
        }
    )

    # Create state store
    state_id = f"progress_reblogs_test_{worker_id}"
    state_store = StateStore(neo4j, state_id)

    # Load data - LoadingProgress is used internally
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)

    # Verify data was loaded successfully (unified stream uses last_status_id)
    final_state = state_store.load()
    assert final_state.last_status_id >= 1208, "Should have loaded reblogs via unified stream"
