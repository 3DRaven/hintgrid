"""Integration tests for loaders batch remainder handling.

Tests verify that remaining items in batch (not filling full batch_size)
are correctly processed after the main batch loop.
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
def test_load_statuses_batch_remainder(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that remaining statuses in batch are processed after main loop."""
    # Insert 23 statuses (not multiple of batch_size=10)
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (3001, 'remainder_user', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        # Insert 23 statuses
        for i in range(1, 24):
            cur.execute(
                """
                INSERT INTO statuses (id, account_id, text, language, visibility)
                VALUES (%s, 3001, %s, 'en', 0)
                ON CONFLICT (id) DO NOTHING;
                """,
                (3000 + i, f"Status {i} for batch remainder testing with enough text"),
            )
        postgres_conn.commit()

    # Create test settings with batch_size=10
    test_settings = settings.model_copy(
        update={
            "llm_provider": "openai",
            "llm_base_url": fasttext_embedding_service["api_base"],
            "llm_model": fasttext_embedding_service["model"],
            "llm_dimensions": settings.fasttext_vector_size,
            "batch_size": 10,  # 23 statuses = 2 full batches + 3 remainder
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": settings.postgres_schema,
        }
    )

    # Create state store
    state_id = f"batch_remainder_statuses_{worker_id}"
    state_store = StateStore(neo4j, state_id)

    # Load data
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)

    # Verify all 23 statuses were loaded
    final_state = state_store.load()
    assert final_state.last_status_id >= 3023, "Should have loaded all statuses including remainder"

    # Verify in Neo4j

    neo4j.label("Post")
    count_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) WHERE p.id >= 3001 AND p.id <= 3023 RETURN count(p) AS cnt",
            {"post": "Post"},
        )
    )
    count = coerce_int(count_result[0].get("cnt"))
    assert count == 23, f"Expected 23 posts in Neo4j, got {count}"


@pytest.mark.integration
def test_load_favourites_batch_remainder(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that remaining favourites in batch are processed after main loop."""
    # Insert 17 favourites (not multiple of batch_size=10)
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (3002, 'remainder_fav_user', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility)
            VALUES (3100, 3002, 'Status for favourite remainder test', 'en', 0)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        # Insert 17 favourites
        for i in range(1, 18):
            cur.execute(
                """
                INSERT INTO favourites (id, account_id, status_id)
                VALUES (%s, 3002, 3100)
                ON CONFLICT (id) DO NOTHING;
                """,
                (4000 + i,),
            )
        postgres_conn.commit()

    # Create test settings with batch_size=10
    test_settings = settings.model_copy(
        update={
            "batch_size": 10,  # 17 favourites = 1 full batch + 7 remainder
            "fasttext_min_documents": 1,  # Reduce for test data
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": settings.postgres_schema,
        }
    )

    # Create state store
    state_id = f"batch_remainder_fav_{worker_id}"
    state_store = StateStore(neo4j, state_id)

    # Load data
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)

    # Verify all 17 favourites were loaded
    final_state = state_store.load()
    assert final_state.last_favourite_id >= 4017, "Should have loaded all favourites including remainder"


@pytest.mark.integration
@pytest.mark.integration
def test_load_blocks_batch_remainder(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that remaining blocks in batch are processed after main loop."""
    # Insert 7 blocks (not multiple of batch_size=10)
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (3005, 'remainder_block_user1', NULL), (3006, 'remainder_block_user2', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        # Insert 7 blocks
        for i in range(1, 8):
            cur.execute(
                """
                INSERT INTO blocks (id, account_id, target_account_id)
                VALUES (%s, 3005, 3006)
                ON CONFLICT (id) DO NOTHING;
                """,
                (6000 + i,),
            )
        postgres_conn.commit()

    # Create test settings with batch_size=10
    test_settings = settings.model_copy(
        update={
            "batch_size": 10,  # 7 blocks = 0 full batches + 7 remainder
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": settings.postgres_schema,
        }
    )

    # Create state store
    state_id = f"batch_remainder_blocks_{worker_id}"
    state_store = StateStore(neo4j, state_id)

    # Load data
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)

    # Verify all 7 blocks were loaded
    final_state = state_store.load()
    assert final_state.last_block_id >= 6007, "Should have loaded all blocks including remainder"


@pytest.mark.integration
def test_load_mutes_batch_remainder(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that remaining mutes in batch are processed after main loop."""
    # Insert 19 mutes (not multiple of batch_size=10)
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (3007, 'remainder_mute_user1', NULL), (3008, 'remainder_mute_user2', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        # Insert 19 mutes
        for i in range(1, 20):
            cur.execute(
                """
                INSERT INTO mutes (id, account_id, target_account_id)
                VALUES (%s, 3007, 3008)
                ON CONFLICT (id) DO NOTHING;
                """,
                (7000 + i,),
            )
        postgres_conn.commit()

    # Create test settings with batch_size=10
    test_settings = settings.model_copy(
        update={
            "batch_size": 10,  # 19 mutes = 1 full batch + 9 remainder
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": settings.postgres_schema,
        }
    )

    # Create state store
    state_id = f"batch_remainder_mutes_{worker_id}"
    state_store = StateStore(neo4j, state_id)

    # Load data
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)

    # Verify all 19 mutes were loaded
    final_state = state_store.load()
    assert final_state.last_mute_id >= 7019, "Should have loaded all mutes including remainder"


@pytest.mark.integration
def test_load_replies_batch_remainder(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that remaining replies in batch are processed after main loop."""
    # Insert 11 replies (not multiple of batch_size=10)
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (3009, 'remainder_reply_author', NULL), (3010, 'remainder_reply_target', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, reply, in_reply_to_id, in_reply_to_account_id)
            VALUES (3200, 3010, 'Original post for reply remainder test', 'en', 0, false, NULL, NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        # Insert 11 replies
        for i in range(1, 12):
            cur.execute(
                """
                INSERT INTO statuses (id, account_id, text, language, visibility, reply, in_reply_to_id, in_reply_to_account_id)
                VALUES (%s, 3009, %s, 'en', 0, true, 3200, 3010)
                ON CONFLICT (id) DO NOTHING;
                """,
                (8000 + i, f"Reply {i} for batch remainder testing"),
            )
        postgres_conn.commit()

    # Create test settings with batch_size=10
    test_settings = settings.model_copy(
        update={
            "llm_provider": "openai",
            "llm_base_url": fasttext_embedding_service["api_base"],
            "llm_model": fasttext_embedding_service["model"],
            "llm_dimensions": settings.fasttext_vector_size,
            "batch_size": 10,  # 11 replies = 1 full batch + 1 remainder
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": settings.postgres_schema,
        }
    )

    # Create state store
    state_id = f"batch_remainder_replies_{worker_id}"
    state_store = StateStore(neo4j, state_id)

    # Load data
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)

    # Verify all 11 replies were loaded (unified stream uses last_status_id)
    final_state = state_store.load()
    assert final_state.last_status_id >= 8011, "Should have loaded all replies including remainder"


@pytest.mark.integration
def test_load_reblogs_batch_remainder(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that remaining reblogs in batch are processed after main loop."""
    # Insert 5 reblogs (not multiple of batch_size=10)
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (3011, 'remainder_reblog_user1', NULL), (3012, 'remainder_reblog_user2', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, reblog_of_id)
            VALUES (3300, 3011, 'Original post for reblog remainder test', 'en', 0, NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        # Insert 5 reblogs
        for i in range(1, 6):
            cur.execute(
                """
                INSERT INTO statuses (id, account_id, text, language, visibility, reblog_of_id)
                VALUES (%s, 3012, '', 'en', 0, 3300)
                ON CONFLICT (id) DO NOTHING;
                """,
                (9000 + i,),
            )
        postgres_conn.commit()

    # Create test settings with batch_size=10
    test_settings = settings.model_copy(
        update={
            "llm_provider": "openai",
            "llm_base_url": fasttext_embedding_service["api_base"],
            "llm_model": fasttext_embedding_service["model"],
            "llm_dimensions": settings.fasttext_vector_size,
            "batch_size": 10,  # 5 reblogs = 0 full batches + 5 remainder
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": settings.postgres_schema,
        }
    )

    # Create state store
    state_id = f"batch_remainder_reblogs_{worker_id}"
    state_store = StateStore(neo4j, state_id)

    # Load data
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)

    # Verify all 5 reblogs were loaded (unified stream uses last_status_id)
    final_state = state_store.load()
    assert final_state.last_status_id >= 9005, "Should have loaded all reblogs including remainder"
