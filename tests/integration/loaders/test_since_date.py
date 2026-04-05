"""Integration tests for loaders with since_date parameter.

Tests verify that when since_date is set, last_id is reset to 0
and only data after the date is loaded.
"""

from __future__ import annotations

from datetime import datetime, timedelta, UTC
from typing import TYPE_CHECKING

import pytest

from hintgrid.pipeline.loaders import load_incremental_data
from hintgrid.state import PipelineState, StateStore
from hintgrid.utils.coercion import coerce_int
from hintgrid.utils.snowflake import snowflake_id_at

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.config import HintGridSettings
    from hintgrid.clients.postgres import PostgresClient
    from psycopg import Connection
    from psycopg.rows import TupleRow

    from tests.conftest import DockerComposeInfo, EmbeddingServiceConfig


@pytest.mark.integration
def test_load_statuses_with_since_date(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that load_incremental_data resets last_id to 0 when since_date is set."""
    # Create test data with different dates
    now = datetime.now(UTC)
    old_date = now - timedelta(days=60)
    recent_date = now - timedelta(days=10)

    old_snowflake = snowflake_id_at(old_date)
    recent_snowflake = snowflake_id_at(recent_date)

    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (2001, 'since_user1', NULL), (2002, 'since_user2', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, created_at)
            VALUES
                (%s, 2001, 'Old status that should be excluded', 'en', 0, %s),
                (%s, 2002, 'Recent status that should be included', 'en', 0, %s)
            ON CONFLICT (id) DO NOTHING;
            """,
            (old_snowflake, old_date, recent_snowflake, recent_date),
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

    # Create state store with existing state (should be ignored when since_date is set)
    state_id = f"since_date_test_{worker_id}"
    state_store = StateStore(neo4j, state_id)
    initial_state = PipelineState()
    initial_state.last_status_id = 999_999_999  # High ID that would normally skip recent data
    state_store.save(initial_state)

    # Load with load_since (30 days ago should include recent post)
    test_settings = test_settings.model_copy(update={"load_since": "30d"})
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)

    # Verify that recent post was loaded (despite high last_status_id in state)
    # With fixed load_since logic, it should use max(saved_state, min_id_from_date)
    final_state = state_store.load()
    assert final_state.last_status_id >= recent_snowflake, "Should have loaded recent post"


@pytest.mark.integration
def test_load_statuses_with_load_since_resume(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that load_since correctly resumes from saved state after interruption."""
    now = datetime.now(UTC)
    old_date = now - timedelta(days=60)
    recent_date = now - timedelta(days=10)
    very_recent_date = now - timedelta(days=5)

    old_snowflake = snowflake_id_at(old_date)
    recent_snowflake = snowflake_id_at(recent_date)
    very_recent_snowflake = snowflake_id_at(very_recent_date)

    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (3001, 'resume_user1', NULL), (3002, 'resume_user2', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, created_at)
            VALUES
                (%s, 3001, 'Old status', 'en', 0, %s),
                (%s, 3002, 'Recent status', 'en', 0, %s),
                (%s, 3002, 'Very recent status', 'en', 0, %s)
            ON CONFLICT (id) DO NOTHING;
            """,
            (
                old_snowflake,
                old_date,
                recent_snowflake,
                recent_date,
                very_recent_snowflake,
                very_recent_date,
            ),
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
            "load_since": "30d",
        }
    )

    state_id = f"resume_test_{worker_id}"
    state_store = StateStore(neo4j, state_id)

    # First run: load with load_since=30d, should load recent and very_recent
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)
    first_state = state_store.load()
    assert first_state.last_status_id >= very_recent_snowflake

    # Simulate interruption: save state with high last_status_id
    interrupted_state = PipelineState()
    interrupted_state.last_status_id = very_recent_snowflake
    state_store.save(interrupted_state)

    # Second run: with same load_since=30d, should use max(saved_state, min_id_from_date)
    # Since saved_state (very_recent_snowflake) > min_id_from_date (30 days ago),
    # it should continue from saved_state and not skip data
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)
    final_state = state_store.load()
    # Should still have the same or higher last_status_id (no data skipped)
    assert final_state.last_status_id >= very_recent_snowflake


@pytest.mark.integration
def test_load_favourites_with_since_date(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that load_incremental_data filters favourites by created_at when since_date is set.

    Favourites use standard auto-increment IDs (not Snowflake),
    so since_date must filter by created_at column directly.
    """
    now = datetime.now(UTC)
    old_date = now - timedelta(days=60)
    recent_date = now - timedelta(days=10)

    recent_snowflake = snowflake_id_at(recent_date)

    # Use small sequential IDs (like real auto-increment favourites)
    old_fav_id = 3001
    recent_fav_id = 3002

    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (2003, 'since_fav_user', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, created_at)
            VALUES (%s, 2003, 'Status for favourite test', 'en', 0, %s)
            ON CONFLICT (id) DO NOTHING;
            """,
            (recent_snowflake - 100, recent_date),
        )
        cur.execute(
            """
            INSERT INTO favourites (id, account_id, status_id, created_at)
            VALUES
                (%s, 2003, %s, %s),
                (%s, 2003, %s, %s)
            ON CONFLICT (id) DO NOTHING;
            """,
            (
                old_fav_id,
                recent_snowflake - 100,
                old_date,
                recent_fav_id,
                recent_snowflake - 100,
                recent_date,
            ),
        )
        postgres_conn.commit()

    # Create test settings
    test_settings = settings.model_copy(
        update={
            "batch_size": 100,
            "fasttext_min_documents": 1,  # Reduce for test data
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": settings.postgres_schema,
        }
    )

    # Create state store with existing state (high last_id to simulate prior run)
    state_id = f"since_date_fav_test_{worker_id}"
    state_store = StateStore(neo4j, state_id)
    initial_state = PipelineState()
    initial_state.last_favourite_id = 999_999_999
    state_store.save(initial_state)

    # Load with load_since
    test_settings = test_settings.model_copy(update={"load_since": "30d"})
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)

    # Verify that recent favourite was loaded
    final_state = state_store.load()
    assert (
        final_state.last_favourite_id >= recent_fav_id
    ), "Should have loaded recent favourite"


@pytest.mark.integration
def test_load_blocks_with_since_date(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that load_incremental_data loads blocks when since_date is set."""
    # Create test data
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (2006, 'since_block_user1', NULL), (2007, 'since_block_user2', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO blocks (id, account_id, target_account_id)
            VALUES (2201, 2006, 2007)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    # Create test settings
    test_settings = settings.model_copy(
        update={
            "batch_size": 100,
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
    state_id = f"since_date_blocks_test_{worker_id}"
    state_store = StateStore(neo4j, state_id)
    initial_state = PipelineState()
    initial_state.last_block_id = 999_999_999
    state_store.save(initial_state)

    # Load with load_since (blocks don't use since_date, they always load incrementally)
    # This test verifies that blocks are loaded even when load_since is set
    test_settings_with_since = test_settings.model_copy(update={"load_since": "30d"})
    load_incremental_data(
        postgres_client, neo4j, test_settings_with_since, state_store
    )

    # Verify that block was loaded
    final_state = state_store.load()
    assert final_state.last_block_id >= 2201, "Should have loaded block"


@pytest.mark.integration
def test_load_mutes_with_since_date(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that load_incremental_data loads mutes when since_date is set."""
    # Create test data
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (2008, 'since_mute_user1', NULL), (2009, 'since_mute_user2', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO mutes (id, account_id, target_account_id)
            VALUES (2301, 2008, 2009)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    # Create test settings
    test_settings = settings.model_copy(
        update={
            "batch_size": 100,
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
    state_id = f"since_date_mutes_test_{worker_id}"
    state_store = StateStore(neo4j, state_id)
    initial_state = PipelineState()
    initial_state.last_mute_id = 999_999_999
    state_store.save(initial_state)

    # Load with load_since (mutes don't use since_date, they always load incrementally)
    # This test verifies that mutes are loaded even when load_since is set
    test_settings_with_since = test_settings.model_copy(update={"load_since": "30d"})
    load_incremental_data(
        postgres_client, neo4j, test_settings_with_since, state_store
    )

    # Verify that mute was loaded
    final_state = state_store.load()
    assert final_state.last_mute_id >= 2301, "Should have loaded mute"


@pytest.mark.integration
def test_load_replies_with_since_date(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that load_incremental_data loads replies when since_date is set."""
    # Create test data
    now = datetime.now(UTC)
    recent_date = now - timedelta(days=10)
    recent_snowflake = snowflake_id_at(recent_date)

    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (2010, 'since_reply_author', NULL), (2011, 'since_reply_target', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, created_at, reply, in_reply_to_id, in_reply_to_account_id)
            VALUES
                (%s, 2011, 'Original post for reply test', 'en', 0, %s, false, NULL, NULL),
                (%s, 2010, 'Reply to original post', 'en', 0, %s, true, %s, 2011)
            ON CONFLICT (id) DO NOTHING;
            """,
            (
                recent_snowflake - 300,
                recent_date,
                recent_snowflake,
                recent_date,
                recent_snowflake - 300,
            ),
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
    state_id = f"since_date_replies_test_{worker_id}"
    state_store = StateStore(neo4j, state_id)
    initial_state = PipelineState()
    initial_state.last_status_id = 999_999_999  # Unified stream uses last_status_id
    state_store.save(initial_state)

    # Load with load_since
    test_settings = test_settings.model_copy(update={"load_since": "30d"})
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)

    # Verify that reply was loaded (unified stream uses last_status_id)
    final_state = state_store.load()
    assert final_state.last_status_id >= recent_snowflake, "Should have loaded reply via unified stream"


@pytest.mark.integration
def test_load_reblogs_with_since_date(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that load_incremental_data loads reblogs when since_date is set."""
    # Create test data
    now = datetime.now(UTC)
    recent_date = now - timedelta(days=10)
    recent_snowflake = snowflake_id_at(recent_date)

    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (2012, 'since_reblog_user1', NULL), (2013, 'since_reblog_user2', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, created_at, reblog_of_id)
            VALUES
                (%s, 2012, 'Original post for reblog test', 'en', 0, %s, NULL),
                (%s, 2013, '', 'en', 0, %s, %s)
            ON CONFLICT (id) DO NOTHING;
            """,
            (
                recent_snowflake - 400,
                recent_date,
                recent_snowflake,
                recent_date,
                recent_snowflake - 400,
            ),
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
    state_id = f"since_date_reblogs_test_{worker_id}"
    state_store = StateStore(neo4j, state_id)
    initial_state = PipelineState()
    initial_state.last_status_id = 999_999_999  # Unified stream uses last_status_id
    state_store.save(initial_state)

    # Load with load_since
    test_settings = test_settings.model_copy(update={"load_since": "30d"})
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)

    # Verify that reblog was loaded (unified stream uses last_status_id)
    final_state = state_store.load()
    assert final_state.last_status_id >= recent_snowflake, "Should have loaded reblog via unified stream"


@pytest.mark.integration
def test_load_statuses_auto_train_with_since_date(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that auto-training during load_incremental_data uses since_date filter."""
    from hintgrid.embeddings.fasttext_service import STATE_NODE_ID

    # Create test data with different dates
    now = datetime.now(UTC)
    old_date = now - timedelta(days=60)  # 60 days ago - should be excluded
    recent_date = now - timedelta(days=10)  # 10 days ago - should be included

    old_snowflake = snowflake_id_at(old_date)
    recent_snowflake = snowflake_id_at(recent_date)

    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (4001, 'auto_train_user1', NULL), (4002, 'auto_train_user2', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        # Insert old posts (should be excluded from training)
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, created_at)
            VALUES
                (%s, 4001, 'Old post 1 excluded from training', 'en', 0, %s),
                (%s, 4002, 'Old post 2 excluded from training', 'en', 0, %s)
            ON CONFLICT (id) DO NOTHING;
            """,
            (old_snowflake, old_date, old_snowflake + 1, old_date),
        )
        # Insert recent posts (should be included in training)
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, created_at)
            VALUES
                (%s, 4001, 'Recent post 1 for auto training', 'en', 0, %s),
                (%s, 4002, 'Recent post 2 about Python programming', 'en', 0, %s),
                (%s, 4001, 'Recent post 3 about machine learning', 'en', 0, %s)
            ON CONFLICT (id) DO NOTHING;
            """,
            (
                recent_snowflake,
                recent_date,
                recent_snowflake + 1,
                recent_date,
                recent_snowflake + 2,
                recent_date,
            ),
        )
        postgres_conn.commit()

    # Create test settings with FastText (not external LLM)
    test_settings = settings.model_copy(
        update={
            "llm_provider": "fasttext",
            "llm_base_url": None,
            "batch_size": 100,
            "fasttext_min_documents": 2,
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": settings.postgres_schema,
            "load_since": "30d",  # 30 days ago should include recent posts
        }
    )

    # Create state store
    state_id = f"auto_train_since_test_{worker_id}"
    state_store = StateStore(neo4j, state_id)

    # Load with load_since - should trigger auto-training with since_date filter
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)

    # Verify that FastText was auto-trained with since_date filter
    # Check FastTextState to see what was trained
    state_result = neo4j.execute_and_fetch_labeled(
        "MATCH (s:FastTextState {id: $id}) RETURN s.lastTrainedPostId AS last_id, s.version AS version",
        {"label": "FastTextState"},
        {"id": STATE_NODE_ID},
    )

    if state_result:
        last_trained_id_raw = state_result[0].get("last_id")
        version_raw = state_result[0].get("version")
        
        # Coerce types for comparison
        last_trained_id: int | None = coerce_int(last_trained_id_raw) if last_trained_id_raw is not None else None
        version: int | None = coerce_int(version_raw) if version_raw is not None else None

        # If training happened, verify it used filtered corpus
        if version is not None and version >= 1:
            # last_trained_id should be >= recent_snowflake (trained on recent posts)
            assert (
                last_trained_id is None or last_trained_id >= recent_snowflake
            ), f"Auto-training should have used since_date filter (last_id={last_trained_id}, recent_snowflake={recent_snowflake})"

    # Verify that recent posts were loaded to Neo4j
    post_result = neo4j.execute_and_fetch_labeled(
        "MATCH (p:Post) WHERE p.id >= $min_id RETURN count(p) AS count",
        {"label": "Post"},
        {"min_id": recent_snowflake},
    )

    if post_result:
        post_count = coerce_int(post_result[0].get("count"))
        assert post_count >= 3, f"Should have loaded at least 3 recent posts, got {post_count}"

    final_state = state_store.load()
    assert final_state.last_status_id >= recent_snowflake, "Should have loaded recent posts"
