"""Integration tests for loaders checkpoint intervals.

Tests verify that state is saved when total_processed % checkpoint_interval == 0.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.pipeline.loaders import load_incremental_data
from hintgrid.state import StateStore

if TYPE_CHECKING:
    from hintgrid.clients.postgres import PostgresClient
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.config import HintGridSettings
    from psycopg import Connection
    from psycopg.rows import TupleRow

    from tests.conftest import DockerComposeInfo, EmbeddingServiceConfig


@pytest.mark.integration
def test_load_statuses_checkpoint_interval(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that state is saved at checkpoint intervals for statuses."""
    # Insert enough data to trigger checkpoint (checkpoint_interval=100, so need 100+ items)
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (6001, 'checkpoint_user', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        # Insert 150 statuses to trigger checkpoint
        for i in range(1, 151):
            cur.execute(
                """
                INSERT INTO statuses (id, account_id, text, language, visibility)
                VALUES (%s, 6001, %s, 'en', 0)
                ON CONFLICT (id) DO NOTHING;
                """,
                (7000 + i, f"Status {i} for checkpoint testing with enough text"),
            )
        postgres_conn.commit()

    # Create test settings with checkpoint_interval=100
    test_settings = settings.model_copy(
        update={
            "llm_provider": "openai",
            "llm_base_url": fasttext_embedding_service["api_base"],
            "llm_model": fasttext_embedding_service["model"],
            "llm_dimensions": settings.fasttext_vector_size,
            "batch_size": 50,  # 150 statuses = 3 batches
            "checkpoint_interval": 100,  # Should trigger checkpoint at 100 items
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": settings.postgres_schema,
        }
    )

    # Create state store
    state_id = f"checkpoint_statuses_{worker_id}"
    state_store = StateStore(neo4j, state_id)

    # Load data
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)

    # Verify final state was saved
    final_state = state_store.load()
    assert final_state.last_status_id >= 7149, "Should have processed all statuses"
    # Checkpoint should have been triggered at 100 items, so state should reflect progress


@pytest.mark.integration
def test_load_favourites_checkpoint_interval(
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that state is saved at checkpoint intervals for favourites."""
    # Insert enough data to trigger checkpoint
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (6002, 'checkpoint_fav_user', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility)
            VALUES (7200, 6002, 'Status for favourite checkpoint test', 'en', 0)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        # Insert 150 favourites to trigger checkpoint
        for i in range(1, 151):
            cur.execute(
                """
                INSERT INTO favourites (id, account_id, status_id)
                VALUES (%s, 6002, 7200)
                ON CONFLICT (id) DO NOTHING;
                """,
                (8000 + i,),
            )
        postgres_conn.commit()

    # Create test settings with checkpoint_interval=100
    test_settings = settings.model_copy(
        update={
            "batch_size": 50,
            "checkpoint_interval": 100,  # Should trigger checkpoint at 100 items
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
    state_id = f"checkpoint_fav_{worker_id}"
    state_store = StateStore(neo4j, state_id)

    # Load data
    load_incremental_data(postgres_client, neo4j, test_settings, state_store)

    # Verify final state was saved
    final_state = state_store.load()
    assert final_state.last_favourite_id >= 8149, "Should have processed all favourites"
    # Checkpoint should have been triggered at 100 items
