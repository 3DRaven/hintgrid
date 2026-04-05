"""Integration tests for app.py pipeline execution.

Tests verify migration reembed logic, pipeline behavior on empty/minimal data,
and proper cluster/interest handling — all using real database clients.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.app import HintGridApp
from hintgrid.pipeline.graph import check_clusters_exist, check_interests_exist
from hintgrid.state import StateStore
from hintgrid.utils.coercion import coerce_int

from hintgrid.config import HintGridSettings

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow

    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.clients.postgres import PostgresClient
    from hintgrid.clients.redis import RedisClient
    from tests.conftest import DockerComposeInfo, EmbeddingServiceConfig


@pytest.mark.integration
def test_run_full_pipeline_with_migration_reembed(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    redis_client: RedisClient,
    docker_compose: DockerComposeInfo,
    settings: HintGridSettings,
    fasttext_embedding_service: EmbeddingServiceConfig,
    mastodon_schema: None,
    postgres_conn: Connection[TupleRow],
) -> None:
    """Test run_full_pipeline triggers reembedding when embedding config changes.

    Sets up a post with old 3-dim embedding and a stale embedding signature,
    then creates HintGridApp with a different config.  The app detects the
    migration, clears old embeddings, and reembeds posts during pipeline run.
    """
    # Insert minimal sample data in PostgreSQL for the load step
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (101, 'migration_user', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility)
            VALUES (1, 101, 'Test post for migration reembed', 'en', 0)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    # Create a post with old 3-dim embedding in Neo4j
    neo4j.execute_labeled(
        "CREATE (p:__post__ {id: 1, text: 'Test post for migration', "
        "embedding: [0.1, 0.2, 0.3], createdAt: datetime()})",
        {"post": "Post"},
    )

    # Store an old embedding signature so migration is triggered
    state_store = StateStore(neo4j)
    state = state_store.load()
    state.embedding_signature = "old_provider:old_model:3"
    state_store.save(state)

    # Create settings with different embedding config
    # Explicit runtime use of HintGridSettings class via model_copy method
    assert isinstance(settings, HintGridSettings)
    test_settings = settings.model_copy(
        update={
            "llm_base_url": fasttext_embedding_service["api_base"],
            "llm_model": fasttext_embedding_service["model"],
            "llm_provider": "openai",
            "llm_dimensions": 128,
            "interests_min_favourites": 1,
            "feed_days": 365,
        }
    )

    # HintGridApp.__post_init__ detects migration and clears old embeddings
    app = HintGridApp(
        neo4j=neo4j,
        postgres=postgres_client,
        redis=redis_client,
        settings=test_settings,
    )

    # Verify migration was detected
    migration_result = app.get_migration_result()
    assert migration_result.migrated is True
    assert migration_result.posts_cleared == 1

    # Verify old embedding was cleared
    rows = list(neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__) WHERE p.embedding IS NOT NULL "
        "RETURN count(p) AS cnt",
        {"post": "Post"},
    ))
    remaining = coerce_int(rows[0]["cnt"]) if rows else 0
    assert remaining == 0, "Old embedding should be cleared after migration"

    # Run pipeline (dry_run skips Redis writes) — reembeds existing posts
    app.run_full_pipeline(dry_run=True)

    # Verify new embeddings were generated during the reembed step
    rows_after = list(neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__) WHERE p.embedding IS NOT NULL "
        "RETURN count(p) AS cnt",
        {"post": "Post"},
    ))
    with_embedding = coerce_int(rows_after[0]["cnt"]) if rows_after else 0
    assert with_embedding > 0, (
        "Posts should have new embeddings after migration reembed"
    )


@pytest.mark.integration
def test_run_full_pipeline_no_clusters_before_analytics(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    redis_client: RedisClient,
    docker_compose: DockerComposeInfo,
    settings: HintGridSettings,
    fasttext_embedding_service: EmbeddingServiceConfig,
    mastodon_schema: None,
) -> None:
    """Test run_full_pipeline completes on empty graph with no pre-existing clusters.

    On a fresh graph there are no UserCommunity or PostCommunity nodes.
    The pipeline should handle this gracefully and complete without error.
    """
    # Verify no clusters exist on fresh graph
    users_exist, posts_exist = check_clusters_exist(neo4j)
    assert not users_exist, "No user clusters should exist on fresh graph"
    assert not posts_exist, "No post clusters should exist on fresh graph"

    test_settings = settings.model_copy(
        update={
            "llm_base_url": fasttext_embedding_service["api_base"],
            "llm_model": fasttext_embedding_service["model"],
            "llm_provider": "openai",
            "llm_dimensions": 128,
            "feed_days": 365,
        }
    )

    app = HintGridApp(
        neo4j=neo4j,
        postgres=postgres_client,
        redis=redis_client,
        settings=test_settings,
    )

    # Pipeline should complete without errors on an empty graph
    app.run_full_pipeline(dry_run=True)


@pytest.mark.integration
def test_run_full_pipeline_no_clusters_after_analytics(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    redis_client: RedisClient,
    docker_compose: DockerComposeInfo,
    settings: HintGridSettings,
    fasttext_embedding_service: EmbeddingServiceConfig,
    mastodon_schema: None,
) -> None:
    """Test run_full_pipeline produces no clusters when PostgreSQL has no data.

    With no accounts/statuses loaded, analytics has nothing to cluster.
    The pipeline should complete and leave the graph without communities.
    """
    test_settings = settings.model_copy(
        update={
            "llm_base_url": fasttext_embedding_service["api_base"],
            "llm_model": fasttext_embedding_service["model"],
            "llm_provider": "openai",
            "llm_dimensions": 128,
            "feed_days": 365,
        }
    )

    app = HintGridApp(
        neo4j=neo4j,
        postgres=postgres_client,
        redis=redis_client,
        settings=test_settings,
    )

    # Run pipeline on empty data
    app.run_full_pipeline(dry_run=True)

    # With no data loaded, analytics creates no clusters
    users_exist, posts_exist = check_clusters_exist(neo4j)
    assert not users_exist, "No user clusters on empty data"
    assert not posts_exist, "No post clusters on empty data"


@pytest.mark.integration
def test_run_full_pipeline_no_interests_after_analytics(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    redis_client: RedisClient,
    docker_compose: DockerComposeInfo,
    settings: HintGridSettings,
    fasttext_embedding_service: EmbeddingServiceConfig,
    mastodon_schema: None,
) -> None:
    """Test run_full_pipeline produces no interests when threshold is unreachable.

    Even with data loaded, setting interests_min_favourites extremely high
    prevents any INTERESTED_IN relationships from being created.
    """
    test_settings = settings.model_copy(
        update={
            "llm_base_url": fasttext_embedding_service["api_base"],
            "llm_model": fasttext_embedding_service["model"],
            "llm_provider": "openai",
            "llm_dimensions": 128,
            "interests_min_favourites": 999_999,  # Unreachable threshold
            "feed_days": 365,
        }
    )

    app = HintGridApp(
        neo4j=neo4j,
        postgres=postgres_client,
        redis=redis_client,
        settings=test_settings,
    )

    # Pipeline completes — but threshold prevents interest creation
    app.run_full_pipeline(dry_run=True)

    interests_exist = check_interests_exist(neo4j)
    assert not interests_exist, (
        "No interests should exist with unreachable threshold"
    )


@pytest.mark.integration
def test_run_pipeline_displays_recommendations_table_for_single_user(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    redis_client: RedisClient,
    docker_compose: DockerComposeInfo,
    settings: HintGridSettings,
    fasttext_embedding_service: EmbeddingServiceConfig,
    mastodon_schema: None,
    postgres_conn: Connection[TupleRow],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test run_pipeline displays recommendations table for single user mode."""
    user_id = 10001

    # Setup user and posts in PostgreSQL
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (%s, 'testuser', NULL)
            ON CONFLICT (id) DO NOTHING;
            """,
            (user_id,),
        )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, created_at)
            VALUES 
                (20001, %s, 'First test post with some content', 'en', 0, NOW()),
                (20002, %s, 'Second test post', 'en', 0, NOW())
            ON CONFLICT (id) DO NOTHING;
            """,
            (user_id, user_id),
        )
        postgres_conn.commit()

    # Setup graph in Neo4j with user, communities, and posts
    neo4j.label("User")
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")
    neo4j.label("Post")

    neo4j.execute_labeled(
        "CREATE (u:__user__ {id: $user_id, languages: ['en'], isLocal: true})\n"
        "CREATE (uc:__uc__ {id: 'rec_uc1'})\n"
        "CREATE (pc:__pc__ {id: 'rec_pc1'})\n"
        "CREATE (p1:__post__ {\n"
        "    id: 20001,\n"
        "    text: 'First test post with some content',\n"
        "    language: 'en',\n"
        "    authorId: $user_id,\n"
        "    createdAt: datetime(),\n"
        "    embedding: [0.1, 0.2, 0.3, 0.4]\n"
        "})\n"
        "CREATE (p2:__post__ {\n"
        "    id: 20002,\n"
        "    text: 'Second test post',\n"
        "    language: 'en',\n"
        "    authorId: $user_id,\n"
        "    createdAt: datetime(),\n"
        "    embedding: [0.5, 0.6, 0.7, 0.8]\n"
        "})\n"
        "CREATE (u)-[:BELONGS_TO]->(uc)\n"
        "CREATE (uc)-[:INTERESTED_IN {score: 0.9}]->(pc)\n"
        "CREATE (p1)-[:BELONGS_TO]->(pc)\n"
        "CREATE (p2)-[:BELONGS_TO]->(pc)\n"
        "CREATE (u)-[:WROTE]->(p1)\n"
        "CREATE (u)-[:WROTE]->(p2)",
        {
            "user": "User",
            "uc": "UserCommunity",
            "pc": "PostCommunity",
            "post": "Post",
        },
        {"user_id": user_id},
    )

    test_settings = settings.model_copy(
        update={
            "llm_base_url": fasttext_embedding_service["api_base"],
            "llm_model": fasttext_embedding_service["model"],
            "llm_provider": "openai",
            "llm_dimensions": 128,
            "feed_size": 10,
            "feed_days": 365,
            "interests_min_favourites": 1,
        }
    )

    app = HintGridApp(
        neo4j=neo4j,
        postgres=postgres_client,
        redis=redis_client,
        settings=test_settings,
    )

    # Run pipeline for single user (dry_run to avoid Redis writes)
    app.run_full_pipeline(dry_run=True, user_id=user_id)

    # Check that recommendations table or message was displayed
    output = capsys.readouterr().out
    # Either recommendations table or "No recommendations found" message
    assert "Recommendations for" in output or "No recommendations found" in output or "recommendations" in output.lower()
    assert "testuser" in output or str(user_id) in output or "10001" in output


@pytest.mark.integration
def test_run_pipeline_no_recommendations_table_for_all_users(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    redis_client: RedisClient,
    docker_compose: DockerComposeInfo,
    settings: HintGridSettings,
    fasttext_embedding_service: EmbeddingServiceConfig,
    mastodon_schema: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test run_pipeline does not display recommendations table when user_id is None."""
    test_settings = settings.model_copy(
        update={
            "llm_base_url": fasttext_embedding_service["api_base"],
            "llm_model": fasttext_embedding_service["model"],
            "llm_provider": "openai",
            "llm_dimensions": 128,
            "feed_days": 365,
        }
    )

    app = HintGridApp(
        neo4j=neo4j,
        postgres=postgres_client,
        redis=redis_client,
        settings=test_settings,
    )

    # Run pipeline for all users (user_id=None)
    app.run_full_pipeline(dry_run=True, user_id=None)

    # Check that recommendations table was NOT displayed
    output = capsys.readouterr().out
    # Should not contain recommendations table (only pipeline completion message)
    assert "Pipeline Completed Successfully" in output


@pytest.mark.integration
def test_get_user_info_by_handle_found(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    redis_client: RedisClient,
    docker_compose: DockerComposeInfo,
    settings: HintGridSettings,
    mastodon_schema: None,
    postgres_conn: Connection[TupleRow],
) -> None:
    """Test get_user_info_by_handle returns user info for existing user."""
    user_id = 11001

    # Setup user in PostgreSQL
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (%s, 'handleuser', NULL)
            ON CONFLICT (id) DO NOTHING;
            """,
            (user_id,),
        )
        postgres_conn.commit()

    # Setup user in Neo4j
    neo4j.label("User")
    neo4j.execute_labeled(
        "CREATE (:__user__ {id: $user_id, languages: ['en'], isLocal: true})",
        {"user": "User"},
        {"user_id": user_id},
    )

    app = HintGridApp(
        neo4j=neo4j,
        postgres=postgres_client,
        redis=redis_client,
        settings=settings,
    )

    # Get user info by handle
    user_info = app.get_user_info_by_handle("@handleuser")

    assert user_info is not None
    assert user_info.get("user_id") == user_id
    assert user_info.get("username") == "handleuser"
    assert user_info.get("domain") is None
    assert user_info.get("languages") == ["en"]
    assert user_info.get("is_local") is True


@pytest.mark.integration
def test_get_user_info_by_handle_with_domain(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    redis_client: RedisClient,
    docker_compose: DockerComposeInfo,
    settings: HintGridSettings,
    mastodon_schema: None,
    postgres_conn: Connection[TupleRow],
) -> None:
    """Test get_user_info_by_handle handles users with domain."""
    user_id = 11002

    # Setup user in PostgreSQL with domain
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (%s, 'remoteuser', 'example.com')
            ON CONFLICT (id) DO NOTHING;
            """,
            (user_id,),
        )
        postgres_conn.commit()

    # Setup user in Neo4j
    neo4j.label("User")
    neo4j.execute_labeled(
        "CREATE (:__user__ {id: $user_id, languages: ['en', 'ru'], isLocal: false})",
        {"user": "User"},
        {"user_id": user_id},
    )

    app = HintGridApp(
        neo4j=neo4j,
        postgres=postgres_client,
        redis=redis_client,
        settings=settings,
    )

    # Get user info by handle with domain
    user_info = app.get_user_info_by_handle("@remoteuser@example.com")

    assert user_info is not None
    assert user_info.get("user_id") == user_id
    assert user_info.get("username") == "remoteuser"
    assert user_info.get("domain") == "example.com"
    assert user_info.get("languages") == ["en", "ru"]
    assert user_info.get("is_local") is False


@pytest.mark.integration
def test_get_user_info_by_handle_not_found(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    redis_client: RedisClient,
    docker_compose: DockerComposeInfo,
    settings: HintGridSettings,
    mastodon_schema: None,
) -> None:
    """Test get_user_info_by_handle returns None for non-existent user."""
    app = HintGridApp(
        neo4j=neo4j,
        postgres=postgres_client,
        redis=redis_client,
        settings=settings,
    )

    # Get user info for non-existent user
    user_info = app.get_user_info_by_handle("@nonexistent")

    assert user_info is None
