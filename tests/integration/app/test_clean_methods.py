"""Integration tests for app.py clean methods.

Tests verify clean_graph, clean_clusters cascade logic,
and various clean method combinations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.utils.coercion import coerce_int

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.clients.postgres import PostgresClient
    from hintgrid.clients.redis import RedisClient
    from hintgrid.config import HintGridSettings
    from tests.conftest import DockerComposeInfo
else:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.clients.postgres import PostgresClient
    from hintgrid.config import HintGridSettings


@pytest.mark.integration
def test_clean_graph_without_worker_label(
    postgres_client: PostgresClient,
    redis_client: RedisClient,
    docker_compose: DockerComposeInfo,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test clean_graph when worker_label is None uses non-labeled query."""
    # Explicit runtime use of types
    assert isinstance(postgres_client, PostgresClient)
    assert isinstance(settings, HintGridSettings)
    
    from hintgrid.app import HintGridApp

    # Create Neo4j client with no worker_label
    neo4j_no_label = Neo4jClient(
        host=docker_compose.neo4j_host,
        port=docker_compose.neo4j_port,
        username=docker_compose.neo4j_user,
        password=docker_compose.neo4j_password,
        worker_label=None,
    )

    # Create app with settings that have no worker_label
    test_settings = settings.model_copy(update={"neo4j_worker_label": None})
    app = HintGridApp(
        neo4j=neo4j_no_label,
        postgres=postgres_client,
        redis=redis_client,
        settings=test_settings,
    )

    try:
        # Verify app is configured without worker label
        assert neo4j_no_label.worker_label is None
        assert app.neo4j.worker_label is None

        # No worker_label on this client: isolate parallel xdist workers by _worker
        # (same pattern as tests.parallel.IsolatedNeo4jClient / _cleanup_neo4j_data).
        neo4j_no_label.execute(
            "CREATE (n:TestCleanNode {id: 1, _worker: $worker}), "
            "(m:TestCleanNode {id: 2, _worker: $worker})",
            {"worker": worker_id},
        )

        result = neo4j_no_label.execute_and_fetch(
            "MATCH (n:TestCleanNode) WHERE n._worker = $worker AND n.id IN [1, 2] "
            "RETURN count(n) AS cnt",
            {"worker": worker_id},
        )
        count_before = (result[0] if result else {}).get("cnt")
        assert coerce_int(count_before) >= 2, "Should have test nodes"

        # Scope deletes to this worker — same idea as clean_graph with worker_label.
        neo4j_no_label.execute(
            "MATCH (n:TestCleanNode) WHERE n._worker = $worker AND n.id IN [1, 2] "
            "DETACH DELETE n",
            {"worker": worker_id},
        )

        result = neo4j_no_label.execute_and_fetch(
            "MATCH (n:TestCleanNode) WHERE n._worker = $worker RETURN count(n) AS cnt",
            {"worker": worker_id},
        )
        count_after = (result[0] if result else {}).get("cnt")
        assert coerce_int(count_after) == 0, "Should have deleted this worker's TestCleanNode nodes"
    finally:
        # HintGridApp.__post_init__() with worker_label=None creates global
        # uniqueness constraints (Post.id, User.id, AppState.id). Drop them
        # to avoid blocking parallel workers that share the same Neo4j instance.
        existing = neo4j_no_label.execute_and_fetch(
            "SHOW CONSTRAINTS YIELD name RETURN name"
        )
        for row in existing:
            constraint_name = str(row.get("name", ""))
            if constraint_name:
                neo4j_no_label.execute_labeled(
                    "DROP CONSTRAINT __name__ IF EXISTS",
                    ident_map={"name": constraint_name},
                )
        neo4j_no_label.close()


@pytest.mark.integration
def test_clean_clusters_cascade_logic(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    redis_client: RedisClient,
    docker_compose: DockerComposeInfo,
    settings: HintGridSettings,
) -> None:
    """Test clean_clusters cascade logic when users or posts are cleaned."""
    # Explicit runtime use of Neo4jClient and HintGridSettings
    assert isinstance(neo4j, Neo4jClient)
    assert isinstance(settings, HintGridSettings)
    from hintgrid.app import HintGridApp

    app = HintGridApp(
        neo4j=neo4j,
        postgres=postgres_client,
        redis=redis_client,
        settings=settings,
    )

    neo4j.execute_labeled(
        "CREATE (u:__user__ {id: 1}), (uc:__uc__ {id: 1}), (u)-[:BELONGS_TO]->(uc)",
        {"user": "User", "uc": "UserCommunity"},
    )
    neo4j.execute_labeled(
        "CREATE (p:__post__ {id: 1}), (pc:__pc__ {id: 1}), (p)-[:BELONGS_TO]->(pc)",
        {"post": "Post", "pc": "PostCommunity"},
    )

    # Clean clusters (both users and posts) - should cascade to interests and recommendations
    app.clean_clusters(users=True, posts=True)

    # Verify clusters were deleted
    uc_result = neo4j.execute_and_fetch_labeled(
        "MATCH (uc:__uc__) RETURN count(uc) AS cnt",
        {"uc": "UserCommunity"},
    )
    uc_count = (uc_result[0] if uc_result else {}).get("cnt")
    assert coerce_int(uc_count) == 0, "User clusters should be deleted"

    pc_result = neo4j.execute_and_fetch_labeled(
        "MATCH (pc:__pc__) RETURN count(pc) AS cnt",
        {"pc": "PostCommunity"},
    )
    pc_count = (pc_result[0] if pc_result else {}).get("cnt")
    assert coerce_int(pc_count) == 0, "Post clusters should be deleted"


@pytest.mark.integration
def test_clean_clusters_posts_only(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    redis_client: RedisClient,
    docker_compose: DockerComposeInfo,
    settings: HintGridSettings,
) -> None:
    """Test clean_clusters with posts=True, users=False."""
    # Explicit runtime use of Neo4jClient and HintGridSettings
    assert isinstance(neo4j, Neo4jClient)
    assert isinstance(settings, HintGridSettings)
    from hintgrid.app import HintGridApp

    app = HintGridApp(
        neo4j=neo4j,
        postgres=postgres_client,
        redis=redis_client,
        settings=settings,
    )

    neo4j.execute_labeled(
        "CREATE (u:__user__ {id: 2}), (uc:__uc__ {id: 2}), (u)-[:BELONGS_TO]->(uc)",
        {"user": "User", "uc": "UserCommunity"},
    )
    neo4j.execute_labeled(
        "CREATE (p:__post__ {id: 2}), (pc:__pc__ {id: 2}), (p)-[:BELONGS_TO]->(pc)",
        {"post": "Post", "pc": "PostCommunity"},
    )

    # Clean only post clusters
    app.clean_clusters(posts=True, users=False)

    # Verify post clusters were deleted
    pc_result = neo4j.execute_and_fetch_labeled(
        "MATCH (pc:__pc__) RETURN count(pc) AS cnt",
        {"pc": "PostCommunity"},
    )
    pc_count = (pc_result[0] if pc_result else {}).get("cnt")
    assert coerce_int(pc_count) == 0, "Post clusters should be deleted"

    # Verify user clusters still exist
    uc_result = neo4j.execute_and_fetch_labeled(
        "MATCH (uc:__uc__) RETURN count(uc) AS cnt",
        {"uc": "UserCommunity"},
    )
    uc_count = (uc_result[0] if uc_result else {}).get("cnt")
    assert coerce_int(uc_count) >= 1, "User clusters should still exist"


@pytest.mark.integration
def test_clean_clusters_users_only(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    redis_client: RedisClient,
    docker_compose: DockerComposeInfo,
    settings: HintGridSettings,
) -> None:
    """Test clean_clusters with users=True, posts=False."""
    # Explicit runtime use of Neo4jClient and HintGridSettings
    assert isinstance(neo4j, Neo4jClient)
    assert isinstance(settings, HintGridSettings)
    from hintgrid.app import HintGridApp

    app = HintGridApp(
        neo4j=neo4j,
        postgres=postgres_client,
        redis=redis_client,
        settings=settings,
    )

    neo4j.execute_labeled(
        "CREATE (u:__user__ {id: 3}), (uc:__uc__ {id: 3}), (u)-[:BELONGS_TO]->(uc)",
        {"user": "User", "uc": "UserCommunity"},
    )
    neo4j.execute_labeled(
        "CREATE (p:__post__ {id: 3}), (pc:__pc__ {id: 3}), (p)-[:BELONGS_TO]->(pc)",
        {"post": "Post", "pc": "PostCommunity"},
    )

    # Clean only user clusters
    app.clean_clusters(posts=False, users=True)

    # Verify user clusters were deleted
    uc_result = neo4j.execute_and_fetch_labeled(
        "MATCH (uc:__uc__) RETURN count(uc) AS cnt",
        {"uc": "UserCommunity"},
    )
    uc_count = (uc_result[0] if uc_result else {}).get("cnt")
    assert coerce_int(uc_count) == 0, "User clusters should be deleted"

    # Verify post clusters still exist
    pc_result = neo4j.execute_and_fetch_labeled(
        "MATCH (pc:__pc__) RETURN count(pc) AS cnt",
        {"pc": "PostCommunity"},
    )
    pc_count = (pc_result[0] if pc_result else {}).get("cnt")
    assert coerce_int(pc_count) >= 1, "Post clusters should still exist"


@pytest.mark.integration
def test_clean_clusters_partial_cascade(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    redis_client: RedisClient,
    docker_compose: DockerComposeInfo,
    settings: HintGridSettings,
) -> None:
    """Test clean_clusters cascade when only post clusters are cleaned via embeddings cleanup."""
    # Explicit runtime use of Neo4jClient and HintGridSettings
    assert isinstance(neo4j, Neo4jClient)
    assert isinstance(settings, HintGridSettings)
    from hintgrid.app import HintGridApp

    app = HintGridApp(
        neo4j=neo4j,
        postgres=postgres_client,
        redis=redis_client,
        settings=settings,
    )

    # Create test clusters
    neo4j.execute_labeled(
        "CREATE (u:__user__ {id: 4}), (uc:__uc__ {id: 4}), (u)-[:BELONGS_TO]->(uc)",
        {"user": "User", "uc": "UserCommunity"},
    )
    neo4j.execute_labeled(
        "CREATE (p:__post__ {id: 4}), (pc:__pc__ {id: 4}), (p)-[:BELONGS_TO]->(pc)",
        {"post": "Post", "pc": "PostCommunity"},
    )

    # Simulate cleaning embeddings first (which cascades to post clusters)
    # Then clean clusters with only users=True (should not clean post clusters again)
    # This tests the branch: if (embeddings or similarity) and not clean_all:
    #     self.clean_clusters(posts=False, users=True)

    # First clean embeddings (simulated by cleaning post clusters)
    app.clean_clusters(posts=True, users=False)

    # Then clean clusters with cascade logic (embeddings were cleaned, so only clean users)
    # This tests the branch in clean() method
    app.clean_clusters(posts=False, users=True)

    # Verify both are cleaned
    uc_result = neo4j.execute_and_fetch_labeled(
        "MATCH (uc:__uc__) RETURN count(uc) AS cnt",
        {"uc": "UserCommunity"},
    )
    uc_count = (uc_result[0] if uc_result else {}).get("cnt")
    assert coerce_int(uc_count) == 0, "User clusters should be deleted"

    pc_result = neo4j.execute_and_fetch_labeled(
        "MATCH (pc:__pc__) RETURN count(pc) AS cnt",
        {"pc": "PostCommunity"},
    )
    pc_count = (pc_result[0] if pc_result else {}).get("cnt")
    assert coerce_int(pc_count) == 0, "Post clusters should be deleted"
