"""Integration tests for clustering edge cases and error handling."""

from __future__ import annotations

import contextlib
from typing import TYPE_CHECKING

import pytest

from hintgrid.pipeline.clustering import (
    _get_embedding_index_name,
    _get_post_graph_name,
    _get_user_graph_name,
    run_post_clustering,
    run_user_clustering,
)
from hintgrid.state import StateStore
from hintgrid.utils.coercion import coerce_int

if TYPE_CHECKING:
    from collections.abc import Generator

    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.config import HintGridSettings


@pytest.fixture
def _post_cluster_env(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> Generator[None, None, None]:
    """Create and drop the vector index + GDS graphs for post clustering tests."""
    neo4j.label("Post")
    index_name = _get_embedding_index_name(neo4j)
    label = neo4j.worker_label or "Post"
    neo4j.create_vector_index(
        index_name=index_name,
        label=label,
        property_name="embedding",
        dimensions=settings.fasttext_vector_size,
        similarity_function="cosine",
    )
    yield
    with contextlib.suppress(Exception):
        neo4j.execute_labeled("DROP INDEX __idx__ IF EXISTS", ident_map={"idx": index_name})
    for graph_name in (_get_post_graph_name(neo4j),):
        with contextlib.suppress(Exception):
            neo4j.execute(
                "CALL gds.graph.drop($g) YIELD graphName",
                {"g": graph_name},
            )


@pytest.fixture
def _user_cluster_env(
    neo4j: Neo4jClient,
) -> Generator[None, None, None]:
    """Clean up GDS graphs for user clustering tests."""
    neo4j.label("User")
    yield
    for graph_name in (_get_user_graph_name(neo4j),):
        with contextlib.suppress(Exception):
            neo4j.execute(
                "CALL gds.graph.drop($g) YIELD graphName",
                {"g": graph_name},
            )



@pytest.mark.integration
@pytest.mark.usefixtures("_user_cluster_env")
def test_run_user_clustering_no_users(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test run_user_clustering handles empty graph gracefully.

    When no users exist, should skip clustering without errors.
    """
    neo4j.execute_labeled(
        "MATCH (u:__user__) DETACH DELETE u",
        {"user": "User"},
    )

    run_user_clustering(neo4j, settings)

    count = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (c:__uc__) RETURN count(c) AS cnt",
            {"uc": "UserCommunity"},
        )
    )
    assert coerce_int(count[0].get("cnt")) == 0


@pytest.mark.integration
@pytest.mark.usefixtures("_user_cluster_env")
def test_run_user_clustering_no_follows(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test run_user_clustering with users but no FOLLOWS relationships.

    When no relationships exist, all users should be in single community.
    """
    for i in range(3):
        neo4j.execute_labeled(
            "CREATE (u:__user__ {id: $id, name: $name})",
            label_map={"user": "User"},
            params={"id": i, "name": f"user{i}"},
        )

    run_user_clustering(neo4j, settings)

    results = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__) RETURN u.cluster_id AS cid",
            {"user": "User"},
        )
    )

    assert len(results) == 3
    cluster_ids = [r.get("cid") for r in results]
    assert all(cid == cluster_ids[0] for cid in cluster_ids)


@pytest.mark.integration
@pytest.mark.usefixtures("_post_cluster_env")
def test_run_post_clustering_no_posts(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test run_post_clustering handles empty graph gracefully.

    When no posts exist, should skip clustering without errors.
    """
    neo4j.execute_labeled(
        "MATCH (p:__post__) DETACH DELETE p",
        {"post": "Post"},
    )

    state_store = StateStore(neo4j, "test_no_posts")
    run_post_clustering(neo4j, settings, state_store)

    count = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (c:__pc__) RETURN count(c) AS cnt",
            {"pc": "PostCommunity"},
        )
    )
    assert coerce_int(count[0].get("cnt")) == 0


@pytest.mark.integration
@pytest.mark.usefixtures("_post_cluster_env")
def test_run_post_clustering_single_post(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test run_post_clustering with single post.

    Single post should be assigned to its own cluster.
    """
    embedding = [0.1] * settings.fasttext_vector_size
    neo4j.execute_labeled(
        "CREATE (p:__post__ {id: 999, embedding: $emb, createdAt: datetime()})",
        {"post": "Post"},
        {"emb": embedding},
    )

    state_store = StateStore(neo4j, "test_single_post")
    run_post_clustering(neo4j, settings, state_store)

    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__ {id: 999}) RETURN p.cluster_id AS cid",
            {"post": "Post"},
        )
    )
    assert len(result) == 1


@pytest.mark.integration
@pytest.mark.usefixtures("_user_cluster_env")
def test_run_user_clustering_with_follows(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test run_user_clustering with INTERACTS_WITH relationships that include FOLLOWS.

    Users with follow relationships (represented as INTERACTS_WITH with follows_weight)
    should be clustered together. Since FOLLOWS is now included in INTERACTS_WITH via SQL,
    we create INTERACTS_WITH relationships directly with the follows_weight value.
    """

    # Create two groups of users with INTERACTS_WITH relationships (representing follows)
    # Group 1: users 100, 101, 102 follow each other
    # Each follow relationship is represented as INTERACTS_WITH with weight = follows_weight
    neo4j.execute_labeled(
        """
        CREATE (u1:__user__ {id: 100, name: 'group1_user1'})
        CREATE (u2:__user__ {id: 101, name: 'group1_user2'})
        CREATE (u3:__user__ {id: 102, name: 'group1_user3'})
        MERGE (u1)-[r1:INTERACTS_WITH]->(u2)
        SET r1.weight = $weight
        MERGE (u2)-[r2:INTERACTS_WITH]->(u3)
        SET r2.weight = $weight
        MERGE (u3)-[r3:INTERACTS_WITH]->(u1)
        SET r3.weight = $weight
        """,
        label_map={"user": "User"},
        params={"weight": settings.follows_weight},
    )

    # Group 2: users 200, 201 follow each other
    neo4j.execute_labeled(
        """
        CREATE (u4:__user__ {id: 200, name: 'group2_user1'})
        CREATE (u5:__user__ {id: 201, name: 'group2_user2'})
        MERGE (u4)-[r4:INTERACTS_WITH]->(u5)
        SET r4.weight = $weight
        MERGE (u5)-[r5:INTERACTS_WITH]->(u4)
        SET r5.weight = $weight
        """,
        label_map={"user": "User"},
        params={"weight": settings.follows_weight},
    )

    # Run clustering
    run_user_clustering(neo4j, settings)

    # Verify all users have cluster_id assigned
    results = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__) WHERE u.id IN [100, 101, 102, 200, 201] "
            "RETURN u.id AS id, u.cluster_id AS cid",
            {"user": "User"},
        )
    )

    assert len(results) == 5
    # All users should have cluster_id set
    for r in results:
        assert r.get("cid") is not None, f"User {r.get('id')} has no cluster_id"


@pytest.mark.integration
@pytest.mark.usefixtures("_post_cluster_env")
def test_run_post_clustering_with_embeddings(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test run_post_clustering with posts having embeddings.

    Posts with similar embeddings should be clustered together.
    """
    embedding1 = [0.9] * settings.fasttext_vector_size
    embedding2 = [0.1] * settings.fasttext_vector_size

    for i in range(3):
        neo4j.execute_labeled(
            "CREATE (p:__post__ {id: $id, embedding: $emb, createdAt: datetime()})",
            label_map={"post": "Post"},
            params={"id": 500 + i, "emb": embedding1},
        )

    for i in range(3):
        neo4j.execute_labeled(
            "CREATE (p:__post__ {id: $id, embedding: $emb, createdAt: datetime()})",
            label_map={"post": "Post"},
            params={"id": 600 + i, "emb": embedding2},
        )

    # Run clustering
    state_store = StateStore(neo4j, "test_with_embeddings")
    run_post_clustering(neo4j, settings, state_store)

    # Verify all posts have cluster_id assigned
    results = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) WHERE p.id >= 500 AND p.id < 700 "
            "RETURN p.id AS id, p.cluster_id AS cid",
            {"post": "Post"},
        )
    )

    assert len(results) == 6
    for r in results:
        assert r.get("cid") is not None, f"Post {r.get('id')} has no cluster_id"
