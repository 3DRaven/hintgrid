"""Integration tests for GDS and APOC improvements.

Tests verify:
- apoc.periodic.iterate for large batch writes
- Cypher graph projection with date filtering
- PageRank computation and integration
- Community similarity for smarter serendipity
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.pipeline.clustering import run_pagerank, run_post_clustering
from hintgrid.pipeline.graph import ensure_graph_indexes
from hintgrid.pipeline.interests import (
    compute_community_similarity,
    rebuild_interests,
    seed_serendipity,
)
from hintgrid.state import StateStore
from hintgrid.utils.coercion import coerce_float, coerce_int

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.config import HintGridSettings


@pytest.mark.integration
def test_apoc_periodic_iterate_interests(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test that rebuild_interests uses apoc.periodic.iterate for large batches."""
    # Create test data: multiple users, posts, and communities
    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: 1}), (u2:__user__ {id: 2}), (u3:__user__ {id: 3})",
        {"user": "User"},
    )
    neo4j.execute_labeled(
        "CREATE (p1:__post__ {id: 1, createdAt: datetime()}), "
        "(p2:__post__ {id: 2, createdAt: datetime()}), "
        "(p3:__post__ {id: 3, createdAt: datetime()})",
        {"post": "Post"},
    )
    neo4j.execute_labeled(
        "CREATE (uc1:__uc__ {id: 1}), (uc2:__uc__ {id: 2})",
        {"uc": "UserCommunity"},
    )
    neo4j.execute_labeled(
        "CREATE (pc1:__pc__ {id: 1}), (pc2:__pc__ {id: 2})",
        {"pc": "PostCommunity"},
    )
    neo4j.execute_labeled(
        "MATCH (u1:__user__ {id: 1}), (uc1:__uc__ {id: 1}) "
        "CREATE (u1)-[:BELONGS_TO]->(uc1)",
        {"user": "User", "uc": "UserCommunity"},
    )
    neo4j.execute_labeled(
        "MATCH (u2:__user__ {id: 2}), (uc1:__uc__ {id: 1}) "
        "CREATE (u2)-[:BELONGS_TO]->(uc1)",
        {"user": "User", "uc": "UserCommunity"},
    )
    neo4j.execute_labeled(
        "MATCH (u3:__user__ {id: 3}), (uc2:__uc__ {id: 2}) "
        "CREATE (u3)-[:BELONGS_TO]->(uc2)",
        {"user": "User", "uc": "UserCommunity"},
    )
    neo4j.execute_labeled(
        "MATCH (p1:__post__ {id: 1}), (pc1:__pc__ {id: 1}) "
        "CREATE (p1)-[:BELONGS_TO]->(pc1)",
        {"post": "Post", "pc": "PostCommunity"},
    )
    neo4j.execute_labeled(
        "MATCH (p2:__post__ {id: 2}), (pc1:__pc__ {id: 1}) "
        "CREATE (p2)-[:BELONGS_TO]->(pc1)",
        {"post": "Post", "pc": "PostCommunity"},
    )
    neo4j.execute_labeled(
        "MATCH (p3:__post__ {id: 3}), (pc2:__pc__ {id: 2}) "
        "CREATE (p3)-[:BELONGS_TO]->(pc2)",
        {"post": "Post", "pc": "PostCommunity"},
    )
    # Create enough interactions to pass min_interactions threshold
    neo4j.execute_labeled(
        "MATCH (u1:__user__ {id: 1}), (p1:__post__ {id: 1}) "
        "CREATE (u1)-[:FAVORITED {at: datetime()}]->(p1)",
        {"user": "User", "post": "Post"},
    )
    neo4j.execute_labeled(
        "MATCH (u1:__user__ {id: 1}), (p2:__post__ {id: 2}) "
        "CREATE (u1)-[:FAVORITED {at: datetime()}]->(p2)",
        {"user": "User", "post": "Post"},
    )
    neo4j.execute_labeled(
        "MATCH (u2:__user__ {id: 2}), (p1:__post__ {id: 1}) "
        "CREATE (u2)-[:FAVORITED {at: datetime()}]->(p1)",
        {"user": "User", "post": "Post"},
    )
    neo4j.execute_labeled(
        "MATCH (u2:__user__ {id: 2}), (p2:__post__ {id: 2}) "
        "CREATE (u2)-[:FAVORITED {at: datetime()}]->(p2)",
        {"user": "User", "post": "Post"},
    )
    neo4j.execute_labeled(
        "MATCH (u2:__user__ {id: 2}), (p3:__post__ {id: 3}) "
        "CREATE (u2)-[:FAVORITED {at: datetime()}]->(p3)",
        {"user": "User", "post": "Post"},
    )

    # Override min_favourites to allow test data through the threshold
    test_settings = settings.model_copy(update={"interests_min_favourites": 1})

    # Run rebuild_interests (should use apoc.periodic.iterate with batchMode='BATCH')
    rebuild_interests(neo4j, test_settings)

    # Verify INTERESTED_IN relationships were created
    result = neo4j.execute_and_fetch_labeled(
        "MATCH (uc:__uc__)-[i:INTERESTED_IN]->(pc:__pc__) "
        "RETURN count(i) AS count",
        {"uc": "UserCommunity", "pc": "PostCommunity"},
    )
    count = coerce_int(result[0]["count"]) if result else 0
    assert count > 0, "INTERESTED_IN relationships should be created"


@pytest.mark.integration
def test_cypher_projection_date_filtering(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test that post clustering uses Cypher projection with date filtering."""
    dim = 16
    test_settings = settings.model_copy(
        update={
            "fasttext_vector_size": dim,
            "llm_dimensions": dim,
            "similarity_recency_days": 365,
            "similarity_threshold": 0.0,
        }
    )
    ensure_graph_indexes(neo4j, test_settings)

    emb = [0.05] * dim
    neo4j.execute_labeled(
        "CREATE (p1:__post__ {id: 1, embedding: $e, createdAt: datetime()}), "
        "(p2:__post__ {id: 2, embedding: $e, createdAt: datetime() - duration({days: 10})}), "
        "(p3:__post__ {id: 3, embedding: $e, createdAt: datetime() - duration({days: 20})})",
        {"post": "Post"},
        params={"e": emb},
    )

    state_store = StateStore(neo4j, "test_cypher_projection")
    run_post_clustering(neo4j, test_settings, state_store)

    result = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__) WHERE p.cluster_id IS NOT NULL RETURN count(p) AS count",
        {"post": "Post"},
    )
    count = coerce_int(result[0]["count"]) if result else 0
    assert count > 0, "Posts should have cluster_id assigned"


@pytest.mark.integration
def test_pagerank_computation(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test that PageRank is computed and stored on Post nodes."""
    # Create posts with SIMILAR_TO relationships
    neo4j.execute_labeled(
        "CREATE (p1:__post__ {id: 1, createdAt: datetime()}), "
        "(p2:__post__ {id: 2, createdAt: datetime()}), "
        "(p3:__post__ {id: 3, createdAt: datetime()})",
        {"post": "Post"},
    )
    neo4j.execute_labeled(
        "MATCH (p1:__post__ {id: 1}), (p2:__post__ {id: 2}) "
        "CREATE (p1)-[:SIMILAR_TO {weight: 0.8}]->(p2), "
        "(p2)-[:SIMILAR_TO {weight: 0.7}]->(p3)",
        {"post": "Post"},
    )

    # Run PageRank
    run_pagerank(neo4j, settings)

    # Verify PageRank scores were written
    result = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__) WHERE p.pagerank IS NOT NULL RETURN count(p) AS count",
        {"post": "Post"},
    )
    count = coerce_int(result[0]["count"]) if result else 0
    assert count > 0, "Posts should have pagerank property"

    # Verify PageRank scores are positive
    result = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__) WHERE p.pagerank IS NOT NULL RETURN p.pagerank AS score",
        {"post": "Post"},
    )
    for row in result:
        score = coerce_float(row.get("score"))
        assert score is not None
        assert score >= 0.0, "PageRank scores should be non-negative"


@pytest.mark.integration
def test_community_similarity(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test that community similarity is computed using gds.nodeSimilarity."""
    # Create users and communities
    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: 1}), (u2:__user__ {id: 2}), "
        "(u3:__user__ {id: 3}), (u4:__user__ {id: 4})",
        {"user": "User"},
    )
    neo4j.execute_labeled(
        "CREATE (uc1:__uc__ {id: 1}), (uc2:__uc__ {id: 2})",
        {"uc": "UserCommunity"},
    )
    # u1, u2 in uc1; u2, u3 in uc2 (u2 is shared)
    neo4j.execute_labeled(
        "MATCH (u1:__user__ {id: 1}), (uc1:__uc__ {id: 1}) "
        "CREATE (u1)-[:BELONGS_TO]->(uc1)",
        {"user": "User", "uc": "UserCommunity"},
    )
    neo4j.execute_labeled(
        "MATCH (u2:__user__ {id: 2}), (uc1:__uc__ {id: 1}) "
        "CREATE (u2)-[:BELONGS_TO]->(uc1)",
        {"user": "User", "uc": "UserCommunity"},
    )
    neo4j.execute_labeled(
        "MATCH (u2:__user__ {id: 2}), (uc2:__uc__ {id: 2}) "
        "CREATE (u2)-[:BELONGS_TO]->(uc2)",
        {"user": "User", "uc": "UserCommunity"},
    )
    neo4j.execute_labeled(
        "MATCH (u3:__user__ {id: 3}), (uc2:__uc__ {id: 2}) "
        "CREATE (u3)-[:BELONGS_TO]->(uc2)",
        {"user": "User", "uc": "UserCommunity"},
    )

    # Run community similarity (force enabled — env may set HINTGRID_COMMUNITY_SIMILARITY_ENABLED=false)
    test_settings = settings.model_copy(update={"community_similarity_enabled": True})
    compute_community_similarity(neo4j, test_settings)

    # Verify SIMILAR_COMMUNITY relationships were created
    result = neo4j.execute_and_fetch_labeled(
        "MATCH (uc1:__uc__)-[sim:SIMILAR_COMMUNITY]->(uc2:__uc__) "
        "RETURN count(sim) AS count",
        {"uc": "UserCommunity"},
    )
    count = coerce_int(result[0]["count"]) if result else 0
    assert count > 0, "SIMILAR_COMMUNITY relationships should be created"

    # Verify similarity scores are in valid range
    result = neo4j.execute_and_fetch_labeled(
        "MATCH (uc1:__uc__)-[sim:SIMILAR_COMMUNITY]->(uc2:__uc__) "
        "RETURN sim.score AS score",
        {"uc": "UserCommunity"},
    )
    for row in result:
        score = coerce_float(row.get("score"))
        assert score is not None
        assert 0.0 <= score <= 1.0, "Similarity scores should be in [0, 1]"


@pytest.mark.integration
def test_similarity_based_serendipity(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test that serendipity uses community similarity when enabled."""
    # Create communities and interests
    neo4j.execute_labeled(
        "CREATE (uc1:__uc__ {id: 1}), (uc2:__uc__ {id: 2})",
        {"uc": "UserCommunity"},
    )
    neo4j.execute_labeled(
        "CREATE (pc1:__pc__ {id: 1}), (pc2:__pc__ {id: 2})",
        {"pc": "PostCommunity"},
    )
    # uc2 is interested in pc1
    neo4j.execute_labeled(
        "MATCH (uc2:__uc__ {id: 2}), (pc1:__pc__ {id: 1}) "
        "CREATE (uc2)-[:INTERESTED_IN {score: 0.8}]->(pc1)",
        {"uc": "UserCommunity", "pc": "PostCommunity"},
    )
    # uc1 and uc2 are similar
    neo4j.execute_labeled(
        "MATCH (uc1:__uc__ {id: 1}), (uc2:__uc__ {id: 2}) "
        "CREATE (uc1)-[:SIMILAR_COMMUNITY {score: 0.6}]->(uc2)",
        {"uc": "UserCommunity"},
    )

    # Run serendipity with similarity enabled
    test_settings = settings.model_copy(update={"community_similarity_enabled": True})
    seed_serendipity(neo4j, test_settings)

    # Verify uc1 got interested in pc1 through similarity
    result = neo4j.execute_and_fetch_labeled(
        "MATCH (uc1:__uc__ {id: 1})-[i:INTERESTED_IN]->(pc1:__pc__ {id: 1}) "
        "WHERE i.serendipity = true RETURN count(i) AS count",
        {"uc": "UserCommunity", "pc": "PostCommunity"},
    )
    count = coerce_int(result[0]["count"]) if result else 0
    assert count > 0, "Serendipity relationships should be created via similarity"
