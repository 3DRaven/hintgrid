"""Integration tests for query tolerance to missing data and relationship types.

Tests verify that queries are tolerant to:
- Missing relationship types (e.g., BOOKMARKED)
- NULL values in aggregations (COALESCE handling)
- Empty data sets
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.utils.coercion import coerce_int

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.clients.postgres import PostgresClient
    from hintgrid.config import HintGridSettings
    from psycopg import Connection
    from psycopg.rows import TupleRow


@pytest.mark.integration
def test_get_extended_user_info_without_bookmarked(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test get_extended_user_info is tolerant to missing BOOKMARKED relationship type.
    
    Query should work correctly even when BOOKMARKED relationships don't exist,
    returning 0 for bookmarked count.
    """
    from hintgrid.pipeline.stats import get_extended_user_info

    user_id = 9100

    # Create user in Neo4j with community
    neo4j.label("User")
    neo4j.label("UserCommunity")
    neo4j.execute_labeled(
        "CREATE (u:__user__ {id: $user_id, languages: ['en'], isLocal: true}) "
        "CREATE (uc:__uc__ {id: 'test_uc', size: 5}) "
        "CREATE (u)-[:BELONGS_TO]->(uc)",
        {"user": "User", "uc": "UserCommunity"},
        {"user_id": user_id},
    )

    # Create posts and interactions (NO BOOKMARKED relationships)
    neo4j.label("Post")
    neo4j.execute_labeled(
        "MATCH (u:__user__ {id: $user_id}) "
        "CREATE (p1:__post__ {id: 2001}) "
        "CREATE (p2:__post__ {id: 2002}) "
        "CREATE (u)-[:WROTE]->(p1) "
        "CREATE (u)-[:WROTE]->(p2) "
        "CREATE (u)-[:FAVORITED]->(p1) "
        "CREATE (u)-[:REBLOGGED]->(p2) "
        "CREATE (u)-[:REPLIED]->(p1)",
        {"user": "User", "post": "Post"},
        {"user_id": user_id},
    )

    # Create account in PostgreSQL
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (%s, 'no_bookmarks_user', NULL)
            ON CONFLICT (id) DO NOTHING;
            """,
            (user_id,),
        )
        postgres_conn.commit()

    # Get extended user info - should work without BOOKMARKED
    user_info = get_extended_user_info(neo4j, postgres_client, user_id)

    assert user_info is not None
    assert user_info.get("user_id") == user_id
    interactions = user_info.get("interactions")
    assert interactions is not None
    # Should return 0 for bookmarked, not None or error
    assert interactions.get("bookmarked") == 0
    assert interactions.get("favorited") == 1
    assert interactions.get("reblogged") == 1
    assert interactions.get("replied") == 1


@pytest.mark.integration
def test_get_extended_user_info_no_interactions(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test get_extended_user_info handles user with no interactions.
    
    COALESCE should ensure all interaction counts are 0, not NULL.
    """
    from hintgrid.pipeline.stats import get_extended_user_info

    user_id = 9101

    # Create user in Neo4j with community but no interactions
    neo4j.label("User")
    neo4j.label("UserCommunity")
    neo4j.execute_labeled(
        "CREATE (u:__user__ {id: $user_id, languages: ['en'], isLocal: true}) "
        "CREATE (uc:__uc__ {id: 'test_uc2', size: 3}) "
        "CREATE (u)-[:BELONGS_TO]->(uc)",
        {"user": "User", "uc": "UserCommunity"},
        {"user_id": user_id},
    )

    # Create account in PostgreSQL
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (%s, 'no_interactions_user', NULL)
            ON CONFLICT (id) DO NOTHING;
            """,
            (user_id,),
        )
        postgres_conn.commit()

    # Get extended user info - should return 0 for all interactions
    user_info = get_extended_user_info(neo4j, postgres_client, user_id)

    assert user_info is not None
    assert user_info.get("user_id") == user_id
    interactions = user_info.get("interactions")
    assert interactions is not None
    # All should be 0, not None (COALESCE ensures this)
    assert interactions.get("favorited") == 0
    assert interactions.get("reblogged") == 0
    assert interactions.get("replied") == 0
    assert interactions.get("bookmarked") == 0


@pytest.mark.integration
def test_build_active_user_query_without_bookmarked(
    neo4j: Neo4jClient,
) -> None:
    """Test _build_active_user_query is tolerant to missing BOOKMARKED relationship type."""
    from hintgrid.pipeline.stats import _build_active_user_query

    # Create community and users
    neo4j.label("User")
    neo4j.label("UserCommunity")
    neo4j.execute_labeled(
        "CREATE (uc:__uc__ {id: 'comm1'}) "
        "CREATE (u1:__user__ {id: 9201}) "
        "CREATE (u2:__user__ {id: 9202}) "
        "CREATE (u1)-[:BELONGS_TO]->(uc) "
        "CREATE (u2)-[:BELONGS_TO]->(uc)",
        {"uc": "UserCommunity", "user": "User"},
    )

    # Create posts and interactions (NO BOOKMARKED)
    neo4j.label("Post")
    neo4j.execute_labeled(
        "MATCH (u1:__user__ {id: 9201}), (u2:__user__ {id: 9202}) "
        "CREATE (p1:__post__ {id: 3001}) "
        "CREATE (p2:__post__ {id: 3002}) "
        "CREATE (u1)-[:FAVORITED]->(p1) "
        "CREATE (u1)-[:FAVORITED]->(p2) "
        "CREATE (u1)-[:REBLOGGED]->(p1) "
        "CREATE (u2)-[:FAVORITED]->(p1)",
        {"user": "User", "post": "Post"},
    )

    # Execute query - should work without BOOKMARKED
    query = _build_active_user_query()
    result = list(
        neo4j.execute_and_fetch_labeled(
            query,
            {"uc": "UserCommunity", "user": "User"},
            {"comm_id": "comm1"},
        )
    )

    assert len(result) == 1
    assert result[0].get("user_id") == 9201
    # u1 has 3 interactions (2 favorited + 1 reblogged), u2 has 1
    assert coerce_int(result[0].get("interactions_count")) == 3


@pytest.mark.integration
def test_build_avg_activity_query_without_bookmarked(
    neo4j: Neo4jClient,
) -> None:
    """Test _build_avg_activity_query is tolerant to missing BOOKMARKED relationship type."""
    from hintgrid.pipeline.stats import _build_avg_activity_query

    # Create community and users
    neo4j.label("User")
    neo4j.label("UserCommunity")
    neo4j.execute_labeled(
        "CREATE (uc:__uc__ {id: 'comm2'}) "
        "CREATE (u1:__user__ {id: 9301}) "
        "CREATE (u2:__user__ {id: 9302}) "
        "CREATE (u3:__user__ {id: 9303}) "
        "CREATE (u1)-[:BELONGS_TO]->(uc) "
        "CREATE (u2)-[:BELONGS_TO]->(uc) "
        "CREATE (u3)-[:BELONGS_TO]->(uc)",
        {"uc": "UserCommunity", "user": "User"},
    )

    # Create posts and interactions (NO BOOKMARKED)
    neo4j.label("Post")
    neo4j.execute_labeled(
        "MATCH (u1:__user__ {id: 9301}), (u2:__user__ {id: 9302}), (u3:__user__ {id: 9303}) "
        "CREATE (p1:__post__ {id: 4001}) "
        "CREATE (p2:__post__ {id: 4002}) "
        "CREATE (u1)-[:FAVORITED]->(p1) "
        "CREATE (u1)-[:FAVORITED]->(p2) "
        "CREATE (u2)-[:REBLOGGED]->(p1) "
        "CREATE (u3)-[:REPLIED]->(p1)",
        {"user": "User", "post": "Post"},
    )

    # Execute query - should work without BOOKMARKED
    query = _build_avg_activity_query()
    result = list(
        neo4j.execute_and_fetch_labeled(
            query,
            {"uc": "UserCommunity", "user": "User"},
            {"comm_id": "comm2"},
        )
    )

    assert len(result) == 1
    # u1: 2, u2: 1, u3: 1 -> avg = (2 + 1 + 1) / 3 = 1.33...
    result_dict = result[0]
    if isinstance(result_dict, dict):
        avg_interactions = result_dict.get("avg_interactions")
        assert avg_interactions is not None
        if isinstance(avg_interactions, (int, float)):
            avg_float = float(avg_interactions)
            assert avg_float > 1.0
            assert avg_float < 2.0
        else:
            assert False, f"Expected numeric avg_interactions, got {type(avg_interactions)}"
    else:
        assert False, "Expected dict result"


@pytest.mark.integration
def test_build_avg_activity_query_no_interactions(
    neo4j: Neo4jClient,
) -> None:
    """Test _build_avg_activity_query handles community with no interactions.
    
    COALESCE should ensure avg_interactions is 0.0, not NULL.
    """
    from hintgrid.pipeline.stats import _build_avg_activity_query

    # Create community and users with no interactions
    neo4j.label("User")
    neo4j.label("UserCommunity")
    neo4j.execute_labeled(
        "CREATE (uc:__uc__ {id: 'comm3'}) "
        "CREATE (u1:__user__ {id: 9401}) "
        "CREATE (u2:__user__ {id: 9402}) "
        "CREATE (u1)-[:BELONGS_TO]->(uc) "
        "CREATE (u2)-[:BELONGS_TO]->(uc)",
        {"uc": "UserCommunity", "user": "User"},
    )

    # Execute query - should return 0.0, not NULL
    query = _build_avg_activity_query()
    result = list(
        neo4j.execute_and_fetch_labeled(
            query,
            {"uc": "UserCommunity", "user": "User"},
            {"comm_id": "comm3"},
        )
    )

    assert len(result) == 1
    avg_interactions = result[0].get("avg_interactions")
    # COALESCE should ensure this is 0.0, not None
    assert avg_interactions == 0.0


@pytest.mark.integration
def test_collect_user_community_stats_with_coalesce(
    neo4j: Neo4jClient,
) -> None:
    """Test _collect_user_community_stats handles empty communities with COALESCE."""
    from hintgrid.pipeline.stats import _collect_user_community_stats

    # Create empty graph
    neo4j.label("User")
    neo4j.label("UserCommunity")

    # Get stats - should return 0 values, not NULL
    stats = _collect_user_community_stats(neo4j)

    assert stats is not None
    assert stats["total_communities"] == 0
    assert stats["avg_size"] == 0.0  # COALESCE ensures this
    assert stats["median_size"] == 0.0  # COALESCE ensures this
    assert stats["min_size"] == 0  # COALESCE ensures this
    assert stats["max_size"] == 0  # COALESCE ensures this
    assert stats["isolated_count"] == 0  # COALESCE ensures this


@pytest.mark.integration
def test_collect_post_community_stats_with_coalesce(
    neo4j: Neo4jClient,
) -> None:
    """Test _collect_post_community_stats handles empty communities with COALESCE."""
    from hintgrid.pipeline.stats import _collect_post_community_stats

    # Create empty graph
    neo4j.label("Post")
    neo4j.label("PostCommunity")

    # Get stats - should return 0 values, not NULL
    stats = _collect_post_community_stats(neo4j)

    assert stats is not None
    assert stats["total_communities"] == 0
    assert stats["avg_size"] == 0.0  # COALESCE ensures this
    assert stats["median_size"] == 0.0  # COALESCE ensures this
    assert stats["min_size"] == 0  # COALESCE ensures this
    assert stats["max_size"] == 0  # COALESCE ensures this
    assert stats["isolated_count"] == 0  # COALESCE ensures this


@pytest.mark.integration
def test_collect_interaction_stats_with_coalesce(
    neo4j: Neo4jClient,
) -> None:
    """Test _collect_interaction_stats handles empty graph with COALESCE."""
    from hintgrid.pipeline.stats import _collect_interaction_stats

    # Create empty graph
    neo4j.label("User")
    neo4j.label("Post")

    # Get stats - should return 0.0, not NULL
    stats = _collect_interaction_stats(neo4j)

    assert stats is not None
    # COALESCE ensures these are 0.0, not None
    assert stats["avg_interactions_per_user"] == 0.0
    assert stats["avg_interactions_per_post"] == 0.0


@pytest.mark.integration
def test_collect_similarity_statistics_with_coalesce(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test _collect_similarity_statistics handles no relationships with COALESCE."""
    from hintgrid.pipeline.clustering import _collect_similarity_statistics

    # Create posts but no SIMILAR_TO relationships
    neo4j.label("Post")
    neo4j.execute_labeled(
        "CREATE (p1:__post__ {id: 5001}) "
        "CREATE (p2:__post__ {id: 5002})",
        {"post": "Post"},
    )

    # Get stats - should handle NULL gracefully
    stats = _collect_similarity_statistics(neo4j, settings)

    assert stats is not None
    # If no relationships, these should be handled (either 0.0 or not in dict)
    # The function should not crash on NULL values
    assert "total_relationships" in stats
    assert stats["total_relationships"] == 0
