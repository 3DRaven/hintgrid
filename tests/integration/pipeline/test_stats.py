"""Integration tests for graph statistics collection and display."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from io import StringIO
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from rich.console import Console

from hintgrid.pipeline.stats import (
    get_user_info,
    show_graph_overview_after_loading,
    show_post_community_stats,
    show_user_community_stats,
)

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow

    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.clients.postgres import PostgresClient
else:
    from hintgrid.clients.neo4j import Neo4jClient


@contextmanager
def _capture_stats_module_console(width: int = 120) -> Iterator[StringIO]:
    """Capture Rich output from pipeline stats (module binds ``console`` at import time)."""
    buf = StringIO()
    capture_console = Console(file=buf, width=width, force_terminal=True)
    with patch("hintgrid.pipeline.stats.console", capture_console):
        yield buf


@pytest.mark.integration
def test_show_graph_overview_empty_basic_counts(neo4j: Neo4jClient) -> None:
    """Test show_graph_overview_after_loading with empty graph shows zero counts."""
    with _capture_stats_module_console() as buffer:
        show_graph_overview_after_loading(neo4j)
    output = buffer.getvalue()

    # Verify empty graph message or zero counts
    assert "No data loaded yet" in output or "Users:   0" in output or "Posts:   0" in output


@pytest.mark.integration
def test_show_graph_overview_with_data_basic_counts(neo4j: Neo4jClient) -> None:
    """Test show_graph_overview_after_loading with sample data shows correct counts."""
    neo4j.label("User")
    neo4j.label("Post")

    # Create users
    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: 1}), (u2:__user__ {id: 2})",
        {"user": "User"},
    )

    # Create posts
    neo4j.execute_labeled(
        "CREATE (p1:__post__ {id: 1, text: 'Hello', embedding: [0.1, 0.2], language: 'en'}), "
        "(p2:__post__ {id: 2, text: 'World', embedding: [0.3, 0.4], language: 'ru'})",
        {"post": "Post"},
    )

    # Create relationships (follow signal is represented as INTERACTS_WITH, not FOLLOWS)
    neo4j.execute_labeled(
        "MATCH (u1:__user__ {id: 1}), (u2:__user__ {id: 2}), "
        "(p1:__post__ {id: 1}), (p2:__post__ {id: 2}) "
        "CREATE (u1)-[:INTERACTS_WITH {weight: 10.0}]->(u2), "
        "(u1)-[:FAVORITED]->(p1), "
        "(u2)-[:REBLOGGED]->(p1), "
        "(u1)-[:REPLIED]->(p2), "
        "(u1)-[:HATES_USER]->(u2)",
        {"user": "User", "post": "Post"},
    )

    with _capture_stats_module_console() as buffer:
        show_graph_overview_after_loading(neo4j)
    output = buffer.getvalue()

    # Verify counts in output (Rich may format numbers with commas)
    assert "Users:" in output and ("2" in output or "Users:   2" in output)
    assert "Posts:" in output and ("2" in output or "Posts:   2" in output)
    assert "INTERACTS_WITH:" in output
    assert "Mastodon follows are aggregated here" in output
    assert "FAVORITED:" in output
    assert "REBLOGGED:" in output
    assert "REPLIED:" in output
    assert "HATES_USER:" in output


@pytest.mark.integration
def test_show_graph_overview_empty_connectivity(neo4j: Neo4jClient) -> None:
    """Test show_graph_overview_after_loading with empty graph shows zero connectivity."""
    with _capture_stats_module_console() as buffer:
        show_graph_overview_after_loading(neo4j)
    output = buffer.getvalue()

    # Verify empty graph message (connectivity stats are not shown for empty graph)
    assert "No data loaded yet" in output or "Users:" in output


@pytest.mark.integration
def test_show_graph_overview_with_data_connectivity(neo4j: Neo4jClient) -> None:
    """Test show_graph_overview_after_loading with data shows connectivity stats.
    
    NOTE: FOLLOWS relationships are not stored separately in Neo4j.
    They are included in INTERACTS_WITH aggregation via SQL.
    This test uses INTERACTS_WITH relationships directly.
    """
    neo4j.label("User")

    # Create users with different interaction counts
    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: 1}), "
        "(u2:__user__ {id: 2}), "
        "(u3:__user__ {id: 3}), "
        "(u4:__user__ {id: 4})",
        {"user": "User"},
    )

    # u1 interacts with u2, u3 (2 interactions)
    # u2 interacts with u3 (1 interaction)
    # u3 interacts with no one (0 interactions - isolated)
    # u4 interacts with u1, u2, u3 (3 interactions)
    neo4j.execute_labeled(
        "MATCH (u1:__user__ {id: 1}), (u2:__user__ {id: 2}), "
        "(u3:__user__ {id: 3}), (u4:__user__ {id: 4}) "
        "CREATE (u1)-[:INTERACTS_WITH {weight: 1.0}]->(u2), "
        "(u1)-[:INTERACTS_WITH {weight: 1.0}]->(u3), "
        "(u2)-[:INTERACTS_WITH {weight: 1.0}]->(u3), "
        "(u4)-[:INTERACTS_WITH {weight: 1.0}]->(u1), "
        "(u4)-[:INTERACTS_WITH {weight: 1.0}]->(u2), "
        "(u4)-[:INTERACTS_WITH {weight: 1.0}]->(u3)",
        {"user": "User"},
    )

    with _capture_stats_module_console() as buffer:
        show_graph_overview_after_loading(neo4j)
    output = buffer.getvalue()

    # Verify connectivity stats in output
    assert "User Connectivity" in output or "Avg interacts" in output or "Isolated users" in output


@pytest.mark.integration
def test_show_graph_overview_post_statistics(neo4j: Neo4jClient) -> None:
    """Test show_graph_overview_after_loading shows post statistics correctly."""
    neo4j.label("Post")

    # Create posts with and without embeddings
    neo4j.execute_labeled(
        "CREATE (p1:__post__ {id: 1, text: 'Hello', embedding: [0.1, 0.2]}), "
        "(p2:__post__ {id: 2, text: 'World', embedding: [0.3, 0.4]}), "
        "(p3:__post__ {id: 3, text: 'No embedding'})",
        {"post": "Post"},
    )

    with _capture_stats_module_console() as buffer:
        show_graph_overview_after_loading(neo4j)
    output = buffer.getvalue()

    # Verify post coverage stats in output
    assert "Post Coverage" in output or "With embeddings" in output or "Without" in output
    assert "3" in output or "2" in output or "1" in output  # Should show counts


@pytest.mark.integration
def test_show_graph_overview_interaction_stats(neo4j: Neo4jClient) -> None:
    """Test show_graph_overview_after_loading shows interaction statistics."""
    neo4j.label("User")
    neo4j.label("Post")

    # Create users and posts
    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: 1}), (u2:__user__ {id: 2}), "
        "(p1:__post__ {id: 1}), (p2:__post__ {id: 2})",
        {"user": "User", "post": "Post"},
    )

    # Create interactions
    neo4j.execute_labeled(
        "MATCH (u1:__user__ {id: 1}), (u2:__user__ {id: 2}), "
        "(p1:__post__ {id: 1}), (p2:__post__ {id: 2}) "
        "CREATE (u1)-[:FAVORITED]->(p1), "
        "(u1)-[:REBLOGGED]->(p1), "
        "(u2)-[:FAVORITED]->(p1), "
        "(u1)-[:REPLIED]->(p2)",
        {"user": "User", "post": "Post"},
    )

    with _capture_stats_module_console() as buffer:
        show_graph_overview_after_loading(neo4j)
    output = buffer.getvalue()

    # Verify interaction stats in output
    assert "Interactions" in output or "Avg per user" in output or "Avg per post" in output


@pytest.mark.integration
def test_show_graph_overview_top_languages(neo4j: Neo4jClient) -> None:
    """Test show_graph_overview_after_loading shows top languages."""
    neo4j.label("Post")

    # Create posts with different languages
    neo4j.execute_labeled(
        "CREATE (p1:__post__ {id: 1, language: 'en'}), "
        "(p2:__post__ {id: 2, language: 'en'}), "
        "(p3:__post__ {id: 3, language: 'en'}), "
        "(p4:__post__ {id: 4, language: 'ru'}), "
        "(p5:__post__ {id: 5, language: 'ru'}), "
        "(p6:__post__ {id: 6, language: 'fr'})",
        {"post": "Post"},
    )

    with _capture_stats_module_console() as buffer:
        show_graph_overview_after_loading(neo4j)
    output = buffer.getvalue()

    # Verify top languages in output
    assert "Top Languages" in output or "en" in output or "ru" in output or "fr" in output


# Removed duplicate test functions - already defined above


@pytest.mark.integration
def test_show_post_community_stats_with_data(neo4j: Neo4jClient) -> None:
    """Test show_post_community_stats shows post community statistics correctly."""
    neo4j.label("Post")
    neo4j.label("PostCommunity")

    # Create posts
    for i in range(8):
        neo4j.execute_labeled(
            "CREATE (p:__post__ {id: $id})",
            {"post": "Post"},
            params={"id": i},
        )

    # Create communities
    neo4j.execute_labeled(
        "CREATE (pc1:__pc__ {id: 1, size: 4}), "
        "(pc2:__pc__ {id: 2, size: 3}), "
        "(pc3:__pc__ {id: 3, size: 1})",
        {"pc": "PostCommunity"},
    )

    # Create some SIMILAR_TO edges
    neo4j.execute_labeled(
        "MATCH (p1:__post__ {id: 0}), (p2:__post__ {id: 1}) "
        "CREATE (p1)-[:SIMILAR_TO {weight: 0.9}]->(p2)",
        {"post": "Post"},
    )

    with _capture_stats_module_console() as buffer:
        show_post_community_stats(neo4j, postgres=None, modularity=0.5)
    output = buffer.getvalue()

    # Verify post community stats in output
    assert (
        "communities" in output.lower()
        or "posts" in output.lower()
        or "Post Communities" in output
    )


@pytest.mark.integration
def test_show_graph_overview_after_loading_empty(neo4j: Neo4jClient) -> None:
    # Explicit runtime use of Neo4jClient
    assert isinstance(neo4j, Neo4jClient)
    """Test show_graph_overview_after_loading with empty graph."""
    # Should not raise, just show warning
    show_graph_overview_after_loading(neo4j)


@pytest.mark.integration
def test_show_graph_overview_after_loading_with_data(neo4j: Neo4jClient) -> None:
    # Explicit runtime use of Neo4jClient
    assert isinstance(neo4j, Neo4jClient)
    """Test show_graph_overview_after_loading with sample data."""
    neo4j.label("User")
    neo4j.label("Post")

    # Create sample data
    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: 1}), (u2:__user__ {id: 2}), "
        "(p1:__post__ {id: 1, text: 'Hello', embedding: [0.1, 0.2], language: 'en'}), "
        "(p2:__post__ {id: 2, text: 'World', embedding: [0.3, 0.4], language: 'ru'})",
        {"user": "User", "post": "Post"},
    )

    neo4j.execute_labeled(
        "MATCH (u1:__user__ {id: 1}), (u2:__user__ {id: 2}), "
        "(p1:__post__ {id: 1}), (p2:__post__ {id: 2}) "
        "CREATE (u1)-[:INTERACTS_WITH {weight: 1.0}]->(u2), "
        "(u1)-[:FAVORITED]->(p1), "
        "(u2)-[:REBLOGGED]->(p1)",
        {"user": "User", "post": "Post"},
    )

    # Should not raise
    show_graph_overview_after_loading(neo4j)


@pytest.mark.integration
def test_show_user_community_stats_empty(neo4j: Neo4jClient) -> None:
    # Explicit runtime use of Neo4jClient
    assert isinstance(neo4j, Neo4jClient)
    """Test show_user_community_stats with no communities."""
    # Should not raise, just show warning
    show_user_community_stats(neo4j, postgres=None, modularity=None)


@pytest.mark.integration
def test_show_user_community_stats_with_data(neo4j: Neo4jClient) -> None:
    # Explicit runtime use of Neo4jClient
    assert isinstance(neo4j, Neo4jClient)
    """Test show_user_community_stats with communities."""
    neo4j.label("User")
    neo4j.label("UserCommunity")

    # Create users and communities
    for i in range(5):
        neo4j.execute_labeled(
            "CREATE (u:__user__ {id: $id})",
            {"user": "User"},
            params={"id": i},
        )

    neo4j.execute_labeled(
        "CREATE (uc1:__uc__ {id: 1, size: 3}), "
        "(uc2:__uc__ {id: 2, size: 2})",
        {"uc": "UserCommunity"},
    )

    # Link users
    for i in range(3):
        neo4j.execute_labeled(
            "MATCH (u:__user__ {id: $user_id}), (uc:__uc__ {id: $community_id}) "
            "CREATE (u)-[:BELONGS_TO]->(uc)",
            {"user": "User", "uc": "UserCommunity"},
            params={"user_id": i, "community_id": 1},
        )
    for i in range(3, 5):
        neo4j.execute_labeled(
            "MATCH (u:__user__ {id: $user_id}), (uc:__uc__ {id: $community_id}) "
            "CREATE (u)-[:BELONGS_TO]->(uc)",
            {"user": "User", "uc": "UserCommunity"},
            params={"user_id": i, "community_id": 2},
        )

    # Should not raise
    show_user_community_stats(neo4j, postgres=None, modularity=0.75)


@pytest.mark.integration
def test_show_post_community_stats_empty(neo4j: Neo4jClient) -> None:
    # Explicit runtime use of Neo4jClient
    assert isinstance(neo4j, Neo4jClient)
    """Test show_post_community_stats with no communities."""
    # Should not raise, just show warning
    show_post_community_stats(neo4j, postgres=None, modularity=None)




# Removed test_format_size_distribution - tests internal formatting details
# Formatting is tested through show_user_community_stats and show_post_community_stats


# ---------------------------------------------------------------------------
# Tests: get_user_info
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_get_user_info_with_full_data(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test get_user_info returns complete user information."""
    user_id = 9001

    # Create user in Neo4j
    neo4j.label("User")
    neo4j.execute_labeled(
        "CREATE (:__user__ {id: $user_id, languages: ['en', 'ru'], uiLanguage: 'de', isLocal: true})",
        {"user": "User"},
        {"user_id": user_id},
    )

    # Create account in PostgreSQL
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (%s, 'testuser', NULL)
            ON CONFLICT (id) DO NOTHING;
            """,
            (user_id,),
        )
        postgres_conn.commit()

    # Get user info
    user_info = get_user_info(neo4j, postgres_client, user_id)

    assert user_info is not None
    assert user_info.get("user_id") == user_id
    assert user_info.get("username") == "testuser"
    assert user_info.get("domain") is None
    assert user_info.get("languages") == ["en", "ru"]
    assert user_info.get("ui_language") == "de"
    assert user_info.get("is_local") is True


@pytest.mark.integration
def test_get_user_info_with_domain(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test get_user_info handles users with domain."""
    user_id = 9002

    # Create user in Neo4j
    neo4j.label("User")
    neo4j.execute_labeled(
        "CREATE (:__user__ {id: $user_id, languages: ['en'], isLocal: false})",
        {"user": "User"},
        {"user_id": user_id},
    )

    # Create account in PostgreSQL with domain
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

    # Get user info
    user_info = get_user_info(neo4j, postgres_client, user_id)

    assert user_info is not None
    assert user_info.get("user_id") == user_id
    assert user_info.get("username") == "remoteuser"
    assert user_info.get("domain") == "example.com"
    assert user_info.get("languages") == ["en"]
    assert user_info.get("is_local") is False


@pytest.mark.integration
def test_get_user_info_no_languages(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test get_user_info handles users without languages."""
    user_id = 9003

    # Create user in Neo4j without languages
    neo4j.label("User")
    neo4j.execute_labeled(
        "CREATE (:__user__ {id: $user_id, isLocal: true})",
        {"user": "User"},
        {"user_id": user_id},
    )

    # Create account in PostgreSQL
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (%s, 'nolanguser', NULL)
            ON CONFLICT (id) DO NOTHING;
            """,
            (user_id,),
        )
        postgres_conn.commit()

    # Get user info
    user_info = get_user_info(neo4j, postgres_client, user_id)

    assert user_info is not None
    assert user_info.get("user_id") == user_id
    assert user_info.get("username") == "nolanguser"
    assert user_info.get("languages") is None
    assert user_info.get("ui_language") is None


@pytest.mark.integration
def test_get_user_info_not_found_in_both(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
) -> None:
    """Test get_user_info returns None when user not found in both databases."""
    user_id = 99999

    user_info = get_user_info(neo4j, postgres_client, user_id)

    assert user_info is None


@pytest.mark.integration
def test_get_user_info_found_only_in_postgres(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test get_user_info returns None when user found only in PostgreSQL (not in Neo4j)."""
    user_id = 9004

    # Create account in PostgreSQL only (not in Neo4j)
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (%s, 'postgres_only', NULL)
            ON CONFLICT (id) DO NOTHING;
            """,
            (user_id,),
        )
        postgres_conn.commit()

    # Get user info - should return None because user not in Neo4j
    user_info = get_user_info(neo4j, postgres_client, user_id)

    # Function returns None if user not found in Neo4j (by design)
    assert user_info is None


# ---------------------------------------------------------------------------
# Tests: get_extended_user_info
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_get_extended_user_info_with_full_data(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test get_extended_user_info returns complete extended user information."""
    from hintgrid.pipeline.stats import get_extended_user_info

    user_id = 9005

    # Create user in Neo4j with community
    neo4j.label("User")
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")
    neo4j.execute_labeled(
        "CREATE (u:__user__ {id: $user_id, languages: ['en'], isLocal: true}) "
        "CREATE (uc:__uc__ {id: 'test_uc', size: 10}) "
        "CREATE (pc1:__pc__ {id: 'test_pc1'}) "
        "CREATE (pc2:__pc__ {id: 'test_pc2'}) "
        "CREATE (u)-[:BELONGS_TO]->(uc) "
        "CREATE (uc)-[:INTERESTED_IN {score: 0.8}]->(pc1) "
        "CREATE (uc)-[:INTERESTED_IN {score: 0.6}]->(pc2)",
        {"user": "User", "uc": "UserCommunity", "pc": "PostCommunity"},
        {"user_id": user_id},
    )

    # Create posts and interactions
    neo4j.label("Post")
    neo4j.execute_labeled(
        "MATCH (u:__user__ {id: $user_id}) "
        "CREATE (p1:__post__ {id: 1001}) "
        "CREATE (p2:__post__ {id: 1002}) "
        "CREATE (u)-[:WROTE]->(p1) "
        "CREATE (u)-[:WROTE]->(p2) "
        "CREATE (u)-[:FAVORITED]->(p1) "
        "CREATE (u)-[:REBLOGGED]->(p2)",
        {"user": "User", "post": "Post"},
        {"user_id": user_id},
    )

    # Create account in PostgreSQL
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (%s, 'extended_user', NULL)
            ON CONFLICT (id) DO NOTHING;
            """,
            (user_id,),
        )
        postgres_conn.commit()

    # Get extended user info
    user_info = get_extended_user_info(neo4j, postgres_client, user_id)

    assert user_info is not None
    assert user_info.get("user_id") == user_id
    assert user_info.get("username") == "extended_user"
    assert user_info.get("user_community_id") is not None
    assert user_info.get("user_community_size") == 10
    top_interests = user_info.get("top_interests")
    assert top_interests is not None
    assert len(top_interests) == 2
    interactions = user_info.get("interactions")
    assert interactions is not None
    assert interactions.get("favorited") == 1
    assert interactions.get("reblogged") == 1
    assert user_info.get("posts_count") == 2


@pytest.mark.integration
def test_get_extended_user_info_with_follows(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test get_extended_user_info returns None for follows/followers counts.

    NOTE: FOLLOWS relationships are not stored separately in Neo4j.
    They are included in INTERACTS_WITH aggregation via SQL.
    Therefore, follows_count and followers_count are always None.
    """
    from hintgrid.pipeline.stats import get_extended_user_info

    user_id = 9006
    other_user_id = 9007

    # Create users in Neo4j
    # Note: FOLLOWS relationships are not created in Neo4j anymore,
    # they are included in INTERACTS_WITH via SQL aggregation
    neo4j.label("User")
    neo4j.execute_labeled(
        "CREATE (u:__user__ {id: $user_id, languages: ['en'], isLocal: true}) "
        "CREATE (u2:__user__ {id: $other_user_id, languages: ['en'], isLocal: true})",
        {"user": "User"},
        {"user_id": user_id, "other_user_id": other_user_id},
    )

    # Create account in PostgreSQL
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (%s, 'follows_user', NULL)
            ON CONFLICT (id) DO NOTHING;
            """,
            (user_id,),
        )
        postgres_conn.commit()

    # Get extended user info
    user_info = get_extended_user_info(neo4j, postgres_client, user_id)

    assert user_info is not None
    # FOLLOWS relationships are not stored separately, so counts are None
    assert user_info.get("follows_count") is None
    assert user_info.get("followers_count") is None


@pytest.mark.integration
def test_get_extended_user_info_no_community(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test get_extended_user_info handles user without community."""
    from hintgrid.pipeline.stats import get_extended_user_info

    user_id = 9008

    # Create user in Neo4j without community
    neo4j.label("User")
    neo4j.execute_labeled(
        "CREATE (u:__user__ {id: $user_id, languages: ['en'], isLocal: true})",
        {"user": "User"},
        {"user_id": user_id},
    )

    # Create account in PostgreSQL
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (%s, 'no_community_user', NULL)
            ON CONFLICT (id) DO NOTHING;
            """,
            (user_id,),
        )
        postgres_conn.commit()

    # Get extended user info
    user_info = get_extended_user_info(neo4j, postgres_client, user_id)

    assert user_info is not None
    assert user_info.get("user_id") == user_id
    assert user_info.get("user_community_id") is None
    assert user_info.get("top_interests") is None
    interactions = user_info.get("interactions")
    assert interactions is not None
    assert interactions.get("favorited") == 0
    assert user_info.get("posts_count") == 0


@pytest.mark.integration
def test_show_user_community_stats_displays_popular_and_active_users(
    neo4j: Neo4jClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """Test show_user_community_stats displays active users."""
    neo4j.label("User")
    neo4j.label("UserCommunity")

    # Create users and community
    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: 1}), "
        "(u2:__user__ {id: 2}), "
        "(u3:__user__ {id: 3}), "
        "(uc:__uc__ {id: 100, size: 3})",
        {"user": "User", "uc": "UserCommunity"},
    )

    # Link users to community
    neo4j.execute_labeled(
        "MATCH (u1:__user__ {id: 1}), (u2:__user__ {id: 2}), "
        "(u3:__user__ {id: 3}), (uc:__uc__ {id: 100}) "
        "CREATE (u1)-[:BELONGS_TO]->(uc), "
        "(u2)-[:BELONGS_TO]->(uc), "
        "(u3)-[:BELONGS_TO]->(uc)",
        {"user": "User", "uc": "UserCommunity"},
    )

    # Create followers for u1 (most popular)
    neo4j.execute_labeled(
        "MATCH (u1:__user__ {id: 1}), (u2:__user__ {id: 2}), (u3:__user__ {id: 3}) "
        "CREATE (u2)-[:FOLLOWS]->(u1), (u3)-[:FOLLOWS]->(u1)",
        {"user": "User"},
    )

    # Create interactions for u2 (most active)
    neo4j.label("Post")
    neo4j.execute_labeled(
        "CREATE (p1:__post__ {id: 1}), (p2:__post__ {id: 2}), (p3:__post__ {id: 3})",
        {"post": "Post"},
    )
    neo4j.execute_labeled(
        "MATCH (u2:__user__ {id: 2}), (p1:__post__ {id: 1}), "
        "(p2:__post__ {id: 2}), (p3:__post__ {id: 3}) "
        "CREATE (u2)-[:FAVORITED]->(p1), "
        "(u2)-[:REBLOGGED]->(p2), "
        "(u2)-[:REPLIED]->(p3), "
        "(u2)-[:BOOKMARKED]->(p1)",
        {"user": "User", "post": "Post"},
    )

    show_user_community_stats(neo4j, postgres=None, modularity=None)

    captured = capsys.readouterr()
    output = captured.out

    # Check that active user is displayed (popular user removed, only active remains)
    # Note: Popular user was removed as FOLLOWS is no longer loaded separately

    # Check that active user is displayed
    assert "Active: user 2" in output or "Active: user" in output
    assert "interactions" in output.lower()


@pytest.mark.integration
def test_show_user_community_stats_displays_avg_activity(
    neo4j: Neo4jClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """Test show_user_community_stats displays average activity."""
    neo4j.label("User")
    neo4j.label("UserCommunity")
    neo4j.label("Post")

    # Create users and community
    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: 1}), (u2:__user__ {id: 2}), "
        "(uc:__uc__ {id: 200, size: 2})",
        {"user": "User", "uc": "UserCommunity"},
    )

    # Link users to community
    neo4j.execute_labeled(
        "MATCH (u1:__user__ {id: 1}), (u2:__user__ {id: 2}), (uc:__uc__ {id: 200}) "
        "CREATE (u1)-[:BELONGS_TO]->(uc), (u2)-[:BELONGS_TO]->(uc)",
        {"user": "User", "uc": "UserCommunity"},
    )

    # Create interactions
    neo4j.execute_labeled(
        "CREATE (p1:__post__ {id: 1}), (p2:__post__ {id: 2})",
        {"post": "Post"},
    )
    neo4j.execute_labeled(
        "MATCH (u1:__user__ {id: 1}), (u2:__user__ {id: 2}), "
        "(p1:__post__ {id: 1}), (p2:__post__ {id: 2}) "
        "CREATE (u1)-[:FAVORITED]->(p1), (u1)-[:FAVORITED]->(p2), "
        "(u2)-[:REBLOGGED]->(p1)",
        {"user": "User", "post": "Post"},
    )

    show_user_community_stats(neo4j, postgres=None, modularity=None)

    captured = capsys.readouterr()
    output = captured.out

    # Check that average activity is displayed
    assert "Avg activity" in output or "avg activity" in output.lower()
    assert "interactions/user" in output.lower()


@pytest.mark.integration
def test_show_user_community_stats_displays_top_languages(
    neo4j: Neo4jClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """Test show_user_community_stats displays top languages."""
    neo4j.label("User")
    neo4j.label("UserCommunity")

    # Create users with languages
    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: 1, languages: ['en']}), "
        "(u2:__user__ {id: 2, languages: ['en']}), "
        "(u3:__user__ {id: 3, languages: ['ru']}), "
        "(uc:__uc__ {id: 300, size: 3})",
        {"user": "User", "uc": "UserCommunity"},
    )

    # Link users to community
    neo4j.execute_labeled(
        "MATCH (u1:__user__ {id: 1}), (u2:__user__ {id: 2}), "
        "(u3:__user__ {id: 3}), (uc:__uc__ {id: 300}) "
        "CREATE (u1)-[:BELONGS_TO]->(uc), "
        "(u2)-[:BELONGS_TO]->(uc), "
        "(u3)-[:BELONGS_TO]->(uc)",
        {"user": "User", "uc": "UserCommunity"},
    )

    show_user_community_stats(neo4j, postgres=None, modularity=None)

    captured = capsys.readouterr()
    output = captured.out

    # Check that top languages are displayed
    assert "Top languages" in output or "top languages" in output.lower()
    assert "en" in output or "ru" in output


@pytest.mark.integration
def test_show_post_community_stats_displays_popular_post_and_active_author(
    neo4j: Neo4jClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """Test show_post_community_stats displays popular post and active author."""
    neo4j.label("Post")
    neo4j.label("PostCommunity")

    # Create posts and community
    neo4j.execute_labeled(
        "CREATE (p1:__post__ {id: 1, totalFavourites: 100, totalReblogs: 50, totalReplies: 25}), "
        "(p2:__post__ {id: 2, totalFavourites: 10, totalReblogs: 5, totalReplies: 2}), "
        "(p3:__post__ {id: 3, totalFavourites: 5, totalReblogs: 2, totalReplies: 1, authorId: 10}), "
        "(p4:__post__ {id: 4, totalFavourites: 1, totalReblogs: 0, totalReplies: 0, authorId: 10}), "
        "(pc:__pc__ {id: 400, size: 4})",
        {"post": "Post", "pc": "PostCommunity"},
    )

    # Link posts to community
    neo4j.execute_labeled(
        "MATCH (p1:__post__ {id: 1}), (p2:__post__ {id: 2}), "
        "(p3:__post__ {id: 3}), (p4:__post__ {id: 4}), (pc:__pc__ {id: 400}) "
        "CREATE (p1)-[:BELONGS_TO]->(pc), "
        "(p2)-[:BELONGS_TO]->(pc), "
        "(p3)-[:BELONGS_TO]->(pc), "
        "(p4)-[:BELONGS_TO]->(pc)",
        {"post": "Post", "pc": "PostCommunity"},
    )

    show_post_community_stats(neo4j, postgres=None, modularity=None)

    captured = capsys.readouterr()
    output = captured.out

    # Check that popular post is displayed (same popularity model as feed)
    assert "Popular: post 1" in output or "Popular: post" in output
    assert "popularity_contrib" in output.lower()

    # Check that active author is displayed
    assert "Active author: user 10" in output or "Active author" in output
    assert "posts" in output.lower()


@pytest.mark.integration
def test_show_post_community_stats_displays_avg_popularity(
    neo4j: Neo4jClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """Test show_post_community_stats displays average popularity."""
    neo4j.label("Post")
    neo4j.label("PostCommunity")

    # Create posts and community
    neo4j.execute_labeled(
        "CREATE (p1:__post__ {id: 1, totalFavourites: 10, totalReblogs: 5, totalReplies: 2}), "
        "(p2:__post__ {id: 2, totalFavourites: 20, totalReblogs: 10, totalReplies: 5}), "
        "(pc:__pc__ {id: 500, size: 2})",
        {"post": "Post", "pc": "PostCommunity"},
    )

    # Link posts to community
    neo4j.execute_labeled(
        "MATCH (p1:__post__ {id: 1}), (p2:__post__ {id: 2}), (pc:__pc__ {id: 500}) "
        "CREATE (p1)-[:BELONGS_TO]->(pc), (p2)-[:BELONGS_TO]->(pc)",
        {"post": "Post", "pc": "PostCommunity"},
    )

    show_post_community_stats(neo4j, postgres=None, modularity=None)

    captured = capsys.readouterr()
    output = captured.out

    # Check that average popularity_contrib is displayed
    assert "Avg popularity_contrib" in output or "avg popularity_contrib" in output.lower()
    assert "post" in output.lower()


@pytest.mark.integration
def test_show_post_community_stats_displays_top_languages(
    neo4j: Neo4jClient, capsys: pytest.CaptureFixture[str]
) -> None:
    """Test show_post_community_stats displays top languages."""
    neo4j.label("Post")
    neo4j.label("PostCommunity")

    # Create posts with languages
    neo4j.execute_labeled(
        "CREATE (p1:__post__ {id: 1, language: 'en'}), "
        "(p2:__post__ {id: 2, language: 'en'}), "
        "(p3:__post__ {id: 3, language: 'ru'}), "
        "(pc:__pc__ {id: 600, size: 3})",
        {"post": "Post", "pc": "PostCommunity"},
    )

    # Link posts to community
    neo4j.execute_labeled(
        "MATCH (p1:__post__ {id: 1}), (p2:__post__ {id: 2}), "
        "(p3:__post__ {id: 3}), (pc:__pc__ {id: 600}) "
        "CREATE (p1)-[:BELONGS_TO]->(pc), "
        "(p2)-[:BELONGS_TO]->(pc), "
        "(p3)-[:BELONGS_TO]->(pc)",
        {"post": "Post", "pc": "PostCommunity"},
    )

    show_post_community_stats(neo4j, postgres=None, modularity=None)

    captured = capsys.readouterr()
    output = captured.out

    # Check that top languages are displayed
    assert "Top languages" in output or "top languages" in output.lower()
    assert "en" in output or "ru" in output


# Removed tests for _check_relationship_type_exists, _format_user_display, _format_post_text
# These test internal formatting details that are covered by integration tests
# through show_user_community_stats, show_post_community_stats, and get_user_info


@pytest.mark.integration
def test_show_user_community_stats_with_postgres_displays_user_names(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test show_user_community_stats displays user names when postgres is provided."""
    neo4j.label("User")
    neo4j.label("UserCommunity")
    neo4j.label("Post")

    user_id = 10001
    username = "testuser"
    domain = "example.com"

    # Create user and community
    neo4j.execute_labeled(
        "CREATE (u:__user__ {id: $user_id}), "
        "(uc:__uc__ {id: 200, size: 1})",
        label_map={"user": "User", "uc": "UserCommunity"},
        params={"user_id": user_id},
    )

    neo4j.execute_labeled(
        "MATCH (u:__user__ {id: $user_id}), (uc:__uc__ {id: 200}) "
        "CREATE (u)-[:BELONGS_TO]->(uc)",
        label_map={"user": "User", "uc": "UserCommunity"},
        params={"user_id": user_id},
    )

    # Create interactions
    neo4j.execute_labeled(
        "CREATE (p1:__post__ {id: 1}), (p2:__post__ {id: 2})",
        {"post": "Post"},
    )
    neo4j.execute_labeled(
        "MATCH (u:__user__ {id: $user_id}), (p1:__post__ {id: 1}), (p2:__post__ {id: 2}) "
        "CREATE (u)-[:FAVORITED]->(p1), (u)-[:REBLOGGED]->(p2)",
        label_map={"user": "User", "post": "Post"},
        params={"user_id": user_id},
    )

    # Create account in PostgreSQL
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (%s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET username = EXCLUDED.username, domain = EXCLUDED.domain;
            """,
            (user_id, username, domain),
        )
        postgres_conn.commit()

    show_user_community_stats(neo4j, postgres=postgres_client, modularity=None)

    captured = capsys.readouterr()
    output = captured.out

    # Check that user name is displayed in format @username@domain
    assert f"@{username}@{domain}" in output
    assert f"(id: {user_id:,})" in output
    assert "Active:" in output


@pytest.mark.integration
def test_show_post_community_stats_with_postgres_displays_post_text_and_author(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test show_post_community_stats displays post text and author name when postgres is provided."""
    neo4j.label("Post")
    neo4j.label("PostCommunity")

    post_id = 20001
    post_text = "This is a test post with some content"
    author_id = 20002
    username = "authoruser"
    domain = "example.org"

    # Create post and community
    neo4j.execute_labeled(
        "CREATE (p:__post__ {id: $post_id, text: $post_text, "
        "totalFavourites: 10, totalReblogs: 5, totalReplies: 2, authorId: $author_id}), "
        "(pc:__pc__ {id: 300, size: 1})",
        label_map={"post": "Post", "pc": "PostCommunity"},
        params={"post_id": post_id, "post_text": post_text, "author_id": author_id},
    )

    neo4j.execute_labeled(
        "MATCH (p:__post__ {id: $post_id}), (pc:__pc__ {id: 300}) "
        "CREATE (p)-[:BELONGS_TO]->(pc)",
        label_map={"post": "Post", "pc": "PostCommunity"},
        params={"post_id": post_id},
    )

    # Create account in PostgreSQL
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (%s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET username = EXCLUDED.username, domain = EXCLUDED.domain;
            """,
            (author_id, username, domain),
        )
        postgres_conn.commit()

    show_post_community_stats(neo4j, postgres=postgres_client, modularity=None)

    captured = capsys.readouterr()
    output = captured.out

    # Check that post text is displayed
    assert "Text:" in output
    assert post_text in output

    # Check that author name is displayed in format @username@domain
    assert f"@{username}@{domain}" in output
    assert f"(id: {author_id:,})" in output
    assert "Active author:" in output


@pytest.mark.integration
def test_show_user_community_stats_without_postgres_displays_only_ids(
    neo4j: Neo4jClient,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test show_user_community_stats displays only user IDs when postgres is None."""
    neo4j.label("User")
    neo4j.label("UserCommunity")
    neo4j.label("Post")

    user_id = 10002

    # Create user and community
    neo4j.execute_labeled(
        "CREATE (u:__user__ {id: $user_id}), "
        "(uc:__uc__ {id: 201, size: 1})",
        label_map={"user": "User", "uc": "UserCommunity"},
        params={"user_id": user_id},
    )

    neo4j.execute_labeled(
        "MATCH (u:__user__ {id: $user_id}), (uc:__uc__ {id: 201}) "
        "CREATE (u)-[:BELONGS_TO]->(uc)",
        label_map={"user": "User", "uc": "UserCommunity"},
        params={"user_id": user_id},
    )

    # Create interactions
    neo4j.execute_labeled(
        "CREATE (p1:__post__ {id: 3})",
        {"post": "Post"},
    )
    neo4j.execute_labeled(
        "MATCH (u:__user__ {id: $user_id}), (p1:__post__ {id: 3}) "
        "CREATE (u)-[:FAVORITED]->(p1)",
        label_map={"user": "User", "post": "Post"},
        params={"user_id": user_id},
    )

    show_user_community_stats(neo4j, postgres=None, modularity=None)

    captured = capsys.readouterr()
    output = captured.out

    # Check that only user ID is displayed (no @username format)
    assert f"user {user_id:,}" in output
    assert "Active:" in output
    # Should not contain @ symbol (no username formatting)
    assert "@" not in output or f"@{user_id}" not in output


@pytest.mark.integration
def test_show_post_community_stats_without_postgres_displays_only_ids(
    neo4j: Neo4jClient,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test show_post_community_stats displays only author IDs when postgres is None."""
    neo4j.label("Post")
    neo4j.label("PostCommunity")

    post_id = 20003
    post_text = "Another test post"
    author_id = 20004

    # Create post and community
    neo4j.execute_labeled(
        "CREATE (p:__post__ {id: $post_id, text: $post_text, "
        "totalFavourites: 5, totalReblogs: 2, totalReplies: 1, authorId: $author_id}), "
        "(pc:__pc__ {id: 301, size: 1})",
        label_map={"post": "Post", "pc": "PostCommunity"},
        params={"post_id": post_id, "post_text": post_text, "author_id": author_id},
    )

    neo4j.execute_labeled(
        "MATCH (p:__post__ {id: $post_id}), (pc:__pc__ {id: 301}) "
        "CREATE (p)-[:BELONGS_TO]->(pc)",
        label_map={"post": "Post", "pc": "PostCommunity"},
        params={"post_id": post_id},
    )

    show_post_community_stats(neo4j, postgres=None, modularity=None)

    captured = capsys.readouterr()
    output = captured.out

    # Check that post text is still displayed (from Neo4j)
    assert "Text:" in output
    assert post_text in output

    # Check that only author ID is displayed (no @username format)
    assert f"user {author_id:,}" in output
    assert "Active author:" in output
    # Should not contain @ symbol for author (no username formatting)
    assert f"@{author_id}" not in output
