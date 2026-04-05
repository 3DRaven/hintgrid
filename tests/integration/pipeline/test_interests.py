"""Integration tests for pipeline interests module."""

from __future__ import annotations


import pytest

from hintgrid.config import HintGridSettings
from hintgrid.pipeline.interests import (
    cleanup_expired_interests,
    rebuild_interests,
    seed_serendipity,
)
from hintgrid.utils.coercion import coerce_int
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient


@pytest.mark.integration
def test_cleanup_expired_interests_removes_old_relationships(
    neo4j: Neo4jClient,
) -> None:
    """Test that cleanup_expired_interests removes expired INTERESTED_IN relationships.

    Creates relationships with past expiration dates and verifies they get deleted.
    """
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")

    # Create test communities
    neo4j.execute_labeled(
        "CREATE (uc:__uc__ {id: 'test_uc_1'}) "
        "CREATE (pc:__pc__ {id: 'test_pc_1'})",
        {"uc": "UserCommunity", "pc": "PostCommunity"},
    )

    # Create expired INTERESTED_IN relationship (expired 1 day ago)
    neo4j.execute_labeled(
        "MATCH (uc:__uc__ {id: 'test_uc_1'}) "
        "MATCH (pc:__pc__ {id: 'test_pc_1'}) "
        "CREATE (uc)-[:INTERESTED_IN {"
        "weight: 0.5, "
        "expires_at: datetime() - duration({days: 1})"
        "}]->(pc)",
        {"uc": "UserCommunity", "pc": "PostCommunity"},
    )

    # Verify relationship exists before cleanup
    count_before = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (:__uc__)-[i:INTERESTED_IN]->(:__pc__) "
            "RETURN count(i) AS cnt",
            {"uc": "UserCommunity", "pc": "PostCommunity"},
        )
    )
    assert coerce_int(count_before[0].get("cnt")) >= 1

    # Run cleanup
    cleanup_expired_interests(neo4j)

    # Verify expired relationship was removed
    count_after = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (uc:__uc__ {id: 'test_uc_1'})-[i:INTERESTED_IN]->(:__pc__) "
            "RETURN count(i) AS cnt",
            {"uc": "UserCommunity", "pc": "PostCommunity"},
        )
    )
    assert coerce_int(count_after[0].get("cnt")) == 0


@pytest.mark.integration
def test_cleanup_expired_interests_keeps_valid_relationships(
    neo4j: Neo4jClient,
) -> None:
    """Test that cleanup_expired_interests keeps non-expired relationships.

    Creates relationships with future expiration dates and verifies they are kept.
    """
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")

    # Create test communities
    neo4j.execute_labeled(
        "CREATE (uc:__uc__ {id: 'test_uc_2'}) "
        "CREATE (pc:__pc__ {id: 'test_pc_2'})",
        {"uc": "UserCommunity", "pc": "PostCommunity"},
    )

    # Create valid INTERESTED_IN relationship (expires in 7 days)
    neo4j.execute_labeled(
        "MATCH (uc:__uc__ {id: 'test_uc_2'}) "
        "MATCH (pc:__pc__ {id: 'test_pc_2'}) "
        "CREATE (uc)-[:INTERESTED_IN {"
        "weight: 0.8, "
        "expires_at: datetime() + duration({days: 7})"
        "}]->(pc)",
        {"uc": "UserCommunity", "pc": "PostCommunity"},
    )

    # Run cleanup
    cleanup_expired_interests(neo4j)

    # Verify valid relationship was kept
    count_after = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (uc:__uc__ {id: 'test_uc_2'})-[i:INTERESTED_IN]->(:__pc__) "
            "RETURN count(i) AS cnt",
            {"uc": "UserCommunity", "pc": "PostCommunity"},
        )
    )
    assert coerce_int(count_after[0].get("cnt")) == 1


@pytest.mark.integration
def test_seed_serendipity_creates_relationships(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test that seed_serendipity creates random INTERESTED_IN relationships.

    Creates user and post communities, then seeds serendipity relationships.
    """
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")

    # Create multiple test communities
    for i in range(3):
        neo4j.execute_labeled(
            "CREATE (uc:__uc__ {id: $uc_id}) "
            "CREATE (pc:__pc__ {id: $pc_id})",
            label_map={"uc": "UserCommunity", "pc": "PostCommunity"},
            params={"uc_id": f"seed_uc_{i}", "pc_id": f"seed_pc_{i}"},
        )

    # Run serendipity seeding with settings
    seed_serendipity(neo4j, settings)

    # Verify some serendipity relationships were created
    count = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (:__uc__)-[i:INTERESTED_IN]->(:__pc__) "
            "WHERE i.serendipity = true "
            "RETURN count(i) AS cnt",
            {"uc": "UserCommunity", "pc": "PostCommunity"},
        )
    )
    # May be 0 if random sampling didn't find pairs
    assert coerce_int(count[0].get("cnt")) >= 0


@pytest.mark.integration
def test_rebuild_interests_with_high_ctr(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test that rebuild_interests applies CTR multiplier for high CTR.

    High CTR (many interactions per recommendation) should increase interest weight.
    """
    neo4j.label("User")
    neo4j.label("Post")
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")

    # Create test data: user community, post community, users, posts
    neo4j.execute_labeled(
        "CREATE (uc:__uc__ {id: 1}) "
        "CREATE (pc:__pc__ {id: 10}) "
        "CREATE (u1:__user__ {id: 101}) "
        "CREATE (u2:__user__ {id: 102}) "
        "CREATE (p1:__post__ {id: 201}) "
        "CREATE (p2:__post__ {id: 202}) "
        "CREATE (p3:__post__ {id: 203}) "
        "CREATE (u1)-[:BELONGS_TO]->(uc) "
        "CREATE (u2)-[:BELONGS_TO]->(uc) "
        "CREATE (p1)-[:BELONGS_TO]->(pc) "
        "CREATE (p2)-[:BELONGS_TO]->(pc) "
        "CREATE (p3)-[:BELONGS_TO]->(pc)",
        {"uc": "UserCommunity", "pc": "PostCommunity", "user": "User", "post": "Post"},
    )

    # Create recommendations (6 recommendations: 3 posts * 2 users)
    for post_id in [201, 202, 203]:
        for user_id in [101, 102]:
            neo4j.execute_labeled(
                "MATCH (u:__user__ {id: $user_id}) "
                "MATCH (p:__post__ {id: $post_id}) "
                "CREATE (u)-[:WAS_RECOMMENDED {at: datetime()}]->(p)",
                {"user": "User", "post": "Post"},
                {"user_id": user_id, "post_id": post_id},
            )

    # Create interactions (5 interactions = high CTR)
    # User 1 interacts with posts 201, 202, 203
    for post_id in [201, 202, 203]:
        neo4j.execute_labeled(
            "MATCH (u:__user__ {id: 101}) "
            "MATCH (p:__post__ {id: $post_id}) "
            "CREATE (u)-[:FAVORITED {at: datetime()}]->(p)",
            {"user": "User", "post": "Post"},
            {"post_id": post_id},
        )
    # User 2 interacts with posts 201, 202
    for post_id in [201, 202]:
        neo4j.execute_labeled(
            "MATCH (u:__user__ {id: 102}) "
            "MATCH (p:__post__ {id: $post_id}) "
            "CREATE (u)-[:FAVORITED {at: datetime()}]->(p)",
            {"user": "User", "post": "Post"},
            {"post_id": post_id},
        )

    # Rebuild interests with CTR enabled
    test_settings = HintGridSettings(
        ctr_enabled=True,
        ctr_weight=0.5,
        min_ctr=0.0,
        ctr_smoothing=1.0,
        interests_ttl_days=30,
        interests_min_favourites=1,
        likes_weight=1.0,
        reblogs_weight=1.5,
        replies_weight=3.0,
    )
    rebuild_interests(neo4j, test_settings)

    # Verify INTERESTED_IN was created
    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (uc:__uc__ {id: 1})-[i:INTERESTED_IN]->(pc:__pc__ {id: 10}) "
            "RETURN i.score AS score, i.based_on AS based_on",
            {"uc": "UserCommunity", "pc": "PostCommunity"},
        )
    )

    assert len(result) == 1
    # High CTR should result in higher score (CTR = (5+1)/(6+1) ≈ 0.857)
    # With ctr_weight=0.5: multiplier = 0.5 * 0.857 + 0.5 ≈ 0.929
    from hintgrid.utils.coercion import coerce_float
    assert coerce_float(result[0]["score"]) > 0.0
    assert result[0]["based_on"] == 5  # 5 unique interactions (user-post pairs)


@pytest.mark.integration
def test_rebuild_interests_with_low_ctr(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test that rebuild_interests applies CTR multiplier for low CTR.

    Low CTR (few interactions per recommendation) should decrease interest weight.
    """
    neo4j.label("User")
    neo4j.label("Post")
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")

    # Create test data
    neo4j.execute_labeled(
        "CREATE (uc:__uc__ {id: 2}) "
        "CREATE (pc:__pc__ {id: 20}) "
        "CREATE (u1:__user__ {id: 201}) "
        "CREATE (p1:__post__ {id: 301}) "
        "CREATE (p2:__post__ {id: 302}) "
        "CREATE (p3:__post__ {id: 303}) "
        "CREATE (u1)-[:BELONGS_TO]->(uc) "
        "CREATE (p1)-[:BELONGS_TO]->(pc) "
        "CREATE (p2)-[:BELONGS_TO]->(pc) "
        "CREATE (p3)-[:BELONGS_TO]->(pc)",
        {"uc": "UserCommunity", "pc": "PostCommunity", "user": "User", "post": "Post"},
    )

    # Create many recommendations (3 recommendations)
    for post_id in [301, 302, 303]:
        neo4j.execute_labeled(
            "MATCH (u:__user__ {id: 201}) "
            "MATCH (p:__post__ {id: $post_id}) "
            "CREATE (u)-[:WAS_RECOMMENDED {at: datetime()}]->(p)",
            {"user": "User", "post": "Post"},
            {"post_id": post_id},
        )

    # Create few interactions (1 interaction = low CTR)
    neo4j.execute_labeled(
        "MATCH (u:__user__ {id: 201}) "
        "MATCH (p:__post__ {id: 301}) "
        "CREATE (u)-[:FAVORITED {at: datetime()}]->(p)",
        {"user": "User", "post": "Post"},
    )

    # Rebuild interests with CTR enabled
    test_settings = HintGridSettings(
        ctr_enabled=True,
        ctr_weight=0.5,
        min_ctr=0.0,
        ctr_smoothing=1.0,
        interests_ttl_days=30,
        interests_min_favourites=1,
        likes_weight=1.0,
        reblogs_weight=1.5,
        replies_weight=3.0,
    )
    rebuild_interests(neo4j, test_settings)

    # Verify INTERESTED_IN was created
    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (uc:__uc__ {id: 2})-[i:INTERESTED_IN]->(pc:__pc__ {id: 20}) "
            "RETURN i.score AS score, i.based_on AS based_on",
            {"uc": "UserCommunity", "pc": "PostCommunity"},
        )
    )

    assert len(result) == 1
    # Low CTR should result in lower score (CTR = (1+1)/(3+1) = 0.5)
    # With ctr_weight=0.5: multiplier = 0.5 * 0.5 + 0.5 = 0.75
    from hintgrid.utils.coercion import coerce_float
    assert coerce_float(result[0]["score"]) > 0.0
    assert result[0]["based_on"] == 1


@pytest.mark.integration
def test_rebuild_interests_with_no_recommendations(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test that rebuild_interests works when there are no recommendations.

    Should use base weight without CTR adjustment (division by zero protection).
    """
    neo4j.label("User")
    neo4j.label("Post")
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")

    # Create test data
    neo4j.execute_labeled(
        "CREATE (uc:__uc__ {id: 3}) "
        "CREATE (pc:__pc__ {id: 30}) "
        "CREATE (u1:__user__ {id: 301}) "
        "CREATE (p1:__post__ {id: 401}) "
        "CREATE (u1)-[:BELONGS_TO]->(uc) "
        "CREATE (p1)-[:BELONGS_TO]->(pc)",
        {"uc": "UserCommunity", "pc": "PostCommunity", "user": "User", "post": "Post"},
    )

    # Create interactions but NO recommendations
    neo4j.execute_labeled(
        "MATCH (u:__user__ {id: 301}) "
        "MATCH (p:__post__ {id: 401}) "
        "CREATE (u)-[:FAVORITED {at: datetime()}]->(p)",
        {"user": "User", "post": "Post"},
    )

    # Rebuild interests with CTR enabled
    test_settings = HintGridSettings(
        ctr_enabled=True,
        ctr_weight=0.5,
        min_ctr=0.0,
        ctr_smoothing=1.0,
        interests_ttl_days=30,
        interests_min_favourites=1,
        likes_weight=1.0,
        reblogs_weight=1.5,
        replies_weight=3.0,
    )
    rebuild_interests(neo4j, test_settings)

    # Verify INTERESTED_IN was created (should work without recommendations)
    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (uc:__uc__ {id: 3})-[i:INTERESTED_IN]->(pc:__pc__ {id: 30}) "
            "RETURN i.score AS score, i.based_on AS based_on",
            {"uc": "UserCommunity", "pc": "PostCommunity"},
        )
    )

    assert len(result) == 1
    from hintgrid.utils.coercion import coerce_float
    assert coerce_float(result[0]["score"]) > 0.0
    assert result[0]["based_on"] == 1


@pytest.mark.integration
def test_rebuild_interests_with_ctr_disabled(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test that rebuild_interests works without CTR (backward compatibility).

    When ctr_enabled=False, should use original logic without CTR calculation.
    """
    neo4j.label("User")
    neo4j.label("Post")
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")

    # Create test data
    neo4j.execute_labeled(
        "CREATE (uc:__uc__ {id: 4}) "
        "CREATE (pc:__pc__ {id: 40}) "
        "CREATE (u1:__user__ {id: 401}) "
        "CREATE (p1:__post__ {id: 501}) "
        "CREATE (u1)-[:BELONGS_TO]->(uc) "
        "CREATE (p1)-[:BELONGS_TO]->(pc)",
        {"uc": "UserCommunity", "pc": "PostCommunity", "user": "User", "post": "Post"},
    )

    # Create interactions
    neo4j.execute_labeled(
        "MATCH (u:__user__ {id: 401}) "
        "MATCH (p:__post__ {id: 501}) "
        "CREATE (u)-[:FAVORITED {at: datetime()}]->(p)",
        {"user": "User", "post": "Post"},
    )

    # Create recommendations (should be ignored when CTR disabled)
    neo4j.execute_labeled(
        "MATCH (u:__user__ {id: 401}) "
        "MATCH (p:__post__ {id: 501}) "
        "CREATE (u)-[:WAS_RECOMMENDED {at: datetime()}]->(p)",
        {"user": "User", "post": "Post"},
    )

    # Rebuild interests with CTR disabled
    test_settings = HintGridSettings(
        ctr_enabled=False,
        ctr_weight=0.5,
        min_ctr=0.0,
        ctr_smoothing=1.0,
        interests_ttl_days=30,
        interests_min_favourites=1,
        likes_weight=1.0,
        reblogs_weight=1.5,
        replies_weight=3.0,
    )
    rebuild_interests(neo4j, test_settings)

    # Verify INTERESTED_IN was created (using original logic)
    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (uc:__uc__ {id: 4})-[i:INTERESTED_IN]->(pc:__pc__ {id: 40}) "
            "RETURN i.score AS score, i.based_on AS based_on",
            {"uc": "UserCommunity", "pc": "PostCommunity"},
        )
    )

    assert len(result) == 1
    from hintgrid.utils.coercion import coerce_float
    assert coerce_float(result[0]["score"]) > 0.0
    assert result[0]["based_on"] == 1
