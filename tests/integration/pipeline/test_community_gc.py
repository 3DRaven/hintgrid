"""Integration tests for orphan UserCommunity/PostCommunity GC after BELONGS_TO rebuild."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.pipeline.community_structure import (
    create_post_community_structure,
    create_user_community_structure,
)
from hintgrid.utils.coercion import coerce_int

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.config import HintGridSettings


@pytest.mark.integration
def test_gc_removes_preseeded_orphan_user_community(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Stale UserCommunity nodes without BELONGS_TO are deleted after structure rebuild."""
    neo4j.execute_labeled(
        "MATCH (u:__user__) DETACH DELETE u",
        {"user": "User"},
    )
    neo4j.execute_labeled(
        "MATCH (uc:__uc__) DETACH DELETE uc",
        {"uc": "UserCommunity"},
    )
    neo4j.execute_labeled(
        "CREATE (orphan:__uc__ {id: 999}), (u:__user__ {id: 1, cluster_id: 5})",
        {"uc": "UserCommunity", "user": "User"},
    )
    create_user_community_structure(neo4j, settings, progress=None)

    rows = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (uc:__uc__) RETURN uc.id AS id ORDER BY uc.id",
            {"uc": "UserCommunity"},
        )
    )
    ids = {coerce_int(r["id"]) for r in rows}
    assert ids == {5}
    assert 999 not in ids


@pytest.mark.integration
def test_gc_removes_preseeded_orphan_post_community(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Stale PostCommunity nodes without BELONGS_TO are deleted after structure rebuild."""
    neo4j.execute_labeled(
        "MATCH (p:__post__) DETACH DELETE p",
        {"post": "Post"},
    )
    neo4j.execute_labeled(
        "MATCH (pc:__pc__) DETACH DELETE pc",
        {"pc": "PostCommunity"},
    )
    neo4j.execute_labeled(
        "CREATE (orphan:__pc__ {id: 888}), (p:__post__ {id: 1, cluster_id: 7})",
        {"pc": "PostCommunity", "post": "Post"},
    )
    create_post_community_structure(neo4j, settings, progress=None)

    rows = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (pc:__pc__) RETURN pc.id AS id ORDER BY pc.id",
            {"pc": "PostCommunity"},
        )
    )
    ids = {coerce_int(r["id"]) for r in rows}
    assert ids == {7}
    assert 888 not in ids


@pytest.mark.integration
def test_community_structure_total_zero_gc_clears_orphan_communities(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """When no entity has cluster_id, BELONGS_TO is dropped and orphan communities are removed."""
    neo4j.execute_labeled(
        "MATCH (u:__user__) DETACH DELETE u",
        {"user": "User"},
    )
    neo4j.execute_labeled(
        "MATCH (uc:__uc__) DETACH DELETE uc",
        {"uc": "UserCommunity"},
    )
    neo4j.execute_labeled(
        "CREATE (uc:__uc__ {id: 1}), (uc2:__uc__ {id: 2})",
        {"uc": "UserCommunity"},
    )
    create_user_community_structure(neo4j, settings, progress=None)

    cnt = neo4j.execute_and_fetch_labeled(
        "MATCH (uc:__uc__) RETURN count(uc) AS c",
        {"uc": "UserCommunity"},
    )
    assert coerce_int(cnt[0]["c"]) == 0
