"""Integration tests for batched UserCommunity/PostCommunity creation."""

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
def test_create_user_community_structure_batched(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """UserCommunity and BELONGS_TO are created from cluster_id via batched APOC."""
    neo4j.execute_labeled(
        "MATCH (u:__user__) DETACH DELETE u",
        {"user": "User"},
    )
    neo4j.execute_labeled(
        "MATCH (uc:__uc__) DETACH DELETE uc",
        {"uc": "UserCommunity"},
    )
    for uid, cid in ((1, 10), (2, 10), (3, 20)):
        neo4j.execute_labeled(
            "CREATE (u:__user__ {id: $id, cluster_id: $cid})",
            {"user": "User"},
            params={"id": uid, "cid": cid},
        )

    small_batch = settings.model_copy(update={"apoc_batch_size": 1})
    create_user_community_structure(neo4j, small_batch, progress=None)

    uc_rows = neo4j.execute_and_fetch_labeled(
        "MATCH (uc:__uc__) RETURN uc.id AS id ORDER BY uc.id",
        {"uc": "UserCommunity"},
    )
    uc_ids = [coerce_int(r["id"]) for r in uc_rows]
    assert uc_ids == [10, 20]

    link_count = neo4j.execute_and_fetch_labeled(
        "MATCH (u:__user__)-[:BELONGS_TO]->(uc:__uc__) RETURN count(*) AS c",
        {"user": "User", "uc": "UserCommunity"},
    )
    assert coerce_int(link_count[0]["c"]) == 3


@pytest.mark.integration
def test_create_post_community_structure_batched(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """PostCommunity and BELONGS_TO are created from cluster_id via batched APOC."""
    neo4j.execute_labeled(
        "MATCH (p:__post__) DETACH DELETE p",
        {"post": "Post"},
    )
    neo4j.execute_labeled(
        "MATCH (pc:__pc__) DETACH DELETE pc",
        {"pc": "PostCommunity"},
    )
    for pid, cid in ((100, 1), (101, 2)):
        neo4j.execute_labeled(
            "CREATE (p:__post__ {id: $id, cluster_id: $cid})",
            {"post": "Post"},
            params={"id": pid, "cid": cid},
        )

    small_batch = settings.model_copy(update={"apoc_batch_size": 1})
    create_post_community_structure(neo4j, small_batch, progress=None)

    pc_rows = neo4j.execute_and_fetch_labeled(
        "MATCH (pc:__pc__) RETURN pc.id AS id ORDER BY pc.id",
        {"pc": "PostCommunity"},
    )
    pc_ids = [coerce_int(r["id"]) for r in pc_rows]
    assert pc_ids == [1, 2]

    link_count = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__)-[:BELONGS_TO]->(pc:__pc__) RETURN count(*) AS c",
        {"post": "Post", "pc": "PostCommunity"},
    )
    assert coerce_int(link_count[0]["c"]) == 2


@pytest.mark.integration
def test_user_community_structure_rebuild_after_cluster_id_change(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Batched delete removes stale BELONGS_TO when cluster_id changes before merge."""
    neo4j.execute_labeled(
        "MATCH (u:__user__) DETACH DELETE u",
        {"user": "User"},
    )
    neo4j.execute_labeled(
        "MATCH (uc:__uc__) DETACH DELETE uc",
        {"uc": "UserCommunity"},
    )
    for uid, cid in ((1, 10), (2, 10)):
        neo4j.execute_labeled(
            "CREATE (u:__user__ {id: $id, cluster_id: $cid})",
            {"user": "User"},
            params={"id": uid, "cid": cid},
        )

    small_batch = settings.model_copy(update={"apoc_batch_size": 1})
    create_user_community_structure(neo4j, small_batch, progress=None)

    neo4j.execute_labeled(
        "MATCH (u:__user__ {id: $id}) SET u.cluster_id = $cid",
        {"user": "User"},
        params={"id": 1, "cid": 99},
    )
    create_user_community_structure(neo4j, small_batch, progress=None)

    u1_comm = neo4j.execute_and_fetch_labeled(
        "MATCH (u:__user__ {id: 1})-[:BELONGS_TO]->(uc:__uc__) RETURN uc.id AS id",
        {"user": "User", "uc": "UserCommunity"},
    )
    assert len(u1_comm) == 1
    assert coerce_int(u1_comm[0]["id"]) == 99

    u2_comm = neo4j.execute_and_fetch_labeled(
        "MATCH (u:__user__ {id: 2})-[:BELONGS_TO]->(uc:__uc__) RETURN uc.id AS id",
        {"user": "User", "uc": "UserCommunity"},
    )
    assert len(u2_comm) == 1
    assert coerce_int(u2_comm[0]["id"]) == 10

    total_belongs = neo4j.execute_and_fetch_labeled(
        "MATCH ()-[r:BELONGS_TO]->(:__uc__) RETURN count(r) AS c",
        {"uc": "UserCommunity"},
    )
    assert coerce_int(total_belongs[0]["c"]) == 2


@pytest.mark.integration
def test_post_community_structure_rebuild_after_cluster_id_change(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Batched delete removes stale post BELONGS_TO when cluster_id changes."""
    neo4j.execute_labeled(
        "MATCH (p:__post__) DETACH DELETE p",
        {"post": "Post"},
    )
    neo4j.execute_labeled(
        "MATCH (pc:__pc__) DETACH DELETE pc",
        {"pc": "PostCommunity"},
    )
    for pid, cid in ((100, 1), (200, 1)):
        neo4j.execute_labeled(
            "CREATE (p:__post__ {id: $id, cluster_id: $cid})",
            {"post": "Post"},
            params={"id": pid, "cid": cid},
        )

    small_batch = settings.model_copy(update={"apoc_batch_size": 1})
    create_post_community_structure(neo4j, small_batch, progress=None)

    neo4j.execute_labeled(
        "MATCH (p:__post__ {id: $id}) SET p.cluster_id = $cid",
        {"post": "Post"},
        params={"id": 100, "cid": 7},
    )
    create_post_community_structure(neo4j, small_batch, progress=None)

    p100 = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__ {id: 100})-[:BELONGS_TO]->(pc:__pc__) RETURN pc.id AS id",
        {"post": "Post", "pc": "PostCommunity"},
    )
    assert len(p100) == 1
    assert coerce_int(p100[0]["id"]) == 7

    total_belongs = neo4j.execute_and_fetch_labeled(
        "MATCH ()-[r:BELONGS_TO]->(:__pc__) RETURN count(r) AS c",
        {"pc": "PostCommunity"},
    )
    assert coerce_int(total_belongs[0]["c"]) == 2
