"""Integration tests for singleton cluster collapse to noise community id."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.pipeline.community_structure import (
    create_post_community_structure,
    create_user_community_structure,
)
from hintgrid.pipeline.singleton_cluster_collapse import (
    collapse_singleton_post_clusters,
    collapse_singleton_user_clusters,
)
from hintgrid.utils.coercion import coerce_int

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.config import HintGridSettings


@pytest.mark.integration
def test_collapse_singleton_post_clusters_in_transactions_path(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Batched CALL/IN TRANSACTIONS path matches single-transaction semantics."""
    neo4j.execute_labeled(
        "CREATE (a:__post__ {id: 1, cluster_id: 10}), "
        "(b:__post__ {id: 2, cluster_id: 10}), "
        "(c:__post__ {id: 3, cluster_id: 20})",
        {"post": "Post"},
    )
    noise = settings.noise_community_id
    batched = settings.model_copy(update={"singleton_collapse_in_transactions_of": 1})
    collapse_singleton_post_clusters(neo4j, batched)
    rows = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) RETURN p.id AS id, p.cluster_id AS cid ORDER BY p.id",
            {"post": "Post"},
        )
    )
    by_id = {coerce_int(r["id"]): coerce_int(r["cid"]) for r in rows}
    assert by_id[1] == 10
    assert by_id[2] == 10
    assert by_id[3] == noise


@pytest.mark.integration
def test_collapse_singleton_post_clusters_rewrites_cluster_ids(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Posts in size-1 Leiden clusters receive noise_community_id; multi-member stays."""
    neo4j.execute_labeled(
        "CREATE (a:__post__ {id: 1, cluster_id: 10}), "
        "(b:__post__ {id: 2, cluster_id: 10}), "
        "(c:__post__ {id: 3, cluster_id: 20}), "
        "(d:__post__ {id: 4, cluster_id: 30})",
        {"post": "Post"},
    )
    noise = settings.noise_community_id
    collapse_singleton_post_clusters(neo4j, settings)
    rows = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) RETURN p.id AS id, p.cluster_id AS cid ORDER BY p.id",
            {"post": "Post"},
        )
    )
    by_id = {coerce_int(r["id"]): coerce_int(r["cid"]) for r in rows}
    assert by_id[1] == 10
    assert by_id[2] == 10
    assert by_id[3] == noise
    assert by_id[4] == noise


@pytest.mark.integration
def test_collapse_singleton_disabled_no_op(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """When singleton_collapse_enabled is False, cluster_id is unchanged."""
    neo4j.execute_labeled(
        "CREATE (:__post__ {id: 1, cluster_id: 99})",
        {"post": "Post"},
    )
    s = settings.model_copy(update={"singleton_collapse_enabled": False})
    collapse_singleton_post_clusters(neo4j, s)
    row = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__) RETURN p.cluster_id AS cid",
        {"post": "Post"},
    )[0]
    assert coerce_int(row.get("cid")) == 99


@pytest.mark.integration
def test_collapse_then_community_structure_single_noise_pc(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Two singleton clusters merge to one noise id; one PostCommunity node after structure."""
    neo4j.execute_labeled(
        "MATCH (p:__post__) DETACH DELETE p",
        {"post": "Post"},
    )
    neo4j.execute_labeled(
        "CREATE (a:__post__ {id: 1, cluster_id: 1}), "
        "(b:__post__ {id: 2, cluster_id: 2})",
        {"post": "Post"},
    )
    noise = settings.noise_community_id
    collapse_singleton_post_clusters(neo4j, settings)
    create_post_community_structure(neo4j, settings)
    pcs = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (pc:__pc__) RETURN pc.id AS id ORDER BY pc.id",
            {"pc": "PostCommunity"},
        )
    )
    ids = {coerce_int(r["id"]) for r in pcs}
    assert ids == {noise}


@pytest.mark.integration
def test_collapse_singleton_user_clusters_rewrites_cluster_ids(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Users in size-1 clusters receive noise_community_id."""
    neo4j.execute_labeled(
        "CREATE (a:__user__ {id: 1, cluster_id: 10}), "
        "(b:__user__ {id: 2, cluster_id: 10}), "
        "(c:__user__ {id: 3, cluster_id: 20})",
        {"user": "User"},
    )
    noise = settings.noise_community_id
    collapse_singleton_user_clusters(neo4j, settings)
    rows = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__) RETURN u.id AS id, u.cluster_id AS cid ORDER BY u.id",
            {"user": "User"},
        )
    )
    by_id = {coerce_int(r["id"]): coerce_int(r["cid"]) for r in rows}
    assert by_id[1] == 10
    assert by_id[2] == 10
    assert by_id[3] == noise


@pytest.mark.integration
def test_user_collapse_then_community_structure(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Two user singleton clusters merge to one noise UserCommunity node."""
    neo4j.execute_labeled(
        "MATCH (u:__user__) DETACH DELETE u",
        {"user": "User"},
    )
    neo4j.execute_labeled(
        "CREATE (a:__user__ {id: 1, cluster_id: 1}), "
        "(b:__user__ {id: 2, cluster_id: 2})",
        {"user": "User"},
    )
    noise = settings.noise_community_id
    collapse_singleton_user_clusters(neo4j, settings)
    create_user_community_structure(neo4j, settings)
    ucs = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (uc:__uc__) RETURN uc.id AS id ORDER BY uc.id",
            {"uc": "UserCommunity"},
        )
    )
    ids = {coerce_int(r["id"]) for r in ucs}
    assert ids == {noise}
