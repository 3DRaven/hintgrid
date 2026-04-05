"""Integration tests for Leiden graph diagnostics (Neo4j)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.pipeline.leiden_diagnostics import (
    collect_post_similarity_graph_stats,
    collect_user_interaction_graph_stats,
)

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient


@pytest.mark.integration
def test_collect_user_interaction_graph_stats_counts_weights(
    neo4j: Neo4jClient,
    neo4j_id_offset: int,
) -> None:
    """INTERACTS_WITH aggregates match seeded graph."""
    uid1 = neo4j_id_offset + 1
    uid2 = neo4j_id_offset + 2
    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: $id1}) CREATE (u2:__user__ {id: $id2}) "
        "CREATE (u1)-[:INTERACTS_WITH {weight: 5.0}]->(u2) "
        "CREATE (u2)-[:INTERACTS_WITH {weight: 1.0}]->(u1)",
        {"user": "User"},
        {"id1": uid1, "id2": uid2},
    )
    stats = collect_user_interaction_graph_stats(neo4j)
    assert stats.get("rel_count") == 2
    assert stats.get("weight_sum") == 6.0
    assert stats.get("node_count") == 2
    assert stats.get("isolated_nodes") == 0


@pytest.mark.integration
def test_collect_post_similarity_graph_stats_counts_weights(
    neo4j: Neo4jClient,
    neo4j_id_offset: int,
) -> None:
    """SIMILAR_TO aggregates match seeded graph."""
    p1 = neo4j_id_offset + 100
    p2 = neo4j_id_offset + 101
    neo4j.execute_labeled(
        "CREATE (a:__post__ {id: $p1}) CREATE (b:__post__ {id: $p2}) "
        "CREATE (a)-[:SIMILAR_TO {weight: 0.5}]->(b)",
        {"post": "Post"},
        {"p1": p1, "p2": p2},
    )
    stats = collect_post_similarity_graph_stats(neo4j)
    assert stats.get("rel_count") == 1
    assert stats.get("weight_sum") == 0.5
    assert stats.get("node_count") == 2
