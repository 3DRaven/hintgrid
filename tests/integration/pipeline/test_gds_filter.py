"""Integration test: verify GDS graph filtering capabilities on Community Edition.

Tests whether gds.graph.filter() and gds.graph.project.cypher() are available
in Neo4j Community Edition with GDS plugin.

Results determine the strategy for filtering inactive users from GDS projections.
"""

from __future__ import annotations

import pytest

from hintgrid.utils.coercion import coerce_int
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient


# Number of test users to create
ACTIVE_USERS = 3
INACTIVE_USERS = 2
TOTAL_USERS = ACTIVE_USERS + INACTIVE_USERS
EXPECTED_FILTERED_NODES = ACTIVE_USERS
EXPECTED_COMMUNITIES_UPPER_BOUND = ACTIVE_USERS


@pytest.mark.integration
def test_gds_graph_filter_availability(neo4j: Neo4jClient) -> None:
    """Test that gds.graph.filter() is available and works with node property filter.

    Creates a graph with active/inactive users, projects all, then filters
    to only active users using gds.graph.filter().
    """
    base_graph = f"filter-test-base-{neo4j.worker_label or 'master'}"
    filtered_graph = f"filter-test-filtered-{neo4j.worker_label or 'master'}"

    # Create test users: 3 active (lastActive=1), 2 inactive (lastActive=0)
    for i in range(1, ACTIVE_USERS + 1):
        neo4j.execute_labeled(
            "CREATE (:__user__ {id: $id, lastActive: 1})",
            {"user": "User"},
            {"id": i},
        )
    for i in range(ACTIVE_USERS + 1, TOTAL_USERS + 1):
        neo4j.execute_labeled(
            "CREATE (:__user__ {id: $id, lastActive: 0})",
            {"user": "User"},
            {"id": i},
        )

    # Create FOLLOWS relationships between all active users
    for i in range(1, ACTIVE_USERS + 1):
        for j in range(i + 1, ACTIVE_USERS + 1):
            neo4j.execute_labeled(
                "MATCH (a:__user__ {id: $a_id}), (b:__user__ {id: $b_id}) "
                "CREATE (a)-[:FOLLOWS]->(b)",
                {"user": "User"},
                {"a_id": i, "b_id": j},
            )

    # Step 1: Native projection of all users
    project_label = neo4j.worker_label or "User"
    neo4j.execute_labeled(
        "CALL gds.graph.project("
        "  '__graph_name__', '__node_label__', "
        "  {FOLLOWS: {orientation: 'UNDIRECTED'}}, "
        "  {nodeProperties: ['lastActive']}"
        ")",
        ident_map={"graph_name": base_graph, "node_label": project_label},
    )

    try:
        # Step 2: Filter to only active users using gds.graph.filter()
        # Graph names are dynamic, so we use execute with parameters
        neo4j.execute(
            "CALL gds.graph.filter($filtered_graph, $base_graph, "
            "'n.lastActive = 1', '*') "
            "YIELD graphName, nodeCount "
            "RETURN graphName, nodeCount",
            {"filtered_graph": filtered_graph, "base_graph": base_graph},
        )

        # Step 3: Verify filtered graph has only active users
        result = neo4j.execute_and_fetch(
            "CALL gds.graph.list($graph_name) "
            "YIELD nodeCount RETURN nodeCount",
            {"graph_name": filtered_graph},
        )
        assert len(result) == 1, "Filtered graph should exist"
        node_count = coerce_int(result[0]["nodeCount"])
        assert node_count == EXPECTED_FILTERED_NODES, (
            f"Filtered graph should have {EXPECTED_FILTERED_NODES} nodes, got {node_count}"
        )

        # Step 4: Run Leiden on filtered graph to verify it works end-to-end
        leiden_result = neo4j.execute_and_fetch(
            "CALL gds.leiden.stream($graph_name) "
            "YIELD nodeId, communityId "
            "RETURN count(DISTINCT communityId) AS communities",
            {"graph_name": filtered_graph},
        )
        communities = coerce_int(leiden_result[0]["communities"])
        assert communities >= 1, "Leiden should find at least 1 community"
        assert communities <= EXPECTED_COMMUNITIES_UPPER_BOUND, (
            f"Communities should be <= {EXPECTED_COMMUNITIES_UPPER_BOUND}"
        )

        print(
            f"gds.graph.filter() works: "
            f"{TOTAL_USERS} total -> {node_count} filtered, "
            f"{communities} communities"
        )

    finally:
        # Cleanup both graphs
        for graph_name in (filtered_graph, base_graph):
            neo4j.execute(
                "CALL gds.graph.drop($name, false) YIELD graphName",
                {"name": graph_name},
            )


@pytest.mark.integration
def test_cypher_projection_availability(neo4j: Neo4jClient) -> None:
    """Test that gds.graph.project.cypher() is available (deprecated but may work).

    Creates users with lastActive property, uses Cypher Projection to filter
    only active users during projection.
    """
    graph_name = f"cypher-proj-test-{neo4j.worker_label or 'master'}"

    # Create test users: 3 active, 2 inactive
    for i in range(1, ACTIVE_USERS + 1):
        neo4j.execute_labeled(
            "CREATE (:__user__ {id: $id, lastActive: 1})",
            {"user": "User"},
            {"id": i},
        )
    for i in range(ACTIVE_USERS + 1, TOTAL_USERS + 1):
        neo4j.execute_labeled(
            "CREATE (:__user__ {id: $id, lastActive: 0})",
            {"user": "User"},
            {"id": i},
        )

    # Create FOLLOWS between active users
    for i in range(1, ACTIVE_USERS + 1):
        for j in range(i + 1, ACTIVE_USERS + 1):
            neo4j.execute_labeled(
                "MATCH (a:__user__ {id: $a_id}), (b:__user__ {id: $b_id}) "
                "CREATE (a)-[:FOLLOWS]->(b)",
                {"user": "User"},
                {"a_id": i, "b_id": j},
            )

    try:
        # Cypher Projection with filter
        # Graph name and labels are dynamic, so we use execute with parameters
        node_query = "MATCH (n:__user__) WHERE n.lastActive = 1 RETURN id(n) AS id"
        rel_query = (
            "MATCH (a:__user__)-[:FOLLOWS]->(b:__user__) "
            "WHERE a.lastActive = 1 AND b.lastActive = 1 "
            "RETURN id(a) AS source, id(b) AS target"
        )
        neo4j.execute(
            "CALL gds.graph.project.cypher($graph_name, $node_query, $rel_query)",
            {
                "graph_name": graph_name,
                "node_query": node_query.replace("__user__", neo4j.label("User")),
                "rel_query": rel_query.replace("__user__", neo4j.label("User")),
            },
        )

        # Verify node count
        result = neo4j.execute_and_fetch(
            "CALL gds.graph.list($graph_name) "
            "YIELD nodeCount RETURN nodeCount",
            {"graph_name": graph_name},
        )
        assert len(result) == 1, "Cypher-projected graph should exist"
        node_count = coerce_int(result[0]["nodeCount"])
        assert node_count == EXPECTED_FILTERED_NODES, (
            f"Cypher projection should have {EXPECTED_FILTERED_NODES} nodes, got {node_count}"
        )

        print(
            f"gds.graph.project.cypher() works: "
            f"{TOTAL_USERS} total -> {node_count} filtered"
        )

    finally:
        neo4j.execute(
            "CALL gds.graph.drop($name, false) YIELD graphName",
            {"name": graph_name},
        )
