"""Shared fixtures and constants for documentation tests."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient


# Documentation display constants
MAX_DOCS_PARAMS = 30  # Maximum documentation parameters to show
MAX_MODULE_PROCEDURES = 5  # Maximum procedures to show per module


def gds_project_with_embedding(neo4j: "Neo4jClient", graph_name: str, label: str) -> None:
    """Create GDS graph with embedding property using Cypher projection."""
    # Graph name and label are dynamic, use parameterized query
    node_query = f"MATCH (n:{label}) WHERE n.embedding IS NOT NULL RETURN id(n) AS id, n.embedding AS embedding"
    neo4j.execute(
        "CALL gds.graph.project.cypher($graph_name, $node_query, 'RETURN null AS source, null AS target LIMIT 0')",
        {"graph_name": graph_name, "node_query": node_query},
    )


def gds_drop_graph(neo4j: "Neo4jClient", graph_name: str) -> None:
    """Drop GDS graph if exists."""
    neo4j.execute(
        "CALL gds.graph.drop($graph_name, false)",
        {"graph_name": graph_name},
    )
