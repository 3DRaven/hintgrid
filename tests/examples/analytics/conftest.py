"""Shared fixtures and constants for analytics tests."""

from typing import TYPE_CHECKING, cast

from neo4j.exceptions import ClientError

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient

# ============================================================================
# Type Helpers
# ============================================================================


# Removed _as_embedding - replaced with direct cast() in tests


# ============================================================================
# GDS Projection Helpers
# ============================================================================


def gds_project(neo4j: "Neo4jClient", graph_name: str, label: str, rel_type: str = "FOLLOWS") -> None:
    """Create GDS graph using aggregation function for multi-label support.
    
    Note: label should be like 'User:worker_gw0' which Cypher interprets as two labels.
    """
    # Extract base label (part before ':') for execute_labeled
    base_label = label.split(":")[0] if ":" in label else label
    neo4j.execute_labeled(
        """
        MATCH (n:__label__)-[r:__rel_type__]->(m:__label__)
        WITH gds.graph.project(
            $graph_name,
            n,
            m
        ) AS g
        RETURN g.graphName
        """,
        label_map={"label": base_label},
        ident_map={"rel_type": rel_type},
        params={"graph_name": graph_name},
    )


def gds_project_with_embedding(neo4j: "Neo4jClient", graph_name: str, label: str) -> None:
    """Create GDS graph with embedding property for K-Means clustering.
    
    Uses Cypher projection to correctly handle node properties.
    Note: gds.graph.project.cypher is deprecated but works reliably for this case.
    """
    # Extract base label (part before ':') for execute_labeled
    base_label = label.split(":")[0] if ":" in label else label
    neo4j.execute_labeled(
        """
        CALL gds.graph.project.cypher(
            $graph_name,
            'MATCH (n:__label__) WHERE n.embedding IS NOT NULL RETURN id(n) AS id, n.embedding AS embedding',
            'RETURN null AS source, null AS target LIMIT 0'
        )
        """,
        label_map={"label": base_label},
        params={"graph_name": graph_name},
    )


def gds_project_undirected(neo4j: "Neo4jClient", graph_name: str, label: str) -> None:
    """Create undirected GDS graph using aggregation function."""
    # Extract base label (part before ':') for execute_labeled
    base_label = label.split(":")[0] if ":" in label else label
    neo4j.execute_labeled(
        """
        MATCH (n:__label__)-[r:FOLLOWS]->(m:__label__)
        WITH gds.graph.project(
            $graph_name,
            n,
            m,
            {},
            {undirectedRelationshipTypes: ['*']}
        ) AS g
        RETURN g.graphName
        """,
        label_map={"label": base_label},
        params={"graph_name": graph_name},
    )


def gds_project_undirected_weighted(neo4j: "Neo4jClient", graph_name: str, label: str) -> None:
    """Create undirected weighted GDS graph using aggregation function."""
    # Extract base label (part before ':') for execute_labeled
    base_label = label.split(":")[0] if ":" in label else label
    neo4j.execute_labeled(
        """
        MATCH (n:__label__)-[r:FOLLOWS]->(m:__label__)
        WITH gds.graph.project(
            $graph_name,
            n,
            m,
            {},
            {relationshipProperties: r {.weight}, undirectedRelationshipTypes: ['*']}
        ) AS g
        RETURN g.graphName
        """,
        label_map={"label": base_label},
        params={"graph_name": graph_name},
    )


# ============================================================================
# Exception Types
# ============================================================================

KMEANS_EMPTY_EXCEPTIONS: tuple[type[BaseException], ...] = (
    ClientError,
    RuntimeError,
)
KMEANS_INVALID_EXCEPTIONS: tuple[type[BaseException], ...] = (
    ClientError,
    ValueError,
    RuntimeError,
)

# ============================================================================
# FastRP Constants
# ============================================================================

NODE2VEC_P = 1.0  # Return parameter
NODE2VEC_Q = 1.0  # In-out parameter
NODE2VEC_walks_SHORT = 5  # Quick tests
NODE2VEC_walks_LONG = 10  # Standard tests
NODE2VEC_walks_QUALITY = 20  # Quality tests
NODE2VEC_WALK_LENGTH = 10  # Single walk length
NODE2VEC_WALK_LENGTH_LONG = 20  # Large graphs
NODE2VEC_WALK_LENGTH_QUALITY = 40  # Quality tests

# ============================================================================
# Embedding Constants
# ============================================================================

EMBEDDING_DIM_SMALL = 8  # Quick tests
EMBEDDING_DIM_MEDIUM = 16  # Quality tests

# ============================================================================
# Clustering Constants
# ============================================================================

NUM_CLUSTERS = 2
MIN_CLUSTER_COHESION = 0.6
QUALITY_MULTIPLIER = 2

# ============================================================================
# Graph Sizes
# ============================================================================

MINIMAL_GRAPH_NODES = 2
SMALL_GRAPH_NODES = 5
MEDIUM_GRAPH_NODES = 10
LARGE_GRAPH_NODES = 20
NODES_PER_GROUP = 5

# ============================================================================
# Edge Counts
# ============================================================================

SMALL_GRAPH_EDGES = 5
LARGE_GRAPH_EDGES = 20

# ============================================================================
# Community Constants
# ============================================================================

COMMUNITY_INTEREST_SCORE = 0.8
FAVOURITES_COUNT = 1

# ============================================================================
# Edge Cases
# ============================================================================

SINGLE_NODE = 1
THREE_NODES = 3
SIX_NODES = 6
ZERO_CLUSTERS = 0
NEGATIVE_CLUSTERS = -1
ZERO_walks = 0
