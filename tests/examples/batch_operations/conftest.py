"""Shared fixtures and constants for batch operation tests."""

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from tests.conftest import DockerComposeInfo

# ============================================================================
# Batch Size Constants
# ============================================================================

SMALL_BATCH_SIZE = 10
MEDIUM_BATCH_SIZE = 100
LARGE_BATCH_SIZE = 500
VERY_LARGE_BATCH_SIZE = 1000

# ============================================================================
# Graph Size Constants
# ============================================================================

SMALL_GRAPH_NODES = 10
MEDIUM_GRAPH_NODES = 50
LARGE_GRAPH_NODES = 100

# ============================================================================
# Community Constants
# ============================================================================

COMMUNITY_SIZE = 5
SEQUENTIAL_BATCH_POSTS = 20
SEQUENTIAL_BATCH_USERS = 5


# ============================================================================
# PostgreSQL DSN Fixture
# ============================================================================


@pytest.fixture
def postgres_dsn(
    docker_compose: "DockerComposeInfo",
    worker_schema: str,
) -> str:
    """PostgreSQL DSN string for direct connection tests."""
    return (
        f"postgresql://{docker_compose.postgres_user}:{docker_compose.postgres_password}"
        f"@{docker_compose.postgres_host}:{docker_compose.postgres_port}"
        f"/{docker_compose.postgres_db}?options=-c%20search_path%3D{worker_schema}"
    )


# ============================================================================
# GDS Projection Helpers
# ============================================================================


def gds_project_undirected(neo4j: "Neo4jClient", graph_name: str, label: str) -> None:
    """Create undirected GDS graph using aggregation function."""
    neo4j.execute_labeled(
        """
        MATCH (n:__label__)-[r:FOLLOWS]->(m:__label__)
        WITH gds.graph.project(
            '__graph_name__',
            n,
            m,
            {},
            {undirectedRelationshipTypes: ['*']}
        ) AS g
        RETURN g.graphName
        """,
        label_map={"label": label},
        ident_map={"graph_name": graph_name},
    )
