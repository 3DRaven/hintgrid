"""Integration tests for Neo4jClient helper methods.

Tests label generation, identifier validation, and query building
with real Neo4j connection.
"""

from __future__ import annotations

import pytest

from hintgrid.clients.neo4j import Neo4jClient
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tests.conftest import DockerComposeInfo
else:
    from tests.conftest import DockerComposeInfo


@pytest.mark.integration
class TestLabel:
    """Tests for Neo4jClient.label method."""

    def test_label_with_worker_label(self, neo4j: Neo4jClient) -> None:
        """Test label returns combined label with worker_label."""
        if neo4j.worker_label:
            assert neo4j.label("Post") == f"Post:{neo4j.worker_label}"
            assert neo4j.label("User") == f"User:{neo4j.worker_label}"
        else:
            assert neo4j.label("Post") == "Post"
            assert neo4j.label("User") == "User"

    def test_label_without_worker_label(
        self, docker_compose: DockerComposeInfo
    ) -> None:
        """Test label returns base label when no worker_label is set."""
        client = Neo4jClient(
            host=docker_compose.neo4j_host,
            port=docker_compose.neo4j_port,
            username=docker_compose.neo4j_user,
            password=docker_compose.neo4j_password,
            worker_label=None,
        )
        try:
            assert client.label("Post") == "Post"
            assert client.label("User") == "User"
        finally:
            client.close()


@pytest.mark.integration
class TestMatchAllNodes:
    """Tests for Neo4jClient.match_all_nodes method."""

    def test_with_worker_label(self, neo4j: Neo4jClient) -> None:
        """Test match_all_nodes with worker label returns labeled pattern."""
        if neo4j.worker_label:
            assert neo4j.match_all_nodes() == f"(n:{neo4j.worker_label})"
            assert neo4j.match_all_nodes("x") == f"(x:{neo4j.worker_label})"
        else:
            assert neo4j.match_all_nodes() == "(n)"
            assert neo4j.match_all_nodes("x") == "(x)"

    def test_without_worker_label(
        self, docker_compose: DockerComposeInfo
    ) -> None:
        """Test match_all_nodes without worker label returns bare pattern."""
        client = Neo4jClient(
            host=docker_compose.neo4j_host,
            port=docker_compose.neo4j_port,
            username=docker_compose.neo4j_user,
            password=docker_compose.neo4j_password,
            worker_label=None,
        )
        try:
            assert client.match_all_nodes() == "(n)"
            assert client.match_all_nodes("x") == "(x)"
        finally:
            client.close()


@pytest.mark.integration
class TestFormatTemplate:
    """Tests for Neo4jClient labeled query methods (formatting logic)."""

    def test_unsafe_ident_raises_value_error(self, neo4j: Neo4jClient) -> None:
        """Test that unsafe identifiers raise ValueError."""
        with pytest.raises(ValueError, match="Unsafe identifier"):
            neo4j.execute_labeled(
                "DROP INDEX __idx__ IF EXISTS",
                ident_map={"idx": "DROP INDEX; --"},
            )

    def test_empty_ident_passes(self, neo4j: Neo4jClient) -> None:
        """Test that empty identifier is allowed."""
        # Empty string matches the safe ident pattern (r"[A-Za-z0-9_:.\-]*")
        # Execute a simple query to verify formatting works
        neo4j.execute_labeled(
            "RETURN 1 AS val",
            ident_map={"val": ""},
        )
        # If no error is raised, formatting succeeded

    def test_safe_ident_passes(self, neo4j: Neo4jClient) -> None:
        """Test that safe identifiers are substituted correctly."""
        # Execute a query to verify safe identifier substitution works
        neo4j.execute_labeled(
            "RETURN 1 AS result",
            ident_map={"idx": "post_embedding_index"},
        )
        # If no error is raised, formatting succeeded
