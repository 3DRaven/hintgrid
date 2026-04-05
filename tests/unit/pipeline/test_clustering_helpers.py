"""Integration tests for clustering module helper functions.

Tests GDS name validation and embedding index name generation
with real Neo4j client.
"""

from __future__ import annotations

import pytest

from hintgrid.pipeline.clustering import (
    POST_EMBEDDING_INDEX_BASE_NAME,
    _get_embedding_index_name,
    validate_gds_name,
)
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from tests.conftest import DockerComposeInfo


class TestValidateGdsName:
    """Tests for validate_gds_name function."""

    def test_valid_names_pass(self) -> None:
        """Test that valid GDS names pass validation."""
        assert validate_gds_name("user-graph") == "user-graph"
        assert validate_gds_name("post-graph") == "post-graph"
        assert validate_gds_name("worker_gw0_posts") == "worker_gw0_posts"
        assert validate_gds_name("MyGraph123") == "MyGraph123"

    def test_invalid_names_raise(self) -> None:
        """Test that invalid GDS names raise ValueError."""
        with pytest.raises(ValueError, match="Invalid GDS name"):
            validate_gds_name("")

        with pytest.raises(ValueError, match="Invalid GDS name"):
            validate_gds_name("123starts_with_number")

        with pytest.raises(ValueError, match="Invalid GDS name"):
            validate_gds_name("has spaces")

        with pytest.raises(ValueError, match="Invalid GDS name"):
            validate_gds_name("has@special")


@pytest.mark.integration
class TestGetEmbeddingIndexName:
    """Tests for _get_embedding_index_name function."""

    def test_with_worker_label(self, neo4j: Neo4jClient) -> None:
        """Test index name with worker label for label-based isolation."""
        # neo4j fixture already has worker_label set
        result = _get_embedding_index_name(neo4j)
        if neo4j.worker_label:
            assert result == f"{neo4j.worker_label}_posts"
        else:
            assert result == POST_EMBEDDING_INDEX_BASE_NAME

    def test_without_worker_label(
        self, docker_compose: DockerComposeInfo
    ) -> None:
        """Test index name without worker label (global index)."""
        from hintgrid.clients.neo4j import Neo4jClient

        # Create client without worker_label
        client = Neo4jClient(
            host=docker_compose.neo4j_host,
            port=docker_compose.neo4j_port,
            username=docker_compose.neo4j_user,
            password=docker_compose.neo4j_password,
            worker_label=None,
        )
        try:
            result = _get_embedding_index_name(client)
            assert result == POST_EMBEDDING_INDEX_BASE_NAME
        finally:
            client.close()
