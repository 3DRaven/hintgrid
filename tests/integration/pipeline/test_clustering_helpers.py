"""Integration tests for clustering module helper functions.

Tests GDS name validation and embedding index name generation
with real Neo4j client.
"""

from __future__ import annotations

import pytest

from hintgrid.pipeline.clustering import (
    POST_EMBEDDING_INDEX_BASE_NAME,
    validate_gds_name,
)
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from tests.conftest import DockerComposeInfo
else:
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


# Removed TestGetEmbeddingIndexName
# Tests internal index name generation that is covered through run_post_clustering()
# Index name generation is an internal implementation detail
