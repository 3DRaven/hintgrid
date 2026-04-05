"""Integration tests for stress scenarios.

Covers:
- Large data volumes
- Concurrent operations
"""
from __future__ import annotations
import pytest
from hintgrid.pipeline.graph import merge_posts
from hintgrid.utils.coercion import coerce_int, convert_batch_decimals
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient

@pytest.mark.integration
@pytest.mark.slow
def test_merge_posts_large_batch(neo4j: Neo4jClient) -> None:
    """Test merge_posts with large batch."""
    batch: list[dict[str, object]] = [{'id': 80000 + i, 'authorId': 90000 + i, 'text': f'Post {i}', 'language': 'en', 'createdAt': '2024-01-01T00:00:00Z'} for i in range(1000)]
    merge_posts(neo4j, convert_batch_decimals(batch))
    result = list(neo4j.execute_and_fetch('MATCH (p:Post) WHERE p.id >= 80000 AND p.id < 81000 RETURN count(p) AS count'))
    count = coerce_int(result[0].get('count')) if result else 0
    assert count == 1000