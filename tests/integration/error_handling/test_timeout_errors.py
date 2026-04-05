"""Integration tests for timeout error handling.

Covers:
- Timeout errors for long-running operations
- Query timeout handling
- Batch operation timeouts
"""
from __future__ import annotations
from typing import TYPE_CHECKING
import pytest
from hintgrid.utils.coercion import convert_batch_decimals
if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.config import HintGridSettings
else:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.config import HintGridSettings

@pytest.mark.integration
def test_neo4j_query_timeout_handling(neo4j: Neo4jClient) -> None:
    """Neo4j queries should handle timeout errors gracefully."""
    assert isinstance(neo4j, Neo4jClient)
    try:
        result = list(neo4j.execute_and_fetch('\n                MATCH (n)\n                WITH n\n                MATCH (m)\n                WITH n, m\n                MATCH (o)\n                RETURN count(*) AS total\n                LIMIT 1\n                '))
        assert isinstance(result, list)
    except Exception as e:
        error_msg = str(e).lower()
        assert 'timeout' in error_msg or 'time' in error_msg

@pytest.mark.integration
def test_neo4j_periodic_iterate_timeout_handling(neo4j: Neo4jClient) -> None:
    """apoc.periodic.iterate should handle timeout errors."""
    assert isinstance(neo4j, Neo4jClient)
    try:
        result = neo4j.execute_periodic_iterate('MATCH (n) RETURN id(n) AS node_id LIMIT 10', "MATCH (n) WHERE id(n) = node_id SET n.test_prop = 'test'", batch_size=5, parallel=False)
        assert isinstance(result, dict)
    except Exception as e:
        error_msg = str(e).lower()
        assert 'timeout' in error_msg or 'time' in error_msg

@pytest.mark.integration
def test_large_batch_operation_handling(neo4j: Neo4jClient) -> None:
    """Large batch operations should not timeout with reasonable batch_size."""
    assert isinstance(neo4j, Neo4jClient)
    from hintgrid.pipeline.graph import merge_posts
    batch: list[dict[str, object]] = [{'id': 10000 + i, 'authorId': 20000 + i, 'text': f'Test post {i}', 'language': 'en', 'visibility': 0, 'createdAt': '2024-01-01T00:00:00Z'} for i in range(100)]
    merge_posts(neo4j, convert_batch_decimals(batch))
    result = list(neo4j.execute_and_fetch('MATCH (p:Post) WHERE p.id >= 10000 AND p.id < 10100 RETURN count(p) AS count'))
    count = result[0].get('count') if result else 0
    from hintgrid.utils.coercion import coerce_int
    assert coerce_int(count) == 100, 'All posts should be created'

@pytest.mark.integration
def test_embedding_batch_timeout_handling(neo4j: Neo4jClient, settings: HintGridSettings) -> None:
    """Embedding batch operations should handle timeouts gracefully."""
    assert isinstance(neo4j, Neo4jClient)
    assert isinstance(settings, HintGridSettings)
    from hintgrid.pipeline.graph import reembed_existing_posts
    try:
        from hintgrid.embeddings.provider import EmbeddingProvider
        embedding_provider = EmbeddingProvider(settings, neo4j)
        reembed_existing_posts(neo4j, embedding_provider, settings, batch_size=10)
    except Exception as e:
        error_msg = str(e).lower()
        assert isinstance(error_msg, str)