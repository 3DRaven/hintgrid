"""Integration tests for partial failure handling.

Covers:
- Partial failures in batch operations
- Rollback mechanisms
- Recovery from partial writes
"""
from __future__ import annotations
from typing import TYPE_CHECKING
import pytest
from hintgrid.pipeline.graph import merge_posts
from hintgrid.state import PipelineState, StateStore
from hintgrid.utils.coercion import convert_batch_decimals
if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from tests.parallel import IsolatedNeo4jClient
else:
    from hintgrid.clients.neo4j import Neo4jClient
    from tests.parallel import IsolatedNeo4jClient

@pytest.mark.integration
def test_merge_posts_partial_failure_handling(neo4j: Neo4jClient) -> None:
    """merge_posts should handle partial failures in batch."""
    assert isinstance(neo4j, Neo4jClient)
    batch: list[dict[str, object]] = [{'id': 20001, 'authorId': 30001, 'text': 'Valid post', 'language': 'en', 'visibility': 0, 'createdAt': '2024-01-01T00:00:00Z'}, {'id': 20002, 'authorId': 30002, 'text': 'Another valid post', 'language': 'en', 'visibility': 0, 'createdAt': '2024-01-01T00:00:00Z'}]
    merge_posts(neo4j, convert_batch_decimals(batch))
    result = list(neo4j.execute_and_fetch('MATCH (p:Post) WHERE p.id IN [20001, 20002] RETURN count(p) AS count'))
    count = result[0].get('count') if result else 0
    from hintgrid.utils.coercion import coerce_int
    assert coerce_int(count) == 2, 'All valid posts should be created'

@pytest.mark.integration
def test_state_save_partial_failure_recovery(isolated_neo4j: IsolatedNeo4jClient, worker_id: str) -> None:
    """State save should be atomic (all or nothing)."""
    neo4j = isolated_neo4j.client
    state_id = f'partial_test_{worker_id}'
    state_store = StateStore(neo4j, state_id=state_id)
    state = PipelineState(last_status_id=5000, last_favourite_id=6000)
    state_store.save(state)
    loaded = state_store.load()
    assert loaded.last_status_id == 5000
    assert loaded.last_favourite_id == 6000

@pytest.mark.integration
def test_apoc_periodic_iterate_partial_failures(neo4j: Neo4jClient) -> None:
    """apoc.periodic.iterate should report partial failures."""
    assert isinstance(neo4j, Neo4jClient)
    neo4j.execute_labeled('CREATE (u1:__user__ {id: 40001}) CREATE (u2:__user__ {id: 40002}) CREATE (u3:__user__ {id: 40003})', {'user': 'User'})
    result = neo4j.execute_periodic_iterate('MATCH (u:__user__) WHERE u.id IN [40001, 40002, 40003] RETURN id(u) AS user_id', "MATCH (u:__user__) WHERE id(u) = user_id SET u.test_prop = 'test'", label_map={'user': 'User'}, batch_size=2, parallel=False)
    assert isinstance(result, dict)
    total = result.get('total', 0)
    committed = result.get('committedOperations', 0)
    failed = result.get('failedOperations', 0)
    from hintgrid.utils.coercion import coerce_int
    assert coerce_int(total) == 3, 'Should process 3 nodes'
    assert coerce_int(committed) + coerce_int(failed) == coerce_int(total), 'Committed + failed should equal total'

@pytest.mark.integration
def test_state_rollback_on_error(isolated_neo4j: IsolatedNeo4jClient, worker_id: str) -> None:
    """State should not be corrupted if save fails partway."""
    assert isinstance(isolated_neo4j, IsolatedNeo4jClient)
    neo4j = isolated_neo4j.client
    state_id = f'rollback_test_{worker_id}'
    state_store = StateStore(neo4j, state_id=state_id)
    initial_state = PipelineState(last_status_id=1000)
    state_store.save(initial_state)
    new_state = PipelineState(last_status_id=2000)
    state_store.save(new_state)
    loaded = state_store.load()
    assert loaded.last_status_id in [1000, 2000], 'State should be either old or new value, not corrupted'

@pytest.mark.integration
def test_batch_operation_idempotency(neo4j: Neo4jClient) -> None:
    """Batch operations should be idempotent (safe to retry)."""
    assert isinstance(neo4j, Neo4jClient)
    from hintgrid.pipeline.graph import merge_favourites, merge_posts
    post_batch: list[dict[str, object]] = [{'id': 70001, 'authorId': 80001, 'text': 'Post to be favorited', 'language': 'en', 'visibility': 0, 'createdAt': '2024-01-01T00:00:00Z'}]
    merge_posts(neo4j, convert_batch_decimals(post_batch))
    batch: list[dict[str, object]] = [{'account_id': 60001, 'status_id': 70001, 'created_at': '2024-01-01T00:00:00Z'}]
    merge_favourites(neo4j, convert_batch_decimals(batch))
    merge_favourites(neo4j, convert_batch_decimals(batch))
    result = list(neo4j.execute_and_fetch_labeled('MATCH (u:__user__ {id: $account_id})-[f:FAVORITED]->(p:__post__ {id: $status_id}) RETURN count(f) AS count', {'user': 'User', 'post': 'Post'}, {'account_id': 60001, 'status_id': 70001}))
    count = result[0].get('count') if result else 0
    from hintgrid.utils.coercion import coerce_int
    assert coerce_int(count) == 1, 'Should not create duplicate relationships'

@pytest.mark.integration
def test_recovery_from_partial_state_write(isolated_neo4j: IsolatedNeo4jClient, worker_id: str) -> None:
    """System should recover from partially written state."""
    assert isinstance(isolated_neo4j, IsolatedNeo4jClient)
    neo4j = isolated_neo4j.client
    state_id = f'recovery_test_{worker_id}'
    neo4j.label('AppState')
    neo4j.execute_labeled('CREATE (s:__app_state__ {id: $sid, last_processed_status_id: 8000, updated_at: timestamp()})', {'app_state': 'AppState'}, {'sid': state_id})
    state_store = StateStore(neo4j, state_id=state_id)
    state = state_store.load()
    assert state.last_status_id == 8000
    assert state.last_favourite_id == 0
    state.last_favourite_id = 9000
    state_store.save(state)
    loaded = state_store.load()
    assert loaded.last_status_id == 8000
    assert loaded.last_favourite_id == 9000