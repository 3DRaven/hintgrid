"""Batch merge tests for posts and favourites.

All tests use worker-isolated labels via neo4j.label() for parallel execution.
"""
import pytest
from hintgrid.clients.neo4j import Neo4jClient
from hintgrid.utils.coercion import convert_batch_decimals
from .conftest import LARGE_BATCH_SIZE, MEDIUM_BATCH_SIZE, SEQUENTIAL_BATCH_POSTS, SEQUENTIAL_BATCH_USERS, SMALL_BATCH_SIZE

@pytest.mark.integration
@pytest.mark.smoke
def test_small_batch_merge_posts(neo4j: Neo4jClient) -> None:
    """
    Test merging small batch of posts (10 items).

    Uses worker-isolated labels for parallel test execution.
    """
    batch = [{'id': i, 'account_id': i % 3 + 1, 'text': f'Post {i}', 'language': 'en', 'created_at': '2024-01-01T00:00:00Z'} for i in range(1, SMALL_BATCH_SIZE + 1)]
    neo4j.execute_labeled('\n        UNWIND $batch AS row\n        MERGE (u:__user__ {id: row.account_id})\n        MERGE (p:__post__ {id: row.id})\n        ON CREATE SET p.text = row.text,\n                      p.language = row.language,\n                      p.createdAt = datetime(row.created_at)\n        MERGE (u)-[:WROTE]->(p);\n        ', label_map={'user': 'User', 'post': 'Post'}, params={'batch': convert_batch_decimals(batch)})
    count = neo4j.execute_and_fetch_labeled('MATCH (p:__post__) RETURN count(p) AS count;', label_map={'post': 'Post'})[0]['count']
    assert count == SMALL_BATCH_SIZE
    print(f'✅ Small batch ({SMALL_BATCH_SIZE} posts) merged successfully')

@pytest.mark.integration
def test_medium_batch_merge_posts(neo4j: Neo4jClient) -> None:
    """
    Test merging medium batch of posts (100 items).

    This batch size is typical for incremental data loading.
    """
    batch = [{'id': i, 'account_id': i % 10 + 1, 'text': f'Post {i} with some longer text to simulate real data', 'language': 'en', 'created_at': '2024-01-01T00:00:00Z'} for i in range(1, MEDIUM_BATCH_SIZE + 1)]
    neo4j.execute_labeled('\n        UNWIND $batch AS row\n        MERGE (u:__user__ {id: row.account_id})\n        MERGE (p:__post__ {id: row.id})\n        ON CREATE SET p.text = row.text,\n                      p.language = row.language,\n                      p.createdAt = datetime(row.created_at)\n        MERGE (u)-[:WROTE]->(p);\n        ', label_map={'user': 'User', 'post': 'Post'}, params={'batch': convert_batch_decimals(batch)})
    count = neo4j.execute_and_fetch_labeled('MATCH (p:__post__) RETURN count(p) AS count;', label_map={'post': 'Post'})[0]['count']
    assert count == MEDIUM_BATCH_SIZE
    print(f'✅ Medium batch ({MEDIUM_BATCH_SIZE} posts) merged successfully')

@pytest.mark.integration
def test_large_batch_merge_posts(neo4j: Neo4jClient) -> None:
    """
    Test merging large batch of posts (500 items).

    This tests the upper limit of batch operations.
    If this fails with segfault, reduce batch size in production code.
    """
    batch = [{'id': i, 'account_id': i % 50 + 1, 'text': f'Post {i} ' * 10, 'language': 'en', 'created_at': '2024-01-01T00:00:00Z'} for i in range(1, LARGE_BATCH_SIZE + 1)]
    neo4j.execute_labeled('\n        UNWIND $batch AS row\n        MERGE (u:__user__ {id: row.account_id})\n        MERGE (p:__post__ {id: row.id})\n        ON CREATE SET p.text = row.text,\n                      p.language = row.language,\n                      p.createdAt = datetime(row.created_at)\n        MERGE (u)-[:WROTE]->(p);\n        ', label_map={'user': 'User', 'post': 'Post'}, params={'batch': convert_batch_decimals(batch)})
    count = neo4j.execute_and_fetch_labeled('MATCH (p:__post__) RETURN count(p) AS count;', label_map={'post': 'Post'})[0]['count']
    assert count == LARGE_BATCH_SIZE
    print(f'✅ Large batch ({LARGE_BATCH_SIZE} posts) merged successfully')

@pytest.mark.integration
def test_small_batch_merge_favourites(neo4j: Neo4jClient) -> None:
    """
    Test merging small batch of favourites/likes (10 items).
    """
    for i in range(1, SMALL_BATCH_SIZE + 1):
        neo4j.execute_labeled('MERGE (u:__user__ {id: $id})', label_map={'user': 'User'}, params={'id': i})
        neo4j.execute_labeled('MERGE (p:__post__ {id: $id})', label_map={'post': 'Post'}, params={'id': i})
    batch = [{'account_id': i, 'status_id': i, 'created_at': '2024-01-01T00:00:00Z'} for i in range(1, SMALL_BATCH_SIZE + 1)]
    neo4j.execute_labeled('\n        UNWIND $batch AS row\n        MERGE (u:__user__ {id: row.account_id})\n        MERGE (p:__post__ {id: row.status_id})\n        MERGE (u)-[f:FAVORITED]->(p)\n        ON CREATE SET f.at = row.created_at;\n        ', label_map={'user': 'User', 'post': 'Post'}, params={'batch': convert_batch_decimals(batch)})
    count = neo4j.execute_and_fetch_labeled('MATCH (:__user__)-[f:FAVORITED]->(:__post__) RETURN count(f) AS count;', label_map={'user': 'User', 'post': 'Post'})[0]['count']
    assert count == SMALL_BATCH_SIZE
    print(f'✅ Small batch ({SMALL_BATCH_SIZE} favourites) merged successfully')

@pytest.mark.integration
def test_multiple_sequential_batches(neo4j: Neo4jClient) -> None:
    """
    Test multiple sequential batch operations (simulating real pipeline).

    This test demonstrates the complete data loading workflow:
    1. Load posts with authors
    2. Load favourites/likes
    3. Load follows

    If this test fails but individual tests pass, the issue is with
    connection reuse or state management between operations.
    """
    batch1 = [{'id': i, 'account_id': i % SEQUENTIAL_BATCH_USERS + 1, 'text': f'Post {i}', 'language': 'en', 'created_at': '2024-01-01T00:00:00Z'} for i in range(1, SEQUENTIAL_BATCH_POSTS + 1)]
    neo4j.execute_labeled('\n        UNWIND $batch AS row\n        MERGE (u:__user__ {id: row.account_id})\n        MERGE (p:__post__ {id: row.id})\n        ON CREATE SET p.text = row.text,\n                      p.language = row.language,\n                      p.createdAt = datetime(row.created_at)\n        MERGE (u)-[:WROTE]->(p);\n        ', label_map={'user': 'User', 'post': 'Post'}, params={'batch': convert_batch_decimals(batch1)})
    print('✅ Batch 1: 20 posts merged')
    batch2 = [{'account_id': i % SEQUENTIAL_BATCH_USERS + 1, 'status_id': i, 'created_at': '2024-01-01T00:00:00Z'} for i in range(1, SEQUENTIAL_BATCH_POSTS + 1)]
    neo4j.execute_labeled('\n        UNWIND $batch AS row\n        MERGE (u:__user__ {id: row.account_id})\n        MERGE (p:__post__ {id: row.status_id})\n        MERGE (u)-[f:FAVORITED]->(p)\n        ON CREATE SET f.at = row.created_at;\n        ', label_map={'user': 'User', 'post': 'Post'}, params={'batch': convert_batch_decimals(batch2)})
    print('✅ Batch 2: 20 favourites merged')
    batch3 = [{'account_id': i, 'target_account_id': i % SEQUENTIAL_BATCH_USERS + 1, 'created_at': '2024-01-01T00:00:00Z'} for i in range(1, SEQUENTIAL_BATCH_USERS + 1)]
    neo4j.execute_labeled('\n        UNWIND $batch AS row\n        MERGE (u1:__user__ {id: row.account_id})\n        MERGE (u2:__user__ {id: row.target_account_id})\n        MERGE (u1)-[:FOLLOWS]->(u2);\n        ', label_map={'user': 'User'}, params={'batch': convert_batch_decimals(batch3)})
    print('✅ Batch 3: 5 follows merged')
    posts = neo4j.execute_and_fetch_labeled('MATCH (p:__post__) RETURN count(p) AS count;', label_map={'post': 'Post'})[0]['count']
    users = neo4j.execute_and_fetch_labeled('MATCH (u:__user__) RETURN count(u) AS count;', label_map={'user': 'User'})[0]['count']
    likes = neo4j.execute_and_fetch_labeled('MATCH (:__user__)-[f:FAVORITED]->(:__post__) RETURN count(f) AS count;', label_map={'user': 'User', 'post': 'Post'})[0]['count']
    follows = neo4j.execute_and_fetch_labeled('MATCH (:__user__)-[f:FOLLOWS]->(:__user__) RETURN count(f) AS count;', label_map={'user': 'User'})[0]['count']
    assert posts == SEQUENTIAL_BATCH_POSTS
    assert users == SEQUENTIAL_BATCH_USERS
    assert likes == SEQUENTIAL_BATCH_POSTS
    assert follows == SEQUENTIAL_BATCH_USERS
    print(f'✅ All batches completed: {posts} posts, {users} users, {likes} likes, {follows} follows')