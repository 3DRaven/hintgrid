"""Integration tests for graph merge functions."""
from __future__ import annotations
import pytest
from hintgrid.pipeline.graph import merge_blocks, merge_bookmarks, merge_favourites, merge_mutes, merge_posts, merge_reblogs, merge_replies, merge_status_stats, update_user_activity
from hintgrid.state import StateStore
from hintgrid.utils.coercion import coerce_float, coerce_int, convert_batch_decimals
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient

@pytest.mark.integration
def test_merge_posts_empty_batch(neo4j: Neo4jClient) -> None:
    """Merge posts with empty batch should be a no-op."""
    merge_posts(neo4j, convert_batch_decimals([]))

@pytest.mark.integration
def test_merge_favourites_empty_batch(neo4j: Neo4jClient) -> None:
    """Merge favourites with empty batch should be a no-op."""
    merge_favourites(neo4j, convert_batch_decimals([]))

@pytest.mark.integration
def test_merge_blocks_empty_batch(neo4j: Neo4jClient) -> None:
    """Merge blocks with empty batch should be a no-op."""
    merge_blocks(neo4j, convert_batch_decimals([]))

@pytest.mark.integration
def test_merge_mutes_empty_batch(neo4j: Neo4jClient) -> None:
    """Merge mutes with empty batch should be a no-op."""
    merge_mutes(neo4j, convert_batch_decimals([]))

@pytest.mark.integration
def test_merge_reblogs_empty_batch(neo4j: Neo4jClient) -> None:
    """Merge reblogs with empty batch should be a no-op."""
    merge_reblogs(neo4j, convert_batch_decimals([]))

@pytest.mark.integration
def test_merge_replies_empty_batch(neo4j: Neo4jClient) -> None:
    """Merge replies with empty batch should be a no-op."""
    merge_replies(neo4j, convert_batch_decimals([]))

@pytest.mark.integration
def test_merge_posts_with_data(neo4j: Neo4jClient) -> None:
    """Merge posts creates Post nodes and WROTE relationships."""
    neo4j.label('Post')
    neo4j.label('User')
    batch: list[dict[str, object]] = [{'id': 50001, 'authorId': 60001, 'text': 'Test post for merge', 'language': 'en', 'embedding': [0.1, 0.2, 0.3], 'createdAt': '2024-01-01T00:00:00Z'}]
    merge_posts(neo4j, convert_batch_decimals(batch))
    result = list(neo4j.execute_and_fetch_labeled('\n            MATCH (u:__user__ {id: 60001})-[w:WROTE]->(p:__post__ {id: 50001})\n            RETURN p.text AS text, p.language AS lang, p.pagerank AS pr\n            ', {'user': 'User', 'post': 'Post'}))
    assert len(result) == 1
    assert result[0].get('text') == 'Test post for merge'
    assert result[0].get('lang') == 'en'
    assert coerce_float(result[0].get('pr')) == 0.0

@pytest.mark.integration
def test_merge_favourites_with_data(neo4j: Neo4jClient) -> None:
    """Merge favourites creates FAVORITED relationships."""
    neo4j.label('User')
    neo4j.label('Post')
    neo4j.execute_labeled('CREATE (u:__user__ {id: 60010})', {'user': 'User'})
    neo4j.execute_labeled('CREATE (p:__post__ {id: 50010})', {'post': 'Post'})
    batch: list[dict[str, object]] = [{'account_id': 60010, 'status_id': 50010, 'created_at': '2024-01-01T00:00:00Z'}]
    merge_favourites(neo4j, convert_batch_decimals(batch))
    result = list(neo4j.execute_and_fetch_labeled('\n            MATCH (u:__user__ {id: 60010})-[f:FAVORITED]->(p:__post__ {id: 50010})\n            RETURN count(f) AS cnt\n            ', {'user': 'User', 'post': 'Post'}))
    assert coerce_int(result[0].get('cnt')) == 1

@pytest.mark.integration
def test_merge_blocks_with_data(neo4j: Neo4jClient) -> None:
    """Merge blocks creates HATES_USER relationships."""
    neo4j.label('User')
    batch: list[dict[str, object]] = [{'account_id': 60030, 'target_account_id': 60031}]
    merge_blocks(neo4j, convert_batch_decimals(batch))
    result = list(neo4j.execute_and_fetch_labeled('\n            MATCH (u1:__user__ {id: 60030})-[h:HATES_USER]->(u2:__user__ {id: 60031})\n            RETURN count(*) AS cnt\n            ', {'user': 'User'}))
    assert coerce_int(result[0].get('cnt')) == 1

@pytest.mark.integration
def test_merge_mutes_with_data(neo4j: Neo4jClient) -> None:
    """Merge mutes creates HATES_USER relationships."""
    neo4j.label('User')
    batch: list[dict[str, object]] = [{'account_id': 60032, 'target_account_id': 60033}]
    merge_mutes(neo4j, convert_batch_decimals(batch))
    result = list(neo4j.execute_and_fetch_labeled('\n            MATCH (u1:__user__ {id: 60032})-[h:HATES_USER]->(u2:__user__ {id: 60033})\n            RETURN count(*) AS cnt\n            ', {'user': 'User'}))
    assert coerce_int(result[0].get('cnt')) == 1

@pytest.mark.integration
def test_merge_reblogs_with_data(neo4j: Neo4jClient) -> None:
    """Merge reblogs creates REBLOGGED relationships."""
    neo4j.label('User')
    neo4j.label('Post')
    neo4j.execute_labeled('CREATE (p:__post__ {id: 50040})', {'post': 'Post'})
    batch: list[dict[str, object]] = [{'account_id': 60040, 'reblog_of_id': 50040, 'created_at': '2024-01-01T00:00:00Z'}]
    merge_reblogs(neo4j, convert_batch_decimals(batch))
    result = list(neo4j.execute_and_fetch_labeled('MATCH (u:__user__ {id: 60040})-[r:REBLOGGED]->(p:__post__ {id: 50040}) RETURN count(*) AS cnt', {'user': 'User', 'post': 'Post'}))
    assert coerce_int(result[0].get('cnt')) == 1

@pytest.mark.integration
def test_merge_replies_with_data(neo4j: Neo4jClient) -> None:
    """Merge replies creates REPLIED relationships."""
    neo4j.label('User')
    neo4j.label('Post')
    neo4j.execute_labeled('CREATE (p:__post__ {id: 50050})', {'post': 'Post'})
    batch: list[dict[str, object]] = [{'account_id': 60050, 'in_reply_to_id': 50050, 'created_at': '2024-01-01T00:00:00Z'}]
    merge_replies(neo4j, convert_batch_decimals(batch))
    result = list(neo4j.execute_and_fetch_labeled('MATCH (u:__user__ {id: 60050})-[r:REPLIED]->(p:__post__ {id: 50050}) RETURN count(*) AS cnt', {'user': 'User', 'post': 'Post'}))
    assert coerce_int(result[0].get('cnt')) == 1

@pytest.mark.integration
def test_merge_posts_atomic_state_update(neo4j: Neo4jClient) -> None:
    """Test atomic state update when merge_posts is called with state_id."""
    state_store = StateStore(neo4j, state_id='test_atomic')
    initial_state = state_store.load()
    assert initial_state.last_status_id == 0
    batch: list[dict[str, object]] = [{'id': 70001, 'authorId': 80001, 'text': 'Post 1 for atomic test', 'language': 'en', 'embedding': [0.1, 0.2, 0.3], 'createdAt': '2024-01-01T00:00:00Z'}, {'id': 70002, 'authorId': 80002, 'text': 'Post 2 for atomic test', 'language': 'en', 'embedding': [0.2, 0.3, 0.4], 'createdAt': '2024-01-01T00:00:00Z'}]
    result = merge_posts(neo4j, convert_batch_decimals(batch), state_id='test_atomic', batch_max_id=70002)
    assert result == 70002, 'Should return max ID from batch'
    updated_state = state_store.load()
    assert updated_state.last_status_id == 70002, 'State should be updated to max ID'
    neo4j.label('Post')
    post_result = list(neo4j.execute_and_fetch_labeled('MATCH (p:__post__) WHERE p.id IN [70001, 70002] RETURN count(p) AS cnt', {'post': 'Post'}))
    assert coerce_int(post_result[0].get('cnt')) == 2

@pytest.mark.integration
def test_merge_posts_state_update_empty_batch(neo4j: Neo4jClient) -> None:
    """Test that merge_posts returns None for empty batch even with state_id."""
    result = merge_posts(neo4j, convert_batch_decimals([]), state_id='test_empty')
    assert result is None, 'Empty batch should return None'

@pytest.mark.integration
def test_merge_posts_backward_compatibility(neo4j: Neo4jClient) -> None:
    """Test that merge_posts without state_id works as before (backward compatibility)."""
    neo4j.label('Post')
    neo4j.label('User')
    batch: list[dict[str, object]] = [{'id': 70010, 'authorId': 80010, 'text': 'Post for backward compat test', 'language': 'en', 'embedding': [0.1, 0.2, 0.3], 'createdAt': '2024-01-01T00:00:00Z'}]
    result = merge_posts(neo4j, convert_batch_decimals(batch))
    assert result is None, 'Should return None when state_id is not provided'
    post_result = list(neo4j.execute_and_fetch_labeled('MATCH (u:__user__ {id: 80010})-[w:WROTE]->(p:__post__ {id: 70010}) RETURN p.text AS text', {'user': 'User', 'post': 'Post'}))
    assert len(post_result) == 1
    assert post_result[0].get('text') == 'Post for backward compat test'

@pytest.mark.integration
def test_merge_favourites_atomic_state_update(neo4j: Neo4jClient) -> None:
    """Test atomic state update when merge_favourites is called with state_id."""
    state_store = StateStore(neo4j, state_id='test_favourites_atomic')
    initial_state = state_store.load()
    assert initial_state.last_favourite_id == 0
    neo4j.execute_labeled('CREATE (p:__post__ {id: $post_id})', {'post': 'Post'}, {'post_id': 50020})
    batch: list[dict[str, object]] = [{'account_id': 60020, 'status_id': 50020, 'created_at': '2024-01-01T00:00:00Z'}, {'account_id': 60021, 'status_id': 50020, 'created_at': '2024-01-01T00:00:00Z'}]
    result = merge_favourites(neo4j, convert_batch_decimals(batch), state_id='test_favourites_atomic', batch_max_id=20002)
    assert result == 20002, 'Should return max ID from batch'
    updated_state = state_store.load()
    assert updated_state.last_favourite_id == 20002, 'State should be updated to max ID'
    fav_result = list(neo4j.execute_and_fetch_labeled('MATCH (u:__user__)-[f:FAVORITED]->(p:__post__ {id: $post_id}) RETURN count(f) AS cnt', {'user': 'User', 'post': 'Post'}, {'post_id': 50020}))
    assert coerce_int(fav_result[0].get('cnt')) == 2

@pytest.mark.integration
def test_merge_bookmarks_atomic_state_update(neo4j: Neo4jClient) -> None:
    """Test atomic state update when merge_bookmarks is called with state_id."""
    state_store = StateStore(neo4j, state_id='test_bookmarks_atomic')
    initial_state = state_store.load()
    assert initial_state.last_bookmark_id == 0
    neo4j.execute_labeled('CREATE (p:__post__ {id: $post_id})', {'post': 'Post'}, {'post_id': 50030})
    batch: list[dict[str, object]] = [{'account_id': 60030, 'status_id': 50030, 'created_at': '2024-01-01T00:00:00Z'}]
    result = merge_bookmarks(neo4j, convert_batch_decimals(batch), state_id='test_bookmarks_atomic', batch_max_id=30001)
    assert result == 30001, 'Should return max ID from batch'
    updated_state = state_store.load()
    assert updated_state.last_bookmark_id == 30001, 'State should be updated to max ID'
    bookmark_result = list(neo4j.execute_and_fetch_labeled('MATCH (u:__user__ {id: $user_id})-[b:BOOKMARKED]->(p:__post__ {id: $post_id}) RETURN count(b) AS cnt', {'user': 'User', 'post': 'Post'}, {'user_id': 60030, 'post_id': 50030}))
    assert coerce_int(bookmark_result[0].get('cnt')) == 1

@pytest.mark.integration
def test_merge_status_stats_atomic_state_update(neo4j: Neo4jClient) -> None:
    """Test atomic state update when merge_status_stats is called with state_id."""
    state_store = StateStore(neo4j, state_id='test_status_stats_atomic')
    initial_state = state_store.load()
    assert initial_state.last_status_stats_id == 0
    neo4j.execute_labeled('CREATE (p:__post__ {id: $post_id})', {'post': 'Post'}, {'post_id': 50040})
    batch: list[dict[str, object]] = [{'id': 50040, 'total_favourites': 10, 'total_reblogs': 5, 'total_replies': 3}]
    result = merge_status_stats(neo4j, convert_batch_decimals(batch), state_id='test_status_stats_atomic', batch_max_id=40001)
    assert result == 40001, 'Should return max ID from batch'
    updated_state = state_store.load()
    assert updated_state.last_status_stats_id == 40001, 'State should be updated to max ID'
    stats_result = list(neo4j.execute_and_fetch_labeled('MATCH (p:__post__ {id: $post_id}) RETURN p.totalFavourites AS favs, p.totalReblogs AS reblogs', {'post': 'Post'}, {'post_id': 50040}))
    assert coerce_int(stats_result[0].get('favs')) == 10
    assert coerce_int(stats_result[0].get('reblogs')) == 5

@pytest.mark.integration
def test_merge_blocks_atomic_state_update(neo4j: Neo4jClient) -> None:
    """Test atomic state update when merge_blocks is called with state_id."""
    state_store = StateStore(neo4j, state_id='test_blocks_atomic')
    initial_state = state_store.load()
    assert initial_state.last_block_id == 0
    batch: list[dict[str, object]] = [{'account_id': 60050, 'target_account_id': 60051}]
    result = merge_blocks(neo4j, convert_batch_decimals(batch), state_id='test_blocks_atomic', batch_max_id=50001)
    assert result == 50001, 'Should return max ID from batch'
    updated_state = state_store.load()
    assert updated_state.last_block_id == 50001, 'State should be updated to max ID'
    hates_result = list(neo4j.execute_and_fetch_labeled('MATCH (u1:__user__ {id: $user1_id})-[h:HATES_USER]->(u2:__user__ {id: $user2_id}) RETURN count(h) AS cnt', {'user': 'User'}, {'user1_id': 60050, 'user2_id': 60051}))
    assert coerce_int(hates_result[0].get('cnt')) == 1

@pytest.mark.integration
def test_merge_mutes_atomic_state_update(neo4j: Neo4jClient) -> None:
    """Test atomic state update when merge_mutes is called with state_id."""
    state_store = StateStore(neo4j, state_id='test_mutes_atomic')
    initial_state = state_store.load()
    assert initial_state.last_mute_id == 0
    batch: list[dict[str, object]] = [{'account_id': 60052, 'target_account_id': 60053}]
    result = merge_mutes(neo4j, convert_batch_decimals(batch), state_id='test_mutes_atomic', batch_max_id=50002)
    assert result == 50002, 'Should return max ID from batch'
    updated_state = state_store.load()
    assert updated_state.last_mute_id == 50002, 'State should be updated to max ID'
    hates_result = list(neo4j.execute_and_fetch_labeled('MATCH (u1:__user__ {id: $user1_id})-[h:HATES_USER]->(u2:__user__ {id: $user2_id}) RETURN count(h) AS cnt', {'user': 'User'}, {'user1_id': 60052, 'user2_id': 60053}))
    assert coerce_int(hates_result[0].get('cnt')) == 1

@pytest.mark.integration
def test_update_user_activity_atomic_state_update(neo4j: Neo4jClient) -> None:
    """Test atomic state update when update_user_activity is called with state_id."""
    state_store = StateStore(neo4j, state_id='test_activity_atomic')
    initial_state = state_store.load()
    assert initial_state.last_activity_account_id == 0
    neo4j.execute_labeled('CREATE (u:__user__ {id: $user_id})', {'user': 'User'}, {'user_id': 60060})
    batch: list[dict[str, object]] = [
        {
            'account_id': 60060,
            'last_active': '2024-01-01T00:00:00Z',
            'is_local': True,
            'ui_language': 'en',
            'languages': ['en', 'ru'],
        }
    ]
    result = update_user_activity(neo4j, convert_batch_decimals(batch), state_id='test_activity_atomic', batch_max_id=60060)
    assert result == 60060, 'Should return max account_id from batch'
    updated_state = state_store.load()
    assert updated_state.last_activity_account_id == 60060, 'State should be updated to max account_id'
    user_result = list(
        neo4j.execute_and_fetch_labeled(
            'MATCH (u:__user__ {id: $user_id}) RETURN u.isLocal AS is_local, '
            'u.languages AS langs, u.uiLanguage AS ui_lang',
            {'user': 'User'},
            {'user_id': 60060},
        )
    )
    assert user_result[0].get('is_local') is True
    assert user_result[0].get('langs') == ['en', 'ru']
    assert user_result[0].get('ui_lang') == 'en'