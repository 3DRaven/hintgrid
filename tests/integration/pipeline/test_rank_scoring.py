"""Integration tests for rank-based scoring in Redis feeds.

Covers:
- write_feed_to_redis uses rank-based scoring (not chronological)
- Most interesting post gets highest Redis score
- HintGrid entries outrank Mastodon native entries
- remove_hintgrid_recommendations preserves Mastodon entries
- remove_hintgrid_entries_from_key works on public timelines
"""

from __future__ import annotations


import pytest

from hintgrid.clients.redis import RedisClient
from hintgrid.config import HintGridSettings
from hintgrid.pipeline.feed import write_feed_to_redis
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import redis as redis_lib


# ---------------------------------------------------------------------------
# Tests: write_feed_to_redis rank-based scoring
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_write_feed_to_redis_rank_based_order(
    redis_client: redis_lib.Redis,
) -> None:
    """write_feed_to_redis assigns scores based on interest rank, not post_id.

    Post 100 (most interesting) should have higher Redis score than
    Post 300 (less interesting), even though 300 > 100 numerically.
    """
    recs: list[dict[str, float]] = [
        {"post_id": 100, "score": 0.95},  # rank 0 — highest interest
        {"post_id": 300, "score": 0.80},  # rank 1
        {"post_id": 200, "score": 0.65},  # rank 2 — lowest interest
    ]

    test_settings = HintGridSettings(feed_score_multiplier=10)
    redis_wrapper = RedisClient(redis_client)
    write_feed_to_redis(redis_wrapper, 77001, recs, test_settings)

    key = "feed:home:77001"
    entries = redis_client.zrevrange(key, 0, -1, withscores=True)

    assert len(entries) == 3

    # Entries should be ordered by interest, not by post_id
    ordered_ids = [int(member) for member, _ in entries]
    assert ordered_ids == [100, 300, 200], (
        f"Order should be by interest rank: [100, 300, 200], got {ordered_ids}"
    )


@pytest.mark.integration
def test_write_feed_to_redis_scores_above_max_post_id(
    redis_client: redis_lib.Redis,
) -> None:
    """All HintGrid Redis scores should be > max(post_id) * multiplier.

    This ensures HintGrid entries outrank any Mastodon native entries.
    """
    recs: list[dict[str, float]] = [
        {"post_id": 5000, "score": 0.90},
        {"post_id": 3000, "score": 0.70},
    ]

    multiplier = 10
    test_settings = HintGridSettings(feed_score_multiplier=multiplier)
    redis_wrapper = RedisClient(redis_client)
    write_feed_to_redis(redis_wrapper, 77002, recs, test_settings)

    key = "feed:home:77002"
    entries = redis_client.zrange(key, 0, -1, withscores=True)

    # base = max(5000, 3000) * 10 = 50000
    base = 5000 * multiplier
    for member, redis_score in entries:
        # member can be bytes if decode_responses=False, convert to str for f-string
        # In tests, member is always bytes when decode_responses=False
        member_str = member.decode() if isinstance(member, bytes) else str(member)
        assert redis_score > base, (
            f"Post {member_str} score {redis_score} should be > base {base}"
        )


@pytest.mark.integration
def test_write_feed_to_redis_empty_recs_no_op(
    redis_client: redis_lib.Redis,
) -> None:
    """write_feed_to_redis does nothing for empty recommendations."""
    test_settings = HintGridSettings(feed_score_multiplier=10)
    redis_wrapper = RedisClient(redis_client)
    write_feed_to_redis(redis_wrapper, 77003, [], test_settings)

    key = "feed:home:77003"
    assert redis_client.zcard(key) == 0


@pytest.mark.integration
def test_write_feed_to_redis_score_formula(
    redis_client: redis_lib.Redis,
) -> None:
    """Verify exact scoring formula: redis_score = base + (N - rank).

    base = max(post_id) * multiplier
    N = total number of recommendations
    rank 0 = most interesting → highest score
    """
    recs: list[dict[str, float]] = [
        {"post_id": 1000, "score": 0.99},  # rank 0
        {"post_id": 2000, "score": 0.88},  # rank 1
        {"post_id": 3000, "score": 0.77},  # rank 2
    ]

    multiplier = 5
    test_settings = HintGridSettings(feed_score_multiplier=multiplier)
    redis_wrapper = RedisClient(redis_client)
    write_feed_to_redis(redis_wrapper, 77004, recs, test_settings)

    key = "feed:home:77004"
    entries = redis_client.zrange(key, 0, -1, withscores=True)
    score_map = {int(member): score for member, score in entries}

    # base = max(3000) * 5 = 15000, N = 3
    base = 3000 * multiplier

    assert score_map[1000] == base + 3  # rank 0: base + (3-0) = 15003
    assert score_map[2000] == base + 2  # rank 1: base + (3-1) = 15002
    assert score_map[3000] == base + 1  # rank 2: base + (3-2) = 15001


# ---------------------------------------------------------------------------
# Tests: remove_hintgrid_recommendations
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_remove_hintgrid_recommendations_preserves_mastodon_entries(
    redis_client: redis_lib.Redis,
) -> None:
    """remove_hintgrid_recommendations removes HintGrid entries but keeps Mastodon's.

    Mastodon entries: score == post_id (member value equals score)
    HintGrid entries: score != post_id (rank-based, always > post_id)
    """
    key = "feed:home:77010"

    # Simulate Mastodon native entries (score = post_id)
    redis_client.zadd(key, {"4000": 4000, "4001": 4001, "4002": 4002})

    # Add HintGrid recommendations (rank-based scores, >> post_id)
    recs: list[dict[str, float]] = [
        {"post_id": 5000, "score": 0.90},  # rank 0
        {"post_id": 5001, "score": 0.80},  # rank 1
    ]

    test_settings = HintGridSettings(feed_score_multiplier=10)
    redis_wrapper = RedisClient(redis_client)
    write_feed_to_redis(redis_wrapper, 77010, recs, test_settings)

    # Verify both types exist
    total_before = redis_client.zcard(key)
    assert total_before == 5  # 3 Mastodon + 2 HintGrid

    # Remove HintGrid entries
    removed = redis_wrapper.remove_hintgrid_recommendations(77010, score_multiplier=10)
    assert removed == 2

    # Only Mastodon entries should remain
    remaining = redis_client.zrange(key, 0, -1, withscores=True)
    assert len(remaining) == 3

    for member, score in remaining:
        member_id = int(member)
        # Mastodon: score == post_id
        assert abs(score - member_id) < 1, (
            f"Remaining entry {member_id} should be Mastodon (score=post_id), "
            f"got score={score}"
        )


@pytest.mark.integration
def test_remove_hintgrid_recommendations_with_low_multiplier(
    redis_client: redis_lib.Redis,
) -> None:
    """remove_hintgrid_recommendations returns 0 when multiplier <= 1."""
    redis_wrapper = RedisClient(redis_client)
    result = redis_wrapper.remove_hintgrid_recommendations(77020, score_multiplier=1)
    assert result == 0


@pytest.mark.integration
def test_remove_hintgrid_recommendations_empty_feed(
    redis_client: redis_lib.Redis,
) -> None:
    """remove_hintgrid_recommendations returns 0 for non-existent feed."""
    redis_wrapper = RedisClient(redis_client)
    result = redis_wrapper.remove_hintgrid_recommendations(99999, score_multiplier=10)
    assert result == 0


# ---------------------------------------------------------------------------
# Tests: remove_hintgrid_entries_from_key
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_remove_hintgrid_entries_from_key_cleans_public_timeline(
    redis_client: redis_lib.Redis,
) -> None:
    """remove_hintgrid_entries_from_key removes HintGrid entries from public timelines.

    Same logic as remove_hintgrid_recommendations but with an arbitrary key.
    """
    key = "timeline:public:test_clean"

    # Simulate Mastodon entries
    redis_client.zadd(key, {"6000": 6000, "6001": 6001})

    # Add HintGrid entries with rank-based scores
    # base = 7000 * 10 = 70000, total = 1
    redis_client.zadd(key, {"7000": 70001})

    redis_wrapper = RedisClient(redis_client)

    # Remove HintGrid entries
    removed = redis_wrapper.remove_hintgrid_entries_from_key(key, score_multiplier=10)
    assert removed == 1

    # Only Mastodon entries remain
    remaining = redis_client.zrange(key, 0, -1, withscores=True)
    assert len(remaining) == 2

    remaining_ids = {int(member) for member, _ in remaining}
    assert remaining_ids == {6000, 6001}


@pytest.mark.integration
def test_remove_hintgrid_entries_from_key_empty_key(
    redis_client: redis_lib.Redis,
) -> None:
    """remove_hintgrid_entries_from_key returns 0 for non-existent key."""
    redis_wrapper = RedisClient(redis_client)
    result = redis_wrapper.remove_hintgrid_entries_from_key(
        "nonexistent:key", score_multiplier=10
    )
    assert result == 0


@pytest.mark.integration
def test_remove_hintgrid_entries_from_key_low_multiplier(
    redis_client: redis_lib.Redis,
) -> None:
    """remove_hintgrid_entries_from_key returns 0 when multiplier <= 1."""
    redis_wrapper = RedisClient(redis_client)
    result = redis_wrapper.remove_hintgrid_entries_from_key(
        "timeline:public", score_multiplier=1
    )
    assert result == 0
