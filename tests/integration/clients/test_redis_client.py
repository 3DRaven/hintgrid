"""Integration tests for Redis client methods."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.clients.redis import RedisClient
from hintgrid.exceptions import RedisConnectionError

if TYPE_CHECKING:
    import redis
    from hintgrid.config import HintGridSettings


@pytest.mark.integration
def test_redis_client_zrevrange_with_scores(
    redis_client: redis.Redis,
    settings: HintGridSettings,
) -> None:
    """Test RedisClient.zrevrange_with_scores returns elements in reverse order."""
    from hintgrid.clients.redis import RedisClient

    client = RedisClient(
        redis_client,
        score_tolerance=settings.redis_score_tolerance,
        host=settings.redis_host,
        port=settings.redis_port,
    )

    # Add test data
    key = "test:zrevrange_scores"
    redis_client.delete(key)
    redis_client.zadd(key, {"a": 1.0, "b": 2.0, "c": 3.0})

    # Get range with scores
    result = client.zrevrange_with_scores(key, 0, -1)

    # Verify reverse order (highest score first)
    # Note: Redis may return str or bytes depending on configuration
    assert len(result) == 3
    first_key = result[0][0] if isinstance(result[0][0], str) else result[0][0].decode()
    last_key = result[2][0] if isinstance(result[2][0], str) else result[2][0].decode()
    assert first_key == "c"  # Highest score
    assert result[0][1] == 3.0
    assert last_key == "a"  # Lowest score
    assert result[2][1] == 1.0


@pytest.mark.integration
def test_redis_client_zrevrange_without_scores(
    redis_client: redis.Redis,
    settings: HintGridSettings,
) -> None:
    """Test RedisClient.zrevrange returns elements without scores."""
    from hintgrid.clients.redis import RedisClient

    client = RedisClient(
        redis_client,
        score_tolerance=settings.redis_score_tolerance,
        host=settings.redis_host,
        port=settings.redis_port,
    )

    key = "test:zrevrange_no_scores"
    redis_client.delete(key)
    redis_client.zadd(key, {"x": 10.0, "y": 20.0, "z": 30.0})

    result = client.zrevrange(key, 0, -1)

    assert len(result) == 3
    first = result[0] if isinstance(result[0], str) else result[0].decode()
    last = result[2] if isinstance(result[2], str) else result[2].decode()
    assert first == "z"  # Highest score first
    assert last == "x"  # Lowest score last


@pytest.mark.integration
def test_redis_client_zrange_with_scores(
    redis_client: redis.Redis,
    settings: HintGridSettings,
) -> None:
    """Test RedisClient.zrange_with_scores returns elements in order."""
    from hintgrid.clients.redis import RedisClient

    client = RedisClient(
        redis_client,
        score_tolerance=settings.redis_score_tolerance,
        host=settings.redis_host,
        port=settings.redis_port,
    )

    key = "test:zrange_scores"
    redis_client.delete(key)
    redis_client.zadd(key, {"m": 5.0, "n": 15.0, "o": 25.0})

    result = client.zrange_with_scores(key, 0, -1)

    # Verify ascending order (lowest score first)
    assert len(result) == 3
    first_key = result[0][0] if isinstance(result[0][0], str) else result[0][0].decode()
    last_key = result[2][0] if isinstance(result[2][0], str) else result[2][0].decode()
    assert first_key == "m"  # Lowest score
    assert result[0][1] == 5.0
    assert last_key == "o"  # Highest score
    assert result[2][1] == 25.0


@pytest.mark.integration
def test_redis_client_zrange_without_scores(
    redis_client: redis.Redis,
    settings: HintGridSettings,
) -> None:
    """Test RedisClient.zrange returns elements without scores."""
    from hintgrid.clients.redis import RedisClient

    client = RedisClient(
        redis_client,
        score_tolerance=settings.redis_score_tolerance,
        host=settings.redis_host,
        port=settings.redis_port,
    )

    key = "test:zrange_no_scores"
    redis_client.delete(key)
    redis_client.zadd(key, {"p": 100.0, "q": 200.0, "r": 300.0})

    result = client.zrange(key, 0, -1)

    assert len(result) == 3
    first = result[0] if isinstance(result[0], str) else result[0].decode()
    last = result[2] if isinstance(result[2], str) else result[2].decode()
    assert first == "p"  # Lowest score first
    assert last == "r"  # Highest score last


@pytest.mark.integration
def test_redis_client_zrem_removes_elements(
    redis_client: redis.Redis,
    settings: HintGridSettings,
) -> None:
    """Test RedisClient.zrem removes elements from sorted set."""
    from hintgrid.clients.redis import RedisClient

    client = RedisClient(
        redis_client,
        score_tolerance=settings.redis_score_tolerance,
        host=settings.redis_host,
        port=settings.redis_port,
    )

    key = "test:zrem"
    redis_client.delete(key)
    redis_client.zadd(key, {"item1": 1.0, "item2": 2.0, "item3": 3.0})

    # Remove one element (use bytes for consistency)
    removed = client.zrem(key, b"item2")
    assert removed == 1

    # Verify only 2 elements remain
    remaining = client.zrange(key, 0, -1)
    assert len(remaining) == 2
    # Check that item2 is not in remaining (handle str or bytes)
    remaining_strs = [
        r if isinstance(r, str) else r.decode() for r in remaining
    ]
    assert "item2" not in remaining_strs


@pytest.mark.integration
def test_redis_client_pipeline_batched_operations(
    redis_client: redis.Redis,
    settings: HintGridSettings,
) -> None:
    """Test RedisClient.pipeline for batched operations."""
    from hintgrid.clients.redis import RedisClient

    client = RedisClient(
        redis_client,
        score_tolerance=settings.redis_score_tolerance,
        host=settings.redis_host,
        port=settings.redis_port,
    )

    key = "test:pipeline"
    redis_client.delete(key)

    # Use pipeline for batched writes
    pipe = client.pipeline()
    pipe.zadd(key, {"batch1": 1.0})
    pipe.zadd(key, {"batch2": 2.0})
    pipe.zadd(key, {"batch3": 3.0})
    pipe.execute()

    # Verify all elements were added
    result = client.zrange(key, 0, -1)
    assert len(result) == 3


@pytest.mark.integration
def test_redis_client_remove_hintgrid_recommendations_empty_set(
    redis_client: redis.Redis,
    settings: HintGridSettings,
) -> None:
    """Test remove_hintgrid_recommendations handles empty feed."""
    from hintgrid.clients.redis import RedisClient

    client = RedisClient(
        redis_client,
        score_tolerance=settings.redis_score_tolerance,
        host=settings.redis_host,
        port=settings.redis_port,
    )

    # Remove from non-existent key
    removed = client.remove_hintgrid_recommendations(
        user_id=99999, score_multiplier=1000000
    )
    assert removed == 0


@pytest.mark.integration
def test_redis_client_remove_hintgrid_recommendations_with_invalid_members(
    redis_client: redis.Redis,
    settings: HintGridSettings,
) -> None:
    """Test remove_hintgrid_recommendations handles non-numeric members."""
    from hintgrid.clients.redis import RedisClient

    client = RedisClient(
        redis_client,
        score_tolerance=settings.redis_score_tolerance,
        host=settings.redis_host,
        port=settings.redis_port,
    )

    key = "feed:home:88888"
    redis_client.delete(key)

    # Add non-numeric member
    redis_client.zadd(key, {"not_a_number": 999.0, "12345": 12345000000.0})

    # Should handle gracefully (skip non-numeric members)
    removed = client.remove_hintgrid_recommendations(
        user_id=88888, score_multiplier=1000000
    )

    # Only numeric member with matching score should be removed
    assert removed >= 0


@pytest.mark.integration
def test_redis_client_connection_error(
    settings: HintGridSettings,
) -> None:
    """Test RedisClient raises RedisConnectionError on connection failure."""
    bad_settings = settings.model_copy(
        update={
            "redis_host": "invalid-host-that-does-not-exist",
            "redis_port": 99999,
        }
    )

    with pytest.raises(RedisConnectionError) as exc_info:
        RedisClient.from_settings(bad_settings)

    assert "invalid-host-that-does-not-exist" in str(exc_info.value)


@pytest.mark.integration
def test_redis_client_raw_property(
    redis_client: redis.Redis,
    settings: HintGridSettings,
) -> None:
    """Test RedisClient.raw returns underlying redis client."""
    from hintgrid.clients.redis import RedisClient

    client = RedisClient(
        redis_client,
        score_tolerance=settings.redis_score_tolerance,
        host=settings.redis_host,
        port=settings.redis_port,
    )

    # raw should return the underlying client
    raw_client = client.raw
    assert raw_client is redis_client


@pytest.mark.integration
def test_redis_client_context_manager(
    redis_client: redis.Redis,
    settings: HintGridSettings,
) -> None:
    """Test RedisClient works as context manager."""
    from hintgrid.clients.redis import RedisClient

    client = RedisClient(
        redis_client,
        score_tolerance=settings.redis_score_tolerance,
        host=settings.redis_host,
        port=settings.redis_port,
    )

    # Should work as context manager without errors
    with client:
        key = "test:context_manager"
        redis_client.delete(key)
        redis_client.zadd(key, {"test": 1.0})
        result = client.zrange(key, 0, -1)
        assert len(result) == 1
