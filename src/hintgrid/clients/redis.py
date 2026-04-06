"""Redis client wrapper for feed storage."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING

import redis

if TYPE_CHECKING:
    from types import TracebackType

from hintgrid.config import HintGridSettings
from hintgrid.exceptions import RedisConnectionError

MIN_HINTGRID_MULTIPLIER = 1
ZRANGE_START = 0
ZRANGE_END = -1
ZERO_COUNT = 0


class RedisClient(AbstractContextManager["RedisClient"]):
    """Wrapper around redis.Redis with minimal helpers."""

    def __init__(
        self,
        client: redis.Redis,
        score_tolerance: float = 1e-6,
        host: str = "localhost",
        port: int = 6379,
    ) -> None:
        self._client = client
        self._score_tolerance = score_tolerance
        self._host = host
        self._port = port

    @classmethod
    def from_settings(cls, settings: HintGridSettings) -> RedisClient:
        pool = redis.ConnectionPool(
            host=settings.redis_host,
            port=settings.redis_port,
            db=settings.redis_db,
            password=settings.redis_password,
            decode_responses=False,
            # Bounded timeouts so invalid hosts/ports fail fast instead of hanging
            # workers (DNS/connect can otherwise block far longer than test limits).
            socket_connect_timeout=10.0,
            socket_timeout=10.0,
        )
        client = redis.Redis(connection_pool=pool)
        try:
            client.ping()
        except Exception as exc:
            # Catch any connection-related exception from redis
            raise RedisConnectionError(
                settings.redis_host, settings.redis_port, exc
            ) from exc
        return cls(
            client,
            score_tolerance=settings.redis_score_tolerance,
            host=settings.redis_host,
            port=settings.redis_port,
        )

    @property
    def raw(self) -> redis.Redis:
        return self._client

    def zrevrange_with_scores(
        self, name: str, start: int, end: int
    ) -> list[tuple[bytes, float]]:
        """Get range of elements from sorted set in reverse order with scores."""
        result: list[tuple[bytes, float]] = self._client.zrevrange(
            name, start, end, withscores=True
        )
        return result

    def zrevrange(self, name: str, start: int, end: int) -> list[bytes]:
        """Get range of elements from sorted set in reverse order."""
        result: list[bytes] = self._client.zrevrange(name, start, end)
        return result

    def zrange_with_scores(
        self, name: str, start: int, end: int
    ) -> list[tuple[bytes, float]]:
        """Get range of elements from sorted set with scores."""
        result: list[tuple[bytes, float]] = self._client.zrange(
            name, start, end, withscores=True
        )
        return result

    def zrange(self, name: str, start: int, end: int) -> list[bytes]:
        """Get range of elements from sorted set."""
        result: list[bytes] = self._client.zrange(name, start, end)
        return result

    def zscore(self, name: str, member: str) -> float | None:
        """Score of member in sorted set, or None if missing."""
        raw: float | None = self._client.zscore(name, member)
        return float(raw) if raw is not None else None

    def zrevrank(self, name: str, member: str) -> int | None:
        """0-based rank from highest score, or None if missing."""
        raw: int | None = self._client.zrevrank(name, member)
        return int(raw) if raw is not None else None

    def zcard(self, name: str) -> int:
        """Cardinality of sorted set."""
        return int(self._client.zcard(name))

    def zrem(self, name: str, *values: bytes) -> int:
        """Remove elements from sorted set."""
        result: int = self._client.zrem(name, *values)
        return result

    def pipeline(self) -> redis.Pipeline:
        """Get a pipeline for batched operations."""
        return self._client.pipeline()

    def scan_feed_entries(
        self, key: str, match: str = "*", count: int = 100
    ) -> Iterator[tuple[bytes, float]]:
        """Scan sorted set entries in batches using ZSCAN (memory-efficient).
        
        Args:
            key: Redis key of the sorted set
            match: Pattern to match members (default: "*" for all)
            count: Approximate number of elements to scan per iteration
            
        Yields:
            Tuples of (member, score) for each entry in the sorted set
        """
        cursor: int = 0
        while True:
            # redis.Redis.zscan returns (cursor: int, items: list[tuple[bytes, float]])
            scan_result = self._client.zscan(key, cursor, match=match, count=count)
            cursor = scan_result[0]
            items = scan_result[1]
            for member, score in items:
                yield member, score
            if cursor == 0:
                break

    def _remove_in_batches(self, key: str, to_remove: list[bytes], batch_size: int = 100) -> int:
        """Remove elements from sorted set in batches to avoid large ZREM calls.
        
        Args:
            key: Redis key of the sorted set
            to_remove: List of members to remove
            batch_size: Number of elements to remove per batch
            
        Returns:
            Total number of elements removed
        """
        total_removed = 0
        for i in range(0, len(to_remove), batch_size):
            batch = to_remove[i : i + batch_size]
            removed = self.zrem(key, *batch)
            total_removed += removed
        return total_removed

    def remove_hintgrid_recommendations(self, user_id: int, score_multiplier: int) -> int:
        """Remove HintGrid entries using ZSCAN streaming (memory-efficient).
        
        HintGrid entries have rank-based scores: score = base + (N - rank),
        where base = max_post_id * multiplier. These scores are always
        greater than post_id for any post.
        
        Mastodon entries have score = post_id (score equals the member value).
        
        This method removes entries where score != post_id (within tolerance),
        preserving native Mastodon entries.
        
        Args:
            user_id: User ID whose feed should be cleaned
            score_multiplier: Score multiplier used for HintGrid entries
            
        Returns:
            Number of entries removed
        """
        key = f"feed:home:{user_id}"
        if score_multiplier <= MIN_HINTGRID_MULTIPLIER:
            return ZERO_COUNT
        
        # Use ZSCAN to stream entries without loading all into memory
        to_remove: list[bytes] = []
        for member, score in self.scan_feed_entries(key):
            try:
                member_id = int(member)
            except (TypeError, ValueError):
                continue
            # Mastodon entries: score == post_id
            # HintGrid entries: score != post_id (rank-based, always > post_id)
            if abs(float(score) - float(member_id)) > self._score_tolerance:
                to_remove.append(member)
        
        if not to_remove:
            return ZERO_COUNT
        
        # Remove in batches to avoid large ZREM calls
        return self._remove_in_batches(key, to_remove)

    def remove_hintgrid_entries_from_key(self, key: str, score_multiplier: int) -> int:
        """Remove HintGrid entries from any Redis sorted set key.

        Works the same as remove_hintgrid_recommendations but accepts
        an arbitrary key (e.g. "timeline:public") instead of constructing
        it from user_id.

        Args:
            key: Redis sorted set key to clean
            score_multiplier: Score multiplier used for HintGrid entries

        Returns:
            Number of entries removed
        """
        if score_multiplier <= MIN_HINTGRID_MULTIPLIER:
            return ZERO_COUNT

        to_remove: list[bytes] = []
        for member, score in self.scan_feed_entries(key):
            try:
                member_id = int(member)
            except (TypeError, ValueError):
                continue
            # Mastodon entries: score == post_id
            # HintGrid entries: score != post_id (rank-based, always > post_id)
            if abs(float(score) - float(member_id)) > self._score_tolerance:
                to_remove.append(member)

        if not to_remove:
            return ZERO_COUNT

        return self._remove_in_batches(key, to_remove)

    def delete_feed_key(self, user_id: int) -> bool:
        """Delete entire feed key for a user.
        
        Args:
            user_id: User ID whose feed should be deleted
            
        Returns:
            True if key was deleted, False if it didn't exist
        """
        key = f"feed:home:{user_id}"
        result = self._client.delete(key)
        return bool(result)

    def zadd(self, name: str, mapping: dict[str, float] | dict[bytes, float] | dict[int, float] | dict[float, float]) -> int:
        """Add members to sorted set with scores.
        
        Args:
            name: Redis key of the sorted set
            mapping: Dictionary mapping members to scores
            
        Returns:
            Number of elements added to the sorted set
        """
        return self._client.zadd(name, mapping)

    def ping(self) -> bool:
        """Ping Redis server to check connection.
        
        Returns:
            True if connection is alive
        """
        return self._client.ping()

    def close(self) -> None:
        """Close Redis connection."""
        self._client.close()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._client.close()
        return None
