"""Shared fixtures and constants for container tests."""

from typing import Protocol


class RedisPipeline(Protocol):
    """Redis pipeline protocol for type hints."""

    def set(self, name: str, value: str) -> bool: ...

    def delete(self, *names: str) -> int: ...

    def zadd(self, name: str, mapping: dict[str, float]) -> int: ...

    def execute(self) -> list[object]: ...


class RedisClient(Protocol):
    """Redis client protocol for type hints."""

    def ping(self) -> bool: ...

    def set(self, name: str, value: str) -> bool: ...

    def get(self, name: str) -> str | None: ...

    def delete(self, *names: str) -> int: ...

    def zadd(self, name: str, mapping: dict[str, float]) -> int: ...

    def zrevrange(
        self, name: str, start: int, end: int, *, withscores: bool = False
    ) -> list[tuple[str, float]]: ...

    def zcard(self, name: str) -> int: ...

    def expire(self, name: str, time: int) -> bool: ...

    def ttl(self, name: str) -> int: ...

    def pipeline(self, transaction: bool = True) -> RedisPipeline: ...


# Test data constants
EXPECTED_NODES_COUNT = 2  # User + Post nodes
TOP_POSTS_LIMIT = 3  # Number of top-scored posts to retrieve
TOTAL_FEED_ITEMS = 4  # Total items in feed
EXPECTED_STATUSES_COUNT = 3  # Number of test statuses
FAVOURITES_COUNT = 2  # Number of favourites in test
PIPELINE_BATCH_SIZE = 100  # Batch size for pipeline operations
PAGINATION_BATCH_SIZE = 5  # Items per page for pagination
TOTAL_PAGINATION_BATCHES = 4  # Expected number of batches
MAX_DOCS_PARAMS = 30  # Maximum documentation parameters to show
MAX_MODULE_PROCEDURES = 5  # Maximum procedures to show per module
POOL_CONNECTIONS_COUNT = 3  # Number of connections to test in pool
NEO4J_TEST_NODES_COUNT = 6  # Number of nodes in Neo4j test graph
NEO4J_COMMUNITIES_COUNT = 2  # Expected number of communities
