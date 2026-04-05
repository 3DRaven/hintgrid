"""Shared Protocol definitions for test type safety.

Centralized Protocol classes to avoid duplication across test files.
These protocols define the expected interfaces for Redis/Neo4j/Postgres
clients without importing actual implementation details.
"""

from __future__ import annotations

from typing import Protocol


class RedisClientOps(Protocol):
    """Basic Redis client operations."""

    def flushdb(self) -> int: ...

    def ping(self) -> bool: ...


class RedisKeyValueClient(Protocol):
    """Redis key-value operations."""

    def ping(self) -> bool: ...

    def set(self, name: str, value: str) -> bool: ...

    def get(self, name: str) -> str | None: ...

    def delete(self, *names: str) -> int: ...

    def exists(self, name: str) -> int: ...

    def expire(self, name: str, time: int) -> bool: ...

    def ttl(self, name: str) -> int: ...


class RedisPipeline(Protocol):
    """Redis pipeline for batch operations."""

    def set(self, name: str, value: str) -> bool: ...

    def delete(self, *names: str) -> int: ...

    def zadd(self, name: str, mapping: dict[str, float]) -> int: ...

    def execute(self) -> list[object]: ...


class RedisZSetClient(Protocol):
    """Redis Sorted Set operations."""

    def zadd(self, name: str, mapping: dict[str, float]) -> int: ...

    def zrevrange(
        self, name: str, start: int, end: int, *, withscores: bool = False
    ) -> list[tuple[bytes, float]]: ...

    def zrange(
        self, name: str, start: int, end: int, *, withscores: bool = False
    ) -> list[tuple[bytes, float]]: ...

    def zcard(self, name: str) -> int: ...

    def exists(self, name: str) -> int: ...

    def ttl(self, name: str) -> int: ...


class RedisFullClient(RedisKeyValueClient, RedisZSetClient, Protocol):
    """Full Redis client with all operations."""

    def flushdb(self) -> int: ...

    def pipeline(self, transaction: bool = True) -> RedisPipeline: ...
