from collections.abc import Mapping
from typing import Literal, Self, overload

# Type aliases for Redis key/value types
KeyT = str | bytes
ValueT = str | bytes | int | float
ZAddMapping = (
    Mapping[str, float]
    | Mapping[bytes, float]
    | Mapping[int, float]
    | Mapping[float, float]
)


class Pipeline:
    """Redis pipeline for batched commands. Methods return Self for chaining."""

    def set(self, name: KeyT, value: ValueT) -> Self: ...
    def delete(self, *names: KeyT) -> Self: ...
    def zadd(self, name: KeyT, mapping: ZAddMapping) -> Self: ...
    def expire(self, name: KeyT, time: int) -> Self: ...
    def execute(self) -> list[bool | int]: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None: ...


class ConnectionPool:
    def __init__(
        self,
        *,
        connection_class: type[object] | None = ...,
        max_connections: int | None = ...,
        host: str = ...,
        port: int = ...,
        db: int = ...,
        password: str | None = ...,
        socket_timeout: float | None = ...,
        socket_connect_timeout: float | None = ...,
        decode_responses: bool = ...,
        encoding: str = ...,
    ) -> None: ...
    def disconnect(self) -> None: ...


class Redis:
    def __init__(
        self,
        host: str = ...,
        port: int = ...,
        db: int = ...,
        password: str | None = ...,
        socket_timeout: float | None = ...,
        socket_connect_timeout: float | None = ...,
        socket_keepalive: bool | None = ...,
        connection_pool: ConnectionPool | None = ...,
        unix_socket_path: str | None = ...,
        encoding: str = ...,
        encoding_errors: str = ...,
        decode_responses: bool = ...,
        retry_on_timeout: bool = ...,
        ssl: bool = ...,
        ssl_keyfile: str | None = ...,
        ssl_certfile: str | None = ...,
        ssl_ca_certs: str | None = ...,
    ) -> None: ...
    def ping(self) -> bool: ...
    def set(self, name: KeyT, value: ValueT) -> bool: ...
    def get(self, name: KeyT) -> bytes | str | None: ...
    def delete(self, *names: KeyT) -> int: ...
    def zadd(self, name: KeyT, mapping: ZAddMapping) -> int: ...
    @overload
    def zrevrange(
        self, name: KeyT, start: int, end: int, *, withscores: Literal[False] = ...
    ) -> list[bytes]: ...
    @overload
    def zrevrange(
        self, name: KeyT, start: int, end: int, *, withscores: Literal[True]
    ) -> list[tuple[bytes, float]]: ...
    @overload
    def zrevrange(
        self, name: KeyT, start: int, end: int, *, withscores: bool
    ) -> list[bytes] | list[tuple[bytes, float]]: ...
    def zrevrangebyscore(
        self,
        name: KeyT,
        max: float,
        min: float,
        *,
        start: int | None = None,
        num: int | None = None,
    ) -> list[bytes]: ...
    @overload
    def zrange(
        self, name: KeyT, start: int, end: int, *, withscores: Literal[False] = ...
    ) -> list[bytes]: ...
    @overload
    def zrange(
        self, name: KeyT, start: int, end: int, *, withscores: Literal[True]
    ) -> list[tuple[bytes, float]]: ...
    @overload
    def zrange(
        self, name: KeyT, start: int, end: int, *, withscores: bool
    ) -> list[bytes] | list[tuple[bytes, float]]: ...
    def zcard(self, name: KeyT) -> int: ...
    def zscore(self, name: KeyT, value: ValueT) -> float | None: ...
    def zrevrank(self, name: KeyT, value: ValueT) -> int | None: ...
    def zrem(self, name: KeyT, *values: ValueT) -> int: ...
    def zscan(
        self,
        name: KeyT,
        cursor: int = 0,
        match: str | None = None,
        count: int | None = None,
    ) -> tuple[int, list[tuple[bytes, float]]]: ...
    def zremrangebyrank(self, name: KeyT, start: int, end: int) -> int: ...
    def expire(self, name: KeyT, time: int) -> bool: ...
    def ttl(self, name: KeyT) -> int: ...
    def pipeline(self, transaction: bool = True) -> Pipeline: ...
    def close(self) -> None: ...
    def flushdb(self, asynchronous: bool = False) -> bool: ...
