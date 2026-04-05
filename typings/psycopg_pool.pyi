"""Stub for psycopg_pool library."""

from typing import Generic, TypeVar
from psycopg import Connection

CT = TypeVar("CT", bound=Connection)

class ConnectionPool(Generic[CT]):
    def __init__(
        self,
        conninfo: str,
        *,
        min_size: int = ...,
        max_size: int = ...,
        timeout: float = ...,
        max_idle: float = ...,
        max_lifetime: float = ...,
        open: bool = ...,
    ) -> None: ...
    
    def wait(self, timeout: float = ...) -> None: ...
    
    def connection(self) -> CT: ...
    
    def getconn(self) -> CT: ...
    
    def putconn(self, conn: CT) -> None: ...
    
    def get_stats(self) -> dict[str, int]: ...
    
    def close(self) -> None: ...
