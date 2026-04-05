"""Type stubs for rich library."""

from typing import Protocol
from collections.abc import Iterator
from contextlib import AbstractContextManager

class Status(AbstractContextManager["Status"]):
    """Rich status context manager."""
    
    def __enter__(self) -> "Status":
        ...
    
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        ...

class Console:
    """Rich console for output."""
    
    def __init__(self, stderr: bool = False) -> None:
        ...
    
    def status(self, message: str) -> Status:
        """Create a status context manager."""
        ...
    
    def print(self, *objects: object, **kwargs: object) -> None:
        ...
    
    def print_exception(self) -> None:
        ...
