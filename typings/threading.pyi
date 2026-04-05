"""Type stubs for threading module with extended types."""

class Event:
    """Event object for thread synchronization."""
    
    def __init__(self) -> None:
        ...
    
    def set(self) -> None:
        """Set the event."""
        ...
    
    def clear(self) -> None:
        """Clear the event."""
        ...
    
    def is_set(self) -> bool:
        """Check if event is set."""
        ...
    
    def wait(self, timeout: float | None = None) -> bool:
        """Wait until the event is set or timeout expires.
        
        Args:
            timeout: Maximum time to wait in seconds. None means wait indefinitely.
        
        Returns:
            True if event was set, False if timeout expired.
        """
        ...


class Lock:
    """Lock object for thread synchronization."""
    
    def __init__(self) -> None:
        ...
    
    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        """Acquire the lock.
        
        Args:
            blocking: If True, block until lock is acquired.
            timeout: Maximum time to wait in seconds. -1 means wait indefinitely.
        
        Returns:
            True if lock was acquired, False otherwise.
        """
        ...
    
    def release(self) -> None:
        """Release the lock."""
        ...
    
    def __enter__(self) -> "Lock":
        """Context manager entry."""
        ...
    
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Context manager exit."""
        ...


class Barrier:
    """Barrier object for thread synchronization."""
    
    def __init__(self, parties: int, action: object | None = None, timeout: float | None = None) -> None:
        """Initialize barrier.
        
        Args:
            parties: Number of threads that must call wait() before barrier is released.
            action: Optional callable to execute when barrier is released.
            timeout: Default timeout for wait() calls.
        """
        ...
    
    def wait(self, timeout: float | None = None) -> int:
        """Wait until all parties have called wait().
        
        Args:
            timeout: Maximum time to wait in seconds. None uses default timeout.
        
        Returns:
            Party index (0 to parties-1).
        """
        ...
    
    def reset(self) -> None:
        """Reset the barrier."""
        ...


class Thread:
    """Thread with stop_event attribute (set dynamically for polling threads)."""
    
    stop_event: Event  # stop_event for polling threads (set dynamically)
    
    def __init__(
        self,
        target: object | None = None,
        name: str | None = None,
        args: tuple[object, ...] = ...,
        kwargs: dict[str, object] | None = ...,
        *,
        daemon: bool | None = ...,
    ) -> None:
        ...
    
    def start(self) -> None:
        ...
    
    def join(self, timeout: float | None = ...) -> None:
        ...
