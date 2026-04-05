"""Shared fixtures and constants for Redis feed tests."""

# Test constants (matching Mastodon and HintGrid config)
MAX_FEED_SIZE = 500  # Maximum posts in feed (Mastodon uses 800)
FEED_TTL_SECONDS = 86400  # 24 hours
INTERESTS_TTL_DAYS = 30  # TTL for INTERESTED_IN relationships

# Base IDs - will be offset by worker for parallel safety
BASE_USER_ID = 12345
BASE_USER_ID_2 = 67890


def feed_key(user_id: int) -> str:
    """Generate Redis key for user's feed (Mastodon-style naming)."""
    return f"feed:home:{user_id}"


def worker_user_id(base_id: int, worker_id: str) -> int:
    """Generate worker-specific user ID for parallel test isolation."""
    if worker_id == "master":
        return base_id
    # Use worker number as multiplier to avoid collisions
    worker_num = int(worker_id.replace("gw", "")) if worker_id.startswith("gw") else 0
    return base_id + (worker_num * 100000)
