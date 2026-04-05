"""Fixtures specific to unit tests.

Unit tests run without external dependencies (no containers).
They should be fast and deterministic.
"""

from __future__ import annotations

import pytest


@pytest.fixture
def sample_cursor_data() -> dict[str, int]:
    """Sample cursor data for testing state serialization."""
    return {
        "last_status_id": 12345,
        "last_favourite_id": 67890,
        "last_follow_id": 11111,
        "last_block_id": 22222,
        "last_mute_id": 33333,
        "last_reblog_id": 44444,
        "last_reply_id": 55555,
    }


@pytest.fixture
def sample_embedding_signature() -> str:
    """Sample embedding signature for testing."""
    return "openai:text-embedding-3-small:768"
