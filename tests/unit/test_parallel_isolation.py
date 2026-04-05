"""Unit tests for parallel test isolation utilities.

These tests verify the IsolatedNeo4jClient and WorkerContext
work correctly for parallel test execution.
"""

from __future__ import annotations

from tests.parallel import (
    IsolatedNeo4jClient,
    WorkerContext,
    cleanup_worker_data,
    create_post_with_worker,
    create_user_with_worker,
    ensure_worker_indexes,
    parse_worker_number,
)


class TestParseWorkerNumber:
    """Tests for parse_worker_number function."""

    def test_master_returns_zero(self) -> None:
        """Master worker should return 0."""
        result = parse_worker_number("master")
        assert result == 0

    def test_gw0_returns_zero(self) -> None:
        """Worker gw0 should return 0."""
        result = parse_worker_number("gw0")
        assert result == 0

    def test_gw1_returns_one(self) -> None:
        """Worker gw1 should return 1."""
        result = parse_worker_number("gw1")
        assert result == 1

    def test_gw15_returns_fifteen(self) -> None:
        """Worker gw15 should return 15."""
        result = parse_worker_number("gw15")
        assert result == 15

    def test_gw16_wraps_to_zero(self) -> None:
        """Worker gw16 should wrap to 0 (mod 16)."""
        result = parse_worker_number("gw16")
        assert result == 0

    def test_gw17_wraps_to_one(self) -> None:
        """Worker gw17 should wrap to 1 (mod 16)."""
        result = parse_worker_number("gw17")
        assert result == 1

    def test_invalid_returns_zero(self) -> None:
        """Invalid worker ID should return 0."""
        result = parse_worker_number("invalid")
        assert result == 0


class TestWorkerContext:
    """Tests for WorkerContext dataclass."""

    def test_master_context(self) -> None:
        """Master context should use public schema and db 0."""
        ctx = WorkerContext(worker_id="master", worker_num=0)

        assert ctx.redis_db == 0
        assert ctx.postgres_schema == "public"

    def test_gw0_context(self) -> None:
        """Worker gw0 context should use test_gw0 schema."""
        ctx = WorkerContext(worker_id="gw0", worker_num=0)

        assert ctx.redis_db == 0
        assert ctx.postgres_schema == "test_gw0"

    def test_gw5_context(self) -> None:
        """Worker gw5 context should use test_gw5 schema and db 5."""
        ctx = WorkerContext(worker_id="gw5", worker_num=5)

        assert ctx.redis_db == 5
        assert ctx.postgres_schema == "test_gw5"

    def test_redis_key_no_prefix(self) -> None:
        """Redis key should not have prefix (using separate DBs)."""
        ctx = WorkerContext(worker_id="gw0", worker_num=0)

        key = ctx.redis_key("feed:home:123")
        assert key == "feed:home:123"

    def test_gds_graph_name(self) -> None:
        """GDS graph name should include worker_id prefix."""
        ctx = WorkerContext(worker_id="gw0", worker_num=0)

        name = ctx.gds_graph_name("similarity")
        assert name == "gw0_similarity"


# Removed _assert_imports - unused function
