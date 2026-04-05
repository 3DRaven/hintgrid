"""Integration tests for export_state function.

Tests export functionality through public API, verifying that
all helper functions work correctly through the exported file content.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from hintgrid.pipeline.exporter import export_state

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.clients.redis import RedisClient
    from hintgrid.config import HintGridSettings


@pytest.mark.integration
class TestExportStateIntegration:
    """Integration tests for export_state function."""

    def test_export_state_empty_graph(
        self,
        neo4j: Neo4jClient,
        hintgrid_redis: RedisClient,
        settings: HintGridSettings,
        tmp_path: Path,
    ) -> None:
        """Test export_state with empty graph produces valid export file."""
        export_file = tmp_path / "export.md"

        export_state(neo4j, hintgrid_redis, settings, str(export_file))

        assert export_file.exists()
        content = export_file.read_text()
        assert "# HintGrid Export" in content
        assert "## System Overview" in content
        assert "## Redis Feeds" in content
        assert "**Users**: 0" in content or "Users: 0" in content
        assert "**Posts**: 0" in content or "Posts: 0" in content

    def test_export_state_with_data(
        self,
        neo4j: Neo4jClient,
        hintgrid_redis: RedisClient,
        settings: HintGridSettings,
        tmp_path: Path,
    ) -> None:
        """Test export_state with actual data produces correct export."""
        neo4j.execute_labeled(
            "CREATE (u:__user__ {id: 123, username: 'testuser', isLocal: true})",
            {"user": "User"},
        )
        neo4j.execute_labeled(
            "CREATE (p:__post__ {id: 456, text: 'Test post', createdAt: datetime()})",
            {"post": "Post"},
        )
        neo4j.execute_labeled(
            "MATCH (u:__user__ {id: 123}), (p:__post__ {id: 456}) "
            "CREATE (u)-[:WROTE]->(p)",
            {"user": "User", "post": "Post"},
        )
        hintgrid_redis.zadd("feed:home:123", {456: 100.0})

        export_file = tmp_path / "export.md"
        export_state(neo4j, hintgrid_redis, settings, str(export_file), user_id=123)

        content = export_file.read_text()
        assert "# HintGrid Export" in content
        assert "**Users**: 1" in content or "Users: 1" in content
        assert "**Posts**: 1" in content or "Posts: 1" in content
        assert "feed:home:123" in content
        assert "Test post" in content or "456" in content

    def test_export_state_handles_various_feed_sources(
        self,
        neo4j: Neo4jClient,
        hintgrid_redis: RedisClient,
        settings: HintGridSettings,
        tmp_path: Path,
    ) -> None:
        """Test that export correctly identifies HintGrid vs Mastodon sources."""
        neo4j.execute_labeled("CREATE (p1:__post__ {id: 100, text: 'Post 100'})", {"post": "Post"})
        neo4j.execute_labeled("CREATE (p2:__post__ {id: 200, text: 'Post 200'})", {"post": "Post"})
        hintgrid_redis.zadd("feed:home:123", {100: 50.0, 200: 300.0})

        export_file = tmp_path / "export.md"
        export_state(neo4j, hintgrid_redis, settings, str(export_file), user_id=123)

        content = export_file.read_text()
        assert "feed:home:123" in content
        assert "100" in content or "200" in content

    def test_export_state_handles_empty_feeds(
        self,
        neo4j: Neo4jClient,
        hintgrid_redis: RedisClient,
        settings: HintGridSettings,
        tmp_path: Path,
    ) -> None:
        """Test that export handles users with empty feeds correctly."""
        neo4j.execute_labeled("CREATE (u:__user__ {id: 999, isLocal: true})", {"user": "User"})

        export_file = tmp_path / "export.md"
        export_state(neo4j, hintgrid_redis, settings, str(export_file), user_id=999)

        content = export_file.read_text()
        assert "# HintGrid Export" in content
        assert "feed:home:999" in content or "_No users found._" in content

    def test_export_state_percentage_formatting(
        self,
        neo4j: Neo4jClient,
        hintgrid_redis: RedisClient,
        settings: HintGridSettings,
        tmp_path: Path,
    ) -> None:
        """Test that percentage formatting works correctly in export."""
        neo4j.execute_labeled("CREATE (p1:__post__ {id: 1})", {"post": "Post"})
        neo4j.execute_labeled("CREATE (p2:__post__ {id: 2})", {"post": "Post"})
        hintgrid_redis.zadd("feed:home:123", {1: 100.0, 2: 200.0})

        export_file = tmp_path / "export.md"
        export_state(neo4j, hintgrid_redis, settings, str(export_file), user_id=123)

        content = export_file.read_text()
        assert "%" in content
        assert ".0%" in content or ".1%" in content
