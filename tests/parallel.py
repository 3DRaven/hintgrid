"""Utilities for parallel test execution with data isolation.

This module provides helpers to ensure tests running in parallel
(via pytest-xdist) don't interfere with each other.

Isolation Strategies:
- Neo4j Community: label-based isolation per worker
- Redis: DB numbers (0-15) for complete isolation
- PostgreSQL: Schemas per worker for complete isolation

Usage:
    @pytest.fixture
    def isolated_neo4j(neo4j: Neo4jClient, worker_id: str) -> IsolatedNeo4jClient:
        return IsolatedNeo4jClient(neo4j, worker_id)

    def test_something(isolated_neo4j: IsolatedNeo4jClient):
        isolated_neo4j.create_user(1, username="alice")
        # Data automatically tagged with _worker for isolation
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, LiteralString

from hintgrid.utils.coercion import coerce_int, convert_batch_decimals

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient


# =============================================================================
# Worker Number Parsing
# =============================================================================


def parse_worker_number(worker_id: str) -> int:
    """Extract numeric worker ID from pytest-xdist worker name.

    Args:
        worker_id: Worker ID string (e.g., 'gw0', 'gw1', 'master')

    Returns:
        Worker number (0-15, wraps with modulo for Redis DB compatibility)
    """
    if worker_id == "master":
        return 0

    try:
        return int(worker_id.replace("gw", "")) % 16
    except ValueError:
        return 0


# =============================================================================
# Neo4j Worker Isolation Indexes
# =============================================================================


def ensure_worker_indexes(neo4j: Neo4jClient) -> None:
    """Create indexes for efficient worker-based data isolation.

    Creates indexes on _worker property for User, Post, PipelineState, UserCommunity.
    Should be called once at session startup. Indexes are IF NOT EXISTS,
    so safe to call multiple times.
    """
    neo4j.execute("CREATE INDEX worker_user IF NOT EXISTS FOR (n:User) ON (n._worker)")

    neo4j.execute("CREATE INDEX worker_post IF NOT EXISTS FOR (n:Post) ON (n._worker)")

    neo4j.execute("CREATE INDEX worker_state IF NOT EXISTS FOR (n:PipelineState) ON (n._worker)")

    neo4j.execute("CREATE INDEX worker_community IF NOT EXISTS FOR (n:UserCommunity) ON (n._worker)")


# =============================================================================
# IsolatedNeo4jClient - Wrapper for parallel test isolation
# =============================================================================


@dataclass
class IsolatedNeo4jClient:
    """Neo4j client wrapper that automatically adds _worker property for isolation.

    All nodes created through this wrapper are tagged with the worker ID,
    enabling safe parallel test execution with a single Neo4j container.

    Example:
        client = IsolatedNeo4jClient(neo4j, "gw0")
        client.create_user(1, username="alice")
        # Creates: (:User {id: 1, username: "alice", _worker: "gw0"})
    """

    client: Neo4jClient
    worker_id: str
    worker_label: str | None = None

    def _label(self, base_label: str) -> str:
        if self.worker_label:
            return f"{base_label}:{self.worker_label}"
        return base_label

    # -------------------------------------------------------------------------
    # Node Creation
    # -------------------------------------------------------------------------

    def create_user(
        self,
        user_id: int,
        username: str = "test_user",
        **props: object,
    ) -> None:
        """Create a User node with worker isolation.

        Args:
            user_id: Unique user ID
            username: Username
            **props: Additional properties (e.g., domain, created_at)
        """
        all_props = {"username": username, **props}
        converted_props = convert_batch_decimals([all_props])[0]
        self.client.execute_labeled(
            "CREATE (u:__user__ {id: $user_id, _worker: $worker}) SET u += $props",
            {"user": "User"},
            {"user_id": user_id, "worker": self.worker_id, "props": converted_props},
        )

    def create_post(
        self,
        post_id: int,
        author_id: int,
        text: str = "Test post",
        embedding: list[float] | None = None,
        **props: object,
    ) -> None:
        """Create a Post node with worker isolation.

        Args:
            post_id: Unique post ID
            author_id: Author's user ID
            text: Post text content
            embedding: Optional embedding vector
            **props: Additional properties
        """
        all_props = {"text": text, **props}
        if embedding is not None:
            all_props["embedding"] = embedding

        converted_props = convert_batch_decimals([all_props])[0]
        self.client.execute_labeled(
            "CREATE (p:__post__ {id: $post_id, authorId: $author_id, _worker: $worker}) SET p += $props",
            {"post": "Post"},
            {"post_id": post_id, "author_id": author_id, "worker": self.worker_id, "props": converted_props},
        )

    def create_community(
        self,
        community_id: int,
        name: str = "Test Community",
        **props: object,
    ) -> None:
        """Create a UserCommunity node with worker isolation.

        Args:
            community_id: Unique community ID
            name: Community name
            **props: Additional properties
        """
        all_props = {"name": name, **props}
        converted_props = convert_batch_decimals([all_props])[0]
        self.client.execute_labeled(
            "CREATE (c:__uc__ {id: $community_id, _worker: $worker}) SET c += $props",
            {"uc": "UserCommunity"},
            {"community_id": community_id, "worker": self.worker_id, "props": converted_props},
        )

    def create_pipeline_state(
        self,
        last_status_id: int = 0,
        embedding_signature: str = "",
        **props: object,
    ) -> None:
        """Create a PipelineState node with worker isolation.

        Args:
            last_status_id: Last processed status ID
            embedding_signature: Embedding configuration signature
            **props: Additional state properties
        """
        all_props = {
            "last_status_id": last_status_id,
            "embedding_signature": embedding_signature,
            **props,
        }
        converted_props = convert_batch_decimals([all_props])[0]
        self.client.execute_labeled(
            "MERGE (s:__state__ {_worker: $worker}) SET s += $props",
            {"state": "PipelineState"},
            {"worker": self.worker_id, "props": converted_props},
        )

    # -------------------------------------------------------------------------
    # Relationship Creation
    # -------------------------------------------------------------------------

    def create_wrote(self, user_id: int, post_id: int) -> None:
        """Create WROTE relationship between User and Post.

        Both nodes must belong to this worker.
        """
        self.client.execute_labeled(
            "MATCH (u:__user__ {id: $user_id, _worker: $worker}) "
            "MATCH (p:__post__ {id: $post_id, _worker: $worker}) "
            "CREATE (u)-[:WROTE]->(p)",
            {"user": "User", "post": "Post"},
            {"user_id": user_id, "post_id": post_id, "worker": self.worker_id},
        )

    def create_favorited(self, user_id: int, post_id: int) -> None:
        """Create FAVORITED relationship between User and Post."""
        self.client.execute_labeled(
            "MATCH (u:__user__ {id: $user_id, _worker: $worker}) "
            "MATCH (p:__post__ {id: $post_id, _worker: $worker}) "
            "CREATE (u)-[:FAVORITED]->(p)",
            {"user": "User", "post": "Post"},
            {"user_id": user_id, "post_id": post_id, "worker": self.worker_id},
        )

    def create_follows(self, follower_id: int, target_id: int) -> None:
        """Create FOLLOWS relationship between two Users."""
        self.client.execute_labeled(
            "MATCH (a:__user__ {id: $follower_id, _worker: $worker}) "
            "MATCH (b:__user__ {id: $target_id, _worker: $worker}) "
            "CREATE (a)-[:FOLLOWS]->(b)",
            {"user": "User"},
            {"follower_id": follower_id, "target_id": target_id, "worker": self.worker_id},
        )

    def create_belongs_to(self, user_id: int, community_id: int) -> None:
        """Create BELONGS_TO relationship between User and Community."""
        self.client.execute_labeled(
            "MATCH (u:__user__ {id: $user_id, _worker: $worker}) "
            "MATCH (c:__uc__ {id: $community_id, _worker: $worker}) "
            "CREATE (u)-[:BELONGS_TO]->(c)",
            {"user": "User", "uc": "UserCommunity"},
            {"user_id": user_id, "community_id": community_id, "worker": self.worker_id},
        )

    # -------------------------------------------------------------------------
    # Queries (worker-scoped)
    # -------------------------------------------------------------------------

    def count_users(self) -> int:
        """Count User nodes belonging to this worker."""
        result = list(
            self.client.execute_and_fetch_labeled(
                "MATCH (n:__user__) RETURN count(n) AS count",
                {"user": "User"},
            )
        )
        return coerce_int(result[0]["count"]) if result else 0

    def count_posts(self) -> int:
        """Count Post nodes belonging to this worker."""
        result = list(
            self.client.execute_and_fetch_labeled(
                "MATCH (n:__post__) RETURN count(n) AS count",
                {"post": "Post"},
            )
        )
        return coerce_int(result[0]["count"]) if result else 0

    def count_all_nodes(self) -> int:
        """Count all nodes belonging to this worker."""
        if self.worker_label:
            # Worker label is dynamic, use parameterized query
            result = list(
                self.client.execute_and_fetch(
                    "MATCH (n) WHERE $label IN labels(n) RETURN count(n) AS count",
                    {"label": self.worker_label},
                )
            )
        else:
            result = list(
                self.client.execute_and_fetch(
                    "MATCH (n {_worker: $worker}) RETURN count(n) AS count",
                    {"worker": self.worker_id},
                )
            )
        return coerce_int(result[0]["count"]) if result else 0

    def get_users(self) -> list[dict[str, object]]:
        """Get all User nodes belonging to this worker."""
        result = list(
            self.client.execute_and_fetch_labeled(
                "MATCH (u:__user__) RETURN u.id AS id, u.username AS username",
                {"user": "User"},
            )
        )
        # Convert Neo4jValue to object for return type compatibility
        return [{k: v for k, v in row.items()} for row in result]

    def get_posts(self) -> list[dict[str, object]]:
        """Get all Post nodes belonging to this worker."""
        result = list(
            self.client.execute_and_fetch_labeled(
                "MATCH (p:__post__) RETURN p.id AS id, p.authorId AS authorId, p.text AS text",
                {"post": "Post"},
            )
        )
        # Convert Neo4jValue to object for return type compatibility
        return [{k: v for k, v in row.items()} for row in result]

    # -------------------------------------------------------------------------
    # GDS Graph Management (worker-scoped)
    # -------------------------------------------------------------------------

    def gds_graph_name(self, base_name: str) -> str:
        """Generate worker-specific GDS graph name.

        Example:
            client.gds_graph_name("similarity") -> "gw0_similarity"
        """
        prefix = self.worker_label or self.worker_id
        return f"{prefix}_{base_name}"

    # -------------------------------------------------------------------------
    # Cleanup
    # -------------------------------------------------------------------------

    def cleanup(self) -> int:
        """Remove all nodes belonging to this worker.

        Returns:
            Number of nodes deleted
        """
        if self.worker_label:
            # Worker label is dynamic, use parameterized query
            result = list(
                self.client.execute_and_fetch(
                    "MATCH (n) WHERE $label IN labels(n) "
                    "WITH count(n) AS total "
                    "MATCH (n) WHERE $label IN labels(n) "
                    "DETACH DELETE n "
                    "RETURN total",
                    {"label": self.worker_label},
                )
            )
        else:
            result = list(
                self.client.execute_and_fetch(
                    """
                    MATCH (n {_worker: $worker})
                    WITH count(n) AS total
                    MATCH (n {_worker: $worker})
                    DETACH DELETE n
                    RETURN total
                    """,
                    {"worker": self.worker_id},
                )
            )
        return coerce_int(result[0]["total"]) if result else 0

    # -------------------------------------------------------------------------
    # Direct Access (for complex queries)
    # -------------------------------------------------------------------------

    def execute(self, query: LiteralString, params: dict[str, object] | None = None) -> None:
        """Execute a Cypher query directly.

        Note: You must handle _worker property yourself for isolation.
        Query must be a literal string for security.
        """
        from hintgrid.clients.neo4j import Neo4jParameter
        converted_params: dict[str, Neo4jParameter] | None = None
        if params is not None:
            converted_params = convert_batch_decimals([params])[0]
        self.client.execute(query, converted_params)

    def execute_and_fetch(
        self, query: LiteralString, params: dict[str, object] | None = None
    ) -> list[dict[str, object]]:
        """Execute a Cypher query and return results.

        Note: You must handle _worker property yourself for isolation.
        Query must be a literal string for security.
        """
        from hintgrid.clients.neo4j import Neo4jParameter
        converted_params: dict[str, Neo4jParameter] | None = None
        if params is not None:
            converted_params = convert_batch_decimals([params])[0]
        result = list(self.client.execute_and_fetch(query, converted_params))
        # Convert Neo4jValue to object for return type compatibility
        return [{k: v for k, v in row.items()} for row in result]


# =============================================================================
# WorkerContext - Convenience class for all services
# =============================================================================


@dataclass
class WorkerContext:
    """Context for parallel test execution across all services.

    Provides consistent namespace generation for Neo4j, Redis, and PostgreSQL.
    """

    worker_id: str
    worker_num: int  # 0-15 for Redis DB

    @property
    def redis_db(self) -> int:
        """Redis DB number for this worker (0-15)."""
        return self.worker_num

    @property
    def postgres_schema(self) -> str:
        """PostgreSQL schema name for this worker."""
        if self.worker_id == "master":
            return "public"
        return f"test_{self.worker_id}"

    def redis_key(self, key: str) -> str:
        """Generate Redis key (no prefix needed - using separate DBs)."""
        return key

    def gds_graph_name(self, base_name: str) -> str:
        """Generate worker-isolated GDS graph name."""
        return f"{self.worker_id}_{base_name}"


# =============================================================================
# Legacy Helper Functions (for backward compatibility)
# =============================================================================


def create_user_with_worker(
    neo4j: Neo4jClient,
    user_id: int,
    worker_id: str,
    username: str = "test_user",
    worker_label: str | None = None,
) -> None:
    """Create a User node with worker isolation tag.

    Deprecated: Use IsolatedNeo4jClient.create_user() instead.
    """
    neo4j.execute_labeled(
        "CREATE (u:__user__ {id: $user_id, username: $username, _worker: $worker})",
        {"user": "User"},
        {"user_id": user_id, "username": username, "worker": worker_id},
    )


def create_post_with_worker(
    neo4j: Neo4jClient,
    post_id: int,
    author_id: int,
    worker_id: str,
    text: str = "Test post",
    embedding: list[float] | None = None,
    worker_label: str | None = None,
) -> None:
    """Create a Post node with worker isolation tag.

    Deprecated: Use IsolatedNeo4jClient.create_post() instead.
    """
    params: dict[str, object] = {
        "post_id": post_id,
        "author_id": author_id,
        "text": text,
        "worker": worker_id,
    }

    converted_params = convert_batch_decimals([params])[0]
    if embedding is not None:
        final_params = {**converted_params, "embedding": embedding}
        if worker_label:
            neo4j.execute_labeled(
                "CREATE (p:__post__ {id: $post_id, authorId: $author_id, text: $text, embedding: $embedding, _worker: $worker})",
                {"post": "Post"},
                final_params,
            )
        else:
            neo4j.execute_labeled(
                "CREATE (p:__post__ {id: $post_id, authorId: $author_id, text: $text, embedding: $embedding, _worker: $worker})",
                {"post": "Post"},
                final_params,
            )
    else:
        if worker_label:
            neo4j.execute_labeled(
                "CREATE (p:__post__ {id: $post_id, authorId: $author_id, text: $text, _worker: $worker})",
                {"post": "Post"},
                converted_params,
            )
        else:
            neo4j.execute_labeled(
                "CREATE (p:__post__ {id: $post_id, authorId: $author_id, text: $text, _worker: $worker})",
                {"post": "Post"},
                converted_params,
            )


def cleanup_worker_data(
    neo4j: Neo4jClient, worker_id: str, worker_label: str | None = None
) -> int:
    """Remove all nodes belonging to a specific worker.

    Deprecated: Use IsolatedNeo4jClient.cleanup() instead.
    """
    if worker_label:
        # Worker label is dynamic, use parameterized query
        result = list(
            neo4j.execute_and_fetch(
                "MATCH (n) WHERE $label IN labels(n) "
                "WITH count(n) AS total "
                "MATCH (n) WHERE $label IN labels(n) "
                "DETACH DELETE n "
                "RETURN total",
                {"label": worker_label},
            )
        )
    else:
        result = list(
            neo4j.execute_and_fetch(
                """
                MATCH (n {_worker: $worker})
                WITH count(n) AS total
                MATCH (n {_worker: $worker})
                DETACH DELETE n
                RETURN total
                """,
                {"worker": worker_id},
            )
        )
    return coerce_int(result[0]["total"]) if result else 0
