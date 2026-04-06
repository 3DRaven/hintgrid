"""Neo4j client wrapper and graph helpers."""

from __future__ import annotations

import logging
import re
import time
import uuid
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, LiteralString, Self

from neo4j import GraphDatabase

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence
    from datetime import datetime
    from types import TracebackType

    from neo4j import Driver

    from hintgrid.cli.progress_display import HintGridProgress
    from hintgrid.config import HintGridSettings
    from rich.progress import TaskID

    # Neo4j parameter types - what can be passed to queries
    # Using Sequence and Mapping for covariance - allows list[dict[str, float]] to be passed
    # datetime is supported - Neo4j accepts datetime objects and converts them in queries
    # list[float] is supported - Neo4j accepts list[float] for embeddings
    Neo4jParameter = (
        int
        | float
        | str
        | bool
        | datetime
        | list[int]
        | list[float]
        | list[str]
        | Sequence[Mapping[str, "Neo4jParameter"]]
        | Mapping[str, "Neo4jParameter"]
        | None
    )

    # Neo4j return value types - what can be returned from queries
    # Using Sequence and Mapping for covariance
    # list[float] is included for embeddings (vector embeddings returned from Neo4j)
    # list[int] is included for collect() results (e.g., collect(p.id), collect(u.id))
    Neo4jValue = (
        int
        | float
        | str
        | bool
        | datetime
        | list[str]
        | list[int]
        | list[float]
        | Sequence[Mapping[str, "Neo4jValue"]]
        | Mapping[str, "Neo4jValue"]
        | None
    )
from hintgrid.exceptions import GDSNotAvailableError, Neo4jConnectionError
from hintgrid.utils.coercion import coerce_int

logger = logging.getLogger(__name__)

# Export Neo4jValue for runtime use (even though it's primarily for type checking)
# This allows other modules to import it without TYPE_CHECKING guards
if not TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from datetime import datetime

    Neo4jValue = (
        int
        | float
        | str
        | bool
        | datetime
        | list[str]
        | list[int]
        | list[float]
        | Sequence[Mapping[str, "Neo4jValue"]]
        | Mapping[str, "Neo4jValue"]
        | None
    )

# Pattern for safe identifiers in query templates (labels, index names, GDS graph names)
_SAFE_IDENT_RE = re.compile(r"[A-Za-z0-9_:.\-]*")


class Neo4jClient(AbstractContextManager["Neo4jClient"]):
    """Thin wrapper over neo4j.Driver with helpers used in pipeline."""

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        worker_label: str | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._worker_label = worker_label
        self._uri = f"bolt://{host}:{port}"
        self._driver: Driver = GraphDatabase.driver(self._uri, auth=(username, password))

    @classmethod
    def from_settings(cls, settings: HintGridSettings) -> Neo4jClient:
        client = cls(
            host=settings.neo4j_host,
            port=settings.neo4j_port,
            username=settings.neo4j_username,
            password=settings.neo4j_password,
            worker_label=settings.neo4j_worker_label,
        )
        client._wait_until_ready(
            retries=settings.neo4j_ready_retries,
            sleep_seconds=settings.neo4j_ready_sleep_seconds,
        )
        client._verify_gds()
        return client

    def _wait_until_ready(self, retries: int, sleep_seconds: int) -> None:
        last_error: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                with self._driver.session() as session:
                    session.run("RETURN 1")
                logger.info("Neo4j ready after %s attempt(s)", attempt)
                return
            except Exception as exc:  # pragma: no cover - best effort
                last_error = exc
                if attempt < retries:
                    logger.debug(
                        "Neo4j connection attempt %s/%s failed: %s",
                        attempt,
                        retries,
                        exc,
                    )
                    time.sleep(sleep_seconds)
        raise Neo4jConnectionError(self._host, self._port, last_error)

    def _verify_gds(self) -> None:
        try:
            with self._driver.session() as session:
                result = session.run("RETURN gds.version() AS version")
                record = result.single()
                if record:
                    version = record["version"]
                    logger.info("Neo4j GDS version: %s", version)
        except Exception as exc:  # pragma: no cover - best effort
            raise GDSNotAvailableError(exc) from exc

    def execute(
        self, query: LiteralString, params: Mapping[str, Neo4jParameter] | None = None
    ) -> None:
        with self._driver.session() as session:
            result = session.run(query, dict(params or {}))
            result.consume()

    def execute_and_fetch(
        self, query: LiteralString, params: Mapping[str, Neo4jParameter] | None = None
    ) -> list[dict[str, Neo4jValue]]:
        with self._driver.session() as session:
            result = session.run(query, dict(params or {}))
            return [record.data() for record in result]

    def stream_query(
        self,
        query: LiteralString,
        params: Mapping[str, Neo4jParameter] | None = None,
        fetch_size: int = 1000,
    ) -> Iterator[dict[str, Neo4jValue]]:
        """Stream query results with server-side cursor.

        Neo4j driver is lazy by default - records are fetched
        as you iterate, not all at once. This is O(N) complexity
        compared to O(N²) for SKIP+LIMIT pagination.

        Args:
            query: Cypher query
            params: Query parameters
            fetch_size: Buffer size for fetching (default 1000)

        Yields:
            Record data dictionaries
        """
        with self._driver.session(fetch_size=fetch_size) as session:
            result = session.run(query, dict(params or {}))
            for record in result:
                yield record.data()

    def labels_list(self, base_label: str) -> list[str]:
        """Get compound label as list for APOC procedures.

        APOC procedures accept labels as a list of strings.
        Example: 'User' → ['User', 'worker_gw0'] (or ['User'] without worker).
        """
        return self.label(base_label).split(":")

    def execute_labeled(
        self,
        template: LiteralString,
        label_map: Mapping[str, str] | None = None,
        params: Mapping[str, Neo4jParameter] | None = None,
        *,
        ident_map: Mapping[str, str] | None = None,
    ) -> None:
        """Execute query with validated label/identifier interpolation.

        Template uses __key__ placeholders for labels and identifiers.
        label_map values are base labels resolved via self.label().
        ident_map values are pre-validated identifiers (for DDL/GDS names).
        """
        # Format template by replacing __key__ placeholders
        subs: dict[str, str] = {}
        if label_map:
            for key, base_label in label_map.items():
                subs[key] = self.label(base_label)
        if ident_map:
            for key, value in ident_map.items():
                if value and not _SAFE_IDENT_RE.fullmatch(value):
                    raise ValueError(
                        f"Unsafe identifier '{key}': {value!r}. Must match {_SAFE_IDENT_RE.pattern}"
                    )
                subs[key] = value
        query = str(template)
        for key, value in subs.items():
            query = query.replace(f"__{key}__", value)

        with self._driver.session() as session:
            # Safe: all values validated before substitution
            result = session.run(query, dict(params or {}))
            result.consume()

    def execute_and_fetch_labeled(
        self,
        template: LiteralString,
        label_map: Mapping[str, str] | None = None,
        params: Mapping[str, Neo4jParameter] | None = None,
        *,
        ident_map: Mapping[str, str] | None = None,
    ) -> list[dict[str, Neo4jValue]]:
        """Execute query with label interpolation and return results."""
        # Format template by replacing __key__ placeholders
        subs: dict[str, str] = {}
        if label_map:
            for key, base_label in label_map.items():
                subs[key] = self.label(base_label)
        if ident_map:
            for key, value in ident_map.items():
                if value and not _SAFE_IDENT_RE.fullmatch(value):
                    raise ValueError(
                        f"Unsafe identifier '{key}': {value!r}. Must match {_SAFE_IDENT_RE.pattern}"
                    )
                subs[key] = value
        query = str(template)
        for key, value in subs.items():
            query = query.replace(f"__{key}__", value)

        with self._driver.session() as session:
            # Safe: all values validated before substitution
            result = session.run(query, dict(params or {}))
            return [record.data() for record in result]

    def stream_query_labeled(
        self,
        template: LiteralString,
        label_map: Mapping[str, str] | None = None,
        params: Mapping[str, Neo4jParameter] | None = None,
        *,
        ident_map: Mapping[str, str] | None = None,
        fetch_size: int = 1000,
    ) -> Iterator[dict[str, Neo4jValue]]:
        """Stream query results with validated label interpolation."""
        # Format template by replacing __key__ placeholders
        subs: dict[str, str] = {}
        if label_map:
            for key, base_label in label_map.items():
                subs[key] = self.label(base_label)
        if ident_map:
            for key, value in ident_map.items():
                if value and not _SAFE_IDENT_RE.fullmatch(value):
                    raise ValueError(
                        f"Unsafe identifier '{key}': {value!r}. Must match {_SAFE_IDENT_RE.pattern}"
                    )
                subs[key] = value
        query = str(template)
        for key, value in subs.items():
            query = query.replace(f"__{key}__", value)

        with self._driver.session(fetch_size=fetch_size) as session:
            # Safe: all values validated before substitution
            result = session.run(query, dict(params or {}))
            for record in result:
                yield record.data()

    def stream_user_ids(self) -> Iterator[int]:
        """Stream user IDs without loading all into memory.

        Yields:
            User IDs one by one
        """
        for row in self.stream_query_labeled(
            "MATCH (u:__user__) RETURN u.id AS id",
            {"user": "User"},
        ):
            value = row.get("id")
            if value is not None:
                try:
                    yield coerce_int(value, field="user.id", strict=True)
                except (TypeError, ValueError):
                    continue

    def stream_active_user_ids(self, active_days: int) -> Iterator[int]:
        """Stream user IDs of active LOCAL users (lastActive within threshold).

        Returns local users whose lastActive is within the given number of days.
        Users without lastActive property are also included (treated as active
        because they may be newly created and not yet have activity data).
        Only returns users with isLocal = true (feed generation is pointless
        for remote users since they have their own Redis on another server).

        Args:
            active_days: Number of days threshold for activity

        Yields:
            User IDs of active local users
        """
        for row in self.stream_query_labeled(
            "MATCH (u:__user__) "
            "WHERE u.isLocal = true "
            "  AND (u.lastActive IS NULL "
            "   OR u.lastActive >= datetime() - duration({days: $active_days})) "
            "RETURN u.id AS id",
            {"user": "User"},
            {"active_days": active_days},
        ):
            value = row.get("id")
            if value is not None:
                try:
                    yield coerce_int(value, field="user.id", strict=True)
                except (TypeError, ValueError):
                    continue

    def stream_dirty_user_ids(
        self,
        active_days: int,
        feed_size: int,
        rel_types: frozenset[str] | None = None,
        *,
        noise_community_id: int = -1,
    ) -> Iterator[int]:
        """Stream user IDs of local users that need feed regeneration.

        A user is "dirty" (needs feed refresh) if any of:
        1. feedGeneratedAt IS NULL (never generated)
        2. New posts appeared in user's PostCommunity clusters since feedGeneratedAt
        3. INTERESTED_IN relationships were updated since feedGeneratedAt
        4. User has consumed most of their feed (high WAS_RECOMMENDED count
           relative to feed_size since feedGeneratedAt)

        Only returns users with isLocal = true.

        Args:
            active_days: Number of days threshold for user activity
            feed_size: Feed size to determine consumption threshold
            rel_types: Relationship types present in the graph.

        Yields:
            User IDs of dirty local users needing feed refresh
        """
        # Consumption threshold: user consumed >= 80% of their feed
        consumption_threshold = int(feed_size * 0.8)

        query: LiteralString = (
            "MATCH (u:__user__) "
            "WHERE u.isLocal = true "
            "  AND (u.lastActive IS NULL "
            "   OR u.lastActive >= datetime() - duration({days: $active_days})) "
            "AND ( "
            "  u.feedGeneratedAt IS NULL "
            "  OR EXISTS { "
            "    MATCH (u)-[:BELONGS_TO]->(uc:__uc__)-[:INTERESTED_IN]->(pc:__pc__) "
            "          <-[:BELONGS_TO]-(p:__post__) "
            "    WHERE p.createdAt > u.feedGeneratedAt "
            "      AND uc.id <> $noise_community_id AND pc.id <> $noise_community_id "
            "  } "
            "  OR EXISTS { "
            "    MATCH (u)-[:BELONGS_TO]->(uc:__uc__)-[i:INTERESTED_IN]->(pc:__pc__) "
            "    WHERE i.last_updated > u.feedGeneratedAt "
            "      AND uc.id <> $noise_community_id AND pc.id <> $noise_community_id "
            "  } "
        )
        if rel_types is None or "WAS_RECOMMENDED" in rel_types:
            query = (
                query + "  OR size([(u)-[wr:WAS_RECOMMENDED]->(:__post__) "
                "    WHERE wr.at > u.feedGeneratedAt | wr]) >= $consumption_threshold "
            )
        query = query + ") RETURN u.id AS id ORDER BY u.id"

        for row in self.stream_query_labeled(
            query,
            {"user": "User", "uc": "UserCommunity", "pc": "PostCommunity", "post": "Post"},
            {
                "active_days": active_days,
                "consumption_threshold": consumption_threshold,
                "noise_community_id": noise_community_id,
            },
        ):
            value = row.get("id")
            if value is not None:
                try:
                    yield coerce_int(value, field="user.id", strict=True)
                except (TypeError, ValueError):
                    continue

    def stream_local_user_ids(self) -> Iterator[int]:
        """Stream user IDs of local users only.

        Used by clean_redis to only clean feeds for local users.

        Yields:
            User IDs of local users
        """
        for row in self.stream_query_labeled(
            "MATCH (u:__user__) WHERE u.isLocal = true RETURN u.id AS id",
            {"user": "User"},
        ):
            value = row.get("id")
            if value is not None:
                try:
                    yield coerce_int(value, field="user.id", strict=True)
                except (TypeError, ValueError):
                    continue

    def prune_similarity_links(self, settings: HintGridSettings) -> None:
        """Prune SIMILAR_TO relationships based on pruning strategy."""
        if settings.similarity_pruning == "aggressive":
            self.delete_all_similar_to_relationships(
                batch_size=settings.apoc_batch_size,
            )
            return

        if settings.similarity_pruning == "partial":
            partial_iterate: LiteralString = (
                "MATCH (p:__post__)-[r:SIMILAR_TO]->() "
                "WHERE r.weight < $threshold RETURN id(r) AS rid"
            )
            self._delete_similar_to_by_rel_id_batches(
                partial_iterate,
                settings.apoc_batch_size,
                params={"threshold": settings.prune_similarity_threshold},
                log_label="SIMILAR_TO partial pruning",
            )
            return

        if settings.similarity_pruning == "temporal":
            temporal_iterate: LiteralString = (
                "MATCH (p:__post__)-[r:SIMILAR_TO]->() "
                "WHERE p.createdAt < datetime() - duration({days: $days}) "
                "RETURN id(r) AS rid"
            )
            self._delete_similar_to_by_rel_id_batches(
                temporal_iterate,
                settings.apoc_batch_size,
                params={"days": settings.prune_days},
                log_label="SIMILAR_TO temporal pruning",
            )
            return

        logger.info("Similarity pruning disabled (strategy=%s)", settings.similarity_pruning)

    def _delete_similar_to_by_rel_id_batches(
        self,
        iterate_query: LiteralString,
        batch_size: int,
        *,
        params: Mapping[str, Neo4jParameter] | None = None,
        log_label: str = "SIMILAR_TO batch delete",
    ) -> None:
        """Delete SIMILAR_TO edges in batches via ``apoc.periodic.iterate``.

        ``iterate_query`` must ``RETURN id(r) AS rid`` for each relationship to
        remove. The action phase deletes by internal id to bound transaction
        memory (same pattern as aggressive pruning).
        """
        if batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {batch_size}")

        action_query: LiteralString = (
            "UNWIND $_batch AS row "
            "MATCH (p:__post__)-[r:SIMILAR_TO]->() WHERE id(r) = row.rid "
            "DELETE r"
        )
        result = self.execute_periodic_iterate(
            iterate_query,
            action_query,
            label_map={"post": "Post"},
            batch_size=batch_size,
            parallel=False,
            batch_mode="BATCH",
            params=params,
        )
        failed = coerce_int(result.get("failedOperations", 0))
        if failed > 0:
            logger.warning(
                "%s had failures: %s",
                log_label,
                result.get("errorMessages", []),
            )
        logger.info(
            "%s: batches=%s total=%s committed=%s failed=%s",
            log_label,
            coerce_int(result.get("batches", 0)),
            coerce_int(result.get("total", 0)),
            coerce_int(result.get("committedOperations", 0)),
            failed,
        )

    def delete_all_similar_to_relationships(self, batch_size: int) -> None:
        """Delete all SIMILAR_TO edges using apoc.periodic.iterate.

        Avoids a single large DELETE transaction that can exceed
        ``dbms.memory.transaction.total.max`` on graphs with many edges.
        """
        iterate_query: LiteralString = "MATCH (p:__post__)-[r:SIMILAR_TO]->() RETURN id(r) AS rid"
        self._delete_similar_to_by_rel_id_batches(
            iterate_query,
            batch_size,
            log_label="SIMILAR_TO bulk delete",
        )

    def detach_delete_all_nodes(
        self,
        batch_size: int,
        *,
        progress: HintGridProgress | None = None,
        settings: HintGridSettings | None = None,
    ) -> None:
        """Delete every node via ``apoc.periodic.iterate`` in small transactions.

        A single ``MATCH (n) DETACH DELETE n`` can exceed
        ``dbms.memory.transaction.total.max`` on large graphs; batching keeps
        each transaction bounded.

        When ``progress`` and ``settings`` are provided, updates Neo4j
        ``ProgressTracker`` and the Rich/plain progress UI (same pattern as
        pipeline batch operations).
        """
        if batch_size < 1:
            raise ValueError(f"batch_size must be >= 1, got {batch_size}")
        if progress is not None and settings is None:
            raise ValueError("settings is required when progress is not None")

        if self._worker_label:
            count_rows = self.execute_and_fetch_labeled(
                "MATCH (n:__worker__) RETURN count(n) AS total",
                ident_map={"worker": self._worker_label},
            )
        else:
            count_rows = self.execute_and_fetch("MATCH (n) RETURN count(n) AS total")
        node_total = coerce_int(count_rows[0].get("total", 0)) if count_rows else 0

        operation_id = f"clean_graph_delete_{uuid.uuid4().hex[:12]}"
        polling_thread = None
        task_id: TaskID | None = None
        result: dict[str, Neo4jValue] = {}

        if progress is not None:
            assert settings is not None
            self.create_progress_tracker(operation_id, node_total)
            task_id = progress.add_task(
                "[cyan]Deleting Neo4j nodes...",
                total=float(node_total),
            )
            from hintgrid.cli.console import track_periodic_iterate_progress

            polling_thread = track_periodic_iterate_progress(
                self,
                operation_id,
                progress,
                task_id,
                poll_interval=settings.progress_poll_interval_seconds,
            )

        try:
            if self._worker_label:
                iterate_query: LiteralString = "MATCH (n:__worker__) RETURN id(n) AS nid"
                action_query: LiteralString = (
                    "UNWIND $_batch AS row MATCH (n:__worker__) WHERE id(n) = row.nid "
                    "DETACH DELETE n"
                )
                result = self.execute_periodic_iterate(
                    iterate_query,
                    action_query,
                    ident_map={"worker": self._worker_label},
                    batch_size=batch_size,
                    parallel=False,
                    batch_mode="BATCH",
                    progress_tracker_id=operation_id if progress is not None else None,
                )
            else:
                iterate_query = "MATCH (n) RETURN id(n) AS nid"
                action_query = (
                    "UNWIND $_batch AS row MATCH (n) WHERE id(n) = row.nid DETACH DELETE n"
                )
                result = self.execute_periodic_iterate(
                    iterate_query,
                    action_query,
                    batch_size=batch_size,
                    parallel=False,
                    batch_mode="BATCH",
                    progress_tracker_id=operation_id if progress is not None else None,
                )
        finally:
            if polling_thread is not None:
                polling_thread.stop_event.set()
                polling_thread.join(timeout=2.0)
            if progress is not None:
                self.cleanup_progress_tracker(operation_id)
                if task_id is not None:
                    progress.update(
                        task_id,
                        description="[green]Neo4j nodes deleted",
                    )

        failed = coerce_int(result.get("failedOperations", 0))
        if failed > 0:
            logger.warning(
                "Full graph delete had failures: %s",
                result.get("errorMessages", []),
            )
        logger.info(
            "Graph node bulk delete: batches=%s total=%s committed=%s failed=%s",
            coerce_int(result.get("batches", 0)),
            coerce_int(result.get("total", 0)),
            coerce_int(result.get("committedOperations", 0)),
            failed,
        )
        self.invalidate_rel_types_cache()

    def get_existing_rel_types(self) -> frozenset[str]:
        """Return the set of relationship types currently present in the graph.

        The result is cached for the lifetime of this client instance so that
        repeated calls during a single pipeline run do not hit the database.
        Call ``invalidate_rel_types_cache()`` after bulk writes that create
        new relationship types.
        """
        if hasattr(self, "_rel_types_cache"):
            return self._rel_types_cache
        with self._driver.session() as session:
            result = session.run(
                "CALL db.relationshipTypes() YIELD relationshipType "
                "RETURN collect(relationshipType) AS types"
            )
            record = result.single()
            types_payload: Neo4jValue = record["types"] if record else []
            raw = (
                [str(item) for item in types_payload]
                if isinstance(types_payload, list)
                else []
            )
        self._rel_types_cache: frozenset[str] = frozenset(raw)
        logger.debug("Existing relationship types: %s", self._rel_types_cache)
        return self._rel_types_cache

    def invalidate_rel_types_cache(self) -> None:
        """Drop the cached set of relationship types.

        Should be called after data loading stages that may create
        new relationship types (e.g. BOOKMARKED, FAVORITED).
        """
        if hasattr(self, "_rel_types_cache"):
            del self._rel_types_cache

    @property
    def worker_label(self) -> str | None:
        return self._worker_label

    def label(self, base_label: str) -> str:
        self._validate_label(base_label)
        if self._worker_label:
            self._validate_label(self._worker_label)
            return f"{base_label}:{self._worker_label}"
        return base_label

    def _validate_label(self, label: str) -> None:
        if not label or not re.fullmatch(r"[A-Za-z0-9_]+", label):
            raise ValueError(f"Invalid label: {label}")

    def match_all_nodes(self, var: str = "n") -> str:
        if self._worker_label:
            self._validate_label(self._worker_label)
            return f"({var}:{self._worker_label})"
        return f"({var})"

    def create_vector_index(
        self,
        index_name: str,
        label: str,
        property_name: str,
        dimensions: int,
        similarity_function: str = "cosine",
    ) -> None:
        """Create vector index with validated parameters.

        Neo4j DDL statements don't support bind parameters for OPTIONS,
        so this method validates all inputs and constructs the DDL safely.

        Args:
            index_name: Name for the index (alphanumeric + underscore only)
            label: Node label (alphanumeric only)
            property_name: Property to index (alphanumeric only)
            dimensions: Vector dimensions (1-4096)
            similarity_function: 'cosine' or 'euclidean'

        Raises:
            ValueError: If any parameter is invalid
        """
        # Validate index_name (alphanumeric + underscore)
        if not index_name or not all(c.isalnum() or c == "_" for c in index_name):
            raise ValueError(f"Invalid index name: {index_name}")

        # Validate label (alphanumeric + underscore)
        if not label or not all(c.isalnum() or c == "_" for c in label):
            raise ValueError(f"Invalid label: {label}")

        # Validate property_name (alphanumeric only)
        if not property_name or not property_name.isalnum():
            raise ValueError(f"Invalid property name: {property_name}")

        # Validate dimensions range (type guaranteed by signature, check range)
        if dimensions < 1 or dimensions > 4096:
            raise ValueError(f"Invalid dimensions: {dimensions}. Must be 1-4096.")

        # Validate similarity function
        valid_functions = ("cosine", "euclidean")
        if similarity_function not in valid_functions:
            raise ValueError(
                f"Invalid similarity function: {similarity_function}. "
                f"Must be one of {valid_functions}"
            )

        # Build DDL with validated values (safe - all inputs validated above)
        # Using string formatting here is safe because:
        # 1. All string values are validated to be alphanumeric
        # 2. dimensions is validated as int in range
        # 3. Neo4j CREATE INDEX doesn't accept bind parameters for OPTIONS
        ddl = (
            f"CREATE VECTOR INDEX {index_name} IF NOT EXISTS "
            f"FOR (n:{label}) "
            f"ON n.{property_name} "
            f"OPTIONS {{ "
            f"indexConfig: {{ "
            f"`vector.dimensions`: {dimensions}, "
            f"`vector.similarity_function`: '{similarity_function}' "
            f"}} }}"
        )

        with self._driver.session() as session:
            # DDL with validated inputs (no bind params for CREATE INDEX OPTIONS).
            # Safe: all values validated before formatting
            session.run(ddl)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._driver.close()

    def _build_action_query_with_progress(self, base_action: str, progress_tracker_id: str) -> str:
        """Build complete action query with progress tracking.

        Args:
            base_action: Base action query (already formatted, no __key__ placeholders)
            progress_tracker_id: Progress tracker ID for tracking

        Returns:
            Complete action query with progress tracking
        """
        if base_action.strip().startswith("UNWIND $_batch"):
            # BATCH mode: action uses UNWIND $_batch AS row ...
            # Append progress tracking AFTER the action with count(*) as
            # aggregation barrier to collapse UNWIND rows into a single row.
            # Use size($_batch) for accurate batch counting (parameters are
            # accessible throughout the entire query regardless of WITH scope).
            return (
                base_action + "\nWITH size($_batch) AS batch_size, $progress_tracker_id AS pt_id, "
                "count(*) AS _agg "
                "MATCH (pt:ProgressTracker {id: pt_id}) "
                "SET pt.batches = pt.batches + 1, "
                "    pt.processed = pt.processed + batch_size, "
                "    pt.last_updated = datetime()"
            )
        else:
            # Non-BATCH mode (MERGE, SET without UNWIND $_batch):
            # count(*) both aggregates and counts processed rows.
            return base_action + (
                "\nWITH $progress_tracker_id AS pt_id, count(*) AS batch_size "
                "MATCH (pt:ProgressTracker {id: pt_id}) "
                "SET pt.batches = pt.batches + 1, "
                "    pt.processed = pt.processed + batch_size, "
                "    pt.last_updated = datetime()"
            )

    def execute_periodic_iterate(
        self,
        cypher_iterate: LiteralString | str,
        cypher_action: LiteralString | str,
        *,
        label_map: Mapping[str, str] | None = None,
        ident_map: Mapping[str, str] | None = None,
        batch_size: int = 10000,
        parallel: bool = False,
        params: Mapping[str, Neo4jParameter] | None = None,
        batch_mode: str = "BATCH",
        progress_tracker_id: str | None = None,
    ) -> dict[str, Neo4jValue]:
        """Execute apoc.periodic.iterate for large batch operations.

        Splits a large operation into smaller batches to prevent OOM
        and transaction log overflow.

        Args:
            cypher_iterate: Cypher query template that returns rows to process
            cypher_action: Cypher query template that processes each row
            label_map: Label placeholders resolved via self.label()
            ident_map: Identifier placeholders for DDL/GDS names
            batch_size: Number of rows per batch (default 10000)
            parallel: Whether to run batches in parallel (default False)
            params: Parameters for both queries
            batch_mode: Batch mode - "BATCH" (one call per batch) or "SINGLE" (one call per row)
            progress_tracker_id: Optional operation ID for progress tracking

        Returns:
            Dictionary with execution statistics from apoc.periodic.iterate
        """

        # Format templates by replacing __key__ placeholders
        def _format_query(template: LiteralString | str) -> str:
            subs: dict[str, str] = {}
            if label_map:
                for key, base_label in label_map.items():
                    subs[key] = self.label(base_label)
            if ident_map:
                for key, value in ident_map.items():
                    if value and not _SAFE_IDENT_RE.fullmatch(value):
                        raise ValueError(
                            f"Unsafe identifier '{key}': {value!r}. "
                            f"Must match {_SAFE_IDENT_RE.pattern}"
                        )
                    subs[key] = value
            query = str(template)
            for key, value in subs.items():
                query = query.replace(f"__{key}__", value)
            return query

        # Resolve __label__ and __ident__ templates before passing to APOC
        iterate_resolved = _format_query(cypher_iterate)
        action_resolved = _format_query(cypher_action)

        # Build complete action query with progress tracking if needed
        if progress_tracker_id:
            action_resolved = self._build_action_query_with_progress(
                action_resolved, progress_tracker_id
            )

        # Prepare parameters for APOC call
        # Parameters from params dict are passed via iterateParams to make them available
        # in both iterate_query and action_query within apoc.periodic.iterate
        iterate_params: dict[str, Neo4jParameter] = dict(params) if params else {}
        if progress_tracker_id:
            iterate_params["progress_tracker_id"] = progress_tracker_id

        query = (
            "CALL apoc.periodic.iterate("
            "  $cypherIterate,"
            "  $cypherAction,"
            "  {batchSize: $batchSize, parallel: $parallel, batchMode: $batchMode, params: $iterateParams}"
            ") "
            "YIELD batches, total, timeTaken, committedOperations, failedOperations, errorMessages "
            "RETURN batches, total, timeTaken, committedOperations, failedOperations, errorMessages"
        )

        execute_params: dict[str, Neo4jParameter] = {
            "cypherIterate": iterate_resolved,
            "cypherAction": action_resolved,
            "batchSize": batch_size,
            "parallel": parallel,
            "batchMode": batch_mode,
            "iterateParams": iterate_params,
        }

        result = self.execute_and_fetch(query, execute_params)
        if not result:
            return {
                "batches": 0,
                "total": 0,
                "timeTaken": 0,
                "committedOperations": 0,
                "failedOperations": 0,
                "errorMessages": [],
            }
        return result[0]

    def create_progress_tracker(self, operation_id: str, total: int | None = None) -> None:
        """Create a ProgressTracker node in Neo4j for tracking operation progress.

        Args:
            operation_id: Unique identifier for the operation
            total: Total number of items to process (optional)
        """
        query = (
            "MERGE (pt:ProgressTracker {id: $operation_id}) "
            "SET pt.processed = 0, "
            "    pt.batches = 0, "
            "    pt.total = $total, "
            "    pt.started_at = datetime(), "
            "    pt.last_updated = datetime()"
        )
        self.execute(query, {"operation_id": operation_id, "total": total})

    def get_progress(self, operation_id: str) -> dict[str, Neo4jValue]:
        """Get current progress from ProgressTracker node.

        Args:
            operation_id: Unique identifier for the operation

        Returns:
            Dictionary with progress information (processed, batches, total, etc.)
            Returns empty dict if tracker not found
        """
        query = (
            "MATCH (pt:ProgressTracker {id: $operation_id}) "
            "RETURN pt.processed AS processed, "
            "       pt.batches AS batches, "
            "       pt.total AS total, "
            "       pt.started_at AS started_at, "
            "       pt.last_updated AS last_updated"
        )
        result = self.execute_and_fetch(query, {"operation_id": operation_id})
        if result:
            return result[0]
        return {}

    def cleanup_progress_tracker(self, operation_id: str) -> None:
        """Delete ProgressTracker node after operation completion.

        Args:
            operation_id: Unique identifier for the operation
        """
        self.execute(
            "MATCH (pt:ProgressTracker {id: $operation_id}) DELETE pt",
            {"operation_id": operation_id},
        )

    def close(self) -> None:
        """Close underlying driver to avoid resource warnings."""
        self._driver.close()
