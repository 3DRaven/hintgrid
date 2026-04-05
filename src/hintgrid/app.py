"""HintGrid CLI entrypoint and orchestration."""

from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, UTC
from pathlib import Path
from typing import TYPE_CHECKING

from hintgrid.config import HintGridSettings, validate_settings
from hintgrid.logging import get_logger
from hintgrid.utils.coercion import coerce_int
from hintgrid.pipeline import (
    EmbeddingMigrationResult,
    check_clusters_exist,
    check_embeddings_exist,
    check_embedding_config,
    check_interests_exist,
    cleanup_expired_interests,
    cleanup_inactive_users,
    count_posts_in_neo4j,
    ensure_graph_indexes,
    export_state,
    force_reindex,
    generate_public_feed,
    generate_user_feed,
    get_embedding_status,
    load_incremental_data,
    mark_recommended,
    rebuild_interests,
    reembed_existing_posts,
    refresh_interests,
    run_post_clustering,
    run_user_clustering,
    seed_serendipity,
    set_feed_generated_at,
    write_feed_to_redis,
    write_public_feed_to_redis,
)
from hintgrid.state import INITIAL_CURSOR, StateStore

if TYPE_CHECKING:
    from collections.abc import Callable

    from hintgrid.cli.console import PipelineMetrics
    from hintgrid.cli.progress_display import HintGridProgress
    from hintgrid.clients import Neo4jClient, PostgresClient, RedisClient
    from hintgrid.cli.shutdown import ShutdownManager
    from hintgrid.pipeline.stats import UserInfo
logger = get_logger(__name__)


@dataclass
class HintGridApp:
    """Main application orchestrator for HintGrid pipeline.

    Can be used both from CLI and from integration tests by injecting clients.
    """

    neo4j: Neo4jClient
    postgres: PostgresClient
    redis: RedisClient
    settings: HintGridSettings

    def __post_init__(self) -> None:
        """Initialize with validation and embedding config check."""
        # Step 1: Validate all settings
        validate_settings(self.settings)

        # Step 2: Initialize state store
        self.state_store = StateStore(self.neo4j)

        # Step 3: Check embedding config BEFORE creating indexes
        self._migration_result = check_embedding_config(self.neo4j, self.settings, self.state_store)

        if self._migration_result.migrated:
            logger.info(
                "Embedding config changed (%s -> %s). "
                "%d posts will be reembedded on next pipeline run.",
                self._migration_result.previous_signature,
                self._migration_result.current_signature,
                self._migration_result.posts_cleared,
            )

        # Step 4: Create indexes (with correct dimensions from current settings)
        ensure_graph_indexes(self.neo4j, self.settings)

    def get_migration_result(self) -> EmbeddingMigrationResult:
        """Get the result of embedding configuration check.

        Returns:
            EmbeddingMigrationResult with migration status and details.
        """
        return self._migration_result

    def load_data(self, shutdown: ShutdownManager | None = None) -> None:
        """Load incremental data from Postgres to Neo4j."""
        load_incremental_data(
            self.postgres,
            self.neo4j,
            self.settings,
            self.state_store,
            shutdown,
        )
        self.neo4j.invalidate_rel_types_cache()

    def run_analytics(self, shutdown: ShutdownManager | None = None) -> None:
        """Run clustering and interest analysis.

        Checks shutdown flag between each analytics step for graceful
        Ctrl+C handling. Each step is individually tracked.
        """
        from hintgrid.cli.console import create_pipeline_progress
        from hintgrid.pipeline.clustering import run_pagerank, run_similarity_pruning
        from hintgrid.pipeline.interests import compute_community_similarity

        _steps: list[tuple[str, str, Callable[[HintGridProgress | None], None]]] = [
            (
                "user_clustering",
                "User clustering",
                lambda progress: run_user_clustering(
                    self.neo4j,
                    self.settings,
                    progress,
                ),
            ),
            (
                "post_clustering",
                "Post clustering",
                lambda progress: run_post_clustering(
                    self.neo4j, self.settings, self.state_store, progress
                ),
            ),
            (
                "pagerank",
                "PageRank",
                lambda progress: run_pagerank(self.neo4j, self.settings),
            ),
            (
                "similarity_pruning",
                "Similarity pruning",
                lambda progress: run_similarity_pruning(self.neo4j, self.settings),
            ),
            (
                "interests",
                "Rebuilding interests",
                lambda progress: self._rebuild_interests_and_record(progress),
            ),
            (
                "community_similarity",
                "Community similarity",
                lambda progress: compute_community_similarity(self.neo4j, self.settings),
            ),
            (
                "serendipity",
                "Serendipity",
                lambda progress: seed_serendipity(self.neo4j, self.settings),
            ),
        ]

        with create_pipeline_progress(self.settings) as progress:
            main_task = progress.add_task("[cyan]Running analytics...", total=len(_steps))

            for step_name, step_description, step_fn in _steps:
                if shutdown and shutdown.shutdown_requested:
                    return
                if shutdown:
                    shutdown.begin_step(step_name)
                progress.update(main_task, description=f"[cyan]{step_description}...")
                step_fn(progress)
                progress.advance(main_task)
                if shutdown:
                    shutdown.complete_step(step_name)

    def _rebuild_interests_and_record(self, progress: HintGridProgress | None = None) -> None:
        """Rebuild interests and record the timestamp in pipeline state."""
        from datetime import datetime

        rebuild_interests(self.neo4j, self.settings, progress)
        state = self.state_store.load()
        state.last_interests_rebuild_at = datetime.now(UTC).isoformat()
        self.state_store.save(state)

    def run_refresh(self) -> None:
        """Lightweight refresh: apply decay + recompute dirty communities.

        Unlike run_analytics(), this skips clustering and only updates
        interest scores incrementally. Falls back to full rebuild if
        no previous rebuild timestamp exists.
        """
        from datetime import datetime

        state = self.state_store.load()

        if not state.last_interests_rebuild_at:
            logger.info("No previous rebuild timestamp, falling back to full rebuild")
            self._rebuild_interests_and_record()
            return

        refresh_interests(self.neo4j, self.settings, state.last_interests_rebuild_at)

        # Update the timestamp after successful refresh
        state.last_interests_rebuild_at = datetime.now(UTC).isoformat()
        self.state_store.save(state)

    def cleanup_interests(self) -> None:
        """Cleanup expired interests."""
        cleanup_expired_interests(self.neo4j)

    def cleanup_similarity(self) -> None:
        """Prune similarity links."""
        self.neo4j.prune_similarity_links(self.settings)

    def cleanup_inactive(self) -> int:
        """Cascade-delete inactive users and their posts from the graph.

        Users with lastActive older than active_user_days threshold are removed
        along with their posts, relationships, and orphaned communities.

        Returns:
            Number of deleted user nodes
        """
        return cleanup_inactive_users(self.neo4j, self.settings)

    def export_state(self, filename: str, user_id: int | None = None) -> None:
        """Export current system state to Markdown."""
        export_state(self.neo4j, self.redis, self.settings, filename, user_id)

    def clean(
        self,
        *,
        graph: bool = False,
        redis: bool = False,
        models: bool = False,
        embeddings: bool = False,
        clusters: bool = False,
        similarity: bool = False,
        interests: bool = False,
        interactions: bool = False,
        recommendations: bool = False,
        fasttext_state: bool = False,
    ) -> None:
        """Delete data from Neo4j, Redis, and/or model files on disk.

        When no flags are set, cleans everything (backward-compatible).
        When one or more flags are set, only the specified targets are cleaned.

        Computed data flags (embeddings, clusters, etc.) clean only computed data,
        preserving source data (User, Post nodes and their relationships).

        Cascading behavior:
        - clean_embeddings() automatically cleans similarity
        - clean_similarity() automatically cleans post clusters and interests
        - clean_clusters() automatically cleans interests and recommendations
        - clean_interactions() cleans INTERACTS_WITH edges (full refresh each run)
        """
        # Check if any computed data flags are set
        computed_flags = (
            embeddings
            or clusters
            or similarity
            or interests
            or interactions
            or recommendations
            or fasttext_state
        )
        # Check if any basic flags are set
        basic_flags = graph or redis or models

        # If no flags at all, clean everything (backward-compatible)
        clean_all = not basic_flags and not computed_flags

        # Basic cleaning (full graph, redis, models)
        # clean_redis() must be called BEFORE clean_graph() to get user_ids (if needed)
        # NEVER delete all feed keys - always preserve Mastodon entries (score == post_id)
        # Only remove HintGrid entries (score = post_id * multiplier, where multiplier > 1)
        if clean_all or redis or graph:
            # Always preserve Mastodon entries - only remove HintGrid entries
            # This ensures Mastodon entries (score == post_id) are never deleted
            # feed_score_multiplier is validated to be >= 1, so HintGrid entries are always distinguishable
            self.clean_redis()
        if clean_all or graph:
            self.clean_graph()
        if clean_all or models:
            self.clean_models()

        # Computed data cleaning (selective, preserves source data)
        # Methods handle cascading internally, so we call them in dependency order
        # to avoid redundant work when multiple flags are set

        if clean_all or fasttext_state:
            self.clean_fasttext_state()

        # Embeddings cascade to similarity
        if clean_all or embeddings:
            self.clean_embeddings()  # Will cascade to similarity
        elif similarity:
            # Only similarity requested, clean it directly (will cascade to post clusters and interests)
            self.clean_similarity()

        # Clusters: only if not already cascaded from embeddings/similarity
        if clean_all or clusters:
            # If embeddings or similarity were cleaned, post clusters are already cleaned
            # Only clean user clusters in that case, or both if clean_all
            if (embeddings or similarity) and not clean_all:
                # Post clusters already cleaned via cascade, only clean user clusters
                self.clean_clusters(posts=False, users=True)
            else:
                # Clean both user and post clusters (will cascade to interests and recommendations)
                self.clean_clusters()

        # Interactions: full-refresh data, no cascade needed
        if clean_all or interactions:
            self.clean_interactions()

        # Interests: only if explicitly requested and not cascaded
        if interests and not (embeddings or similarity or clusters or clean_all):
            self.clean_interests()

        # Recommendations: only if explicitly requested and not cascaded
        if recommendations and not (clusters or clean_all):
            self.clean_recommendations()

    def clean_graph(self) -> None:
        """Delete all nodes and relationships from Neo4j."""
        if self.neo4j.worker_label:
            self.neo4j.execute_labeled(
                "MATCH (n:__wlabel__) DETACH DELETE n",
                label_map={"wlabel": self.neo4j.worker_label},
            )
        else:
            self.neo4j.execute("MATCH (n) DETACH DELETE n")

    def clean_redis(self) -> None:
        """Remove HintGrid feed recommendations from Redis.

        HintGrid entries have scores above Mastodon entries (rank-based scoring).
        Mastodon entries have score = post_id (multiplier = 1).
        This method removes only HintGrid entries, preserving Mastodon entries.
        Only cleans feeds for local users (remote users never have feeds in our Redis).
        Also cleans public timeline keys (timeline:public, timeline:public:local).
        """
        # Only remove HintGrid entries, preserve Mastodon entries
        # Use stream_local_user_ids() — remote users never have feeds in our Redis
        for user_id in self.neo4j.stream_local_user_ids():
            self.redis.remove_hintgrid_recommendations(user_id, self.settings.feed_score_multiplier)

        # Clean public timelines
        if self.settings.public_feed_enabled:
            from hintgrid.pipeline.feed import namespaced_key

            for timeline_key in (
                self.settings.public_timeline_key,
                self.settings.local_timeline_key,
            ):
                key = namespaced_key(timeline_key, self.settings)
                removed = self.redis.remove_hintgrid_entries_from_key(
                    key,
                    self.settings.feed_score_multiplier,
                )
                if removed > 0:
                    logger.info(
                        "Cleaned %d HintGrid entries from %s",
                        removed,
                        key,
                    )

    def clean_models(self) -> None:
        """Delete FastText model files from disk."""
        model_path = Path(os.path.expanduser(self.settings.fasttext_model_path))
        if not model_path.exists():
            return

        model_patterns = [
            "phrases_v*.pkl",
            "phraser_v*.pkl",
            "fasttext_v*.bin",
            "fasttext_v*.q.bin",
            "fasttext_v*.bin.wv.vectors_ngrams.npy",
        ]
        for pattern in model_patterns:
            for file_path in model_path.glob(pattern):
                try:
                    file_path.unlink()
                    logger.debug("Deleted model file: %s", file_path)
                except Exception as e:
                    logger.warning("Failed to delete model file %s: %s", file_path, e)

    def clean_embeddings(self) -> None:
        """Clear embeddings from posts. Cascades to similarity graph."""
        logger.info("Clearing embeddings from posts")
        self.neo4j.execute_labeled(
            "MATCH (p:__post__) REMOVE p.embedding",
            {"post": "Post"},
        )
        logger.info("Embeddings cleared, cascading to similarity graph")
        # Cascade: similarity depends on embeddings
        self.clean_similarity()

    def clean_similarity(self) -> None:
        """Clear SIMILAR_TO relationships. Cascades to post clusters and interests."""
        logger.info("Clearing SIMILAR_TO relationships")
        self.neo4j.execute_labeled(
            "MATCH (p:__post__)-[r:SIMILAR_TO]->() DELETE r",
            {"post": "Post"},
        )
        logger.info("Similarity graph cleared, cascading to post clusters and interests")
        # Cascade: post clusters and interests depend on similarity
        self.clean_clusters(posts=True, users=False)
        self.clean_interests()

    def clean_clusters(self, *, users: bool = True, posts: bool = True) -> None:
        """Clear cluster assignments and community nodes.

        Args:
            users: If True, clear user clusters (User.cluster_id, UserCommunity, BELONGS_TO)
            posts: If True, clear post clusters (Post.cluster_id, PostCommunity, BELONGS_TO)
        """
        if users:
            logger.info("Clearing user clusters")
            # Remove cluster_id from users
            self.neo4j.execute_labeled(
                "MATCH (u:__user__) REMOVE u.cluster_id",
                {"user": "User"},
            )
            # Delete BELONGS_TO relationships
            self.neo4j.execute_labeled(
                "MATCH (u:__user__)-[r:BELONGS_TO]->(:__uc__) DELETE r",
                {"user": "User", "uc": "UserCommunity"},
            )
            # Delete UserCommunity nodes
            self.neo4j.execute_labeled(
                "MATCH (uc:__uc__) DETACH DELETE uc",
                {"uc": "UserCommunity"},
            )
            logger.info("User clusters cleared")

        if posts:
            logger.info("Clearing post clusters")
            # Remove cluster_id from posts
            self.neo4j.execute_labeled(
                "MATCH (p:__post__) REMOVE p.cluster_id",
                {"post": "Post"},
            )
            # Delete BELONGS_TO relationships
            self.neo4j.execute_labeled(
                "MATCH (p:__post__)-[r:BELONGS_TO]->(:__pc__) DELETE r",
                {"post": "Post", "pc": "PostCommunity"},
            )
            # Delete PostCommunity nodes
            self.neo4j.execute_labeled(
                "MATCH (pc:__pc__) DETACH DELETE pc",
                {"pc": "PostCommunity"},
            )
            logger.info("Post clusters cleared")

        # Cascade: interests and recommendations depend on communities
        if users or posts:
            logger.info("Cascading to interests and recommendations")
            self.clean_interests()
            self.clean_recommendations()

    def clean_interests(self) -> None:
        """Clear INTERESTED_IN relationships."""
        logger.info("Clearing INTERESTED_IN relationships")
        self.neo4j.execute_labeled(
            "MATCH (uc:__uc__)-[i:INTERESTED_IN]->(:__pc__) DELETE i",
            {"uc": "UserCommunity", "pc": "PostCommunity"},
        )
        logger.info("Interests cleared")

    def clean_recommendations(self) -> None:
        """Clear WAS_RECOMMENDED relationships."""
        logger.info("Clearing WAS_RECOMMENDED relationships")
        self.neo4j.execute_labeled(
            "MATCH (u:__user__)-[r:WAS_RECOMMENDED]->(:__post__) DELETE r",
            {"user": "User", "post": "Post"},
        )
        logger.info("Recommendations cleared")

    def clean_interactions(self) -> None:
        """Clear INTERACTS_WITH relationships between users."""
        logger.info("Clearing INTERACTS_WITH relationships")
        self.neo4j.execute_labeled(
            "MATCH (u:__user__)-[r:INTERACTS_WITH]->(:__user__) DELETE r",
            {"user": "User"},
        )
        logger.info("INTERACTS_WITH relationships cleared")

    def clean_fasttext_state(self) -> None:
        """Clear FastTextState node from graph."""
        logger.info("Clearing FastTextState node")
        from hintgrid.embeddings.fasttext_service import STATE_NODE_ID

        self.neo4j.execute_labeled(
            "MATCH (s:__label__ {id: $id}) DETACH DELETE s",
            {"label": "FastTextState"},
            {"id": STATE_NODE_ID},
        )
        logger.info("FastTextState cleared")

    def generate_feed_for_user(
        self,
        user_id: int,
        rel_types: frozenset[str] | None = None,
    ) -> None:
        """Generate feed for specific user and write to Redis."""
        recommendations = generate_user_feed(
            self.neo4j, user_id, self.settings, rel_types=rel_types
        )
        mark_recommended(self.neo4j, user_id, recommendations)
        write_feed_to_redis(self.redis, user_id, recommendations, self.settings)
        set_feed_generated_at(self.neo4j, user_id)

    def generate_feeds_for_all_users(
        self,
        shutdown: ShutdownManager | None = None,
    ) -> None:
        """Generate feeds for users that need refresh, with resumability.

        Uses dirty-user detection to skip users whose graph state hasn't
        changed since their last feed generation. When feed_force_refresh
        is set, falls back to all active users.

        Supports cursor-based resumption: if interrupted, the next run
        skips users already processed (id <= last_feed_user_id).

        When feed_workers > 1, uses ThreadPoolExecutor for concurrent I/O.
        Each user is processed independently (Neo4j sessions and Redis
        pipelines are thread-safe and isolated per call).

        When feed_workers == 1, falls back to sequential processing.

        Checks shutdown flag between users for graceful Ctrl+C handling.
        """
        from hintgrid.cli.console import create_feed_generation_progress

        if shutdown:
            shutdown.begin_step("feed_generation")

        feed_workers = self.settings.feed_workers
        active_days = self.settings.active_user_days
        feed_logger = logging.getLogger(__name__)

        # Load cursor for resumption
        state = self.state_store.load()
        resume_from = state.last_feed_user_id

        rel_types = self.neo4j.get_existing_rel_types()

        # Collect user IDs that need feed refresh
        if self.settings.feed_force_refresh:
            # Force mode: regenerate all active users
            user_ids = [
                uid for uid in self.neo4j.stream_active_user_ids(active_days) if uid > resume_from
            ]
            feed_logger.info(
                "Force refresh: %d active users (skipped %d already processed)",
                len(user_ids),
                resume_from if resume_from > INITIAL_CURSOR else 0,
            )
        else:
            # Selective mode: only dirty users
            user_ids = [
                uid
                for uid in self.neo4j.stream_dirty_user_ids(
                    active_days,
                    self.settings.feed_size,
                    rel_types=rel_types,
                )
                if uid > resume_from
            ]
            feed_logger.info(
                "Selective refresh: %d dirty users need feed update",
                len(user_ids),
            )

        total_users = len(user_ids)
        users_processed = 0
        checkpoint_interval = self.settings.checkpoint_interval

        with create_feed_generation_progress(self.settings) as progress:
            workers_label = f" ({feed_workers} workers)" if feed_workers > 1 else ""
            mode_label = "force" if self.settings.feed_force_refresh else "selective"
            task = progress.add_task(
                f"[cyan]Generating feeds ({mode_label}){workers_label}...",
                total=total_users if total_users > 0 else None,
            )

            if feed_workers <= 1:
                # Sequential mode (backward compatible)
                for user_id in user_ids:
                    self.generate_feed_for_user(user_id, rel_types=rel_types)
                    users_processed += 1
                    progress.advance(task)

                    # Checkpoint cursor periodically
                    if users_processed % checkpoint_interval == 0:
                        state.last_feed_user_id = user_id
                        self.state_store.save(state)

                    if shutdown and shutdown.shutdown_requested:
                        # Save cursor on interruption
                        state.last_feed_user_id = user_id
                        self.state_store.save(state)
                        break
            else:
                # Concurrent mode: process in parallel
                errors = 0
                with ThreadPoolExecutor(max_workers=feed_workers) as pool:
                    futures = {
                        pool.submit(self.generate_feed_for_user, uid, rel_types): uid
                        for uid in user_ids
                    }
                    max_completed_uid = resume_from
                    for future in as_completed(futures):
                        uid = futures[future]
                        try:
                            future.result()
                        except Exception:
                            errors += 1
                            feed_logger.exception(
                                "Failed to generate feed for user %d",
                                uid,
                            )
                        users_processed += 1
                        if uid > max_completed_uid:
                            max_completed_uid = uid
                        progress.advance(task)

                        # Checkpoint cursor periodically
                        if users_processed % checkpoint_interval == 0:
                            state.last_feed_user_id = max_completed_uid
                            self.state_store.save(state)

                        if shutdown and shutdown.shutdown_requested:
                            # Cancel remaining queued futures
                            for f in futures:
                                f.cancel()
                            state.last_feed_user_id = max_completed_uid
                            self.state_store.save(state)
                            break

                    # Final cursor save after all futures complete
                    if not (shutdown and shutdown.shutdown_requested):
                        state.last_feed_user_id = max_completed_uid
                        self.state_store.save(state)

                if errors > 0:
                    feed_logger.warning(
                        "Feed generation completed with %d errors out of %d users",
                        errors,
                        total_users,
                    )

        if shutdown:
            if shutdown.shutdown_requested:
                shutdown.update_step_progress("feed_generation", users_processed)
            else:
                shutdown.complete_step("feed_generation", users_processed)

    def generate_public_feeds(self) -> None:
        """Generate and write public timeline recommendations to Redis.

        Fills timeline:public and timeline:public:local with ranked
        recommendations based on community interests. Strategy is
        configured via public_feed_strategy setting:

        - "local_communities": both timelines use only communities
          with local users (default, more economical)
        - "all_communities": global timeline uses all communities,
          local timeline uses only local communities

        Scoring uses rank-based interest ordering with multiplier
        to outrank native Mastodon entries.
        """
        if not self.settings.public_feed_enabled:
            logger.info("Public feed generation disabled")
            return

        rel_types = self.neo4j.get_existing_rel_types()
        strategy = self.settings.public_feed_strategy
        use_all_for_global = strategy == "all_communities"

        # Generate global public feed (timeline:public)
        logger.info(
            "Generating public feed (strategy=%s, global_interests=%s)",
            strategy,
            "all" if use_all_for_global else "local",
        )
        global_recs = generate_public_feed(
            self.neo4j,
            self.settings,
            local_only_interests=not use_all_for_global,
            local_only_authors=False,
            rel_types=rel_types,
        )
        if global_recs:
            write_public_feed_to_redis(
                self.redis,
                self.settings.public_timeline_key,
                global_recs,
                self.settings,
            )
            logger.info(
                "Public feed: %d recommendations written to %s",
                len(global_recs),
                self.settings.public_timeline_key,
            )

        # Generate local public feed (timeline:public:local)
        local_recs = generate_public_feed(
            self.neo4j,
            self.settings,
            local_only_interests=True,
            local_only_authors=True,
            rel_types=rel_types,
        )
        if local_recs:
            write_public_feed_to_redis(
                self.redis,
                self.settings.local_timeline_key,
                local_recs,
                self.settings,
            )
            logger.info(
                "Local feed: %d recommendations written to %s",
                len(local_recs),
                self.settings.local_timeline_key,
            )

    def get_user_id(self, handle: str) -> int | None:
        """Resolve Mastodon user handle to account id."""
        username, domain = parse_account_handle(handle)
        return self.postgres.fetch_user_id(username, domain)

    def get_user_info_by_handle(self, handle: str) -> UserInfo | None:
        """Get detailed user information by handle.

        Args:
            handle: Mastodon user handle (@user or @user@domain)

        Returns:
            UserInfo dictionary or None if user not found
        """
        from hintgrid.pipeline.stats import get_user_info

        user_id = self.get_user_id(handle)
        if user_id is None:
            return None

        return get_user_info(self.neo4j, self.postgres, user_id)

    def reindex_embeddings(self, *, dry_run: bool = False) -> dict[str, object]:
        """Force re-indexing of all embeddings.

        Drops vector index and recomputes embeddings for all existing posts.
        Does NOT reset cursor - incremental loading continues from last position.
        Preserves all relationships and cluster assignments.

        Args:
            dry_run: If True, only report what would happen

        Returns:
            Dictionary with migration results
        """
        result = force_reindex(self.neo4j, self.settings, self.state_store, dry_run=dry_run)

        reembedded = 0
        if result.migrated and result.posts_cleared > 0:
            # Actually reembed the posts
            from hintgrid.embeddings.provider import EmbeddingProvider

            embedding_client = EmbeddingProvider(self.settings, self.neo4j)
            reembedded = reembed_existing_posts(
                self.neo4j,
                embedding_client,
                self.settings,
                batch_size=self.settings.batch_size,
            )

            # Recreate vector index with correct dimensions
            ensure_graph_indexes(self.neo4j, self.settings)

        return {
            "migrated": result.migrated,
            "previous_signature": result.previous_signature,
            "current_signature": result.current_signature,
            "posts_cleared": result.posts_cleared,
            "posts_reembedded": reembedded,
            "dry_run": dry_run,
        }

    def check_embeddings(self) -> dict[str, str | bool]:
        """Get current embedding configuration status.

        Returns:
            Dictionary with stored/current signatures and match status
        """
        return get_embedding_status(self.settings, self.state_store)

    def train_full(self, since_date: datetime | None = None) -> bool:
        """Perform full training of embedding models.

        Args:
            since_date: Optional date filter for training data

        Returns:
            True if training succeeded
        """
        from hintgrid.embeddings.provider import TrainableEmbeddingProvider

        logger.info("=== Starting full embedding training ===")
        provider = TrainableEmbeddingProvider(self.settings, self.neo4j, self.postgres)

        if not provider.supports_training:
            logger.warning("Training not supported for current embedding provider")
            return False

        success = provider.train_full(since_date)
        if success:
            logger.info("=== Full training completed successfully ===")
        else:
            logger.error("=== Full training failed ===")

        return success

    def train_incremental(self) -> bool:
        """Perform incremental training of embedding models.

        Returns:
            True if training succeeded
        """
        from hintgrid.embeddings.provider import TrainableEmbeddingProvider
        from hintgrid.cli.console import print_warning

        logger.info("=== Starting incremental embedding training ===")

        # Check if FastText state exists (for FastText provider)
        if not self.settings.llm_base_url:
            # FastText provider - check state
            from hintgrid.embeddings.fasttext_service import STATE_NODE_ID

            state_result = self.neo4j.execute_and_fetch_labeled(
                "MATCH (s:__label__ {id: $id}) RETURN count(s) AS count",
                {"label": "FastTextState"},
                {"id": STATE_NODE_ID},
            )
            state_exists = coerce_int(state_result[0].get("count")) if state_result else 0 > 0
            if not state_exists:
                print_warning(
                    "FastTextState not found - incremental training will fall back to full training"
                )
                logger.warning(
                    "FastTextState not found, incremental training will perform full training"
                )

        provider = TrainableEmbeddingProvider(self.settings, self.neo4j, self.postgres)

        if not provider.supports_training:
            logger.warning("Training not supported for current embedding provider")
            return False

        success = provider.train_incremental()
        if success:
            logger.info("=== Incremental training completed successfully ===")
        else:
            logger.error("=== Incremental training failed ===")

        return success

    def _collect_graph_counts(
        self,
        metrics: PipelineMetrics,
    ) -> None:
        """Populate metrics with current node/relationship counts."""
        rows = self.neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__) RETURN count(u) AS cnt",
            {"user": "User"},
        )
        if rows:
            metrics["user_count"] = coerce_int(rows[0].get("cnt", 0))

        rows = self.neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) RETURN count(p) AS cnt",
            {"post": "Post"},
        )
        if rows:
            metrics["post_count"] = coerce_int(rows[0].get("cnt", 0))

        rel_types = self.neo4j.get_existing_rel_types()
        if "INTERACTS_WITH" in rel_types:
            rows = self.neo4j.execute_and_fetch_labeled(
                "MATCH ()-[r:INTERACTS_WITH]->() RETURN count(r) AS cnt",
                {},
            )
            if rows:
                metrics["interaction_count"] = coerce_int(rows[0].get("cnt", 0))

    def _collect_cluster_counts(
        self,
        metrics: PipelineMetrics,
    ) -> None:
        """Populate metrics with community counts after analytics."""
        rows = self.neo4j.execute_and_fetch_labeled(
            "MATCH (uc:__uc__) RETURN count(uc) AS cnt",
            {"uc": "UserCommunity"},
        )
        if rows:
            metrics["user_communities"] = coerce_int(rows[0].get("cnt", 0))

        rows = self.neo4j.execute_and_fetch_labeled(
            "MATCH (pc:__pc__) RETURN count(pc) AS cnt",
            {"pc": "PostCommunity"},
        )
        if rows:
            metrics["post_communities"] = coerce_int(rows[0].get("cnt", 0))

    def run_full_pipeline(
        self,
        *,
        dry_run: bool = False,
        user_id: int | None = None,
        shutdown: ShutdownManager | None = None,
    ) -> None:
        """Run complete pipeline: load data, run analytics, generate feeds.

        If embedding configuration changed, existing posts in Neo4j are
        reembedded first (without reloading from PostgreSQL).

        Args:
            dry_run: If True, skip writing feeds to Redis.
            user_id: If provided, generate feed only for this user.
                     If None, generate feeds for all users.
            shutdown: Optional shutdown manager for graceful Ctrl+C handling.
        """
        import time as _time

        from hintgrid.cli.console import (
            console,
            print_pipeline_complete,
            print_pipeline_summary,
            print_pipeline_start,
            print_settings_table,
            print_step,
            print_success,
            print_warning,
        )

        print_pipeline_start()
        print_settings_table(self.settings)

        if shutdown:
            shutdown.register_steps()

        # Reset feed cursor at the start of each full pipeline run
        # so that feed generation processes all dirty users from scratch
        state = self.state_store.load()
        state.last_feed_user_id = INITIAL_CURSOR
        self.state_store.save(state)

        pipeline_start = _time.monotonic()
        metrics: PipelineMetrics = {"warnings": [], "dry_run": dry_run}

        total_steps = 3
        current_step = 0

        # If embedding config changed, reembed existing posts first
        if self._migration_result.migrated:
            posts_count = count_posts_in_neo4j(self.neo4j)
            if posts_count > 0:
                print_step(0, total_steps, f"Reembedding {posts_count:,} posts with new config...")
                with console.status("[bold blue]Reembedding posts...[/bold blue]"):
                    from hintgrid.embeddings.provider import EmbeddingProvider

                    embedding_client = EmbeddingProvider(self.settings, self.neo4j)
                    reembedded = reembed_existing_posts(
                        self.neo4j,
                        embedding_client,
                        self.settings,
                        batch_size=self.settings.batch_size,
                    )
                print_success(f"Reembedded {reembedded:,} existing posts")

                # Recreate vector index with new dimensions
                ensure_graph_indexes(self.neo4j, self.settings)

        current_step = 1
        print_step(current_step, total_steps, "Loading new data from PostgreSQL...")
        load_start = _time.monotonic()
        with console.status("[bold blue]Loading data...[/bold blue]"):
            self.load_data(shutdown=shutdown)

        if shutdown and shutdown.shutdown_requested:
            return

        metrics["load_duration_s"] = _time.monotonic() - load_start
        print_success("Data loaded")

        # Show graph statistics after data loading
        from hintgrid.pipeline.stats import show_graph_overview_after_loading

        show_graph_overview_after_loading(self.neo4j)

        # Collect graph counts for summary
        self._collect_graph_counts(metrics)

        current_step = 2
        print_step(current_step, total_steps, "Running analytics (clustering, interests)...")

        # Check if embeddings exist before running analytics
        if not check_embeddings_exist(self.neo4j):
            print_warning("No embeddings found. Analytics may produce incomplete results.")
            logger.warning("No embeddings found in graph - clustering will be limited")
            metrics["warnings"].append("No embeddings found")

        # Check if clusters exist before running analytics (warn if missing)
        users_exist_before, posts_exist_before = check_clusters_exist(self.neo4j)
        if not users_exist_before:
            print_warning("No user clusters found. Will attempt to create them during analytics.")
            logger.warning("No user clusters found before analytics")
        if not posts_exist_before:
            print_warning("No post clusters found. Will attempt to create them during analytics.")
            logger.warning("No post clusters found before analytics")

        analytics_start = _time.monotonic()
        with console.status("[bold blue]Running analytics...[/bold blue]"):
            self.run_analytics(shutdown=shutdown)

        if shutdown and shutdown.shutdown_requested:
            return

        metrics["analytics_duration_s"] = _time.monotonic() - analytics_start
        print_success("Analytics complete")

        # Check if clusters were created after analytics
        users_exist_after, posts_exist_after = check_clusters_exist(self.neo4j)
        if not users_exist_after:
            print_warning("No user clusters found after analytics")
            metrics["warnings"].append("No user clusters")
        if not posts_exist_after:
            print_warning("No post clusters found after analytics")
            metrics["warnings"].append("No post clusters")

        # Check if interests were created
        if not check_interests_exist(self.neo4j):
            print_warning("No interests found after analytics")
            metrics["warnings"].append("No interests")

        # Collect cluster counts
        self._collect_cluster_counts(metrics)

        current_step = 3
        if not dry_run:
            print_step(current_step, total_steps, "Generating feeds and writing to Redis...")
            feeds_start = _time.monotonic()
            with console.status("[bold blue]Generating feeds...[/bold blue]"):
                if user_id is not None:
                    self.generate_feed_for_user(user_id)
                    if shutdown:
                        shutdown.begin_step("feed_generation")
                        shutdown.complete_step("feed_generation", 1)
                else:
                    self.generate_feeds_for_all_users(shutdown=shutdown)

            if shutdown and shutdown.shutdown_requested:
                return

            metrics["feeds_duration_s"] = _time.monotonic() - feeds_start
            print_success("Feeds generated")

            # Generate public timeline feeds after home feeds
            if self.settings.public_feed_enabled:
                print_step(
                    current_step,
                    total_steps,
                    "Generating public timeline recommendations...",
                )
                with console.status("[bold blue]Generating public feeds...[/bold blue]"):
                    self.generate_public_feeds()
                print_success("Public timeline feeds generated")
        else:
            print_step(current_step, total_steps, "Dry run - skipping feed generation")
            print_warning("Feeds not written (dry run mode)")
            if shutdown:
                shutdown.begin_step("feed_generation")
                shutdown.complete_step("feed_generation")

        metrics["total_duration_s"] = _time.monotonic() - pipeline_start
        print_pipeline_complete()
        print_pipeline_summary(metrics)

        # Show recommendations table for single user mode
        if user_id is not None:
            from hintgrid.cli.console import console, print_recommendations_table
            from hintgrid.pipeline.feed import get_detailed_recommendations

            try:
                # Get detailed recommendations
                detailed_recs = get_detailed_recommendations(
                    self.neo4j,
                    user_id,
                    self.settings,
                    rel_types=self.neo4j.get_existing_rel_types(),
                )

                if detailed_recs:
                    # Get user account info (username, domain) for table title
                    user_account_info = self.postgres.fetch_account_info([user_id])
                    user_account = user_account_info.get(user_id, {})

                    # Get author IDs
                    author_ids = [
                        rec["author_id"] for rec in detailed_recs if rec.get("author_id", 0) > 0
                    ]
                    author_info = self.postgres.fetch_account_info(author_ids)

                    # Create minimal user_info dict for print_recommendations_table
                    # (only username and domain are used for the table title)
                    user_info: UserInfo = {
                        "user_id": user_id,
                        "username": user_account.get("username"),
                        "domain": user_account.get("domain"),
                        "is_local": False,  # Default value for TypedDict
                    }

                    # Print recommendations table
                    print_recommendations_table(
                        detailed_recs,
                        user_info,
                        author_info,
                        max_items=10,
                        text_preview_limit=50,
                    )
                else:
                    console.print(f"[yellow]⚠ No recommendations found for user {user_id}[/yellow]")
            except Exception as e:
                logger.exception("Error displaying recommendations table: %s", e)
                print_warning("Failed to display recommendations table")


def parse_account_handle(handle: str) -> tuple[str, str | None]:
    """Parse a Mastodon handle into username and domain.

    Accepts formats:
        - @username
        - username
        - @username@domain
        - username@domain
    """
    cleaned = handle.strip()
    if cleaned.startswith("@"):
        cleaned = cleaned[1:]
    if not cleaned:
        raise ValueError("Handle is empty.")

    parts = cleaned.split("@")
    if len(parts) == 1:
        return parts[0], None
    if len(parts) == 2 and parts[0] and parts[1]:
        return parts[0], parts[1]
    raise ValueError("Invalid handle format. Expected @username or @username@domain.")


def main() -> None:
    """CLI entrypoint for HintGrid application using Typer."""
    from hintgrid.cli import app as cli_app

    cli_app()


if __name__ == "__main__":
    main()
