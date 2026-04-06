"""Community detection and clustering using Neo4j GDS."""

from __future__ import annotations

import logging
import re
import uuid
from typing import TYPE_CHECKING, LiteralString

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient, Neo4jValue
    from hintgrid.cli.progress_display import HintGridProgress
    from hintgrid.state import StateStore

    from rich.progress import TaskID

from hintgrid.cli.console import console, print_step, print_success
from hintgrid.config import HintGridSettings
from hintgrid.pipeline.community_structure import (
    create_post_community_structure,
    create_user_community_structure,
)
from hintgrid.pipeline.singleton_cluster_collapse import (
    collapse_singleton_post_clusters,
    collapse_singleton_user_clusters,
)
from hintgrid.pipeline.leiden_diagnostics import (
    collect_post_similarity_graph_stats,
    collect_user_interaction_graph_stats,
    leiden_write_yield_clause,
    log_leiden_clustering_diagnostics,
)
from hintgrid.utils.coercion import coerce_float, coerce_int

logger = logging.getLogger(__name__)

# Clustering step counts
USER_CLUSTERING_STEPS = 4
POST_CLUSTERING_STEPS = 4

# Domain-specific constants for GDS graph names
# These are used in Cypher queries where parameters are not supported
USER_GRAPH_NAME = "user-graph"
POST_GRAPH_NAME = "post-graph"
POST_EMBEDDING_INDEX_BASE_NAME = "post_embedding_index"
DEFAULT_CLUSTER_ID = 0
MIN_KNN_NEIGHBORS = 1


def _get_embedding_index_name(neo4j: Neo4jClient) -> str:
    """Get worker-specific embedding index name for label-based isolation.

    Dynamic Indexing approach:
    - If worker_label is set: use "{worker_label}_posts" (e.g., worker_gw0_posts)
    - If no worker_label: use global "post_embedding_index"

    This matches the index names created in _create_vector_index().
    """
    if neo4j.worker_label:
        return f"{neo4j.worker_label}_posts"
    return POST_EMBEDDING_INDEX_BASE_NAME


def _get_user_graph_name(neo4j: Neo4jClient) -> str:
    """Get user graph name with worker isolation if needed.

    Args:
        neo4j: Neo4j client with optional worker_label

    Returns:
        Graph name (e.g., "worker_gw0-user-graph" or "user-graph")
    """
    if neo4j.worker_label:
        return f"{neo4j.worker_label}-{USER_GRAPH_NAME}"
    return USER_GRAPH_NAME


def _get_post_graph_name(neo4j: Neo4jClient) -> str:
    """Get post graph name with worker isolation if needed.

    Args:
        neo4j: Neo4j client with optional worker_label

    Returns:
        Graph name (e.g., "worker_gw0-post-graph" or "post-graph")
    """
    if neo4j.worker_label:
        return f"{neo4j.worker_label}-{POST_GRAPH_NAME}"
    return POST_GRAPH_NAME


def _get_pagerank_graph_name(neo4j: Neo4jClient) -> str:
    """Get pagerank graph name with worker isolation if needed.

    Args:
        neo4j: Neo4j client with optional worker_label

    Returns:
        Graph name (e.g., "worker_gw0-pagerank-graph" or "pagerank-graph")
    """
    base_name = "pagerank-graph"
    if neo4j.worker_label:
        return f"{neo4j.worker_label}-{base_name}"
    return base_name


# Removed _is_str_dict - replaced with direct isinstance checks


def _extract_vector_dimension(row: dict[str, Neo4jValue]) -> int | None:
    """Extract vector.dimensions from SHOW INDEXES row options.

    Neo4j SHOW INDEXES returns nested dicts
    (options → indexConfig → vector.dimensions).
    We drill down with hasattr-based checks at each level instead of isinstance.
    """
    raw_options: Neo4jValue = row.get("options")
    # Use hasattr instead of isinstance for dict-like check
    # Runtime guarantee: if has get/items, it's a dict-like object compatible with dict[str, Neo4jValue]
    if raw_options is None or not (hasattr(raw_options, "get") and hasattr(raw_options, "items")):
        return None
    # Use dict-like access - runtime will work correctly
    # Neo4jValue includes Mapping[str, Neo4jValue], so this is compatible
    options = raw_options
    raw_config: Neo4jValue = options.get("indexConfig")
    if raw_config is None or not (hasattr(raw_config, "get") and hasattr(raw_config, "items")):
        return None
    config = raw_config
    dim: Neo4jValue = config.get("vector.dimensions")
    if dim is None:
        return None
    return int(str(dim))


# Pattern for safe GDS identifiers (letters, digits, hyphens, underscores)
_SAFE_GDS_NAME_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9_-]*$")


def validate_gds_name(name: str) -> str:
    """Validate that a GDS graph/index name is safe for interpolation.

    GDS procedures don't support parameters for graph names,
    so we must use string interpolation. This validates the name
    contains only safe characters.

    Raises:
        ValueError: If name contains unsafe characters
    """
    if not _SAFE_GDS_NAME_PATTERN.match(name):
        raise ValueError(f"Invalid GDS name: {name!r}. Must match {_SAFE_GDS_NAME_PATTERN.pattern}")
    return name


# Validate constants at module load time
validate_gds_name(USER_GRAPH_NAME)
validate_gds_name(POST_GRAPH_NAME)
validate_gds_name(POST_EMBEDDING_INDEX_BASE_NAME)


def run_user_clustering(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    progress: HintGridProgress | None = None,
) -> None:
    """Run Leiden clustering on User nodes via INTERACTS_WITH relationships.

    INTERACTS_WITH includes aggregated interactions (likes, replies, reblogs,
    mentions) and optionally FOLLOWS relationships (if follows_weight > 0).
    All relationships have weights, so relationshipWeightProperty is always used.

    Inactive users are handled separately by cleanup_inactive_users()
    and stream_active_user_ids() — clustering runs on all users currently
    present in the graph.
    """
    logger.info("  Running user clustering (Leiden)...")
    console.print("[bold magenta]User Clustering[/bold magenta]")
    user_graph_name = _get_user_graph_name(neo4j)
    validate_gds_name(user_graph_name)

    # Step 1: Check data availability
    print_step(1, USER_CLUSTERING_STEPS, "Checking users and relationships...")
    users_result = neo4j.execute_and_fetch_labeled(
        "MATCH (u:__user__) RETURN count(u) AS count",
        {"user": "User"},
    )
    users_count = coerce_int(users_result[0]["count"]) if users_result else 0

    interactions_result = neo4j.execute_and_fetch_labeled(
        "MATCH (u:__user__)-[r:INTERACTS_WITH]->(v:__user__) RETURN count(r) AS count",
        {"user": "User"},
    )
    interactions_count = coerce_int(interactions_result[0]["count"]) if interactions_result else 0

    logger.info(
        "  Found %d users, %d INTERACTS_WITH relationships (includes FOLLOWS if follows_weight > 0)",
        users_count,
        interactions_count,
    )

    if users_count == 0:
        logger.warning("No users found, skipping clustering")
        console.print("[yellow]  No users found, skipping[/yellow]")
        return

    # Step 2: Run Leiden algorithm
    print_step(2, USER_CLUSTERING_STEPS, f"Running Leiden on {users_count:,} users...")

    leiden_result: dict[str, Neo4jValue] | None = None
    if interactions_count == 0:
        logger.warning(
            "No INTERACTS_WITH relationships found, assigning all users to single community"
        )
        neo4j.execute_labeled(
            "MATCH (u:__user__) SET u.cluster_id = $cluster_id",
            {"user": "User"},
            {"cluster_id": DEFAULT_CLUSTER_ID},
        )
    else:
        # Drop existing graph if it exists
        try:
            neo4j.execute(
                "CALL gds.graph.drop($graph_name) YIELD graphName",
                {"graph_name": user_graph_name},
            )
        except Exception:
            pass  # Graph doesn't exist yet

        # Project graph into GDS catalog
        # Only INTERACTS_WITH is used (FOLLOWS is included via SQL if follows_weight > 0)
        project_label = neo4j.worker_label or "User"

        with console.status("[bold blue]Projecting graph...[/bold blue]"):
            neo4j.execute_labeled(
                "CALL gds.graph.project("
                "  '__graph_name__', '__node_label__', "
                "  {INTERACTS_WITH: {orientation: 'UNDIRECTED', "
                "   properties: 'weight'}}"
                ")",
                ident_map={
                    "graph_name": user_graph_name,
                    "node_label": project_label,
                },
            )

        pre_leiden_stats = None
        if settings.leiden_diagnostics:
            pre_leiden_stats = collect_user_interaction_graph_stats(neo4j)

        yield_fragment = leiden_write_yield_clause(
            extended=settings.leiden_diagnostics,
        )
        user_leiden_cypher: LiteralString = (
            "CALL gds.leiden.write('__graph_name__', {"
            "  writeProperty: 'cluster_id',"
            "  gamma: $gamma,"
            "  maxLevels: $max_levels,"
            "  relationshipWeightProperty: 'weight'"
            "}) " + yield_fragment
        )

        # Run Leiden on projected graph
        # Always use relationshipWeightProperty since all edges have weights
        with console.status("[bold blue]Running Leiden algorithm...[/bold blue]"):
            result = neo4j.execute_and_fetch_labeled(
                user_leiden_cypher,
                params={
                    "gamma": settings.leiden_resolution,
                    "max_levels": settings.leiden_max_levels,
                },
                ident_map={"graph_name": user_graph_name},
            )
        if result:
            community_count = coerce_int(result[0].get("communityCount", 0))
            print_success(f"Found {community_count:,} communities")
            leiden_result = result[0]
            if settings.leiden_diagnostics:
                log_leiden_clustering_diagnostics(
                    logger,
                    graph_kind="user_interaction",
                    settings=settings,
                    pre_stats=pre_leiden_stats,
                    leiden_row=result[0],
                )
            else:
                logger.info("Leiden result: %s", result[0])

        # Drop projection
        neo4j.execute(
            "CALL gds.graph.drop($graph_name) YIELD graphName",
            {"graph_name": user_graph_name},
        )

    # Step 3: Create community structure
    print_step(3, USER_CLUSTERING_STEPS, "Creating community structure...")
    collapse_singleton_user_clusters(neo4j, settings, progress)
    create_user_community_structure(neo4j, settings, progress)

    # Step 4: Update community sizes
    print_step(4, USER_CLUSTERING_STEPS, "Updating community sizes...")

    neo4j.execute_labeled(
        "MATCH (u:__user__)-[:BELONGS_TO]->(uc:__uc__) "
        "WITH uc, count(u) AS size SET uc.size = size",
        {"user": "User", "uc": "UserCommunity"},
    )

    print_success("User clustering completed")
    logger.info("  User clustering completed")

    # Show user community statistics
    from hintgrid.pipeline.stats import show_user_community_stats
    from hintgrid.utils.coercion import coerce_float

    modularity = None
    if leiden_result:
        modularity = coerce_float(leiden_result.get("modularity"))
    show_user_community_stats(neo4j, postgres=None, modularity=modularity)


def run_post_clustering(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    state_store: StateStore,
    progress: HintGridProgress | None = None,
) -> None:
    """Run Vector Index similarity + Leiden clustering on Post nodes."""
    logger.info("  Running post clustering (Vector Index + Leiden)...")
    console.print("[bold magenta]Post Clustering[/bold magenta]")

    post_graph_name = _get_post_graph_name(neo4j)
    validate_gds_name(post_graph_name)

    # Step 1: Build SIMILAR_TO graph using Vector Index
    print_step(1, POST_CLUSTERING_STEPS, "Building similarity graph...")

    if progress is None:
        with console.status("[bold blue]Computing vector similarities...[/bold blue]"):
            _build_similarity_graph_vector_index(neo4j, settings, state_store, progress)
    else:
        _build_similarity_graph_vector_index(neo4j, settings, state_store, progress)

    # Step 2: Project and run Leiden
    print_step(2, POST_CLUSTERING_STEPS, "Running Leiden clustering...")

    # Check SIMILAR_TO relationships before projecting (GDS fails if type missing)
    similarity_result = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__)-[r:SIMILAR_TO]->(q:__post__) RETURN count(r) AS count",
        {"post": "Post"},
    )
    similarity_count = coerce_int(similarity_result[0]["count"]) if similarity_result else 0

    leiden_result = None
    if similarity_count == 0:
        logger.warning("No SIMILAR_TO relationships found; assigning single community")
        neo4j.execute_labeled(
            "MATCH (p:__post__) SET p.cluster_id = $cluster_id",
            {"post": "Post"},
            {"cluster_id": DEFAULT_CLUSTER_ID},
        )
    else:
        # Drop existing graph if exists
        try:
            neo4j.execute(
                "CALL gds.graph.drop($graph_name) YIELD graphName",
                {"graph_name": post_graph_name},
            )
        except Exception:
            pass  # Graph doesn't exist yet

        # Project SIMILAR_TO graph using native projection with UNDIRECTED orientation
        # Date filtering is already applied when building SIMILAR_TO edges
        # (in _build_similarity_graph_vector_index), so no need to re-filter here
        project_label = neo4j.worker_label or "Post"
        with console.status("[bold blue]Projecting graph...[/bold blue]"):
            neo4j.execute_labeled(
                "CALL gds.graph.project("
                "  '__graph_name__', '__node_label__', "
                "  {SIMILAR_TO: {orientation: 'UNDIRECTED', properties: 'weight'}}"
                ")",
                ident_map={
                    "graph_name": post_graph_name,
                    "node_label": project_label,
                },
            )
        pre_similarity_stats = None
        if settings.leiden_diagnostics:
            pre_similarity_stats = collect_post_similarity_graph_stats(neo4j)

        post_yield_fragment = leiden_write_yield_clause(
            extended=settings.leiden_diagnostics,
        )
        post_leiden_cypher: LiteralString = (
            "CALL gds.leiden.write('__graph_name__', {"
            "  writeProperty: 'cluster_id',"
            "  relationshipWeightProperty: 'weight',"
            "  gamma: $gamma,"
            "  maxLevels: $max_levels"
            "}) " + post_yield_fragment
        )

        # Run Leiden on similarity graph
        with console.status("[bold blue]Running Leiden algorithm...[/bold blue]"):
            result = neo4j.execute_and_fetch_labeled(
                post_leiden_cypher,
                params={
                    "gamma": settings.leiden_resolution,
                    "max_levels": settings.leiden_max_levels,
                },
                ident_map={"graph_name": post_graph_name},
            )
        if result:
            community_count = coerce_int(result[0].get("communityCount", 0))
            print_success(f"Found {community_count:,} post communities")
            leiden_result = result[0]
            if settings.leiden_diagnostics:
                log_leiden_clustering_diagnostics(
                    logger,
                    graph_kind="post_similarity",
                    settings=settings,
                    pre_stats=pre_similarity_stats,
                    leiden_row=result[0],
                )
            else:
                logger.info("Leiden result: %s", result[0])

        # Drop projection (only when graph was created)
        neo4j.execute(
            "CALL gds.graph.drop($graph_name) YIELD graphName",
            {"graph_name": post_graph_name},
        )

    # Step 3: Create community structure
    print_step(3, POST_CLUSTERING_STEPS, "Creating community structure...")
    collapse_singleton_post_clusters(neo4j, settings, progress)
    create_post_community_structure(neo4j, settings, progress)

    # Step 4: Update community sizes
    print_step(4, POST_CLUSTERING_STEPS, "Updating community sizes...")

    neo4j.execute_labeled(
        "MATCH (p:__post__)-[:BELONGS_TO]->(pc:__pc__) "
        "WITH pc, count(p) AS size SET pc.size = size",
        {"post": "Post", "pc": "PostCommunity"},
    )

    print_success("Post clustering completed")
    logger.info("  Post clustering completed")

    # Show post community statistics
    from hintgrid.pipeline.stats import show_post_community_stats
    from hintgrid.utils.coercion import coerce_float

    modularity = None
    if leiden_result:
        modularity = coerce_float(leiden_result.get("modularity"))
    show_post_community_stats(neo4j, postgres=None, modularity=modularity)


def run_similarity_pruning(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    progress: HintGridProgress | None = None,
) -> None:
    """Prune SIMILAR_TO relationships as a separate pipeline step.

    Runs after PageRank so that PageRank can use the similarity graph.
    Controlled by prune_after_clustering and similarity_pruning settings.
    """
    if not settings.prune_after_clustering:
        logger.info("Similarity pruning disabled (prune_after_clustering=False)")
        console.print("[dim]Similarity pruning disabled[/dim]")
        return

    logger.info("Running similarity pruning (strategy=%s)", settings.similarity_pruning)
    console.print(
        f"[bold magenta]Similarity Pruning[/bold magenta] "
        f"[dim](strategy={settings.similarity_pruning})[/dim]"
    )
    neo4j.prune_similarity_links(settings, progress=progress)
    print_success("Similarity pruning completed")


def _diagnose_similarity_prerequisites(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    embedding_index_name: str,
    top_k: int,
) -> dict[str, Neo4jValue]:
    """Collect diagnostic information before building similarity graph.

    Args:
        neo4j: Neo4j client
        settings: HintGrid settings
        embedding_index_name: Name of the vector index (from _get_embedding_index_name)
        top_k: Number of neighbors to query (for test query)

    Returns dictionary with:
    - vector_index_exists: bool
    - vector_index_state: str | None (ONLINE, POPULATING, FAILED)
    - vector_index_dimensions: int | None
    - posts_with_embeddings: int
    - posts_within_recency: int
    - posts_eligible: int (same as posts_within_recency)
    - sample_query_works: bool
    - sample_post_id: int | None
    - sample_neighbors_found: int
    - sample_scores: list[float]
    - sample_above_threshold: int
    """
    diagnostics: dict[str, Neo4jValue] = {
        "vector_index_name": embedding_index_name,
        "vector_index_exists": False,
        "vector_index_state": None,
        "vector_index_dimensions": None,
        "posts_with_embeddings": 0,
        "posts_within_recency": 0,
        "posts_eligible": 0,
        "sample_query_works": False,
        "sample_post_id": None,
        "sample_neighbors_found": 0,
        "sample_scores": [],
        "sample_above_threshold": 0,
    }

    # Check vector index existence and status
    try:
        index_result = list(
            neo4j.execute_and_fetch(
                "SHOW INDEXES YIELD name, type, state, options "
                "WHERE name = $name AND type = 'VECTOR' "
                "RETURN name, state, options",
                {"name": embedding_index_name},
            )
        )
        if index_result:
            diagnostics["vector_index_exists"] = True
            diagnostics["vector_index_state"] = index_result[0].get("state")
            diagnostics["vector_index_dimensions"] = _extract_vector_dimension(index_result[0])
    except Exception as exc:
        logger.debug("Index check failed: %s", exc)

    # Count posts with embeddings
    try:
        with_embeddings_result = neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) WHERE p.embedding IS NOT NULL RETURN count(p) AS count",
            {"post": "Post"},
        )
        if with_embeddings_result:
            diagnostics["posts_with_embeddings"] = coerce_int(
                with_embeddings_result[0].get("count", 0)
            )
    except Exception as exc:
        logger.debug("Posts with embeddings count failed: %s", exc)

    # Count posts within recency window
    try:
        within_recency_result = neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) "
            "WHERE p.embedding IS NOT NULL "
            "  AND p.createdAt > datetime() - duration({days: $recency_days}) "
            "RETURN count(p) AS count",
            {"post": "Post"},
            {"recency_days": settings.similarity_recency_days},
        )
        if within_recency_result:
            count = coerce_int(within_recency_result[0].get("count", 0))
            diagnostics["posts_within_recency"] = count
            diagnostics["posts_eligible"] = count
    except Exception as exc:
        logger.debug("Posts within recency count failed: %s", exc)

    # Test query to vector index
    posts_eligible = coerce_int(diagnostics.get("posts_eligible", 0))
    if diagnostics.get("vector_index_exists") and posts_eligible > 0:
        try:
            ident_map: dict[str, str] = {"embedding_index": embedding_index_name}
            if neo4j.worker_label:
                ident_map["worker"] = neo4j.worker_label
                test_query = (
                    "MATCH (p:__post__) "
                    "WHERE p.embedding IS NOT NULL "
                    "  AND p.createdAt > datetime() - duration({days: $recency_days}) "
                    "WITH p LIMIT 1 "
                    "CALL db.index.vector.queryNodes('__embedding_index__', $top_k, p.embedding) "
                    "YIELD node, score "
                    "WHERE node.id <> p.id AND node:__worker__ "
                    "RETURN p.id AS test_post_id, "
                    "       collect(score) AS scores, "
                    "       count(node) AS neighbors_found, "
                    "       count(CASE WHEN score > $threshold THEN 1 END) AS above_threshold"
                )
            else:
                test_query = (
                    "MATCH (p:__post__) "
                    "WHERE p.embedding IS NOT NULL "
                    "  AND p.createdAt > datetime() - duration({days: $recency_days}) "
                    "WITH p LIMIT 1 "
                    "CALL db.index.vector.queryNodes('__embedding_index__', $top_k, p.embedding) "
                    "YIELD node, score "
                    "WHERE node.id <> p.id "
                    "RETURN p.id AS test_post_id, "
                    "       collect(score) AS scores, "
                    "       count(node) AS neighbors_found, "
                    "       count(CASE WHEN score > $threshold THEN 1 END) AS above_threshold"
                )

            test_result = neo4j.execute_and_fetch_labeled(
                test_query,
                {"post": "Post"},
                {
                    "top_k": top_k,
                    "threshold": settings.similarity_threshold,
                    "recency_days": settings.similarity_recency_days,
                },
                ident_map=ident_map,
            )

            if test_result and test_result[0].get("test_post_id") is not None:
                diagnostics["sample_query_works"] = True
                diagnostics["sample_post_id"] = test_result[0].get("test_post_id")
                diagnostics["sample_neighbors_found"] = coerce_int(
                    test_result[0].get("neighbors_found", 0)
                )
                diagnostics["sample_above_threshold"] = coerce_int(
                    test_result[0].get("above_threshold", 0)
                )

                scores_raw = test_result[0].get("scores", [])
                # Use Sequence check instead of isinstance
                if (
                    scores_raw is not None
                    and hasattr(scores_raw, "__iter__")
                    and hasattr(scores_raw, "__len__")
                ):
                    # list[float] is not directly Neo4jValue, but we store it anyway
                    # Neo4jValue includes Sequence[Mapping[...]] but not list[float]
                    # This is a diagnostic value, not a Neo4j return value
                    sample_scores: list[float] = []
                    for s in scores_raw:
                        # Use coerce_float for all numeric types (works for int, float, str)
                        try:
                            sample_scores.append(coerce_float(s, 0.0))
                        except (ValueError, TypeError):
                            pass
                    diagnostics["sample_scores"] = sample_scores
        except Exception as exc:
            logger.debug("Test query failed: %s", exc)

    return diagnostics


def _build_similarity_graph_vector_index(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    state_store: StateStore,
    progress: HintGridProgress | None = None,
) -> None:
    """Build SIMILAR_TO graph using Neo4j Vector Index with apoc.periodic.iterate.

    Supports two modes determined automatically by similarity_signature comparison:

    - Full mode: all eligible posts are processed and all existing SIMILAR_TO
      edges are deleted first. Triggered when similarity parameters change
      (knn_neighbors, similarity_threshold, similarity_recency_days) or on
      first run.
    - Incremental mode: only posts without outgoing SIMILAR_TO edges are
      processed. Reuses existing edges from previous runs. Triggered when
      similarity parameters have not changed.
    """
    logger.debug("  Building similarity graph via Vector Index...")

    top_k = max(MIN_KNN_NEIGHBORS, settings.knn_neighbors + settings.knn_self_neighbor_offset)
    embedding_index_name = _get_embedding_index_name(neo4j)

    # Determine build mode via similarity signature comparison
    from hintgrid.config import build_similarity_signature

    current_sig = build_similarity_signature(settings)
    state = state_store.load()
    stored_sig = state.similarity_signature

    full_rebuild = not stored_sig or stored_sig != current_sig
    if full_rebuild:
        if stored_sig:
            logger.info(
                "Similarity parameters changed (%s -> %s), full rebuild",
                stored_sig,
                current_sig,
            )
            console.print(
                f"[yellow]Similarity params changed ({stored_sig} -> {current_sig}), "
                f"full rebuild[/yellow]"
            )
        else:
            logger.info("First similarity build, full mode (signature: %s)", current_sig)

        neo4j.delete_all_similar_to_relationships(
            batch_size=settings.apoc_batch_size,
        )
    else:
        logger.info("Similarity params unchanged, incremental mode")
        console.print("[green]Incremental similarity build (reusing existing edges)[/green]")

    # Step 1: Run diagnostics before building
    console.print("[bold cyan]Running similarity diagnostics...[/bold cyan]")
    diagnostics = _diagnose_similarity_prerequisites(neo4j, settings, embedding_index_name, top_k)
    from hintgrid.cli.console import print_similarity_diagnostics

    print_similarity_diagnostics(diagnostics, settings)

    # Step 2: Check if we should proceed
    if not diagnostics.get("vector_index_exists"):
        console.print("[red]Vector index not found![/red]")
        console.print(f"[yellow]Expected index: {embedding_index_name}[/yellow]")
        raise ValueError(f"Vector index {embedding_index_name} does not exist")

    posts_eligible_check = coerce_int(diagnostics.get("posts_eligible", 0))
    if posts_eligible_check == 0:
        console.print("[yellow]No eligible posts found![/yellow]")
        console.print("[dim]Check recency_days and embedding generation[/dim]")
        return

    # Iterator query depends on build mode
    if full_rebuild:
        iterate_query = (
            "MATCH (p:__post__) "
            "WHERE p.embedding IS NOT NULL "
            "  AND p.createdAt > datetime() - duration({days: $recency_days}) "
            "RETURN id(p) AS post_id"
        )
        count_filter = (
            "MATCH (p:__post__) "
            "WHERE p.embedding IS NOT NULL "
            "  AND p.createdAt > datetime() - duration({days: $recency_days}) "
            "RETURN count(*) AS total"
        )
    else:
        iterate_query = (
            "MATCH (p:__post__) "
            "WHERE p.embedding IS NOT NULL "
            "  AND p.createdAt > datetime() - duration({days: $recency_days}) "
            "  AND NOT EXISTS { (p)-[:SIMILAR_TO]->() } "
            "RETURN id(p) AS post_id"
        )
        count_filter = (
            "MATCH (p:__post__) "
            "WHERE p.embedding IS NOT NULL "
            "  AND p.createdAt > datetime() - duration({days: $recency_days}) "
            "  AND NOT EXISTS { (p)-[:SIMILAR_TO]->() } "
            "RETURN count(*) AS total"
        )

    # Action query: query vector index and create SIMILAR_TO relationships
    ident_map: dict[str, str] = {"embedding_index": embedding_index_name}
    if neo4j.worker_label:
        ident_map["worker"] = neo4j.worker_label
        action_query = (
            "UNWIND $_batch AS row "
            "MATCH (p:__post__) WHERE id(p) = row.post_id "
            "CALL db.index.vector.queryNodes('__embedding_index__', $top_k, p.embedding) "
            "YIELD node AS neighbor, score "
            "WHERE neighbor.id <> p.id AND score > $threshold AND neighbor:__worker__ "
            "MERGE (p)-[r:SIMILAR_TO]->(neighbor) SET r.weight = score"
        )
    else:
        action_query = (
            "UNWIND $_batch AS row "
            "MATCH (p:__post__) WHERE id(p) = row.post_id "
            "CALL db.index.vector.queryNodes('__embedding_index__', $top_k, p.embedding) "
            "YIELD node AS neighbor, score "
            "WHERE neighbor.id <> p.id AND score > $threshold "
            "MERGE (p)-[r:SIMILAR_TO]->(neighbor) SET r.weight = score"
        )

    # Get total count for progress tracking
    count_query: LiteralString = count_filter
    count_result = neo4j.execute_and_fetch_labeled(
        count_query,
        {"post": "Post"},
        {
            "recency_days": settings.similarity_recency_days,
        },
    )
    total = coerce_int(count_result[0].get("total", 0)) if count_result else None

    if not full_rebuild and total == 0:
        logger.info("No new posts to process (incremental mode), skipping build")
        console.print("[green]All posts already have similarity edges, nothing to do[/green]")
        return

    # Create ProgressTracker and start polling if progress is provided
    operation_id = f"build_similarity_graph_{uuid.uuid4().hex[:8]}"
    polling_thread = None
    task_id: TaskID | None = None

    if progress is not None:
        neo4j.create_progress_tracker(operation_id, total)
        task_id = progress.add_task(
            "[cyan]Building similarity graph...",
            total=total,
        )
        from hintgrid.cli.console import track_periodic_iterate_progress

        polling_thread = track_periodic_iterate_progress(
            neo4j,
            operation_id,
            progress,
            task_id,
            poll_interval=settings.progress_poll_interval_seconds,
        )

    try:
        result = neo4j.execute_periodic_iterate(
            iterate_query,
            action_query,
            label_map={"post": "Post"},
            ident_map=ident_map,
            batch_size=settings.similarity_iterate_batch_size,
            parallel=False,  # Avoid locking on MERGE
            batch_mode="BATCH",  # Use batch mode for better performance
            progress_tracker_id=operation_id if progress is not None else None,
            params={
                "top_k": top_k,
                "threshold": settings.similarity_threshold,
                "recency_days": settings.similarity_recency_days,
            },
        )
        logger.info(
            "Similarity graph built: %d batches, %d total, %d committed, %d failed",
            coerce_int(result.get("batches", 0)),
            coerce_int(result.get("total", 0)),
            coerce_int(result.get("committedOperations", 0)),
            coerce_int(result.get("failedOperations", 0)),
        )
        if coerce_int(result.get("failedOperations", 0)) > 0:
            logger.warning("Some operations failed: %s", result.get("errorMessages", []))

        # Collect and display detailed statistics
        similarity_stats = _collect_similarity_statistics(neo4j, settings)
        from hintgrid.cli.console import print_similarity_results

        print_similarity_results(result, similarity_stats, settings)
    finally:
        # Stop polling thread
        if polling_thread is not None:
            polling_thread.stop_event.set()
            polling_thread.join(timeout=2.0)

        # Clean up ProgressTracker
        if progress is not None:
            neo4j.cleanup_progress_tracker(operation_id)
            if task_id is not None:
                progress.update(task_id, description="[green]✓ Similarity graph built")

    # Persist similarity signature after successful build
    state.similarity_signature = current_sig
    state_store.save(state)
    logger.debug("  Similarity graph built successfully (signature: %s)", current_sig)


def _collect_similarity_statistics(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> dict[str, Neo4jValue]:
    """Collect statistics about created SIMILAR_TO relationships.

    Uses execute_and_fetch_labeled with label_map={"post": "Post"}.

    Returns:
    - total_relationships: int
    - unique_posts_with_edges: int
    - avg_edges_per_post: float
    - score_distribution: dict (min, max, avg, median)
    - posts_without_edges: int
    - posts_with_edges: int
    - eligible_posts: int (posts that were processed)
    """
    # score_distribution is a nested dict, so we need to type it separately
    score_distribution: dict[str, float | None] = {
        "min": None,
        "max": None,
        "avg": None,
        "median": None,
    }
    stats: dict[str, Neo4jValue] = {
        "total_relationships": 0,
        "unique_posts_with_edges": 0,
        "avg_edges_per_post": 0.0,
        "score_distribution": score_distribution,
        "posts_without_edges": 0,
        "posts_with_edges": 0,
        "eligible_posts": 0,
    }

    # Count total relationships and basic stats
    try:
        rel_result = neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__)-[r:SIMILAR_TO]->(q:__post__) "
            "RETURN count(r) AS total_relationships, "
            "       count(DISTINCT p) AS posts_with_edges, "
            "       COALESCE(avg(r.weight), 0.0) AS avg_score, "
            "       COALESCE(min(r.weight), 0.0) AS min_score, "
            "       COALESCE(max(r.weight), 0.0) AS max_score",
            {"post": "Post"},
        )
        if rel_result:
            stats["total_relationships"] = coerce_int(rel_result[0].get("total_relationships", 0))
            stats["posts_with_edges"] = coerce_int(rel_result[0].get("posts_with_edges", 0))

            avg_score = rel_result[0].get("avg_score")
            min_score = rel_result[0].get("min_score")
            max_score = rel_result[0].get("max_score")

            # Update score_distribution dict directly (stats is already typed as dict)
            # score_distribution is initialized as dict[str, float | None] in stats
            score_dist_raw = stats.get("score_distribution")
            # Use hasattr instead of isinstance for dict-like check
            # Runtime guarantee: score_dist is dict[str, float | None] from stats initialization
            if (
                score_dist_raw is not None
                and hasattr(score_dist_raw, "get")
                and hasattr(score_dist_raw, "items")
            ):
                # score_distribution is dict[str, float | None], so we can update it
                score_dist: dict[str, float | None] = score_dist_raw  # type: ignore[assignment]
                if avg_score is not None:
                    score_dist["avg"] = coerce_float(avg_score, 0.0)
                if min_score is not None:
                    score_dist["min"] = coerce_float(min_score, 0.0)
                if max_score is not None:
                    score_dist["max"] = coerce_float(max_score, 0.0)

            posts_with_edges_count = coerce_int(stats.get("posts_with_edges", 0))
            if posts_with_edges_count > 0:
                total_rels = coerce_int(stats.get("total_relationships", 0))
                stats["avg_edges_per_post"] = float(total_rels) / float(posts_with_edges_count)
    except Exception as exc:
        logger.debug("Relationship statistics failed: %s", exc)

    # Get median score
    try:
        median_result = neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__)-[r:SIMILAR_TO]->(q:__post__) "
            "RETURN percentileCont(r.weight, 0.5) AS median_score",
            {"post": "Post"},
        )
        if median_result:
            median_score = median_result[0].get("median_score")
            if median_score is not None:
                # Update score_distribution dict directly (stats is already typed as dict)
                # score_distribution is initialized as dict[str, float | None] in stats
                score_dist_median_raw = stats.get("score_distribution")
                # Use hasattr instead of isinstance for dict-like check
                # Runtime guarantee: score_dist is dict[str, float | None] from stats initialization
                if (
                    score_dist_median_raw is not None
                    and hasattr(score_dist_median_raw, "get")
                    and hasattr(score_dist_median_raw, "items")
                ):
                    # score_distribution is dict[str, float | None], so we can update it
                    score_dist_median: dict[str, float | None] = score_dist_median_raw  # type: ignore[assignment]
                    score_dist_median["median"] = coerce_float(median_score, 0.0)
    except Exception as exc:
        logger.debug("Median score calculation failed: %s", exc)

    # Count eligible posts (posts that were processed)
    try:
        eligible_result = neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) "
            "WHERE p.embedding IS NOT NULL "
            "  AND p.createdAt > datetime() - duration({days: $recency_days}) "
            "RETURN count(p) AS count",
            {"post": "Post"},
            {"recency_days": settings.similarity_recency_days},
        )
        if eligible_result:
            stats["eligible_posts"] = coerce_int(eligible_result[0].get("count", 0))
    except Exception as exc:
        logger.debug("Eligible posts count failed: %s", exc)

    # Calculate posts without edges
    eligible_posts_count = coerce_int(stats.get("eligible_posts", 0))
    posts_with_edges_count = coerce_int(stats.get("posts_with_edges", 0))
    if eligible_posts_count > 0:
        stats["posts_without_edges"] = eligible_posts_count - posts_with_edges_count

    return stats


def run_pagerank(neo4j: Neo4jClient, settings: HintGridSettings) -> None:
    """Run PageRank on SIMILAR_TO graph to identify influential posts.

    PageRank scores are written to Post.pagerank property.
    This helps identify the most central/prototypical posts within each community.
    """
    if not settings.pagerank_enabled:
        logger.info("PageRank disabled, skipping")
        return

    logger.info("  Running PageRank on similarity graph...")
    console.print("[bold magenta]PageRank[/bold magenta]")

    # Check if SIMILAR_TO relationships exist before projecting
    sim_result = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__)-[r:SIMILAR_TO]->(q:__post__) RETURN count(r) AS count",
        {"post": "Post"},
    )
    sim_count = coerce_int(sim_result[0]["count"]) if sim_result else 0
    if sim_count == 0:
        logger.warning("No SIMILAR_TO relationships found, skipping PageRank")
        console.print("[yellow]  No SIMILAR_TO relationships, skipping PageRank[/yellow]")
        return

    pagerank_graph_name = _get_pagerank_graph_name(neo4j)
    validate_gds_name(pagerank_graph_name)

    # Drop existing graph if exists
    try:
        neo4j.execute(
            "CALL gds.graph.drop($graph_name) YIELD graphName",
            {"graph_name": pagerank_graph_name},
        )
    except Exception:
        pass  # Graph doesn't exist yet

    # Project SIMILAR_TO graph using native projection
    # Date filtering is already applied when building SIMILAR_TO edges
    project_label = neo4j.worker_label or "Post"
    with console.status("[bold blue]Projecting graph for PageRank...[/bold blue]"):
        neo4j.execute_labeled(
            "CALL gds.graph.project("
            "  '__graph_name__', '__node_label__', "
            "  {SIMILAR_TO: {orientation: 'UNDIRECTED', properties: 'weight'}}"
            ")",
            ident_map={
                "graph_name": pagerank_graph_name,
                "node_label": project_label,
            },
        )

    # Run PageRank
    # GDS procedures don't support parameters for graph names, so we use ident_map
    # The graph name is validated by validate_gds_name above
    with console.status("[bold blue]Running PageRank algorithm...[/bold blue]"):
        result = neo4j.execute_and_fetch_labeled(
            "CALL gds.pageRank.write('__graph_name__', {"
            "  writeProperty: 'pagerank',"
            "  relationshipWeightProperty: 'weight',"
            "  maxIterations: $max_iterations,"
            "  dampingFactor: $damping_factor"
            "}) "
            "YIELD nodePropertiesWritten, ranIterations, didConverge "
            "RETURN nodePropertiesWritten, ranIterations, didConverge",
            ident_map={"graph_name": pagerank_graph_name},
            params={
                "max_iterations": settings.pagerank_max_iterations,
                "damping_factor": settings.pagerank_damping_factor,
            },
        )
        if result:
            written = coerce_int(result[0].get("nodePropertiesWritten", 0))
            iterations = coerce_int(result[0].get("ranIterations", 0))
            converged = bool(result[0].get("didConverge", False))
            print_success(
                f"PageRank computed: {written:,} nodes, {iterations} iterations, "
                f"converged={converged}"
            )
            logger.info("PageRank result: %s", result[0])

    # Drop projection
    neo4j.execute(
        "CALL gds.graph.drop($graph_name) YIELD graphName",
        {"graph_name": pagerank_graph_name},
    )

    logger.info("  PageRank completed")
