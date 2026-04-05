"""Graph statistics collection and display using Rich."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, LiteralString, TypedDict

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient, Neo4jValue
    from hintgrid.clients.postgres import PostgresClient

from hintgrid.cli.console import console
from hintgrid.utils.coercion import coerce_float, coerce_int, coerce_str


class _UserCommunityStats(TypedDict):
    """Typed result of user community statistics collection."""

    total_communities: int
    total_users: int
    avg_size: float
    median_size: float
    min_size: int
    max_size: int
    isolated_count: int
    size_distribution: dict[int, int]


class _PostCommunityStats(TypedDict):
    """Typed result of post community statistics collection."""

    total_communities: int
    total_posts: int
    avg_size: float
    median_size: float
    min_size: int
    max_size: int
    isolated_count: int
    size_distribution: dict[int, int]
    similarity_edges: int


class UserInfo(TypedDict, total=False):
    """User information from Neo4j and PostgreSQL."""

    # Required fields
    user_id: int
    username: str | None
    domain: str | None
    languages: list[str] | None
    is_local: bool

    # Extended fields (optional)
    user_community_id: int | None  # ID сообщества пользователя
    user_community_size: int | None  # Размер сообщества
    top_interests: list[dict[str, Neo4jValue]] | None  # Топ интересов к PostCommunity
    interactions: dict[str, int] | None  # Статистика взаимодействий
    follows_count: int | None  # Исходящие подписки
    followers_count: int | None  # Входящие подписки
    posts_count: int | None  # Количество постов пользователя


logger = logging.getLogger(__name__)


def _count_rel(
    neo4j: Neo4jClient,
    rel_type: str,
    query: LiteralString,
    labels: dict[str, str],
    rel_types: frozenset[str] | None,
) -> int:
    """Return relationship count, skipping the query when *rel_type* is absent."""
    if rel_types is not None and rel_type not in rel_types:
        return 0
    rows = list(neo4j.execute_and_fetch_labeled(query, labels))
    return coerce_int(rows[0]["count"]) if rows else 0


def _collect_basic_counts(
    neo4j: Neo4jClient,
    rel_types: frozenset[str] | None = None,
) -> dict[str, int]:
    """Collect basic node and relationship counts.

    Args:
        neo4j: Neo4j client
        rel_types: Relationship types present in the graph.
            ``None`` means *query all* (backward-compatible).

    Returns:
        Dictionary with counts: users, posts, favorited, reblogged,
        replied, hates_user, interacts_with
    """
    users_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__) RETURN count(u) AS count",
            {"user": "User"},
        )
    )
    users = coerce_int(users_result[0]["count"]) if users_result else 0

    posts_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) RETURN count(p) AS count",
            {"post": "Post"},
        )
    )
    posts = coerce_int(posts_result[0]["count"]) if posts_result else 0

    favorited = _count_rel(
        neo4j,
        "FAVORITED",
        "MATCH (u:__user__)-[r:FAVORITED]->(p:__post__) RETURN count(r) AS count",
        {"user": "User", "post": "Post"},
        rel_types,
    )
    reblogged = _count_rel(
        neo4j,
        "REBLOGGED",
        "MATCH (u:__user__)-[r:REBLOGGED]->(p:__post__) RETURN count(r) AS count",
        {"user": "User", "post": "Post"},
        rel_types,
    )
    replied = _count_rel(
        neo4j,
        "REPLIED",
        "MATCH (u:__user__)-[r:REPLIED]->(p:__post__) RETURN count(r) AS count",
        {"user": "User", "post": "Post"},
        rel_types,
    )
    hates_user = _count_rel(
        neo4j,
        "HATES_USER",
        "MATCH (u:__user__)-[r:HATES_USER]->(v:__user__) RETURN count(r) AS count",
        {"user": "User"},
        rel_types,
    )
    interacts_with = _count_rel(
        neo4j,
        "INTERACTS_WITH",
        "MATCH (u:__user__)-[r:INTERACTS_WITH]->(v:__user__) RETURN count(r) AS count",
        {"user": "User"},
        rel_types,
    )

    return {
        "users": users,
        "posts": posts,
        "favorited": favorited,
        "reblogged": reblogged,
        "replied": replied,
        "hates_user": hates_user,
        "interacts_with": interacts_with,
    }


def _collect_user_connectivity(neo4j: Neo4jClient) -> dict[str, float | int]:
    """Collect user connectivity statistics (INTERACTS_WITH, not FOLLOWS).

    Args:
        neo4j: Neo4j client

    Returns:
        Dictionary with: avg_interacts, median_interacts, max_interacts, isolated_users
    """
    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__) "
            "OPTIONAL MATCH (u)-[r:INTERACTS_WITH]->() "
            "WITH u, count(r) AS out_degree "
            "RETURN "
            "  COALESCE(avg(out_degree), 0.0) AS avg_interacts, "
            "  COALESCE(percentileCont(out_degree, 0.5), 0.0) AS median_interacts, "
            "  COALESCE(max(out_degree), 0) AS max_interacts, "
            "  COALESCE(sum(CASE WHEN out_degree = 0 THEN 1 ELSE 0 END), 0) AS isolated_users",
            {"user": "User"},
        )
    )

    if not result:
        return {
            "avg_interacts": 0.0,
            "median_interacts": 0.0,
            "max_interacts": 0,
            "isolated_users": 0,
        }

    row = result[0]
    return {
        "avg_interacts": coerce_float(row.get("avg_interacts")),
        "median_interacts": coerce_float(row.get("median_interacts")),
        "max_interacts": coerce_int(row.get("max_interacts")),
        "isolated_users": coerce_int(row.get("isolated_users")),
    }


def _collect_post_statistics(neo4j: Neo4jClient) -> dict[str, int | float]:
    """Collect post statistics including embedding coverage.

    Args:
        neo4j: Neo4j client

    Returns:
        Dictionary with: total, with_embedding, without_embedding, coverage_pct
    """
    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) "
            "RETURN "
            "  count(p) AS total, "
            "  sum(CASE WHEN p.embedding IS NOT NULL THEN 1 ELSE 0 END) AS with_embedding, "
            "  sum(CASE WHEN p.embedding IS NULL THEN 1 ELSE 0 END) AS without_embedding",
            {"post": "Post"},
        )
    )

    if not result:
        return {
            "total": 0,
            "with_embedding": 0,
            "without_embedding": 0,
            "coverage_pct": 0.0,
        }

    row = result[0]
    total = coerce_int(row.get("total"))
    with_embedding = coerce_int(row.get("with_embedding"))
    without_embedding = coerce_int(row.get("without_embedding"))

    coverage_pct = (with_embedding / total * 100.0) if total > 0 else 0.0

    return {
        "total": total,
        "with_embedding": with_embedding,
        "without_embedding": without_embedding,
        "coverage_pct": coverage_pct,
    }


def _build_interaction_type_pattern(
    rel_types: frozenset[str] | None,
) -> LiteralString | None:
    """Build a multi-type pattern like ``FAVORITED|REBLOGGED|REPLIED``.

    Returns ``None`` when none of the interaction types exist in the graph.
    """
    candidates: list[LiteralString] = []
    for t in ("FAVORITED", "REBLOGGED", "REPLIED"):
        if rel_types is None or t in rel_types:
            candidates.append(t)
    if not candidates:
        return None
    result: LiteralString = candidates[0]
    for c in candidates[1:]:
        result = result + "|" + c
    return result


def _collect_interaction_stats(
    neo4j: Neo4jClient,
    rel_types: frozenset[str] | None = None,
) -> dict[str, float]:
    """Collect average interaction statistics per user and per post.

    Args:
        neo4j: Neo4j client
        rel_types: Relationship types present in the graph.

    Returns:
        Dictionary with: avg_interactions_per_user, avg_interactions_per_post
    """
    pattern = _build_interaction_type_pattern(rel_types)

    if pattern is None:
        return {
            "avg_interactions_per_user": 0.0,
            "avg_interactions_per_post": 0.0,
        }

    # COUNT {} avoids OPTIONAL MATCH + avg over nullable groups (Neo4j 01G11 warnings).
    user_query: LiteralString = (
        "MATCH (u:__user__) "
        "RETURN COALESCE(avg(COUNT { (u)-[r:" + pattern + "]->() }), 0.0) "
        "AS avg_interactions_per_user"
    )
    user_result = list(neo4j.execute_and_fetch_labeled(user_query, {"user": "User"}))

    post_query: LiteralString = (
        "MATCH (p:__post__) "
        "RETURN COALESCE(avg(COUNT { ()-[r:" + pattern + "]->(p) }), 0.0) "
        "AS avg_interactions_per_post"
    )
    post_result = list(neo4j.execute_and_fetch_labeled(post_query, {"post": "Post"}))

    avg_per_user = coerce_float(user_result[0]["avg_interactions_per_user"]) if user_result else 0.0
    avg_per_post = coerce_float(post_result[0]["avg_interactions_per_post"]) if post_result else 0.0

    return {
        "avg_interactions_per_user": avg_per_user,
        "avg_interactions_per_post": avg_per_post,
    }


def _collect_top_languages(neo4j: Neo4jClient, limit: int = 5) -> list[tuple[str, int]]:
    """Collect top languages by post count.

    Args:
        neo4j: Neo4j client
        limit: Maximum number of languages to return

    Returns:
        List of (language, count) tuples sorted by count descending
    """
    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) "
            "WHERE p.language IS NOT NULL "
            "WITH p.language AS lang, count(p) AS cnt "
            "ORDER BY cnt DESC "
            "LIMIT $limit "
            "RETURN lang, cnt",
            {"post": "Post"},
            {"limit": limit},
        )
    )

    languages: list[tuple[str, int]] = []
    for row in result:
        lang = coerce_str(row.get("lang"), default="unknown")
        cnt = coerce_int(row.get("cnt"))
        languages.append((lang, cnt))

    return languages


def show_graph_overview_after_loading(neo4j: Neo4jClient) -> None:
    """Display comprehensive graph statistics after data loading using Rich.

    Shows nodes, relationships, user connectivity, post coverage, interactions,
    and top languages in a formatted Rich Panel/Table.

    Args:
        neo4j: Neo4j client
    """
    from rich.panel import Panel

    rel_types = neo4j.get_existing_rel_types()

    # Collect all statistics
    basic = _collect_basic_counts(neo4j, rel_types)
    connectivity = _collect_user_connectivity(neo4j)
    post_stats = _collect_post_statistics(neo4j)
    interactions = _collect_interaction_stats(neo4j, rel_types)
    top_languages = _collect_top_languages(neo4j, limit=5)

    # Check for edge cases
    if basic["users"] == 0 and basic["posts"] == 0:
        console.print("[yellow]⚠ No data loaded yet[/yellow]")
        return

    # Build overview text
    lines: list[str] = []

    # Nodes and Relationships section
    lines.append("[bold cyan]Nodes[/bold cyan]")
    lines.append(f"├── Users:   {basic['users']:,}")
    lines.append(f"└── Posts:   {basic['posts']:,}")
    lines.append("")
    lines.append("[bold cyan]Relationships[/bold cyan]")
    lines.append(
        f"├── INTERACTS_WITH:  {basic['interacts_with']:,} "
        "(user-user; Mastodon follows are aggregated here, not as FOLLOWS edges)"
    )
    lines.append(f"├── FAVORITED:       {basic['favorited']:,}")
    lines.append(f"├── REBLOGGED:       {basic['reblogged']:,}")
    lines.append(f"├── REPLIED:         {basic['replied']:,}")
    lines.append(f"└── HATES_USER:      {basic['hates_user']:,}")
    lines.append("")

    # User Connectivity section
    lines.append("[bold cyan]User Connectivity[/bold cyan]")
    avg_interacts = connectivity.get("avg_interacts", 0.0)
    median_interacts = connectivity.get("median_interacts", 0.0)
    max_interacts = connectivity.get("max_interacts", 0)
    isolated = connectivity.get("isolated_users", 0)
    isolated_pct = (isolated / basic["users"] * 100.0) if basic["users"] > 0 else 0.0

    lines.append(f"├── Avg interacts/user:  {avg_interacts:.1f}")
    lines.append(f"├── Median interacts:    {median_interacts:.1f}")
    lines.append(f"├── Max interacts:       {max_interacts:,}")
    lines.append(f"└── Isolated users:   {isolated:,} ({isolated_pct:.1f}%)")
    lines.append("")

    # Post Coverage section
    lines.append("[bold cyan]Post Coverage[/bold cyan]")
    with_emb = post_stats["with_embedding"]
    without_emb = post_stats["without_embedding"]
    coverage = post_stats["coverage_pct"]

    coverage_color = "green" if coverage >= 95.0 else "yellow" if coverage >= 80.0 else "red"
    lines.append(
        f"├── With embeddings:   [{coverage_color}]{with_emb:,} ({coverage:.1f}%)[/{coverage_color}]"
    )
    lines.append(f"└── Without:           {without_emb:,} ({100.0 - coverage:.1f}%)")
    lines.append("")

    # Interactions section
    lines.append("[bold cyan]Interactions[/bold cyan]")
    avg_per_user = interactions["avg_interactions_per_user"]
    avg_per_post = interactions["avg_interactions_per_post"]
    lines.append(f"├── Avg per user:      {avg_per_user:.1f}")
    lines.append(f"└── Avg per post:      {avg_per_post:.1f}")
    lines.append("")

    # Top Languages section
    if top_languages:
        lines.append("[bold cyan]Top Languages[/bold cyan]")
        total_posts = basic["posts"]
        for i, (lang, cnt) in enumerate(top_languages):
            pct = (cnt / total_posts * 100.0) if total_posts > 0 else 0.0
            prefix = "└──" if i == len(top_languages) - 1 else "├──"
            lines.append(f"{prefix} {lang}:  {cnt:,} ({pct:.1f}%)")

    # Create panel
    panel_content = "\n".join(lines)
    panel = Panel(
        panel_content,
        title="[bold]📊 Graph Overview[/bold]",
        border_style="blue",
        expand=False,
    )

    console.print()
    console.print(panel)
    console.print()

    # Show warnings for edge cases
    if isolated == basic["users"] and basic["users"] > 0:
        console.print(
            "[yellow]⚠ All users are isolated (no outgoing INTERACTS_WITH relationships)[/yellow]"
        )

    if post_stats["with_embedding"] == 0 and basic["posts"] > 0:
        console.print("[yellow]⚠ No posts have embeddings yet[/yellow]")


def _collect_user_community_stats(neo4j: Neo4jClient) -> _UserCommunityStats:
    """Collect user community statistics.

    Args:
        neo4j: Neo4j client

    Returns:
        Dictionary with community statistics: total, avg_size, median_size, min_size,
        max_size, isolated_count, size_distribution
    """
    # Get total users and communities
    total_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__) RETURN count(u) AS total_users",
            {"user": "User"},
        )
    )
    total_users = coerce_int(total_result[0]["total_users"]) if total_result else 0

    communities_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (uc:__uc__) RETURN count(uc) AS total_communities",
            {"uc": "UserCommunity"},
        )
    )
    total_communities = (
        coerce_int(communities_result[0]["total_communities"]) if communities_result else 0
    )

    if total_communities == 0:
        return {
            "total_communities": 0,
            "total_users": total_users,
            "avg_size": 0.0,
            "median_size": 0.0,
            "min_size": 0,
            "max_size": 0,
            "isolated_count": 0,
            "size_distribution": {},
        }

    # Get size statistics
    stats_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (uc:__uc__) "
            "RETURN "
            "  COALESCE(avg(uc.size), 0.0) AS avg_size, "
            "  COALESCE(percentileCont(uc.size, 0.5), 0.0) AS median_size, "
            "  COALESCE(min(uc.size), 0) AS min_size, "
            "  COALESCE(max(uc.size), 0) AS max_size, "
            "  COALESCE(sum(CASE WHEN uc.size = 1 THEN 1 ELSE 0 END), 0) AS isolated_count",
            {"uc": "UserCommunity"},
        )
    )

    if not stats_result:
        return {
            "total_communities": total_communities,
            "total_users": total_users,
            "avg_size": 0.0,
            "median_size": 0.0,
            "min_size": 0,
            "max_size": 0,
            "isolated_count": 0,
            "size_distribution": {},
        }

    row = stats_result[0]

    # Get size distribution
    distribution_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (uc:__uc__) "
            "WITH uc.size AS size, count(uc) AS cnt "
            "RETURN size, cnt "
            "ORDER BY size",
            {"uc": "UserCommunity"},
        )
    )

    size_distribution: dict[int, int] = {}
    for dist_row in distribution_result:
        size = coerce_int(dist_row.get("size"))
        cnt = coerce_int(dist_row.get("cnt"))
        size_distribution[size] = cnt

    return {
        "total_communities": total_communities,
        "total_users": total_users,
        "avg_size": coerce_float(row.get("avg_size")),
        "median_size": coerce_float(row.get("median_size")),
        "min_size": coerce_int(row.get("min_size")),
        "max_size": coerce_int(row.get("max_size")),
        "isolated_count": coerce_int(row.get("isolated_count")),
        "size_distribution": size_distribution,
    }


def _collect_post_community_stats(neo4j: Neo4jClient) -> _PostCommunityStats:
    """Collect post community statistics.

    Args:
        neo4j: Neo4j client

    Returns:
        Dictionary with community statistics: total, avg_size, median_size, min_size,
        max_size, isolated_count, size_distribution, similarity_edges
    """
    # Get total posts and communities
    total_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) RETURN count(p) AS total_posts",
            {"post": "Post"},
        )
    )
    total_posts = coerce_int(total_result[0]["total_posts"]) if total_result else 0

    communities_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (pc:__pc__) RETURN count(pc) AS total_communities",
            {"pc": "PostCommunity"},
        )
    )
    total_communities = (
        coerce_int(communities_result[0]["total_communities"]) if communities_result else 0
    )

    # Get SIMILAR_TO count (may be pruned)
    similarity_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__)-[r:SIMILAR_TO]->(q:__post__) RETURN count(r) AS count",
            {"post": "Post"},
        )
    )
    similarity_edges = coerce_int(similarity_result[0]["count"]) if similarity_result else 0

    if total_communities == 0:
        return {
            "total_communities": 0,
            "total_posts": total_posts,
            "avg_size": 0.0,
            "median_size": 0.0,
            "min_size": 0,
            "max_size": 0,
            "isolated_count": 0,
            "size_distribution": {},
            "similarity_edges": similarity_edges,
        }

    # Get size statistics
    stats_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (pc:__pc__) "
            "RETURN "
            "  COALESCE(avg(pc.size), 0.0) AS avg_size, "
            "  COALESCE(percentileCont(pc.size, 0.5), 0.0) AS median_size, "
            "  COALESCE(min(pc.size), 0) AS min_size, "
            "  COALESCE(max(pc.size), 0) AS max_size, "
            "  COALESCE(sum(CASE WHEN pc.size = 1 THEN 1 ELSE 0 END), 0) AS isolated_count",
            {"pc": "PostCommunity"},
        )
    )

    if not stats_result:
        return {
            "total_communities": total_communities,
            "total_posts": total_posts,
            "avg_size": 0.0,
            "median_size": 0.0,
            "min_size": 0,
            "max_size": 0,
            "isolated_count": 0,
            "size_distribution": {},
            "similarity_edges": similarity_edges,
        }

    row = stats_result[0]

    # Get size distribution
    distribution_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (pc:__pc__) "
            "WITH pc.size AS size, count(pc) AS cnt "
            "RETURN size, cnt "
            "ORDER BY size",
            {"pc": "PostCommunity"},
        )
    )

    size_distribution: dict[int, int] = {}
    for dist_row in distribution_result:
        size = coerce_int(dist_row.get("size"))
        cnt = coerce_int(dist_row.get("cnt"))
        size_distribution[size] = cnt

    return {
        "total_communities": total_communities,
        "total_posts": total_posts,
        "avg_size": coerce_float(row.get("avg_size")),
        "median_size": coerce_float(row.get("median_size")),
        "min_size": coerce_int(row.get("min_size")),
        "max_size": coerce_int(row.get("max_size")),
        "isolated_count": coerce_int(row.get("isolated_count")),
        "size_distribution": size_distribution,
        "similarity_edges": similarity_edges,
    }


def _format_user_display(
    user_id: int,
    username: str | None,
    domain: str | None,
) -> str:
    """Format user display string as @username@domain (id: user_id).

    Args:
        user_id: User ID
        username: Username from PostgreSQL
        domain: Domain from PostgreSQL

    Returns:
        Formatted string like "@username@domain.com (id: 12345)" or "user 12345" if no username
    """
    if username:
        handle = f"@{username}"
        if domain:
            handle = f"@{username}@{domain}"
        return f"{handle} (id: {user_id:,})"
    return f"user {user_id:,}"


def _format_post_text(text: str | None, max_length: int = 100) -> str:
    """Format post text with truncation if needed.

    Args:
        text: Post text from Neo4j
        max_length: Maximum length before truncation

    Returns:
        Formatted text with ellipsis if truncated
    """
    if not text:
        return "[dim]—[/dim]"
    text_str = str(text)
    if len(text_str) <= max_length:
        return text_str
    return text_str[:max_length] + "..."


def _format_size_distribution(
    distribution: dict[int, int], total: int
) -> list[tuple[str, int, float]]:
    """Format size distribution into buckets for display.

    Args:
        distribution: Dictionary mapping size to count
        total: Total number of communities

    Returns:
        List of (bucket_name, count, percentage) tuples
    """
    buckets = {
        "1": 0,
        "2-5": 0,
        "6-20": 0,
        "21-100": 0,
        "100+": 0,
    }

    for size, count in distribution.items():
        if size == 1:
            buckets["1"] += count
        elif 2 <= size <= 5:
            buckets["2-5"] += count
        elif 6 <= size <= 20:
            buckets["6-20"] += count
        elif 21 <= size <= 100:
            buckets["21-100"] += count
        else:
            buckets["100+"] += count

    result: list[tuple[str, int, float]] = []
    for bucket_name, count in buckets.items():
        pct = (count / total * 100.0) if total > 0 else 0.0
        result.append((bucket_name, count, pct))

    return result


def _build_active_user_query(
    rel_types: frozenset[str] | None = None,
) -> LiteralString:
    """Build query for finding most active user in community.

    Uses INTERACTS_WITH weight sum as the primary activity metric
    (always present after data loading), with post-interaction counts
    only for relationship types that exist in the graph.
    """
    query: LiteralString = (
        "MATCH (uc:__uc__ {id: $comm_id})<-[:BELONGS_TO]-(u:__user__) "
        "OPTIONAL MATCH (u)-[iw:INTERACTS_WITH]->() "
        "WITH u, sum(COALESCE(iw.weight, 0)) AS iw_weight "
        "WITH u, iw_weight"
    )
    _size_clauses: list[tuple[str, LiteralString]] = [
        ("FAVORITED", " + size([(u)-[f:FAVORITED]->() | f])"),
        ("REBLOGGED", " + size([(u)-[r:REBLOGGED]->() | r])"),
        ("REPLIED", " + size([(u)-[rp:REPLIED]->() | rp])"),
        ("BOOKMARKED", " + size([(u)-[bk:BOOKMARKED]->() | bk])"),
    ]
    for rel_name, clause in _size_clauses:
        if rel_types is None or rel_name in rel_types:
            query = query + clause
    query = (
        query
        + " AS interactions_count "
        + "ORDER BY interactions_count DESC "
        + "LIMIT 1 "
        + "RETURN u.id AS user_id, interactions_count"
    )
    return query


def _build_avg_activity_query(
    rel_types: frozenset[str] | None = None,
) -> LiteralString:
    """Build query for average activity in community.

    Uses INTERACTS_WITH weight sum as the primary activity metric,
    with post-interaction counts for existing relationship types.
    """
    query: LiteralString = (
        "MATCH (uc:__uc__ {id: $comm_id})<-[:BELONGS_TO]-(u:__user__) "
        "OPTIONAL MATCH (u)-[iw:INTERACTS_WITH]->() "
        "WITH u, sum(COALESCE(iw.weight, 0)) AS iw_weight "
        "WITH u, iw_weight"
    )
    _size_clauses: list[tuple[str, LiteralString]] = [
        ("FAVORITED", " + size([(u)-[f:FAVORITED]->() | f])"),
        ("REBLOGGED", " + size([(u)-[r:REBLOGGED]->() | r])"),
        ("REPLIED", " + size([(u)-[rp:REPLIED]->() | rp])"),
        ("BOOKMARKED", " + size([(u)-[bk:BOOKMARKED]->() | bk])"),
    ]
    for rel_name, clause in _size_clauses:
        if rel_types is None or rel_name in rel_types:
            query = query + clause
    query = (
        query + " AS interactions " + "RETURN COALESCE(avg(interactions), 0.0) AS avg_interactions"
    )
    return query


def show_user_community_stats(
    neo4j: Neo4jClient,
    postgres: PostgresClient | None = None,
    modularity: float | None = None,
) -> None:
    """Display user community statistics after clustering using Rich.

    Args:
        neo4j: Neo4j client
        postgres: PostgreSQL client for fetching user account info (optional)
        modularity: Modularity value from Leiden algorithm (optional)
    """
    from rich.panel import Panel

    stats = _collect_user_community_stats(neo4j)

    if stats["total_communities"] == 0:
        console.print("[yellow]⚠ No user communities found[/yellow]")
        return

    lines: list[str] = []

    # Basic stats
    total_communities = stats["total_communities"]
    total_users = stats["total_users"]
    ratio = total_users / total_communities if total_communities > 0 else 0.0

    lines.append(f"[bold cyan]Communities:[/bold cyan] {total_communities:,}")
    lines.append(f"[bold cyan]Users:[/bold cyan] {total_users:,}")
    lines.append(f"[bold cyan]Ratio:[/bold cyan] {ratio:.1f} users/community")
    if modularity is not None:
        lines.append(f"[bold cyan]Modularity:[/bold cyan] {modularity:.3f}")
    lines.append("")

    # Size statistics
    lines.append("[bold cyan]Size Statistics[/bold cyan]")
    lines.append(f"├── Avg size:    {stats['avg_size']:.1f}")
    lines.append(f"├── Median size: {stats['median_size']:.1f}")
    lines.append(f"├── Min size:    {stats['min_size']:,}")
    lines.append(f"└── Max size:    {stats['max_size']:,}")
    lines.append("")

    # Size distribution
    distribution = stats["size_distribution"]
    if distribution:
        lines.append("[bold cyan]Size Distribution[/bold cyan]")
        buckets = _format_size_distribution(distribution, total_communities)
        for i, (bucket_name, count, pct) in enumerate(buckets):
            if count > 0:
                prefix = "└──" if i == len(buckets) - 1 else "├──"
                lines.append(f"{prefix} {bucket_name:6s} users: {count:4d} ({pct:5.1f}%)")
        lines.append("")

    # Top communities with extended information
    top_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (uc:__uc__) RETURN uc.id AS id, uc.size AS size ORDER BY uc.size DESC LIMIT 10",
            {"uc": "UserCommunity"},
        )
    )

    if top_result:
        lines.append("[bold cyan]Top 10 Communities[/bold cyan]")

        rel_types = neo4j.get_existing_rel_types()
        comm_ids = [coerce_int(row.get("id")) for row in top_result]
        comm_sizes: dict[int, int] = {
            coerce_int(row.get("id")): coerce_int(row.get("size")) for row in top_result
        }

        # Batch: most active user per community
        active_query = _build_active_user_query(rel_types)
        active_map: dict[int, dict[str, Neo4jValue]] = {}
        for cid in comm_ids:
            result_rows = list(
                neo4j.execute_and_fetch_labeled(
                    active_query,
                    {"uc": "UserCommunity", "user": "User"},
                    {"comm_id": cid},
                )
            )
            if result_rows:
                active_map[cid] = result_rows[0]

        # Batch: average activity per community
        avg_query = _build_avg_activity_query(rel_types)
        avg_map: dict[int, float] = {}
        for cid in comm_ids:
            result_rows = list(
                neo4j.execute_and_fetch_labeled(
                    avg_query,
                    {"uc": "UserCommunity", "user": "User"},
                    {"comm_id": cid},
                )
            )
            if result_rows:
                avg_map[cid] = coerce_float(
                    result_rows[0].get("avg_interactions"),
                )

        # Batch: top languages per community (single UNWIND query)
        lang_result = list(
            neo4j.execute_and_fetch_labeled(
                "UNWIND $comm_ids AS cid "
                "MATCH (uc:__uc__ {id: cid})<-[:BELONGS_TO]-(u:__user__) "
                "WHERE u.languages IS NOT NULL "
                "UNWIND u.languages AS lang "
                "WITH cid, lang, count(*) AS cnt "
                "ORDER BY cnt DESC "
                "WITH cid, collect({lang: lang, count: cnt})[..3] AS top3 "
                "RETURN cid AS comm_id, top3 AS top_languages",
                {"uc": "UserCommunity", "user": "User"},
                {"comm_ids": comm_ids},
            )
        )
        lang_map: dict[int, list[dict[str, Neo4jValue]]] = {}
        for row in lang_result:
            cid = coerce_int(row.get("comm_id"))
            raw_langs = row.get("top_languages")
            if raw_langs and hasattr(raw_langs, "__iter__"):
                lang_map[cid] = list(raw_langs)  # type: ignore[arg-type]

        # Fetch account info for all active users in batch
        user_ids_to_fetch = [
            coerce_int(v.get("user_id"))
            for v in active_map.values()
            if v.get("user_id") is not None
        ]
        account_info: dict[int, dict[str, str | None]] = {}
        if postgres and user_ids_to_fetch:
            account_info = postgres.fetch_account_info(user_ids_to_fetch)

        # Format output
        for i, cid in enumerate(comm_ids):
            size = comm_sizes.get(cid, 0)
            is_last = i == len(comm_ids) - 1
            prefix = "└──" if is_last else "├──"
            sub_prefix = "│   " if not is_last else "    "

            lines.append(f"{prefix} #{cid}: {size:,} users")

            active_data = active_map.get(cid)
            if active_data:
                user_id = coerce_int(active_data.get("user_id"))
                interactions = coerce_int(
                    active_data.get("interactions_count", 0),
                )
                user_info = account_info.get(user_id, {}) if account_info else {}
                username = user_info.get("username") if user_info else None
                domain = user_info.get("domain") if user_info else None
                user_display = _format_user_display(user_id, username, domain)
                lines.append(
                    f"{sub_prefix}  Active: {user_display} - {interactions:,} interactions"
                )

            avg_interactions = avg_map.get(cid)
            if avg_interactions is not None:
                lines.append(
                    f"{sub_prefix}  Avg activity: {avg_interactions:.1f} interactions/user"
                )

            languages_data = lang_map.get(cid)
            if languages_data:
                lang_parts: list[str] = []
                for lang_item in languages_data:
                    if lang_item is not None and hasattr(lang_item, "get"):
                        lang = str(lang_item.get("lang", ""))
                        count = coerce_int(lang_item.get("count", 0))
                        if lang and count > 0:
                            lang_parts.append(f"{lang} ({count:,})")
                if lang_parts:
                    lines.append(f"{sub_prefix}  Top languages: {', '.join(lang_parts)}")

    # Create panel
    panel_content = "\n".join(lines)
    panel = Panel(
        panel_content,
        title="[bold]👥 User Communities[/bold]",
        border_style="magenta",
        expand=False,
    )

    console.print()
    console.print(panel)
    console.print()

    # Warnings
    isolated = stats["isolated_count"]
    isolated_pct = (isolated / total_communities * 100.0) if total_communities > 0 else 0.0
    if isolated_pct > 50.0:
        console.print(
            f"[yellow]⚠ High percentage of isolated communities: {isolated_pct:.1f}%[/yellow]"
        )

    if ratio < 2.0:
        console.print(
            "[yellow]⚠ Very high community count - clustering may be too granular[/yellow]"
        )


def show_post_community_stats(
    neo4j: Neo4jClient,
    postgres: PostgresClient | None = None,
    modularity: float | None = None,
) -> None:
    """Display post community statistics after clustering using Rich.

    Args:
        neo4j: Neo4j client
        postgres: PostgreSQL client for fetching user account info (optional)
        modularity: Modularity value from Leiden algorithm (optional)
    """
    from rich.panel import Panel

    stats = _collect_post_community_stats(neo4j)

    if stats["total_communities"] == 0:
        console.print("[yellow]⚠ No post communities found[/yellow]")
        return

    lines: list[str] = []

    # Basic stats
    total_communities = stats["total_communities"]
    total_posts = stats["total_posts"]
    ratio = total_posts / total_communities if total_communities > 0 else 0.0
    similarity_edges = stats["similarity_edges"]

    lines.append(f"[bold cyan]Communities:[/bold cyan] {total_communities:,}")
    lines.append(f"[bold cyan]Posts:[/bold cyan] {total_posts:,}")
    lines.append(f"[bold cyan]Ratio:[/bold cyan] {ratio:.1f} posts/community")
    if modularity is not None:
        lines.append(f"[bold cyan]Modularity:[/bold cyan] {modularity:.3f}")
    lines.append(f"[bold cyan]SIMILAR_TO edges:[/bold cyan] {similarity_edges:,}")
    lines.append("")

    # Size statistics
    lines.append("[bold cyan]Size Statistics[/bold cyan]")
    lines.append(f"├── Avg size:    {stats['avg_size']:.1f}")
    lines.append(f"├── Median size: {stats['median_size']:.1f}")
    lines.append(f"├── Min size:    {stats['min_size']:,}")
    lines.append(f"└── Max size:    {stats['max_size']:,}")
    lines.append("")

    # Size distribution
    distribution = stats["size_distribution"]
    if distribution:
        lines.append("[bold cyan]Size Distribution[/bold cyan]")
        buckets = _format_size_distribution(distribution, total_communities)
        for i, (bucket_name, count, pct) in enumerate(buckets):
            if count > 0:
                prefix = "└──" if i == len(buckets) - 1 else "├──"
                lines.append(f"{prefix} {bucket_name:6s} posts: {count:4d} ({pct:5.1f}%)")
        lines.append("")

    # Top communities with extended information
    top_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (pc:__pc__) RETURN pc.id AS id, pc.size AS size ORDER BY pc.size DESC LIMIT 10",
            {"pc": "PostCommunity"},
        )
    )

    if top_result:
        lines.append("[bold cyan]Top 10 Communities[/bold cyan]")

        comm_ids = [coerce_int(row.get("id")) for row in top_result]
        comm_sizes: dict[int, int] = {
            coerce_int(row.get("id")): coerce_int(row.get("size")) for row in top_result
        }

        # Batch: most popular post per community
        popular_map: dict[int, dict[str, Neo4jValue]] = {}
        for cid in comm_ids:
            result_rows = list(
                neo4j.execute_and_fetch_labeled(
                    "MATCH (pc:__pc__ {id: $comm_id})"
                    "<-[:BELONGS_TO]-(p:__post__) "
                    "WITH p, COALESCE(p.totalFavourites, 0) "
                    "+ COALESCE(p.totalReblogs, 0) "
                    "+ COALESCE(p.totalReplies, 0) AS popularity "
                    "ORDER BY popularity DESC LIMIT 1 "
                    "RETURN p.id AS post_id, p.text AS post_text,"
                    " popularity",
                    {"pc": "PostCommunity", "post": "Post"},
                    {"comm_id": cid},
                )
            )
            if result_rows:
                popular_map[cid] = result_rows[0]

        # Batch: most active author per community
        author_map: dict[int, dict[str, Neo4jValue]] = {}
        for cid in comm_ids:
            result_rows = list(
                neo4j.execute_and_fetch_labeled(
                    "MATCH (pc:__pc__ {id: $comm_id})"
                    "<-[:BELONGS_TO]-(p:__post__) "
                    "WITH p.authorId AS author_id,"
                    " count(p) AS posts_count "
                    "ORDER BY posts_count DESC LIMIT 1 "
                    "RETURN author_id, posts_count",
                    {"pc": "PostCommunity", "post": "Post"},
                    {"comm_id": cid},
                )
            )
            if result_rows:
                author_map[cid] = result_rows[0]

        # Batch: average popularity per community
        avg_pop_map: dict[int, float] = {}
        for cid in comm_ids:
            result_rows = list(
                neo4j.execute_and_fetch_labeled(
                    "MATCH (pc:__pc__ {id: $comm_id})"
                    "<-[:BELONGS_TO]-(p:__post__) "
                    "WITH p, COALESCE(p.totalFavourites, 0) "
                    "+ COALESCE(p.totalReblogs, 0) "
                    "+ COALESCE(p.totalReplies, 0) AS popularity "
                    "RETURN avg(popularity) AS avg_popularity",
                    {"pc": "PostCommunity", "post": "Post"},
                    {"comm_id": cid},
                )
            )
            if result_rows:
                avg_pop_map[cid] = coerce_float(
                    result_rows[0].get("avg_popularity"),
                )

        # Batch: top languages per community (single UNWIND query)
        lang_result = list(
            neo4j.execute_and_fetch_labeled(
                "UNWIND $comm_ids AS cid "
                "MATCH (pc:__pc__ {id: cid})<-[:BELONGS_TO]-(p:__post__) "
                "WHERE p.language IS NOT NULL "
                "WITH cid, p.language AS lang, count(*) AS cnt "
                "ORDER BY cnt DESC "
                "WITH cid, collect({lang: lang, count: cnt})[..3] AS top3 "
                "RETURN cid AS comm_id, top3 AS top_languages",
                {"pc": "PostCommunity", "post": "Post"},
                {"comm_ids": comm_ids},
            )
        )
        lang_map: dict[int, list[dict[str, Neo4jValue]]] = {}
        for row in lang_result:
            cid = coerce_int(row.get("comm_id"))
            raw_langs = row.get("top_languages")
            if raw_langs and hasattr(raw_langs, "__iter__"):
                lang_map[cid] = list(raw_langs)  # type: ignore[arg-type]

        # Fetch account info for all authors in batch
        author_ids_to_fetch = [
            coerce_int(v.get("author_id"))
            for v in author_map.values()
            if v.get("author_id") is not None
        ]
        account_info: dict[int, dict[str, str | None]] = {}
        if postgres and author_ids_to_fetch:
            account_info = postgres.fetch_account_info(author_ids_to_fetch)

        # Format output
        for i, cid in enumerate(comm_ids):
            size = comm_sizes.get(cid, 0)
            is_last = i == len(comm_ids) - 1
            prefix = "└──" if is_last else "├──"
            sub_prefix = "│   " if not is_last else "    "

            lines.append(f"{prefix} #{cid}: {size:,} posts")

            popular_data = popular_map.get(cid)
            if popular_data:
                post_id = coerce_int(popular_data.get("post_id"))
                popularity = coerce_int(popular_data.get("popularity", 0))
                post_text = popular_data.get("post_text")
                formatted_text = _format_post_text(
                    coerce_str(post_text) if post_text else None,
                )
                lines.append(f"{sub_prefix}  Popular: post {post_id} ({popularity:,} interactions)")
                lines.append(f"{sub_prefix}  Text: {formatted_text}")

            author_data = author_map.get(cid)
            if author_data:
                author_id = coerce_int(author_data.get("author_id"))
                posts_count = coerce_int(
                    author_data.get("posts_count", 0),
                )
                author_info = account_info.get(author_id, {}) if account_info else {}
                username = author_info.get("username") if author_info else None
                domain = author_info.get("domain") if author_info else None
                author_display = _format_user_display(
                    author_id,
                    username,
                    domain,
                )
                lines.append(
                    f"{sub_prefix}  Active author: {author_display} - {posts_count:,} posts"
                )

            avg_popularity = avg_pop_map.get(cid)
            if avg_popularity is not None:
                lines.append(
                    f"{sub_prefix}  Avg popularity: {avg_popularity:.1f} interactions/post"
                )

            languages_data = lang_map.get(cid)
            if languages_data:
                lang_parts: list[str] = []
                for lang_item in languages_data:
                    if lang_item is not None and hasattr(lang_item, "get"):
                        lang = str(lang_item.get("lang", ""))
                        count = coerce_int(lang_item.get("count", 0))
                        if lang and count > 0:
                            lang_parts.append(f"{lang} ({count:,})")
                if lang_parts:
                    lines.append(f"{sub_prefix}  Top languages: {', '.join(lang_parts)}")

    # Create panel
    panel_content = "\n".join(lines)
    panel = Panel(
        panel_content,
        title="[bold]📝 Post Communities[/bold]",
        border_style="magenta",
        expand=False,
    )

    console.print()
    console.print(panel)
    console.print()

    # Warnings
    isolated = stats["isolated_count"]
    isolated_pct = (isolated / total_communities * 100.0) if total_communities > 0 else 0.0
    if isolated_pct > 50.0:
        console.print(
            f"[yellow]⚠ High percentage of isolated communities: {isolated_pct:.1f}%[/yellow]"
        )

    if ratio < 2.0:
        console.print(
            "[yellow]⚠ Very high community count - clustering may be too granular[/yellow]"
        )


def get_user_info(neo4j: Neo4jClient, postgres: PostgresClient, user_id: int) -> UserInfo | None:
    """Get comprehensive user information from Neo4j and PostgreSQL.

    Args:
        neo4j: Neo4j client for graph data
        postgres: PostgreSQL client for account data
        user_id: User account ID

    Returns:
        UserInfo dictionary with user_id, username, domain, languages, is_local,
        or None if user not found in Neo4j
    """
    # Get user data from Neo4j
    user_neo4j_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__ {id: $user_id}) RETURN u.languages AS languages, u.isLocal AS is_local",
            {"user": "User"},
            {"user_id": user_id},
        )
    )

    if not user_neo4j_result:
        return None

    neo4j_info = user_neo4j_result[0]
    languages_raw = neo4j_info.get("languages")
    languages: list[str] | None = None
    # Use Sequence check instead of isinstance
    if (
        languages_raw is not None
        and hasattr(languages_raw, "__iter__")
        and hasattr(languages_raw, "__len__")
    ):
        languages = []
        for item_raw in languages_raw:
            # Use coerce_str for all types (works for str, int, float)
            languages.append(coerce_str(item_raw, ""))
    is_local_raw = neo4j_info.get("is_local", False)
    # Check for bool using hasattr instead of isinstance
    is_local: bool = (
        bool(is_local_raw)
        if (hasattr(is_local_raw, "__bool__") and type(is_local_raw).__name__ == "bool")
        else False
    )

    # Get account data from PostgreSQL
    account_info = postgres.fetch_account_info([user_id])
    pg_info = account_info.get(user_id, {})
    username = pg_info.get("username")
    domain = pg_info.get("domain")

    return UserInfo(
        user_id=user_id,
        username=username,
        domain=domain,
        languages=languages,
        is_local=is_local,
    )


def get_extended_user_info(
    neo4j: Neo4jClient, postgres: PostgresClient, user_id: int
) -> UserInfo | None:
    """Get extended user information from Neo4j and PostgreSQL.

    Includes basic info from get_user_info plus:
    - User community (ID and size)
    - Top interests (PostCommunity with highest scores)
    - Interaction statistics (favorites, reblogs, replies, bookmarks)
    - Posts count

    Note: Follows/followers counts are not available as FOLLOWS relationships
    are not stored separately in Neo4j (they are included in INTERACTS_WITH).

    Args:
        neo4j: Neo4j client for graph data
        postgres: PostgreSQL client for account data
        user_id: User account ID

    Returns:
        Extended UserInfo dictionary or None if user not found in Neo4j
    """
    # Get basic user info first
    user_info = get_user_info(neo4j, postgres, user_id)
    if user_info is None:
        return None

    # Get user community information
    community_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__ {id: $user_id})-[:BELONGS_TO]->(uc:__uc__) "
            "RETURN uc.id AS uc_id, uc.size AS size "
            "LIMIT 1",
            {"user": "User", "uc": "UserCommunity"},
            {"user_id": user_id},
        )
    )

    user_community_id: int | None = None
    user_community_size: int | None = None
    if community_result:
        uc_id_raw = community_result[0].get("uc_id")
        if uc_id_raw is not None:
            user_community_id = coerce_int(uc_id_raw)
        size_raw = community_result[0].get("size")
        if size_raw is not None:
            user_community_size = coerce_int(size_raw)

    # Get top interests (top 5 PostCommunities by score)
    interests_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__ {id: $user_id})-[:BELONGS_TO]->(uc:__uc__) "
            "      -[i:INTERESTED_IN]->(pc:__pc__) "
            "RETURN pc.id AS pc_id, i.score AS score "
            "ORDER BY i.score DESC "
            "LIMIT 5",
            {"user": "User", "uc": "UserCommunity", "pc": "PostCommunity"},
            {"user_id": user_id},
        )
    )

    top_interests: list[dict[str, Neo4jValue]] | None = None
    if interests_result:
        top_interests = []
        for row in interests_result:
            pc_id_raw = row.get("pc_id")
            score_raw = row.get("score")
            if pc_id_raw is not None and score_raw is not None:
                top_interests.append(
                    {
                        "pc_id": coerce_int(pc_id_raw),
                        "score": coerce_float(score_raw),
                    }
                )

    # Get interaction statistics
    rel_types = neo4j.get_existing_rel_types()
    size_clauses: list[LiteralString] = []
    interaction_keys: list[str] = []
    _interaction_defs: list[tuple[str, LiteralString]] = [
        ("FAVORITED", "  size([(u)-[f:FAVORITED]->() | f]) AS favorited"),
        ("REBLOGGED", "  size([(u)-[r:REBLOGGED]->() | r]) AS reblogged"),
        ("REPLIED", "  size([(u)-[rp:REPLIED]->() | rp]) AS replied"),
        ("BOOKMARKED", "  size([(u)-[bk:BOOKMARKED]->() | bk]) AS bookmarked"),
    ]
    _zero_defs: dict[str, LiteralString] = {
        "FAVORITED": "  0 AS favorited",
        "REBLOGGED": "  0 AS reblogged",
        "REPLIED": "  0 AS replied",
        "BOOKMARKED": "  0 AS bookmarked",
    }
    for rel_name, clause in _interaction_defs:
        if rel_name in rel_types:
            size_clauses.append(clause)
        else:
            size_clauses.append(_zero_defs[rel_name])
        interaction_keys.append(rel_name.lower())

    return_expr: LiteralString = size_clauses[0]
    for c in size_clauses[1:]:
        return_expr = return_expr + ", " + c

    interactions_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__ {id: $user_id}) RETURN " + return_expr,
            {"user": "User"},
            {"user_id": user_id},
        )
    )

    interactions: dict[str, int] | None = None
    if interactions_result:
        row = interactions_result[0]
        interactions = {key: coerce_int(row.get(key, 0)) for key in interaction_keys}

    # Follows/followers counts
    # NOTE: FOLLOWS relationships are not stored separately in Neo4j.
    # They are included in INTERACTS_WITH aggregation via SQL.
    # Setting to None as they cannot be queried from Neo4j graph.
    follows_count: int | None = None
    followers_count: int | None = None

    # Get posts count
    posts_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__ {id: $user_id})-[:WROTE]->(p:__post__) "
            "RETURN count(p) AS posts_count",
            {"user": "User", "post": "Post"},
            {"user_id": user_id},
        )
    )

    posts_count: int | None = None
    if posts_result:
        posts_count = coerce_int(posts_result[0].get("posts_count", 0))

    # Build extended user info
    extended_info: UserInfo = {
        **user_info,
        "user_community_id": user_community_id,
        "user_community_size": user_community_size,
        "top_interests": top_interests,
        "interactions": interactions,
        "follows_count": follows_count,
        "followers_count": followers_count,
        "posts_count": posts_count,
    }

    return extended_info
