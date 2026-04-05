"""Export current HintGrid state to Markdown for documentation."""

from __future__ import annotations

import itertools
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, LiteralString
from collections.abc import Iterable, Sequence

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.clients.redis import RedisClient

from hintgrid.config import HintGridSettings
from hintgrid.utils.coercion import coerce_float, coerce_int

import msgspec

EXPORT_NEWLINE = "\n"
EXPORT_USER_FEED_START = 0
TEXT_PREVIEW_SUFFIX = "..."
PERCENT_FORMAT = "{value:.1f}%"
PERCENT_ZERO = "0.0%"
FEED_SCORE_MULTIPLIER_MIN = 1
ZERO_COUNT = 0
DEFAULT_INT = 0
DEFAULT_FLOAT = 0.0
FEED_SIZE_OFFSET = 1
PERCENT_MULTIPLIER = 100
FIRST_INDEX = 0
COUNT_INCREMENT = 1
COMMUNITY_MEMBER_START = 0


def _extract_object_list(raw: object) -> list[object]:
    """Extract a list of objects from a Neo4j result value.

    Neo4j returns list with unknown inner type (list[int], list[str], etc.).
    We use msgspec for type-safe validation instead of isinstance.
    All Neo4j list types (list[int], list[str], list[float]) are compatible with list[object].
    """
    # Use msgspec for type validation - convert to list if valid Sequence
    try:
        # Try direct conversion - works for list, tuple, etc.
        validated: list[object] = list(raw) if hasattr(raw, "__iter__") else []
        return validated
    except (TypeError, ValueError):
        return []


def export_state(
    neo4j: Neo4jClient,
    redis_client: RedisClient,
    settings: HintGridSettings,
    filename: str,
    user_id: int | None = None,
) -> None:
    """Export system state to a Markdown file."""
    import logging

    from hintgrid.pipeline.graph import (
        check_clusters_exist,
        check_embeddings_exist,
        check_interests_exist,
    )

    logger = logging.getLogger(__name__)

    # Check for missing computed data and warn
    if not check_embeddings_exist(neo4j):
        logger.warning("No embeddings found in graph - export may be incomplete")

    users_exist, posts_exist = check_clusters_exist(neo4j)
    if not users_exist:
        logger.warning("No user clusters found - export may be incomplete")
    if not posts_exist:
        logger.warning("No post clusters found - export may be incomplete")

    if not check_interests_exist(neo4j):
        logger.warning("No interests found - export may be incomplete")

    lines: list[str] = []
    lines.append("# HintGrid Export")
    lines.append("")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    _append_system_overview(lines, neo4j)
    _append_redis_feeds(lines, neo4j, redis_client, settings, user_id)
    _append_community_interest_graph(lines, neo4j, settings)
    _append_user_communities_graph(lines, neo4j, settings)
    _append_post_communities_graph(lines, neo4j, settings)
    _append_interacts_with_graph(lines, neo4j, settings)
    _append_similarity_graph(lines, neo4j, settings)
    _append_detailed_statistics(lines, neo4j, settings)

    Path(filename).write_text(EXPORT_NEWLINE.join(lines) + EXPORT_NEWLINE, encoding="utf-8")


def _append_system_overview(lines: list[str], neo4j: Neo4jClient) -> None:
    labels = {"user": "User", "post": "Post", "uc": "UserCommunity", "pc": "PostCommunity"}
    users = _single_count_labeled(
        neo4j, "MATCH (u:__user__) RETURN count(u) AS count", labels
    )
    posts = _single_count_labeled(
        neo4j, "MATCH (p:__post__) RETURN count(p) AS count", labels
    )
    user_communities = _single_count_labeled(
        neo4j, "MATCH (uc:__uc__) RETURN count(uc) AS count", labels
    )
    post_communities = _single_count_labeled(
        neo4j, "MATCH (pc:__pc__) RETURN count(pc) AS count", labels
    )
    interests = _single_count_labeled(
        neo4j,
        "MATCH (uc:__uc__)-[i:INTERESTED_IN]->(pc:__pc__) RETURN count(i) AS count",
        labels,
    )
    expired_interests = _single_count_labeled(
        neo4j,
        "MATCH (uc:__uc__)-[i:INTERESTED_IN]->(pc:__pc__) "
        "WHERE i.expires_at < datetime() RETURN count(i) AS count",
        labels,
    )
    interacts_with = _single_count_labeled(
        neo4j,
        "MATCH (u:__user__)-[r:INTERACTS_WITH]->(v:__user__) RETURN count(r) AS count",
        labels,
    )
    similar_to = _single_count_labeled(
        neo4j,
        "MATCH (p:__post__)-[r:SIMILAR_TO]->(q:__post__) RETURN count(r) AS count",
        labels,
    )

    lines.append("## System Overview")
    lines.append("")
    lines.append(f"- **Users**: {users}")
    lines.append(f"- **Posts**: {posts}")
    lines.append(f"- **User Communities**: {user_communities}")
    lines.append(f"- **Post Communities**: {post_communities}")
    lines.append(
        f"- **INTERESTED_IN links**: {interests} ({interests - expired_interests} active, {expired_interests} expired)"
    )
    lines.append(
        f"- **INTERACTS_WITH links**: {interacts_with} "
        "(includes follow signal from PostgreSQL; no separate FOLLOWS edges)"
    )
    lines.append(f"- **SIMILAR_TO links**: {similar_to}")
    lines.append("")


def _append_redis_feeds(
    lines: list[str],
    neo4j: Neo4jClient,
    redis_client: RedisClient,
    settings: HintGridSettings,
    user_id: int | None,
) -> None:
    lines.append("## Redis Feeds")
    lines.append("")
    if user_id is not None:
        user_ids_iter: Iterable[int] = [user_id]
    else:
        user_ids_iter = neo4j.stream_user_ids()
    
    # Check if there are any users without loading all into memory
    user_ids_iter_check, user_ids_iter_process = itertools.tee(user_ids_iter, 2)
    if not any(True for _ in user_ids_iter_check):
        lines.append("_No users found._")
        lines.append("")
        return

    for uid in user_ids_iter_process:
        key = f"feed:home:{uid}"
        items = redis_client.zrevrange_with_scores(
            key,
            EXPORT_USER_FEED_START,
            settings.feed_size - FEED_SIZE_OFFSET,
        )
        decoded = [_decode_member_score(item) for item in items]
        post_ids = [post_id for post_id, _ in decoded]
        post_texts = _fetch_post_texts(neo4j, post_ids)
        hintgrid_count, mastodon_count = _count_feed_sources(
            decoded, settings.feed_score_multiplier
        )
        total = hintgrid_count + mastodon_count

        lines.append(f"### {key}")
        lines.append(f"- Total posts: {total}")
        lines.append(
            f"- HintGrid recommendations: {hintgrid_count} ({_pct(hintgrid_count, total)})"
        )
        lines.append(f"- Mastodon posts: {mastodon_count} ({_pct(mastodon_count, total)})")
        lines.append("")
        lines.append("| Post ID | Score | Source | Text Preview |")
        lines.append("|---------|-------|--------|--------------|")
        for post_id, score in decoded[: settings.export_max_items]:
            source = _feed_source(post_id, score, settings.feed_score_multiplier)
            text = post_texts.get(post_id, "")
            preview = (
                text[: settings.text_preview_limit] + TEXT_PREVIEW_SUFFIX
                if len(text) > settings.text_preview_limit
                else text
            )
            preview = preview.replace(EXPORT_NEWLINE, " ").replace("|", "\\|")
            lines.append(
                f"| {post_id} | {score:.{settings.feed_score_decimals}f} | {source} | {preview} |"
            )
        lines.append("")


def _append_community_interest_graph(
    lines: list[str], neo4j: Neo4jClient, settings: HintGridSettings
) -> None:
    # Use streaming for potentially large results
    rows = list(
        neo4j.stream_query_labeled(
            "MATCH (uc:__uc__)-[i:INTERESTED_IN]->(pc:__pc__) "
            "RETURN uc.id AS uc_id, uc.size AS uc_size, "
            "       pc.id AS pc_id, pc.size AS pc_size, i.score AS score "
            "ORDER BY i.score DESC LIMIT $limit",
            {"uc": "UserCommunity", "pc": "PostCommunity"},
            {"limit": settings.community_interest_limit},
        )
    )
    lines.append("## Community Interests Graph")
    lines.append("")
    if not rows:
        lines.append("_No INTERESTED_IN relationships found._")
        lines.append("")
        return
    lines.append("```mermaid")
    lines.append("graph LR")
    for row in rows:
        uc_value = coerce_int(row.get("uc_id"))
        pc_value = coerce_int(row.get("pc_id"))
        uc_size = coerce_int(row.get("uc_size"))
        pc_size = coerce_int(row.get("pc_size"))
        uc_id = _safe_id("UC", uc_value)
        pc_id = _safe_id("PC", pc_value)
        lines.append(f"    {uc_id}[UserCommunity {uc_value}<br/>{uc_size} users]")
        lines.append(f"    {pc_id}[PostCommunity {pc_value}<br/>{pc_size} posts]")
    for row in rows:
        uc_value = coerce_int(row.get("uc_id"))
        pc_value = coerce_int(row.get("pc_id"))
        uc_id = _safe_id("UC", uc_value)
        pc_id = _safe_id("PC", pc_value)
        score = coerce_float(row.get("score"))
        lines.append(f"    {uc_id} -->|score: {score:.2f}| {pc_id}")
    lines.append("```")
    lines.append("")


def _append_user_communities_graph(
    lines: list[str], neo4j: Neo4jClient, settings: HintGridSettings
) -> None:
    # Use streaming for potentially large results
    rows = list(
        neo4j.stream_query_labeled(
            "MATCH (u:__user__)-[:BELONGS_TO]->(uc:__uc__) "
            "WITH uc, collect(u.id) AS all_users "
            "WITH uc, all_users[0..$sample_size] AS users "
            "RETURN uc.id AS uc_id, uc.size AS uc_size, users "
            "ORDER BY uc.size DESC LIMIT $limit",
            {"user": "User", "uc": "UserCommunity"},
            {"sample_size": settings.community_member_sample, "limit": settings.community_sample_limit},
        )
    )
    lines.append("## User Communities Structure")
    lines.append("")
    if not rows:
        lines.append("_No UserCommunity data available._")
        lines.append("")
        return
    lines.append("```mermaid")
    lines.append("graph TD")
    for row in rows:
        uc_value = coerce_int(row.get("uc_id"))
        uc_size = coerce_int(row.get("uc_size"))
        uc_id = _safe_id("UC", uc_value)
        lines.append(f"    {uc_id}[UserCommunity {uc_value}<br/>{uc_size} users]")
        users_list = _extract_object_list(row.get("users"))
        for user_id in users_list:
            user_value = coerce_int(user_id)
            user_node = _safe_id("U", user_value)
            lines.append(f"    {user_node}[User {user_value}] --> {uc_id}")
    lines.append("```")
    lines.append("")


def _append_post_communities_graph(
    lines: list[str], neo4j: Neo4jClient, settings: HintGridSettings
) -> None:
    rows = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__)-[:BELONGS_TO]->(pc:__pc__) "
            "WITH pc, collect(p.id) AS all_posts "
            "WITH pc, all_posts[0..$sample_size] AS posts "
            "RETURN pc.id AS pc_id, pc.size AS pc_size, posts "
            "ORDER BY pc.size DESC LIMIT $limit",
            {"post": "Post", "pc": "PostCommunity"},
            {"sample_size": settings.community_member_sample, "limit": settings.community_sample_limit},
        )
    )
    lines.append("## Post Communities Structure")
    lines.append("")
    if not rows:
        lines.append("_No PostCommunity data available._")
        lines.append("")
        return
    lines.append("```mermaid")
    lines.append("graph TD")
    for row in rows:
        pc_value = coerce_int(row.get("pc_id"))
        pc_size = coerce_int(row.get("pc_size"))
        pc_id = _safe_id("PC", pc_value)
        lines.append(f"    {pc_id}[PostCommunity {pc_value}<br/>{pc_size} posts]")
        posts_list = _extract_object_list(row.get("posts"))
        for post_id in posts_list:
            post_value = coerce_int(post_id)
            post_node = _safe_id("P", post_value)
            lines.append(f"    {post_node}[Post {post_value}] --> {pc_id}")
    lines.append("```")
    lines.append("")


def _append_interacts_with_graph(
    lines: list[str], neo4j: Neo4jClient, settings: HintGridSettings
) -> None:
    # INTERACTS_WITH holds aggregated user-user signal (follows, favs, etc.); no FOLLOWS type.
    rows = list(
        neo4j.stream_query_labeled(
            "MATCH (u1:__user__)-[r:INTERACTS_WITH]->(u2:__user__) "
            "RETURN u1.id AS source, u2.id AS target, r.weight AS weight LIMIT $limit",
            {"user": "User"},
            {"limit": settings.graph_sample_limit},
        )
    )
    lines.append("## User INTERACTS_WITH Graph (sample)")
    lines.append("")
    if not rows:
        lines.append("_No INTERACTS_WITH relationships found._")
        lines.append("")
        return
    lines.append("```mermaid")
    lines.append("graph LR")
    for row in rows:
        src_value = coerce_int(row.get("source"))
        dst_value = coerce_int(row.get("target"))
        weight = coerce_float(row.get("weight") or 0.0)
        src = _safe_id("U", src_value)
        dst = _safe_id("U", dst_value)
        lines.append(
            f"    {src}[User {src_value}] -->|w:{weight:.2f}| {dst}[User {dst_value}]"
        )
    lines.append("```")
    lines.append("")


def _append_similarity_graph(
    lines: list[str], neo4j: Neo4jClient, settings: HintGridSettings
) -> None:
    # Use streaming for potentially large results
    rows = list(
        neo4j.stream_query_labeled(
            "MATCH (p1:__post__)-[r:SIMILAR_TO]->(p2:__post__) "
            "RETURN p1.id AS source, p2.id AS target, r.weight AS weight "
            "LIMIT $limit",
            {"post": "Post"},
            {"limit": settings.graph_sample_limit},
        )
    )
    lines.append("## Post Similarity Graph (sample)")
    lines.append("")
    if not rows:
        lines.append("_No SIMILAR_TO relationships found._")
        lines.append("")
        return
    lines.append("```mermaid")
    lines.append("graph LR")
    for row in rows:
        src_value = coerce_int(row.get("source"))
        dst_value = coerce_int(row.get("target"))
        src = _safe_id("P", src_value)
        dst = _safe_id("P", dst_value)
        weight = coerce_float(row.get("weight"))
        lines.append(f"    {src}[Post {src_value}] -->|{weight:.2f}| {dst}[Post {dst_value}]")
    lines.append("```")
    lines.append("")


def _append_detailed_statistics(
    lines: list[str], neo4j: Neo4jClient, settings: HintGridSettings
) -> None:
    lines.append("## Detailed Statistics")
    lines.append("")
    lines.append("### User Communities")
    lines.append("")
    lines.append("| ID | Size |")
    lines.append("|----|------|")
    # Use streaming for potentially large results
    for row in neo4j.stream_query_labeled(
        "MATCH (uc:__uc__) RETURN uc.id AS id, uc.size AS size "
        "ORDER BY uc.size DESC LIMIT $limit",
        {"uc": "UserCommunity"},
        {"limit": settings.graph_sample_limit},
    ):
        community_id = coerce_int(row.get("id"))
        community_size = coerce_int(row.get("size"))
        lines.append(f"| {community_id} | {community_size} |")
    lines.append("")

    lines.append("### Post Communities")
    lines.append("")
    lines.append("| ID | Size |")
    lines.append("|----|------|")
    # Use streaming for potentially large results
    for row in neo4j.stream_query_labeled(
        "MATCH (pc:__pc__) RETURN pc.id AS id, pc.size AS size "
        "ORDER BY pc.size DESC LIMIT $limit",
        {"pc": "PostCommunity"},
        {"limit": settings.graph_sample_limit},
    ):
        community_id = coerce_int(row.get("id"))
        community_size = coerce_int(row.get("size"))
        lines.append(f"| {community_id} | {community_size} |")
    lines.append("")


def _single_count_labeled(
    neo4j: Neo4jClient,
    template: LiteralString,
    label_map: dict[str, str],
) -> int:
    """Execute a labeled count query and return the count value."""
    rows = list(neo4j.execute_and_fetch_labeled(template, label_map))
    if not rows:
        return ZERO_COUNT
    return coerce_int(rows[FIRST_INDEX].get("count"))


def _decode_member_score(item: tuple[bytes, float] | tuple[str, float]) -> tuple[int, float]:
    """Decode Redis member score tuple, handling bytes and str members."""
    member, score = item
    # Use hasattr to check for decode method instead of isinstance
    if hasattr(member, "decode"):
        member = member.decode("utf-8")
    return int(member), float(score)


def _fetch_post_texts(neo4j: Neo4jClient, post_ids: Iterable[int]) -> dict[int, str]:
    ids = list(post_ids)
    if not ids:
        return {}
    # Use streaming for potentially large results (many post_ids)
    texts: dict[int, str] = {}
    for row in neo4j.stream_query_labeled(
        "MATCH (p:__post__) WHERE p.id IN $ids RETURN p.id AS id, p.text AS text",
        {"post": "Post"},
        {"ids": ids},
    ):
        post_id = coerce_int(row.get("id"))
        text = str(row.get("text") or "")
        texts[post_id] = text
    return texts


def _count_feed_sources(decoded: Iterable[tuple[int, float]], multiplier: int) -> tuple[int, int]:
    hintgrid = ZERO_COUNT
    mastodon = ZERO_COUNT
    for post_id, score in decoded:
        if multiplier > FEED_SCORE_MULTIPLIER_MIN and score >= post_id * multiplier:
            hintgrid += COUNT_INCREMENT
        else:
            mastodon += COUNT_INCREMENT
    return hintgrid, mastodon


def _feed_source(post_id: int, score: float, multiplier: int) -> str:
    if multiplier > FEED_SCORE_MULTIPLIER_MIN and score >= post_id * multiplier:
        return "HintGrid"
    return "Mastodon"


def _pct(part: int, total: int) -> str:
    if total == ZERO_COUNT:
        return PERCENT_ZERO
    return PERCENT_FORMAT.format(value=(part / total) * PERCENT_MULTIPLIER)




def _safe_id(prefix: str, value: int) -> str:
    return f"{prefix}{value}"
