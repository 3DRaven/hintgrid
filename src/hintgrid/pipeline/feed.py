"""Feed generation and Redis storage."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING, LiteralString

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.clients.redis import RedisClient

from hintgrid.config import HintGridSettings
from hintgrid.pipeline.feed_detail import RecommendationDetail, get_detailed_recommendations
from hintgrid.pipeline.feed_queries import (
    build_feed_filters,
    build_popularity_expr,
    language_match_score_case,
    pagerank_binding,
    pagerank_score_weight_line,
)
from hintgrid.utils.coercion import coerce_float, coerce_int

logger = logging.getLogger(__name__)

MIN_HINTGRID_MULTIPLIER = 1
FEED_TTL_DISABLED = "none"

__all__ = [
    "RecommendationDetail",
    "generate_public_feed",
    "generate_user_feed",
    "get_detailed_recommendations",
    "mark_recommended",
    "namespaced_key",
    "set_feed_generated_at",
    "write_feed_to_redis",
    "write_public_feed_to_redis",
]


def namespaced_key(key: str, settings: HintGridSettings) -> str:
    """Prefix Redis key with namespace if configured.

    Mastodon uses REDIS_NAMESPACE to prefix all keys. HintGrid must
    use the same namespace to write to the correct timeline keys.

    Args:
        key: Raw Redis key (e.g. "timeline:public")
        settings: HintGrid settings with optional redis_namespace

    Returns:
        Namespaced key (e.g. "cache:timeline:public") or original key
    """
    if settings.redis_namespace:
        return f"{settings.redis_namespace}:{key}"
    return key


def generate_user_feed(
    neo4j: Neo4jClient,
    user_id: int,
    settings: HintGridSettings,
    rel_types: frozenset[str] | None = None,
) -> list[dict[str, float]]:
    """Generate personalized feed for user with cold start fallback."""
    filters = build_feed_filters(rel_types)
    popularity = build_popularity_expr(rel_types)

    # Try personalized feed based on community interests
    pr_bind = pagerank_binding(settings)
    pr_w = pagerank_score_weight_line(settings)
    personalized_query: LiteralString = (
        "MATCH (u:__user__ {id: $user_id})-[:BELONGS_TO]->(uc:__uc__) "
        "      -[i:INTERESTED_IN]->(pc:__pc__)<-[:BELONGS_TO]-(p:__post__) "
        "WHERE p.createdAt > datetime() - duration({days: $feed_days}) "
        "AND uc.id <> $noise_community_id AND pc.id <> $noise_community_id "
        + filters
        + "WITH u, p, i.score AS interest_score, "
        + popularity
        + ", "
        "     duration.between(datetime(p.createdAt), datetime()).hours AS age_hours, "
        + pr_bind
        + " "
        "WITH p, "
        "     interest_score * $interest_weight + "
        "     log10(popularity + $popularity_smoothing) * $popularity_weight + "
        "     ($recency_numerator / (age_hours / 24.0 + $recency_smoothing)) * $recency_weight + "
        + pr_w
        + language_match_score_case()
        + "AS score "
        "RETURN p.id AS post_id, score "
        "ORDER BY score DESC LIMIT $feed_size"
    )
    rows = list(
        neo4j.execute_and_fetch_labeled(
            personalized_query,
            {"user": "User", "uc": "UserCommunity", "pc": "PostCommunity", "post": "Post"},
            {
                "user_id": user_id,
                "feed_days": settings.feed_days,
                "feed_size": settings.feed_size,
                "noise_community_id": settings.noise_community_id,
                "interest_weight": settings.personalized_interest_weight,
                "popularity_weight": settings.personalized_popularity_weight,
                "recency_weight": settings.personalized_recency_weight,
                "pagerank_weight": settings.pagerank_weight if settings.pagerank_enabled else 0.0,
                "popularity_smoothing": settings.popularity_smoothing,
                "recency_smoothing": settings.recency_smoothing,
                "recency_numerator": settings.recency_numerator,
                "language_match_weight": settings.language_match_weight,
                "ui_language_match_weight": settings.ui_language_match_weight,
            },
        )
    )

    # If no personalized results, use cold start (globally popular posts)
    if not rows:
        logger.info("Cold start for user %s", user_id)
        cold_start_limit = min(settings.feed_size, settings.cold_start_limit)
        cold_start_query: LiteralString = (
            "MATCH (u:__user__ {id: $user_id}) "
            "MATCH (p:__post__) "
            "WHERE p.createdAt > datetime() - duration({days: $feed_days}) "
            "  AND p.embedding IS NOT NULL " + filters + "WITH u, p, " + popularity + ", "
            "     duration.between(datetime(p.createdAt), datetime()).hours AS age_hours "
            "WITH p, "
            "     log10(popularity + $popularity_smoothing) * $popularity_weight + "
            "     ($recency_numerator / (age_hours / 24.0 + $recency_smoothing)) * $recency_weight + "
            + language_match_score_case()
            + "AS score "
            "RETURN p.id AS post_id, score "
            "ORDER BY score DESC LIMIT $cold_start_limit"
        )
        rows = list(
            neo4j.execute_and_fetch_labeled(
                cold_start_query,
                {"user": "User", "post": "Post"},
                {
                    "user_id": user_id,
                    "feed_days": settings.feed_days,
                    "cold_start_limit": cold_start_limit,
                    "popularity_weight": settings.cold_start_popularity_weight,
                    "recency_weight": settings.cold_start_recency_weight,
                    "popularity_smoothing": settings.popularity_smoothing,
                    "recency_smoothing": settings.recency_smoothing,
                    "recency_numerator": settings.recency_numerator,
                    "language_match_weight": settings.language_match_weight,
                    "ui_language_match_weight": settings.ui_language_match_weight,
                },
            )
        )

    recommendations: list[dict[str, float]] = []
    for row in rows:
        post_id = coerce_int(row.get("post_id"))
        score = coerce_float(row.get("score"))
        recommendations.append({"post_id": post_id, "score": score})
    return recommendations


def write_feed_to_redis(
    redis_client: RedisClient,
    user_id: int,
    recommendations: Iterable[dict[str, float]],
    settings: HintGridSettings,
) -> None:
    """Write recommendations to Redis feed with rank-based interest scoring.

    Score is rank-based: most interesting post gets highest score.
    All scores > max_post_id to outrank Mastodon native entries.
    Recommendations must be pre-sorted by interest score DESC
    (guaranteed by Cypher ORDER BY score DESC in generate_user_feed).

    Scoring formula:
        base = max(post_id) * multiplier
        redis_score[rank] = base + (N - rank)
        rank 0 = most interesting → highest redis score
    """
    key = f"feed:home:{user_id}"
    recs = list(recommendations)
    if not recs:
        return

    total = len(recs)
    max_post_id = max(int(rec["post_id"]) for rec in recs)
    base = max_post_id * settings.feed_score_multiplier

    pipe = redis_client.pipeline()
    for rank, rec in enumerate(recs):
        # recs already sorted by interest DESC (from Cypher ORDER BY score DESC)
        # rank 0 = most interesting → highest redis score
        redis_score = base + (total - rank)
        pipe.zadd(key, {str(int(rec["post_id"])): redis_score})
    pipe.execute()
    logger.info("Stored %s items in Redis feed %s", total, key)


def set_feed_generated_at(neo4j: Neo4jClient, user_id: int) -> None:
    """Set feedGeneratedAt timestamp on user after feed is written."""
    neo4j.execute_labeled(
        "MATCH (u:__user__ {id: $user_id}) SET u.feedGeneratedAt = datetime()",
        {"user": "User"},
        {"user_id": user_id},
    )


def mark_recommended(
    neo4j: Neo4jClient, user_id: int, recommendations: Iterable[dict[str, float]]
) -> None:
    """Mark posts as recommended to user (to avoid duplicates)."""
    recs = list(recommendations)
    if not recs:
        return

    neo4j.execute_labeled(
        "MATCH (u:__user__ {id: $user_id}) "
        "UNWIND $batch AS row "
        "MATCH (p:__post__ {id: row.post_id}) "
        "MERGE (u)-[r:WAS_RECOMMENDED]->(p) "
        "ON CREATE SET r.at = datetime(), r.score = row.score",
        {"user": "User", "post": "Post"},
        {"user_id": user_id, "batch": recs},
    )


def generate_public_feed(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    *,
    local_only_interests: bool = True,
    local_only_authors: bool = False,
    rel_types: frozenset[str] | None = None,
) -> list[dict[str, float]]:
    """Generate aggregate public feed based on community interests.

    Cypher query aggregates INTERESTED_IN scores across communities
    to find globally interesting posts. No per-user personalization.

    Args:
        neo4j: Neo4j client
        settings: HintGrid settings
        local_only_interests: If True, only use communities with local users
            (strategy "local_communities"). If False, use all communities
            (strategy "all_communities").
        local_only_authors: If True, only include posts by local authors
            (for timeline:public:local).
        rel_types: Relationship types present in the graph.

    Returns:
        List of recommendations sorted by score DESC.
    """
    popularity = build_popularity_expr(rel_types)

    # Build the query depending on strategy
    if local_only_interests:
        match_clause: LiteralString = (
            "MATCH (u:__user__ {isLocal: true})-[:BELONGS_TO]->(uc:__uc__) "
            "      -[i:INTERESTED_IN]->(pc:__pc__)<-[:BELONGS_TO]-(p:__post__) "
        )
    else:
        match_clause = (
            "MATCH (uc:__uc__)-[i:INTERESTED_IN]->(pc:__pc__)<-[:BELONGS_TO]-(p:__post__) "
        )

    where_clause: LiteralString = (
        "WHERE p.createdAt > datetime() - duration({days: $feed_days}) "
        "AND uc.id <> $noise_community_id AND pc.id <> $noise_community_id "
    )
    if local_only_authors:
        where_clause = (
            where_clause + "  AND EXISTS { MATCH (p)<-[:WROTE]-(author:__user__ {isLocal: true}) } "
        )

    pr_bind = pagerank_binding(settings)
    if settings.pagerank_enabled:
        score_tail: LiteralString = (
            "     ($recency_numerator / (age_hours / 24.0 + $recency_smoothing)) "
            "        * $recency_weight + "
            "     pagerank * $pagerank_weight AS score "
        )
    else:
        score_tail = (
            "     ($recency_numerator / (age_hours / 24.0 + $recency_smoothing)) "
            "        * $recency_weight AS score "
        )

    query: LiteralString = (
        match_clause
        + where_clause
        + "WITH p, "
        + "     sum(i.score) AS community_interest, "
        + popularity
        + ", "
        + pr_bind
        + ", "
        + "     duration.between(datetime(p.createdAt), datetime()).hours AS age_hours "
        + "WITH p, "
        + "     community_interest * $interest_weight + "
        + "     log10(popularity + $popularity_smoothing) * $popularity_weight + "
        + score_tail
        + "RETURN p.id AS post_id, score "
        + "ORDER BY score DESC LIMIT $public_feed_size"
    )

    label_map: dict[str, str] = {
        "uc": "UserCommunity",
        "pc": "PostCommunity",
        "post": "Post",
    }
    if local_only_interests or local_only_authors:
        label_map["user"] = "User"

    rows = list(
        neo4j.execute_and_fetch_labeled(
            query,
            label_map,
            {
                "feed_days": settings.feed_days,
                "public_feed_size": settings.public_feed_size,
                "noise_community_id": settings.noise_community_id,
                "interest_weight": settings.personalized_interest_weight,
                "popularity_weight": settings.personalized_popularity_weight,
                "recency_weight": settings.personalized_recency_weight,
                "pagerank_weight": (settings.pagerank_weight if settings.pagerank_enabled else 0.0),
                "popularity_smoothing": settings.popularity_smoothing,
                "recency_smoothing": settings.recency_smoothing,
                "recency_numerator": settings.recency_numerator,
            },
        )
    )

    recommendations: list[dict[str, float]] = []
    for row in rows:
        post_id = coerce_int(row.get("post_id"))
        score = coerce_float(row.get("score"))
        recommendations.append({"post_id": post_id, "score": score})
    return recommendations


def write_public_feed_to_redis(
    redis_client: RedisClient,
    key: str,
    recommendations: Iterable[dict[str, float]],
    settings: HintGridSettings,
) -> None:
    """Write recommendations to a Mastodon public timeline in Redis.

    Score is rank-based: most interesting post gets highest score.
    All scores > max_post_id to outrank Mastodon native entries.
    Mastodon entries (score = post_id) are automatically pushed out by trim.

    Args:
        redis_client: Redis client
        key: Raw timeline key (e.g. "timeline:public"); namespace is applied here
        recommendations: Pre-sorted by interest score DESC
        settings: HintGrid settings (for multiplier and namespace)
    """
    recs = list(recommendations)
    if not recs:
        return

    total = len(recs)
    max_post_id = max(int(rec["post_id"]) for rec in recs)
    base = max_post_id * settings.feed_score_multiplier

    namespaced = namespaced_key(key, settings)
    pipe = redis_client.pipeline()
    for rank, rec in enumerate(recs):
        # recs already sorted by interest DESC (from Cypher ORDER BY score DESC)
        # rank 0 = most interesting → highest redis score
        redis_score = base + (total - rank)
        pipe.zadd(namespaced, {str(int(rec["post_id"])): redis_score})
    pipe.execute()
    logger.info("Stored %s items in Redis public feed %s", total, namespaced)
