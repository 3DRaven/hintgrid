"""Detailed per-post recommendations (separate module to keep feed.py under size limits)."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, LiteralString, TypedDict

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient

from hintgrid.config import HintGridSettings
from hintgrid.pipeline.feed_queries import (
    build_feed_filters,
    build_popularity_expr,
    pagerank_binding,
    pagerank_score_weight_line,
)
from hintgrid.utils.coercion import coerce_float, coerce_int, coerce_str

logger = logging.getLogger(__name__)


class RecommendationDetail(TypedDict):
    """Detailed recommendation information with score components."""

    post_id: int
    post_text: str
    post_language: str | None
    post_created_at: datetime
    author_id: int
    interest_score: float
    popularity: int
    age_hours: float
    pagerank: float
    language_match: float
    final_score: float


def get_detailed_recommendations(
    neo4j: Neo4jClient,
    user_id: int,
    settings: HintGridSettings,
    rel_types: frozenset[str] | None = None,
) -> list[RecommendationDetail]:
    """Get detailed recommendations with score components for a user."""
    filters = build_feed_filters(rel_types)
    popularity_expr = build_popularity_expr(rel_types)
    pr_bind = pagerank_binding(settings)
    pr_w = pagerank_score_weight_line(settings)

    personalized_query: LiteralString = (
        "MATCH (u:__user__ {id: $user_id})-[:BELONGS_TO]->(uc:__uc__) "
        "      -[i:INTERESTED_IN]->(pc:__pc__)<-[:BELONGS_TO]-(p:__post__) "
        "WHERE p.createdAt > datetime() - duration({days: $feed_days}) "
        "AND uc.id <> $noise_community_id AND pc.id <> $noise_community_id "
        + filters
        + "WITH u, p, i.score AS interest_score, "
        + popularity_expr
        + ", "
        "     duration.between(datetime(p.createdAt), datetime()).hours AS age_hours, "
        + pr_bind
        + " "
        "WITH u, p, interest_score, popularity, age_hours, pagerank, "
        "     interest_score * $interest_weight + "
        "     log10(popularity + $popularity_smoothing) * $popularity_weight + "
        "     ($recency_numerator / (age_hours / 24.0 + $recency_smoothing)) * $recency_weight + "
        + pr_w
        + "     CASE WHEN u.languages IS NULL OR p.language IS NULL "
        "              OR p.language IN u.languages "
        "          THEN $language_match_weight "
        "          ELSE 0.0 "
        "     END AS score, "
        "     CASE WHEN u.languages IS NULL OR p.language IS NULL "
        "              OR p.language IN u.languages "
        "          THEN $language_match_weight "
        "          ELSE 0.0 "
        "     END AS language_match "
        "RETURN p.id AS post_id, "
        "       COALESCE(p.text, '') AS post_text, "
        "       p.language AS post_language, "
        "       p.createdAt AS post_created_at, "
        "       COALESCE(p.authorId, 0) AS author_id, "
        "       interest_score, "
        "       popularity, "
        "       age_hours, "
        "       pagerank, "
        "       language_match, "
        "       score AS final_score "
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
            },
        )
    )

    if not rows:
        logger.info("Cold start for user %s", user_id)
        cold_start_limit = min(settings.feed_size, settings.cold_start_limit)
        cold_start_query: LiteralString = (
            "MATCH (u:__user__ {id: $user_id}) "
            "MATCH (p:__post__) "
            "WHERE p.createdAt > datetime() - duration({days: $feed_days}) "
            "  AND p.embedding IS NOT NULL " + filters + "WITH u, p, " + popularity_expr + ", "
            "     duration.between(datetime(p.createdAt), datetime()).hours AS age_hours, "
            + pr_bind
            + " "
            "WITH u, p, popularity, age_hours, pagerank, "
            "     log10(popularity + $popularity_smoothing) * $popularity_weight + "
            "     ($recency_numerator / (age_hours / 24.0 + $recency_smoothing)) * $recency_weight + "
            "     CASE WHEN u.languages IS NULL OR p.language IS NULL "
            "              OR p.language IN u.languages "
            "          THEN $language_match_weight "
            "          ELSE 0.0 "
            "     END AS score, "
            "     CASE WHEN u.languages IS NULL OR p.language IS NULL "
            "              OR p.language IN u.languages "
            "          THEN $language_match_weight "
            "          ELSE 0.0 "
            "     END AS language_match "
            "RETURN p.id AS post_id, "
            "       COALESCE(p.text, '') AS post_text, "
            "       p.language AS post_language, "
            "       p.createdAt AS post_created_at, "
            "       COALESCE(p.authorId, 0) AS author_id, "
            "       0.0 AS interest_score, "
            "       popularity, "
            "       age_hours, "
            "       pagerank, "
            "       language_match, "
            "       score AS final_score "
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
                },
            )
        )

    recommendations: list[RecommendationDetail] = []
    for row in rows:
        post_id = coerce_int(row.get("post_id"))
        post_text = coerce_str(row.get("post_text") or "")
        post_language = row.get("post_language")
        post_language_str: str | None = str(post_language) if post_language is not None else None
        post_created_at_raw = row.get("post_created_at")
        if (
            post_created_at_raw is not None
            and hasattr(post_created_at_raw, "strftime")
            and hasattr(post_created_at_raw, "tzinfo")
        ):
            post_created_at: datetime = post_created_at_raw  # type: ignore[assignment]
        else:
            post_created_at = datetime.now()
        author_id = coerce_int(row.get("author_id") or 0)
        interest_score = coerce_float(row.get("interest_score") or 0.0)
        popularity = coerce_int(row.get("popularity") or 0)
        age_hours = coerce_float(row.get("age_hours") or 0.0)
        pagerank = coerce_float(row.get("pagerank") or 0.0)
        language_match = coerce_float(row.get("language_match") or 0.0)
        final_score = coerce_float(row.get("final_score") or 0.0)

        recommendations.append(
            RecommendationDetail(
                post_id=post_id,
                post_text=post_text,
                post_language=post_language_str,
                post_created_at=post_created_at,
                author_id=author_id,
                interest_score=interest_score,
                popularity=popularity,
                age_hours=age_hours,
                pagerank=pagerank,
                language_match=language_match,
                final_score=final_score,
            )
        )
    return recommendations
