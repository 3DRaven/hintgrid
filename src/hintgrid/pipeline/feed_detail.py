"""Detailed per-post recommendations (separate module to keep feed.py under size limits)."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, LiteralString, TypedDict

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.config import HintGridSettings
from hintgrid.pipeline.feed_personalized_queries import (
    build_personalized_feed_query,
    personalized_feed_score_params,
)
from hintgrid.pipeline.feed_queries import (
    build_feed_filters,
    build_global_raw_binding,
    build_local_raw_binding,
    build_popularity_contrib_expr,
    language_match_score_case,
    pagerank_binding,
    pagerank_score_weight_line,
    popularity_score_params,
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
    share_i: float
    norm_pc_size: float
    local_raw: float
    global_raw: float
    popularity_contrib: float
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
    local_raw_bind = build_local_raw_binding(rel_types)
    global_raw_bind = build_global_raw_binding()
    pop_contrib = build_popularity_contrib_expr(settings)
    pr_bind = pagerank_binding(settings)
    pr_w = pagerank_score_weight_line(settings)

    personalized_query: LiteralString = build_personalized_feed_query(
        filters=filters,
        local_raw_bind=local_raw_bind,
        global_raw_bind=global_raw_bind,
        pop_contrib=pop_contrib,
        pr_bind=pr_bind,
        pr_w=pr_w,
        return_mode="detail",
    )
    personalized_params: dict[str, float | int] = {
        "user_id": user_id,
        "feed_days": settings.feed_days,
        "feed_size": settings.feed_size,
        "noise_community_id": settings.noise_community_id,
    }
    personalized_params.update(personalized_feed_score_params(settings))
    rows = list(
        neo4j.execute_and_fetch_labeled(
            personalized_query,
            {"user": "User", "uc": "UserCommunity", "pc": "PostCommunity", "post": "Post"},
            personalized_params,
        )
    )

    if not rows:
        logger.info("Cold start for user %s", user_id)
        cold_start_limit = min(settings.feed_size, settings.cold_start_limit)
        cold_start_query: LiteralString = (
            "MATCH (u:__user__ {id: $user_id}) "
            "MATCH (p:__post__) "
            "WHERE p.createdAt > datetime() - duration({days: $feed_days}) "
            "  AND p.embedding IS NOT NULL "
            + filters
            + "WITH u, p, "
            + local_raw_bind
            + ", "
            + global_raw_bind
            + ", "
            "     duration.between(datetime(p.createdAt), datetime()).hours AS age_hours, "
            + pr_bind
            + " "
            "WITH u, p, local_raw, global_raw, age_hours, pagerank, "
            "     (" + pop_contrib + ") AS popularity_contrib "
            "WITH u, p, local_raw, global_raw, age_hours, pagerank, popularity_contrib, "
            "     popularity_contrib * $popularity_weight + "
            "     ($recency_numerator / (age_hours / 24.0 + $recency_smoothing)) * $recency_weight + "
            + pr_w
            + language_match_score_case()
            + "AS score, "
            + language_match_score_case()
            + "AS language_match "
            "RETURN p.id AS post_id, "
            "       COALESCE(p.text, '') AS post_text, "
            "       p.language AS post_language, "
            "       p.createdAt AS post_created_at, "
            "       COALESCE(p.authorId, 0) AS author_id, "
            "       0.0 AS interest_score, "
            "       0.0 AS share_i, "
            "       0.0 AS norm_pc_size, "
            "       local_raw, "
            "       global_raw, "
            "       popularity_contrib, "
            "       age_hours, "
            "       pagerank, "
            "       language_match, "
            "       score AS final_score "
            "ORDER BY score DESC LIMIT $cold_start_limit"
        )
        cold_params: dict[str, float | int] = {
            "user_id": user_id,
            "feed_days": settings.feed_days,
            "cold_start_limit": cold_start_limit,
            "popularity_weight": settings.cold_start_popularity_weight,
            "recency_weight": settings.cold_start_recency_weight,
            "pagerank_weight": settings.pagerank_weight if settings.pagerank_enabled else 0.0,
            "recency_smoothing": settings.recency_smoothing,
            "recency_numerator": settings.recency_numerator,
            "language_match_weight": settings.language_match_weight,
            "ui_language_match_weight": settings.ui_language_match_weight,
        }
        cold_params.update(popularity_score_params(settings))
        rows = list(
            neo4j.execute_and_fetch_labeled(
                cold_start_query,
                {"user": "User", "post": "Post"},
                cold_params,
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
        share_i = coerce_float(row.get("share_i") or 0.0)
        norm_pc_size = coerce_float(row.get("norm_pc_size") or 0.0)
        local_raw = coerce_float(row.get("local_raw") or 0.0)
        global_raw = coerce_float(row.get("global_raw") or 0.0)
        popularity_contrib = coerce_float(row.get("popularity_contrib") or 0.0)
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
                share_i=share_i,
                norm_pc_size=norm_pc_size,
                local_raw=local_raw,
                global_raw=global_raw,
                popularity_contrib=popularity_contrib,
                age_hours=age_hours,
                pagerank=pagerank,
                language_match=language_match,
                final_score=final_score,
            )
        )
    return recommendations
