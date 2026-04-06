"""Post information from Neo4j and PostgreSQL for CLI diagnostics."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, LiteralString, TypedDict

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.clients.postgres import PostgresClient

from hintgrid.utils.coercion import coerce_float, coerce_int, coerce_str

logger = logging.getLogger(__name__)


class PostInfo(TypedDict, total=False):
    """Post information from Neo4j and PostgreSQL."""

    post_id: int
    account_id: int
    uri: str | None
    url: str | None
    text: str | None
    language: str | None
    visibility: int
    visibility_label: str
    reblog_of_id: int | None
    in_reply_to_id: int | None
    sensitive: bool
    created_at_pg: datetime | None
    created_at_neo: object | None
    author_username: str | None
    author_domain: str | None
    post_community_id: int | None
    post_community_size: int | None
    total_favourites: int | None
    total_reblogs: int | None
    total_replies: int | None
    pagerank: float | None


_VISIBILITY_NAMES: dict[int, str] = {
    0: "public",
    1: "unlisted",
    2: "private",
    3: "direct",
}


def _visibility_label(visibility: int) -> str:
    return _VISIBILITY_NAMES.get(visibility, f"unknown ({visibility})")


def get_extended_post_info(
    neo4j: Neo4jClient, postgres: PostgresClient, status_id: int
) -> PostInfo | None:
    """Return extended post information if the post exists in Neo4j.

    PostgreSQL fills federation fields (uri, url, visibility); Neo4j fills
    graph-derived metrics and community assignment.

    Args:
        neo4j: Neo4j client
        postgres: PostgreSQL client
        status_id: Internal ``statuses.id`` (same as ``Post.id``)

    Returns:
        ``PostInfo`` or ``None`` if the post node is missing in Neo4j.
    """
    neo_query: LiteralString = (
        "MATCH (p:__post__ {id: $post_id}) "
        "OPTIONAL MATCH (p)-[:BELONGS_TO]->(pc:__pc__) "
        "RETURN p.id AS post_id, "
        "p.text AS text, "
        "p.language AS language, "
        "p.createdAt AS created_at_neo, "
        "p.authorId AS author_id, "
        "p.totalFavourites AS total_favourites, "
        "p.totalReblogs AS total_reblogs, "
        "p.totalReplies AS total_replies, "
        "p.pagerank AS pagerank, "
        "pc.id AS post_community_id, "
        "pc.size AS post_community_size"
    )
    neo_rows = list(
        neo4j.execute_and_fetch_labeled(
            neo_query,
            {"post": "Post", "pc": "PostCommunity"},
            {"post_id": status_id},
        )
    )
    if not neo_rows:
        return None

    row = neo_rows[0]
    author_id = coerce_int(row.get("author_id") or 0)

    pg_row = postgres.fetch_status_for_info(status_id)
    if pg_row is None:
        logger.warning("Post %s in Neo4j but missing from PostgreSQL", status_id)

    account_id = coerce_int(pg_row["account_id"]) if pg_row is not None else author_id
    uri = pg_row.get("uri") if pg_row is not None else None
    url = pg_row.get("url") if pg_row is not None else None
    text_pg = pg_row.get("text") if pg_row is not None else None
    language_pg = pg_row.get("language") if pg_row is not None else None
    visibility = coerce_int(pg_row.get("visibility") or 0) if pg_row is not None else 0
    reblog_of = pg_row.get("reblog_of_id") if pg_row is not None else None
    in_reply = pg_row.get("in_reply_to_id") if pg_row is not None else None
    sensitive_raw = pg_row.get("sensitive") if pg_row is not None else None
    sensitive: bool = bool(sensitive_raw) if sensitive_raw is not None else False
    created_pg_raw = pg_row.get("created_at") if pg_row is not None else None
    created_at_pg: datetime | None = None
    if created_pg_raw is not None and isinstance(created_pg_raw, datetime):
        created_at_pg = created_pg_raw

    text_neo = row.get("text")
    text_final = coerce_str(text_neo, "") if text_neo is not None else None
    if text_pg is not None:
        text_final = coerce_str(text_pg, "")
    lang_neo = row.get("language")
    language_final: str | None = None
    if lang_neo is not None:
        language_final = coerce_str(lang_neo, "")
    elif language_pg is not None:
        language_final = str(language_pg)

    reblog_of_id: int | None = None
    if reblog_of is not None:
        reblog_of_id = coerce_int(reblog_of)
    in_reply_to_id: int | None = None
    if in_reply is not None:
        in_reply_to_id = coerce_int(in_reply)

    uri_s: str | None = str(uri) if uri is not None else None
    url_s: str | None = str(url) if url is not None else None

    accounts = postgres.fetch_account_info([account_id])
    acc = accounts.get(account_id, {})
    author_username = acc.get("username")
    author_domain = acc.get("domain")

    pc_id_raw = row.get("post_community_id")
    post_community_id: int | None = None
    if pc_id_raw is not None:
        post_community_id = coerce_int(pc_id_raw)
    pc_size_raw = row.get("post_community_size")
    post_community_size: int | None = None
    if pc_size_raw is not None:
        post_community_size = coerce_int(pc_size_raw)

    tf = row.get("total_favourites")
    tr = row.get("total_reblogs")
    tre = row.get("total_replies")
    total_favourites: int | None = coerce_int(tf) if tf is not None else None
    total_reblogs: int | None = coerce_int(tr) if tr is not None else None
    total_replies: int | None = coerce_int(tre) if tre is not None else None

    pr_raw = row.get("pagerank")
    pagerank: float | None = coerce_float(pr_raw) if pr_raw is not None else None

    info: PostInfo = {
        "post_id": status_id,
        "account_id": account_id,
        "uri": uri_s,
        "url": url_s,
        "text": text_final,
        "language": language_final,
        "visibility": visibility,
        "visibility_label": _visibility_label(visibility),
        "reblog_of_id": reblog_of_id,
        "in_reply_to_id": in_reply_to_id,
        "sensitive": sensitive,
        "created_at_pg": created_at_pg,
        "created_at_neo": row.get("created_at_neo"),
        "author_username": author_username,
        "author_domain": author_domain,
        "post_community_id": post_community_id,
        "post_community_size": post_community_size,
        "total_favourites": total_favourites,
        "total_reblogs": total_reblogs,
        "total_replies": total_replies,
        "pagerank": pagerank,
    }
    return info
