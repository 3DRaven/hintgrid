"""PostgreSQL client and incremental data loaders."""

from __future__ import annotations

import logging
from urllib.parse import quote
from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager
from datetime import datetime
from typing import TYPE_CHECKING, LiteralString, Protocol, Self, runtime_checkable

import psycopg
from psycopg import Connection, sql
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

if TYPE_CHECKING:
    from types import TracebackType

from hintgrid.config import HintGridSettings
from hintgrid.exceptions import PostgresConnectionError
from hintgrid.utils.coercion import coerce_int
from hintgrid.utils.msgspec_types import CorpusRow
from hintgrid.utils.snowflake import snowflake_id_at

import msgspec

logger = logging.getLogger(__name__)

# Default batch size for streaming cursor (increased for better throughput)
DEFAULT_CORPUS_BATCH_SIZE = 5000


def build_postgres_dsn(settings: HintGridSettings) -> str:
    """Build a PostgreSQL DSN from settings with proper URL-encoding.

    Special characters in the password (e.g. @, :, /, %, &) are percent-encoded
    to produce a valid RFC 3986 URI.

    The DSN includes client_encoding=UTF8 to ensure proper handling of Unicode
    characters in SQL queries (e.g., Cyrillic comments).
    """
    password = quote(settings.postgres_password or "", safe="")
    dsn = (
        f"postgresql://{settings.postgres_user}:{password}"
        f"@{settings.postgres_host}:{settings.postgres_port}"
        f"/{settings.postgres_database}"
    )
    # Add UTF-8 encoding parameter to handle Unicode characters in SQL queries
    return f"{dsn}?client_encoding=UTF8"


# Minimum text length for corpus (filtered in Python, not SQL, for index efficiency)
# Default 1: Phraser training uses ALL posts with > 0 characters.
# Embedding filtering uses separate min_embedding_tokens + percentile settings.
MIN_TEXT_LENGTH = 1

# Pre-defined SQL queries as LiteralString constants (no dynamic concatenation)
# Optimized: removed length(text) > 10 to allow index usage, filtering in Python
# Added (NOT reply OR in_reply_to_account_id = account_id) to match existing index
_SQL_BASE = """
    SELECT id, text
    FROM statuses
    WHERE text IS NOT NULL
      AND text != ''
      AND deleted_at IS NULL
      AND visibility = %s
      AND reblog_of_id IS NULL
      AND (NOT reply OR in_reply_to_account_id = account_id)
    ORDER BY id ASC
"""

_SQL_WITH_MIN_ID = """
    SELECT id, text
    FROM statuses
    WHERE text IS NOT NULL
      AND text != ''
      AND deleted_at IS NULL
      AND visibility = %s
      AND reblog_of_id IS NULL
      AND (NOT reply OR in_reply_to_account_id = account_id)
      AND id > %s
    ORDER BY id ASC
"""

# COUNT versions for progress tracking (same conditions, no ORDER BY)
_SQL_COUNT_BASE: LiteralString = """
    SELECT COUNT(*) AS cnt
    FROM statuses
    WHERE text IS NOT NULL
      AND text != ''
      AND deleted_at IS NULL
      AND visibility = %s
      AND reblog_of_id IS NULL
      AND (NOT reply OR in_reply_to_account_id = account_id)
"""

_SQL_COUNT_WITH_MIN_ID: LiteralString = """
    SELECT COUNT(*) AS cnt
    FROM statuses
    WHERE text IS NOT NULL
      AND text != ''
      AND deleted_at IS NULL
      AND visibility = %s
      AND reblog_of_id IS NULL
      AND (NOT reply OR in_reply_to_account_id = account_id)
      AND id > %s
"""


@runtime_checkable
class TokenizerProtocol(Protocol):
    """Protocol for tokenizers used with PostgresCorpus."""

    def tokenize(self, text: str) -> list[str]:
        """Tokenize text into list of tokens."""
        ...


class PostgresCorpus:
    """Streaming corpus iterator with server-side cursor.

    Uses PostgreSQL server-side cursor to stream data in chunks,
    avoiding loading entire dataset into memory.

    Features:
    - Server-side cursor with configurable batch size (itersize)
    - Supports incremental loading via min_id or since_date
    - Implements __iter__ for reuse across multiple epochs
    - Yields tokenized text for Gensim training

    Optimizations:
    - Uses Snowflake ID conversion for since_date (index-friendly)
    - Filters short texts in Python to avoid length() in SQL
    - Query conditions match existing Mastodon index
    """

    def __init__(
        self,
        dsn: str,
        tokenizer: TokenizerProtocol | None = None,
        min_id: int = 0,
        since_date: datetime | None = None,
        batch_size: int = DEFAULT_CORPUS_BATCH_SIZE,
        public_visibility: int = 0,
        min_text_length: int = MIN_TEXT_LENGTH,
        schema: str | None = None,
    ) -> None:
        """Initialize streaming corpus.

        Args:
            dsn: PostgreSQL connection string
            tokenizer: Optional tokenizer with tokenize(text) -> list[str] method
            min_id: Start loading from ID greater than this (for incremental)
            since_date: Alternative: start loading from this date (converted to min_id)
            batch_size: Number of rows to fetch per network round-trip
            public_visibility: Mastodon visibility filter value
            min_text_length: Minimum text length (filtered in Python for index efficiency)
            schema: PostgreSQL schema name for search_path (default: public)
        """
        self.dsn = dsn
        self.tokenizer = tokenizer
        self.since_date = since_date
        self.batch_size = batch_size
        self.public_visibility = public_visibility
        self.min_text_length = min_text_length
        self.schema = schema

        # Convert since_date to Snowflake ID for index-friendly filtering
        min_id_from_date = 0
        if since_date is not None:
            min_id_from_date = snowflake_id_at(since_date)

        # Use the larger of provided min_id and date-derived min_id
        self.min_id = max(min_id, min_id_from_date)

        self._sql: LiteralString
        self._params: list[object]
        self._sql, self._params = self._build_query()
        self._max_id: int = 0  # Track max ID seen during iteration
        self._count: int = 0  # Track document count

    def _build_query(self) -> tuple[LiteralString, list[object]]:
        """Build SQL query with filtering conditions.

        Uses pre-defined LiteralString queries to ensure SQL injection safety.
        since_date is already converted to min_id in __init__ for index efficiency.
        """
        if self.min_id > 0:
            return _SQL_WITH_MIN_ID, [self.public_visibility, self.min_id]
        else:
            return _SQL_BASE, [self.public_visibility]

    def __iter__(self) -> Iterator[list[str]]:
        """Yield tokenized documents one by one.

        Each iteration opens a new connection and server-side cursor.
        This allows multiple passes (epochs) for Gensim training.

        Note: Text length filtering is done in Python (not SQL) for index efficiency.
        """
        self._max_id = self.min_id
        self._count = 0

        try:
            with psycopg.connect(self.dsn) as conn:
                # Set search_path if schema specified (for worker isolation)
                if self.schema:
                    with conn.cursor() as setup_cur:
                        setup_cur.execute(
                            sql.SQL("SET search_path TO {}, public").format(
                                sql.Identifier(self.schema)
                            )
                        )

                # Server-side cursor: name parameter is KEY for streaming
                # Use dict_row and convert to msgspec Struct for type-safe validation
                with conn.cursor(name="hintgrid_corpus_cursor", row_factory=dict_row) as cur:
                    cur.itersize = self.batch_size

                    cur.execute(self._sql, self._params)

                    for row_dict in cur:
                        # Convert dict to CorpusRow Struct (msgspec validates types)
                        row = msgspec.convert(row_dict, type=CorpusRow)
                        if row.id is not None:
                            self._max_id = max(self._max_id, row.id)

                        # Filter short texts in Python (removed from SQL for index efficiency)
                        if not row.text or len(row.text) < self.min_text_length:
                            continue

                        self._count += 1

                        # Tokenize if tokenizer provided
                        if self.tokenizer is not None:
                            tokens = self.tokenizer.tokenize(row.text)
                            if tokens:
                                yield tokens
                        else:
                            # Simple whitespace tokenization fallback
                            tokens = row.text.lower().split()
                            if tokens:
                                yield tokens

        except Exception as e:
            logger.error("PostgreSQL streaming error: %s", e)
            raise

    def stream_texts(self) -> Iterator[str]:
        """Stream raw text strings without tokenization.

        Uses server-side cursor for memory-efficient streaming.
        Analogous to Neo4jClient.stream_query().

        Note: Text length filtering is done in Python (not SQL) for index efficiency.
        """
        try:
            with psycopg.connect(self.dsn) as conn:
                # Set search_path for worker-schema isolation
                if self.schema:
                    with conn.cursor() as setup_cur:
                        setup_cur.execute(
                            sql.SQL("SET search_path TO {}, public").format(
                                sql.Identifier(self.schema)
                            )
                        )

                with conn.cursor(name="hintgrid_corpus_raw") as cur:
                    cur.itersize = self.batch_size
                    cur.execute(self._sql, self._params)

                    for record in cur:
                        text = record[1]
                        # Filter short texts in Python for index efficiency
                        if text and len(text) >= self.min_text_length:
                            yield str(text)

        except Exception as e:
            logger.error("PostgreSQL streaming error: %s", e)
            raise

    def stream_with_ids(self) -> Iterator[tuple[int, str]]:
        """Stream (id, text) tuples for tracking progress.

        Uses server-side cursor for memory-efficient streaming.
        Analogous to Neo4jClient.stream_query().

        Note: Text length filtering is done in Python (not SQL) for index efficiency.
        """
        try:
            with psycopg.connect(self.dsn) as conn:
                # Set search_path for worker-schema isolation
                if self.schema:
                    with conn.cursor() as setup_cur:
                        setup_cur.execute(
                            sql.SQL("SET search_path TO {}, public").format(
                                sql.Identifier(self.schema)
                            )
                        )

                with conn.cursor(name="hintgrid_corpus_ids", row_factory=dict_row) as cur:
                    cur.itersize = self.batch_size
                    cur.execute(self._sql, self._params)

                    for row_dict in cur:
                        # Convert dict to CorpusRow Struct (msgspec validates types)
                        row = msgspec.convert(row_dict, type=CorpusRow)
                        # Filter short texts in Python for index efficiency
                        if (
                            row.id is not None
                            and row.text
                            and len(row.text) >= self.min_text_length
                        ):
                            yield (row.id, row.text)

        except Exception as e:
            logger.error("PostgreSQL streaming error: %s", e)
            raise

    # Deprecated aliases for backward compatibility
    def iter_raw(self) -> Iterator[str]:
        """Deprecated: Use stream_texts() instead."""
        return self.stream_texts()

    def iter_with_ids(self) -> Iterator[tuple[int, str]]:
        """Deprecated: Use stream_with_ids() instead."""
        return self.stream_with_ids()

    def total_count(self) -> int:
        """Count total documents matching corpus criteria.

        Runs a COUNT(*) query with the same SQL conditions as __iter__,
        useful for setting progress bar total before starting iteration.
        """
        if self.min_id > 0:
            count_sql = _SQL_COUNT_WITH_MIN_ID
            params: list[object] = [self.public_visibility, self.min_id]
        else:
            count_sql = _SQL_COUNT_BASE
            params = [self.public_visibility]

        try:
            with psycopg.connect(self.dsn) as conn:
                if self.schema:
                    with conn.cursor() as setup_cur:
                        setup_cur.execute(
                            sql.SQL("SET search_path TO {}, public").format(
                                sql.Identifier(self.schema)
                            )
                        )
                with conn.cursor() as cur:
                    cur.execute(count_sql, params)
                    row = cur.fetchone()
                    return int(row[0]) if row else 0
        except Exception as e:
            logger.warning("Failed to count corpus documents: %s", e)
            return 0

    @property
    def max_id(self) -> int:
        """Get maximum ID seen during last iteration."""
        return self._max_id

    @property
    def count(self) -> int:
        """Get document count from last iteration."""
        return self._count


class PostgresClient(AbstractContextManager["PostgresClient"]):
    """Wrapper for PostgreSQL access using a connection pool."""

    @staticmethod
    def _dsn_with_schema(dsn: str, schema: str) -> str:
        if not schema or schema == "public":
            return dsn
        options = f"-c search_path={schema},public"
        encoded = quote(options, safe="")
        # Use & separator if DSN already has parameters (e.g., client_encoding=UTF8)
        separator = "&" if "?" in dsn else "?"
        return f"{dsn}{separator}options={encoded}"

    def __init__(
        self,
        dsn: str,
        *,
        min_size: int = 1,
        max_size: int = 5,
        timeout_seconds: int = 30,
        public_visibility: int = 0,
        account_lookup_limit: int = 1,
        host: str = "localhost",
        port: int = 5432,
        database: str = "unknown",
    ) -> None:
        self._host = host
        self._port = port
        self._database = database
        try:
            self._pool: ConnectionPool[Connection] = ConnectionPool(
                dsn,
                min_size=min_size,
                max_size=max_size,
                timeout=timeout_seconds,
                open=True,
            )
            # Test connection immediately
            with self._pool.connection() as conn:
                conn.execute("SELECT 1")
            logger.info("PostgreSQL connected: %s:%s/%s", self._host, self._port, self._database)
        except Exception as exc:
            raise PostgresConnectionError(host, port, database, exc) from exc
        self._public_visibility = public_visibility
        self._account_lookup_limit = account_lookup_limit

    @classmethod
    def from_settings(cls, settings: HintGridSettings) -> PostgresClient:
        dsn = build_postgres_dsn(settings)
        dsn = cls._dsn_with_schema(dsn, settings.postgres_schema)
        return cls(
            dsn,
            min_size=settings.pg_pool_min_size,
            max_size=settings.pg_pool_max_size,
            timeout_seconds=settings.pg_pool_timeout_seconds,
            public_visibility=settings.mastodon_public_visibility,
            account_lookup_limit=settings.mastodon_account_lookup_limit,
            host=settings.postgres_host,
            port=settings.postgres_port,
            database=settings.postgres_database,
        )

    def stream_statuses(
        self,
        last_id: int,
        since_date: datetime | None = None,
        fetch_size: int = 1000,
    ) -> Iterator[dict[str, object]]:
        """Stream all statuses incrementally using server-side cursor (memory-efficient).

        Returns all status types (regular posts, reblogs, replies) in a single
        ordered stream. Callers dispatch to type-specific processors based on
        reblog_of_id / in_reply_to_id fields.

        Optimization: Converts since_date to Snowflake ID to use index on `id`
        instead of requiring index on `created_at`.

        Args:
            last_id: Start loading from ID greater than this
            since_date: Alternative: start loading from this date (converted to min_id)
            fetch_size: Rows per network round-trip (itersize for server-side cursor)

        Yields:
            Row dictionaries with status data including reblog_of_id and in_reply_to_id
        """
        # Convert since_date to minimum Snowflake ID for index-friendly filtering
        effective_min_id = last_id
        if since_date is not None:
            min_id_from_date = snowflake_id_at(since_date)
            effective_min_id = max(last_id, min_id_from_date)

        # Single unified query for all status types (regular, reblogs, replies)
        # Callers distinguish type via reblog_of_id / in_reply_to_id columns
        query = """
            SELECT id, account_id, text, language, created_at,
                   reblog_of_id, in_reply_to_id
            FROM statuses
            WHERE id > %(min_id)s
              AND deleted_at IS NULL
            ORDER BY id ASC;
        """
        return self.stream_query(
            query,
            {"min_id": effective_min_id},
            fetch_size=fetch_size,
        )

    def stream_favourites(
        self,
        last_id: int,
        since_date: datetime | None = None,
        fetch_size: int = 1000,
    ) -> Iterator[dict[str, object]]:
        """Stream favourites incrementally using server-side cursor (memory-efficient).

        Unlike statuses, favourites use standard auto-increment IDs (not Snowflake).
        When since_date is provided, filtering is done via created_at column
        instead of converting to a Snowflake ID.

        Args:
            last_id: Start loading from ID greater than this
            since_date: Alternative: start loading from this date (uses created_at filter)
            fetch_size: Rows per network round-trip (itersize for server-side cursor)

        Yields:
            Row dictionaries with favourite data
        """
        if since_date is not None:
            # Favourites use auto-increment IDs, NOT Snowflake IDs.
            # Filter by created_at column directly instead of ID conversion.
            query = """
                SELECT id, account_id, status_id, created_at
                FROM favourites
                WHERE id > %(min_id)s
                  AND created_at >= %(since_date)s
                ORDER BY id ASC;
            """
            return self.stream_query(
                query,
                {"min_id": last_id, "since_date": since_date},
                fetch_size=fetch_size,
            )

        query = """
            SELECT id, account_id, status_id, created_at
            FROM favourites
            WHERE id > %(min_id)s
            ORDER BY id ASC;
        """
        return self.stream_query(query, {"min_id": last_id}, fetch_size=fetch_size)

    def stream_blocks(self, last_id: int, fetch_size: int = 1000) -> Iterator[dict[str, object]]:
        """Stream blocks incrementally using server-side cursor (memory-efficient).

        Args:
            last_id: Start loading from ID greater than this
            fetch_size: Rows per network round-trip (itersize for server-side cursor)

        Yields:
            Row dictionaries with block data
        """
        # LIMIT removed: server-side cursor handles streaming without loading all into memory
        query = """
            SELECT id, account_id, target_account_id, 'block' AS type, created_at
            FROM blocks
            WHERE id > %(last_id)s
            ORDER BY id ASC;
        """
        return self.stream_query(query, {"last_id": last_id}, fetch_size=fetch_size)

    def stream_mutes(self, last_id: int, fetch_size: int = 1000) -> Iterator[dict[str, object]]:
        """Stream mutes incrementally using server-side cursor (memory-efficient).

        Args:
            last_id: Start loading from ID greater than this
            fetch_size: Rows per network round-trip (itersize for server-side cursor)

        Yields:
            Row dictionaries with mute data
        """
        # LIMIT removed: server-side cursor handles streaming without loading all into memory
        query = """
            SELECT id, account_id, target_account_id, 'mute' AS type, created_at
            FROM mutes
            WHERE id > %(last_id)s
            ORDER BY id ASC;
        """
        return self.stream_query(query, {"last_id": last_id}, fetch_size=fetch_size)

    def stream_user_activity(
        self,
        active_days: int,
        last_account_id: int = 0,
        fetch_size: int = 1000,
    ) -> Iterator[dict[str, object]]:
        """Stream user activity data for lastActive, isLocal, and languages.

        Joins accounts, account_stats, and users tables to compute
        GREATEST(last_status_at, current_sign_in_at) as last_active.
        Falls back to account created_at when activity columns are NULL.

        Also returns:
        - is_local: True if account is local (domain IS NULL)
        - locale: User UI locale from users.locale (for User.uiLanguage in Neo4j)
        - chosen_languages: User's preferred languages from users table

        Only returns accounts whose computed last_active falls within
        the active_days window, avoiding unnecessary network traffic
        for long-inactive accounts.

        Args:
            active_days: Only include accounts active within this many days
            last_account_id: Only include accounts with ID > this (cursor for resume)
            fetch_size: Rows per network round-trip (itersize for server-side cursor)

        Yields:
            Row dictionaries with account_id, last_active, is_local, locale,
            chosen_languages
        """
        query = """
            SELECT a.id AS account_id,
                   (a.domain IS NULL) AS is_local,
                   u.locale,
                   u.chosen_languages,
                   GREATEST(
                       COALESCE(s.last_status_at, a.created_at),
                       COALESCE(u.current_sign_in_at, a.created_at)
                   ) AS last_active
            FROM accounts a
            LEFT JOIN account_stats s ON s.account_id = a.id
            LEFT JOIN users u ON u.account_id = a.id
            WHERE GREATEST(
                COALESCE(s.last_status_at, a.created_at),
                COALESCE(u.current_sign_in_at, a.created_at)
            ) >= NOW() - %(active_days)s * INTERVAL '1 day'
              AND a.id > %(last_account_id)s
            ORDER BY a.id ASC;
        """
        return self.stream_query(
            query,
            {"active_days": active_days, "last_account_id": last_account_id},
            fetch_size=fetch_size,
        )

    def count_active_users(
        self,
        active_days: int,
        last_account_id: int = 0,
    ) -> int:
        """Count accounts active within the given number of days.

        Uses the same filtering logic as stream_user_activity() to provide
        a total count for progress bars.

        Args:
            active_days: Only count accounts active within this many days
            last_account_id: Only count accounts with ID > this (cursor for resume)

        Returns:
            Number of active accounts
        """
        query = """
            SELECT COUNT(*) AS cnt
            FROM accounts a
            LEFT JOIN account_stats s ON s.account_id = a.id
            LEFT JOIN users u ON u.account_id = a.id
            WHERE GREATEST(
                COALESCE(s.last_status_at, a.created_at),
                COALESCE(u.current_sign_in_at, a.created_at)
            ) >= NOW() - %(active_days)s * INTERVAL '1 day'
              AND a.id > %(last_account_id)s;
        """
        with self._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                query,
                {"active_days": active_days, "last_account_id": last_account_id},
            )
            row = cur.fetchone()
            return coerce_int(row["cnt"]) if row else 0

    def fetch_user_id(self, username: str, domain: str | None) -> int | None:
        query = """
            SELECT id
            FROM accounts
            WHERE lower(username) = lower(%(username)s)
              AND (
                ((%(domain)s)::text IS NULL AND domain IS NULL)
                OR lower(domain) = lower((%(domain)s)::text)
              )
            LIMIT %(limit)s;
        """
        with self._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                query,
                {"username": username, "domain": domain, "limit": self._account_lookup_limit},
            )
            rows = cursor.fetchall()
            if not rows:
                return None
            value = rows[0].get("id")
            if value is None:
                return None
            try:
                return coerce_int(value, field="account.id", strict=True)
            except (TypeError, ValueError):
                return None

    def fetch_account_info(self, account_ids: list[int]) -> dict[int, dict[str, str | None]]:
        """Fetch account information (username, domain) for given account IDs.

        Args:
            account_ids: List of account IDs to fetch

        Returns:
            Dictionary mapping account_id to dict with 'username' and 'domain' keys.
            Domain can be None for local accounts.
        """
        if not account_ids:
            return {}

        query = """
            SELECT id, username, domain
            FROM accounts
            WHERE id = ANY(%(ids)s);
        """
        result: dict[int, dict[str, str | None]] = {}
        with self._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, {"ids": account_ids})
            rows = cursor.fetchall()
            for row in rows:
                account_id = coerce_int(row.get("id"))
                username = str(row.get("username") or "")
                domain = row.get("domain")
                domain_str: str | None = str(domain) if domain is not None else None
                result[account_id] = {"username": username, "domain": domain_str}
        return result

    def stream_user_interactions(
        self,
        last_interaction_favourite_id: int = 0,
        last_interaction_status_id: int = 0,
        last_interaction_mention_id: int = 0,
        last_interaction_follow_id: int = 0,
        follows_weight: float = 1.0,
        likes_weight: float = 1.0,
        replies_weight: float = 1.0,
        reblogs_weight: float = 1.0,
        mentions_weight: float = 1.0,
        fetch_size: int = 1000,
    ) -> Iterator[dict[str, object]]:
        """Stream aggregated user-user interactions incrementally from PostgreSQL.

        Computes weighted edges from favourites, replies, reblogs, mentions,
        and follows in a single UNION ALL query. Each yielded row contains
        ``source_id``, ``target_id``, ``total_weight``, and local maximum IDs
        per interaction type (``max_favourite_id``, ``max_status_id``,
        ``max_mention_id``, ``max_follow_id``).

        Incremental filtering via ``last_interaction_*_id`` cursors processes
        only new records since the last run. Each sub-query uses its own cursor
        (replies and reblogs share ``last_interaction_status_id``).

        Self-interactions (source == target) are excluded.

        Args:
            last_interaction_favourite_id: Process favourites with id > this value
            last_interaction_status_id: Process replies/reblogs with id > this value
            last_interaction_mention_id: Process mentions with id > this value
            last_interaction_follow_id: Process follows with id > this value
            follows_weight: Weight for FOLLOWS relationships in aggregation
            likes_weight: Weight multiplier for favourites (count * likes_weight)
            replies_weight: Weight multiplier for replies (count * replies_weight)
            reblogs_weight: Weight multiplier for reblogs (count * reblogs_weight)
            mentions_weight: Weight multiplier for mentions (count * mentions_weight)
            fetch_size: Rows per network round-trip (itersize)

        Yields:
            Row dictionaries with source_id, target_id, total_weight,
            max_favourite_id, max_status_id, max_mention_id, max_follow_id
        """
        query: LiteralString = """
            SELECT
                source_id,
                target_id,
                SUM(weight) AS total_weight,
                MAX(max_favourite_id) AS max_favourite_id,
                MAX(max_status_id) AS max_status_id,
                MAX(max_mention_id) AS max_mention_id,
                MAX(max_follow_id) AS max_follow_id
            FROM (
                SELECT f.account_id AS source_id, s.account_id AS target_id,
                       count(*) * %(likes_weight)s AS weight,
                       MAX(f.id) AS max_favourite_id,
                       NULL::bigint AS max_status_id,
                       NULL::bigint AS max_mention_id,
                       NULL::bigint AS max_follow_id
                FROM favourites f
                JOIN statuses s ON f.status_id = s.id
                WHERE f.id > %(last_interaction_favourite_id)s
                  AND f.account_id != s.account_id
                GROUP BY f.account_id, s.account_id

                UNION ALL

                SELECT s.account_id, parent.account_id,
                       count(*) * %(replies_weight)s,
                       NULL::bigint, MAX(s.id), NULL::bigint, NULL::bigint
                FROM statuses s
                JOIN statuses parent ON s.in_reply_to_id = parent.id
                WHERE s.id > %(last_interaction_status_id)s
                  AND s.in_reply_to_id IS NOT NULL
                  AND s.account_id != parent.account_id
                GROUP BY s.account_id, parent.account_id

                UNION ALL

                SELECT s.account_id, original.account_id,
                       count(*) * %(reblogs_weight)s,
                       NULL::bigint, MAX(s.id), NULL::bigint, NULL::bigint
                FROM statuses s
                JOIN statuses original ON s.reblog_of_id = original.id
                WHERE s.id > %(last_interaction_status_id)s
                  AND s.reblog_of_id IS NOT NULL
                  AND s.account_id != original.account_id
                GROUP BY s.account_id, original.account_id

                UNION ALL

                SELECT s.account_id, m.account_id,
                       count(*) * %(mentions_weight)s,
                       NULL::bigint, NULL::bigint, MAX(m.id), NULL::bigint
                FROM mentions m
                JOIN statuses s ON m.status_id = s.id
                WHERE m.id > %(last_interaction_mention_id)s
                  AND s.account_id != m.account_id AND m.silent = false
                GROUP BY s.account_id, m.account_id

                UNION ALL

                SELECT f.account_id, f.target_account_id,
                       %(follows_weight)s,
                       NULL::bigint, NULL::bigint, NULL::bigint, MAX(f.id)
                FROM follows f
                WHERE f.id > %(last_interaction_follow_id)s
                  AND f.account_id != f.target_account_id
                GROUP BY f.account_id, f.target_account_id
            ) sub
            GROUP BY source_id, target_id
            ORDER BY source_id, target_id;
        """
        params = {
            "last_interaction_favourite_id": last_interaction_favourite_id,
            "last_interaction_status_id": last_interaction_status_id,
            "last_interaction_mention_id": last_interaction_mention_id,
            "last_interaction_follow_id": last_interaction_follow_id,
            "follows_weight": follows_weight,
            "likes_weight": likes_weight,
            "replies_weight": replies_weight,
            "reblogs_weight": reblogs_weight,
            "mentions_weight": mentions_weight,
        }
        return self.stream_query(query, params=params, fetch_size=fetch_size)

    def stream_bookmarks(
        self,
        last_id: int,
        since_date: datetime | None = None,
        fetch_size: int = 1000,
    ) -> Iterator[dict[str, object]]:
        """Stream bookmarks incrementally using server-side cursor.

        Bookmarks are a strong implicit interest signal (user saves post
        for later reading). Stronger than favourites.

        Unlike statuses, bookmarks use standard auto-increment IDs (not Snowflake).
        When since_date is provided, filtering is done via created_at column
        instead of converting to a Snowflake ID.

        Args:
            last_id: Start loading from ID greater than this
            since_date: Alternative: start loading from this date (uses created_at filter)
            fetch_size: Rows per network round-trip (itersize for server-side cursor)

        Yields:
            Row dictionaries with bookmark data (id, account_id, status_id, created_at)
        """
        if since_date is not None:
            # Bookmarks use auto-increment IDs, NOT Snowflake IDs.
            # Filter by created_at column directly instead of ID conversion.
            query = """
                SELECT id, account_id, status_id, created_at
                FROM bookmarks
                WHERE id > %(min_id)s
                  AND created_at >= %(since_date)s
                ORDER BY id ASC;
            """
            return self.stream_query(
                query,
                {"min_id": last_id, "since_date": since_date},
                fetch_size=fetch_size,
            )

        query = """
            SELECT id, account_id, status_id, created_at
            FROM bookmarks
            WHERE id > %(min_id)s
            ORDER BY id ASC;
        """
        return self.stream_query(query, {"min_id": last_id}, fetch_size=fetch_size)

    def stream_status_stats(
        self,
        last_id: int,
        fetch_size: int = 1000,
    ) -> Iterator[dict[str, object]]:
        """Stream status_stats incrementally for popularity signals.

        Includes both local and federated (untrusted) interaction counts.

        Args:
            last_id: Start from status_id greater than this
            fetch_size: Rows per network round-trip (itersize)

        Yields:
            Row dictionaries with status_id, total_favourites, total_reblogs,
            total_replies
        """
        query = """
            SELECT status_id AS id,
                   COALESCE(favourites_count, 0)
                       + COALESCE(untrusted_favourites_count, 0) AS total_favourites,
                   COALESCE(reblogs_count, 0)
                       + COALESCE(untrusted_reblogs_count, 0) AS total_reblogs,
                   replies_count AS total_replies
            FROM status_stats
            WHERE status_id > %(last_id)s
            ORDER BY status_id;
        """
        return self.stream_query(query, {"last_id": last_id}, fetch_size=fetch_size)

    def stream_query(
        self,
        query: LiteralString,
        params: Mapping[str, object] | None = None,
        fetch_size: int = 1000,
    ) -> Iterator[dict[str, object]]:
        """Stream query results with server-side cursor.

        Uses named cursor for true streaming without loading
        all results into memory. Analogous to Neo4jClient.stream_query().

        Args:
            query: SQL query
            params: Query parameters
            fetch_size: Rows per network round-trip (itersize)

        Yields:
            Row dictionaries
        """
        with (
            self._pool.connection() as conn,
            conn.cursor(name="hintgrid_stream", row_factory=dict_row) as cur,
        ):
            cur.itersize = fetch_size
            cur.execute(query, params or {})
            for row in cur:
                row_dict: dict[str, object] = dict(row)
                yield row_dict

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._pool.close()

    def get_table_stats(self, table_name: str, id_column: str = "id") -> dict[str, object] | None:
        """Get statistics for a table: max ID, date, and total count.

        Args:
            table_name: Name of the table
            id_column: Name of the ID column (default: "id", for status_stats use "status_id")

        Returns:
            Dictionary with max_id, max_date, total_count, or None if table doesn't exist
        """
        # Whitelist of allowed table names for safety
        allowed_tables = {
            "statuses",
            "favourites",
            "blocks",
            "mutes",
            "bookmarks",
            "status_stats",
            "accounts",
        }
        if table_name not in allowed_tables:
            logger.warning("Table %s not in whitelist, skipping stats", table_name)
            return None

        # Whitelist of allowed column names for safety
        allowed_columns = {"id", "status_id"}
        if id_column not in allowed_columns:
            logger.warning("Column %s not in whitelist, skipping stats", id_column)
            return None

        try:
            with self._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
                # Get max ID and total count
                query = sql.SQL("""
                        SELECT 
                            MAX({}) AS max_id,
                            COUNT(*) AS total_count
                        FROM {};
                    """).format(
                    sql.Identifier(id_column),
                    sql.Identifier(table_name),
                )
                cur.execute(query)
                row = cur.fetchone()
                if not row or row.get("max_id") is None:
                    return {"max_id": None, "max_date": None, "total_count": 0}

                max_id = coerce_int(row.get("max_id"))
                total_count = coerce_int(row.get("total_count") or 0)

                # For Snowflake IDs (statuses, accounts, status_stats), extract date from ID
                # For auto-increment IDs, get MAX(created_at)
                if table_name in ("statuses", "accounts"):
                    # Extract date from Snowflake ID directly in the same query
                    date_query = sql.SQL("""
                            SELECT to_timestamp((MAX({}) >> 16) / 1000.0) AS max_date
                            FROM {};
                        """).format(
                        sql.Identifier(id_column),
                        sql.Identifier(table_name),
                    )
                    cur.execute(date_query)
                    date_row = cur.fetchone()
                    max_date = date_row.get("max_date") if date_row else None
                elif table_name == "status_stats":
                    # status_stats.status_id is a Snowflake ID
                    cur.execute(
                        sql.SQL("""
                            SELECT to_timestamp((MAX(status_id) >> 16) / 1000.0) AS max_date
                            FROM status_stats;
                        """)
                    )
                    date_row = cur.fetchone()
                    max_date = date_row.get("max_date") if date_row else None
                else:
                    # For auto-increment IDs, use MAX(created_at)
                    date_query = sql.SQL("""
                            SELECT MAX(created_at) AS max_date
                            FROM {};
                        """).format(sql.Identifier(table_name))
                    cur.execute(date_query)
                    date_row = cur.fetchone()
                    max_date = date_row.get("max_date") if date_row else None

                return {
                    "max_id": max_id,
                    "max_date": max_date,
                    "total_count": total_count,
                }
        except Exception as e:
            logger.warning("Failed to get stats for table %s: %s", table_name, e)
            return None

    def get_database_stats(self) -> dict[str, dict[str, object]]:
        """Get statistics for all tables used in data loading.

        Returns:
            Dictionary mapping table names to their statistics
        """
        tables = [
            ("statuses", "id"),
            ("favourites", "id"),
            ("blocks", "id"),
            ("mutes", "id"),
            ("bookmarks", "id"),
            ("status_stats", "status_id"),
            ("accounts", "id"),
        ]

        stats: dict[str, dict[str, object]] = {}
        for table_name, id_column in tables:
            table_stats = self.get_table_stats(table_name, id_column)
            if table_stats:
                stats[table_name] = table_stats

        return stats

    def close(self) -> None:
        """Close underlying pool to avoid resource warnings."""
        self._pool.close()
