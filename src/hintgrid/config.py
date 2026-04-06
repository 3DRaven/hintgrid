"""Application configuration via env and CLI overrides."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_POSTGRES_HOST = "localhost"
DEFAULT_POSTGRES_PORT = 5432
DEFAULT_POSTGRES_DATABASE = "mastodon_production"
DEFAULT_POSTGRES_USER = "mastodon"
DEFAULT_POSTGRES_SCHEMA = "public"
DEFAULT_NEO4J_HOST = "localhost"
DEFAULT_NEO4J_PORT = 7687
DEFAULT_NEO4J_USERNAME = "neo4j"
DEFAULT_NEO4J_PASSWORD = "password"
DEFAULT_NEO4J_WORKER_LABEL: str | None = None
DEFAULT_REDIS_HOST = "localhost"
DEFAULT_REDIS_PORT = 6379
DEFAULT_REDIS_DB = 0
DEFAULT_LLM_PROVIDER = "ollama"
DEFAULT_LLM_BASE_URL: str | None = None  # None = use FastText by default
DEFAULT_LLM_MODEL = "nomic-embed-text"
DEFAULT_LLM_DIMENSIONS = 768
DEFAULT_LLM_TIMEOUT = 30
DEFAULT_LLM_MAX_RETRIES = 3
DEFAULT_LLM_BATCH_SIZE = 256  # Texts per API call (OpenAI max 2048, Ollama varies)

# FastText embedding settings (for local embedding service)
DEFAULT_FASTTEXT_VECTOR_SIZE = 128  # 64-300 recommended, 768 is overkill
DEFAULT_FASTTEXT_WINDOW = 3
DEFAULT_FASTTEXT_MIN_COUNT = 10  # Aggressive pruning for social media (was 1)
DEFAULT_FASTTEXT_MAX_VOCAB_SIZE = 500_000  # Cap vocab growth to reduce memory
DEFAULT_FASTTEXT_EPOCHS = 5
DEFAULT_FASTTEXT_BUCKET = 10000  # Reduced from 2M for memory efficiency
DEFAULT_FASTTEXT_MIN_DOCUMENTS = 100  # Minimum docs to start training
DEFAULT_FASTTEXT_MODEL_PATH = "~/.hintgrid/models"  # Model storage path
DEFAULT_FASTTEXT_QUANTIZE = True  # Enable model quantization (10-50x size reduction)
DEFAULT_FASTTEXT_QUANTIZE_QDIM = 64  # PQ subquantizers; must divide fasttext_vector_size
DEFAULT_BATCH_SIZE = 10_000
DEFAULT_LOAD_SINCE: str | None = None
DEFAULT_MAX_RETRIES = 3
DEFAULT_USER_COMMUNITIES = "dynamic"
DEFAULT_POST_COMMUNITIES = "dynamic"
DEFAULT_LEIDEN_RESOLUTION = (
    0.1  # Lower gamma = fewer, larger communities (1.0 fragments social graphs)
)
DEFAULT_KNN_NEIGHBORS = 10  # More neighbors = denser similarity graph for post clustering
DEFAULT_SIMILARITY_THRESHOLD = 0.7  # Lower threshold = more edges, prevents graph fragmentation
DEFAULT_SERENDIPITY_PROBABILITY = 0.1
DEFAULT_INTERESTS_TTL_DAYS = 30
DEFAULT_INTERESTS_MIN_FAVOURITES = 5
DEFAULT_LIKES_WEIGHT = 1.0
DEFAULT_REBLOGS_WEIGHT = 3.0
DEFAULT_REPLIES_WEIGHT = 5.0
DEFAULT_FOLLOWS_WEIGHT = 10.0  # Weight for FOLLOWS in INTERACTS_WITH aggregation
DEFAULT_MENTIONS_WEIGHT = 5.0  # Weight for mentions in INTERACTS_WITH aggregation
DEFAULT_DECAY_HALF_LIFE_DAYS = 14
DEFAULT_CTR_ENABLED = True
DEFAULT_CTR_WEIGHT = 0.5
DEFAULT_MIN_CTR = 0.0
DEFAULT_CTR_SMOOTHING = 1.0
DEFAULT_SERENDIPITY_LIMIT = 100
DEFAULT_SERENDIPITY_SCORE = 0.1
DEFAULT_SERENDIPITY_BASED_ON = 0
DEFAULT_FEED_SIZE = 500
DEFAULT_FEED_DAYS = 7
DEFAULT_FEED_TTL = "none"
DEFAULT_FEED_SCORE_MULTIPLIER = 2
DEFAULT_PERSONALIZED_INTEREST_WEIGHT = 0.5
DEFAULT_PERSONALIZED_POPULARITY_WEIGHT = 0.3
DEFAULT_PERSONALIZED_RECENCY_WEIGHT = 0.2
DEFAULT_COLD_START_POPULARITY_WEIGHT = 0.7
DEFAULT_COLD_START_RECENCY_WEIGHT = 0.3
DEFAULT_POPULARITY_SMOOTHING = 1.0
DEFAULT_RECENCY_SMOOTHING = 1.0
DEFAULT_RECENCY_NUMERATOR = 1.0
DEFAULT_COLD_START_FALLBACK = "global_top"
DEFAULT_COLD_START_LIMIT = 500
DEFAULT_SIMILARITY_PRUNING = "temporal"
DEFAULT_PRUNE_AFTER_CLUSTERING = True
DEFAULT_PRUNE_SIMILARITY_THRESHOLD = 0.9
DEFAULT_PRUNE_DAYS = 30
DEFAULT_LEIDEN_MAX_LEVELS = 10
DEFAULT_LEIDEN_DIAGNOSTICS_ENABLED = False
DEFAULT_SINGLETON_COLLAPSE_ENABLED = True
DEFAULT_NOISE_COMMUNITY_ID = -1
# 0 = one Cypher transaction (no CALL/IN TRANSACTIONS). >0 = batch size for
# SET after UNWIND (Neo4j 5+), limits memory per transaction on huge post graphs.
DEFAULT_SINGLETON_COLLAPSE_IN_TRANSACTIONS_OF = 100_000
DEFAULT_SIMILARITY_RECENCY_DAYS = 7
DEFAULT_KNN_SELF_NEIGHBOR_OFFSET = 1
DEFAULT_EXPORT_MAX_ITEMS = 50
DEFAULT_TEXT_PREVIEW_LIMIT = 60
DEFAULT_COMMUNITY_INTEREST_LIMIT = 30
DEFAULT_COMMUNITY_MEMBER_SAMPLE = 5
DEFAULT_COMMUNITY_SAMPLE_LIMIT = 5
DEFAULT_GRAPH_SAMPLE_LIMIT = 10
DEFAULT_FEED_SCORE_DECIMALS = 4
DEFAULT_PG_POOL_MIN_SIZE = 1
DEFAULT_PG_POOL_MAX_SIZE = 5
DEFAULT_PG_POOL_TIMEOUT_SECONDS = 30
DEFAULT_NEO4J_READY_RETRIES = 30
DEFAULT_NEO4J_READY_SLEEP_SECONDS = 1
DEFAULT_REDIS_SCORE_TOLERANCE = 1e-6
DEFAULT_MASTODON_PUBLIC_VISIBILITY = 0
DEFAULT_MASTODON_ACCOUNT_LOOKUP_LIMIT = 1
DEFAULT_CHECKPOINT_INTERVAL = 1000
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOG_FILE = "hintgrid.log"
DEFAULT_PROGRESS_OUTPUT: Literal["auto", "rich", "plain"] = "auto"
DEFAULT_PROGRESS_POLL_INTERVAL_SECONDS = 0.5
DEFAULT_APOC_BATCH_SIZE = 10000
# SIMILAR_TO build runs vector index queries per post; large batches exceed Neo4j
# transaction memory (dbms.memory.transaction.total.max) on typical instances.
DEFAULT_SIMILARITY_ITERATE_BATCH_SIZE = 2000
DEFAULT_PAGERANK_ENABLED = True
DEFAULT_PAGERANK_WEIGHT = 0.1
DEFAULT_PAGERANK_DAMPING_FACTOR = 0.85
DEFAULT_PAGERANK_MAX_ITERATIONS = 20
DEFAULT_COMMUNITY_SIMILARITY_ENABLED = True
DEFAULT_COMMUNITY_SIMILARITY_TOP_K = 5
DEFAULT_ACTIVE_USER_DAYS = 90
DEFAULT_FEED_FORCE_REFRESH = False

# Language boost for feed scoring (chosen_languages)
DEFAULT_LANGUAGE_MATCH_WEIGHT = 0.3
# Boost when post language matches users.locale (UI), typically >= language_match_weight
DEFAULT_UI_LANGUAGE_MATCH_WEIGHT = 0.5

# Bookmarks weight in interest calculation (higher than likes)
DEFAULT_BOOKMARK_WEIGHT = 2.0

# Public timelines (timeline:public, timeline:public:local)
DEFAULT_PUBLIC_FEED_SIZE = 400  # Mastodon default limit
DEFAULT_PUBLIC_FEED_ENABLED = True  # Enable filling public timelines
DEFAULT_PUBLIC_FEED_STRATEGY = "local_communities"  # "all_communities" or "local_communities"
DEFAULT_PUBLIC_TIMELINE_KEY = "timeline:public"
DEFAULT_LOCAL_TIMELINE_KEY = "timeline:public:local"

# Redis namespace (for Mastodon REDIS_NAMESPACE support)
DEFAULT_REDIS_NAMESPACE: str | None = None

# Embedding filtering: token-based + percentile
DEFAULT_MIN_EMBEDDING_TOKENS = 1  # Posts with fewer tokens get no embedding
DEFAULT_EMBEDDING_SKIP_PERCENTILE = 0.0  # 0.05 = skip shortest 5% by char length

# Concurrency settings
DEFAULT_FASTTEXT_TRAINING_WORKERS = 0  # 0 = auto-detect (os.cpu_count())
DEFAULT_FEED_WORKERS = 1  # ThreadPool workers for feed generation
DEFAULT_LOADER_WORKERS = 1  # ThreadPool workers for entity loading


class HintGridSettings(BaseSettings):
    """Configuration loaded from environment variables with HINTGRID_ prefix.

    Configuration priority (highest to lowest):
        1. CLI flags (--postgres-host, etc.)
        2. Environment variables (HINTGRID_POSTGRES_HOST, etc.)
        3. .env file in current directory
        4. .env.local file (for local development overrides, not committed to git)
        5. Default values defined in this class

    Environment variables are read as HINTGRID_<FIELD_NAME> in uppercase.
    For example: HINTGRID_POSTGRES_HOST, HINTGRID_NEO4J_PORT, etc.

    Example .env file:
        HINTGRID_POSTGRES_HOST=db.example.com
        HINTGRID_POSTGRES_PORT=5432
        HINTGRID_NEO4J_PASSWORD=secret
    """

    model_config = SettingsConfigDict(
        env_prefix="HINTGRID_",
        case_sensitive=False,
        # Load .env files with priority (later files override earlier ones)
        env_file=(".env", ".env.local"),
        env_file_encoding="utf-8",
        # Don't fail if .env files don't exist
        env_ignore_empty=True,
        # Extra fields are ignored (for forward compatibility)
        extra="ignore",
    )

    # PostgreSQL connection
    postgres_host: str = Field(default=DEFAULT_POSTGRES_HOST)
    postgres_port: int = Field(default=DEFAULT_POSTGRES_PORT)
    postgres_database: str = Field(default=DEFAULT_POSTGRES_DATABASE)
    postgres_user: str = Field(default=DEFAULT_POSTGRES_USER)
    postgres_password: str | None = Field(default=None)
    postgres_schema: str = Field(default=DEFAULT_POSTGRES_SCHEMA)

    # Neo4j connection
    neo4j_host: str = Field(default=DEFAULT_NEO4J_HOST)
    neo4j_port: int = Field(default=DEFAULT_NEO4J_PORT)
    neo4j_username: str = Field(default=DEFAULT_NEO4J_USERNAME)
    neo4j_password: str = Field(default=DEFAULT_NEO4J_PASSWORD)
    neo4j_worker_label: str | None = Field(default=DEFAULT_NEO4J_WORKER_LABEL)

    # Redis connection
    redis_host: str = Field(default=DEFAULT_REDIS_HOST)
    redis_port: int = Field(default=DEFAULT_REDIS_PORT)
    redis_db: int = Field(default=DEFAULT_REDIS_DB)
    redis_password: str | None = Field(default=None)

    # LLM/embeddings provider
    llm_provider: str = Field(default=DEFAULT_LLM_PROVIDER)
    llm_base_url: str | None = Field(default=DEFAULT_LLM_BASE_URL)
    llm_model: str = Field(default=DEFAULT_LLM_MODEL)
    llm_dimensions: int = Field(default=DEFAULT_LLM_DIMENSIONS)
    llm_timeout: int = Field(default=DEFAULT_LLM_TIMEOUT)
    llm_max_retries: int = Field(default=DEFAULT_LLM_MAX_RETRIES)
    llm_batch_size: int = Field(default=DEFAULT_LLM_BATCH_SIZE)
    llm_api_key: str | None = Field(default=None)

    # FastText embedding settings (for local embedding service)
    fasttext_vector_size: int = Field(default=DEFAULT_FASTTEXT_VECTOR_SIZE)
    fasttext_window: int = Field(default=DEFAULT_FASTTEXT_WINDOW)
    fasttext_min_count: int = Field(default=DEFAULT_FASTTEXT_MIN_COUNT)
    fasttext_max_vocab_size: int = Field(default=DEFAULT_FASTTEXT_MAX_VOCAB_SIZE)
    fasttext_epochs: int = Field(default=DEFAULT_FASTTEXT_EPOCHS)
    fasttext_bucket: int = Field(default=DEFAULT_FASTTEXT_BUCKET)
    fasttext_min_documents: int = Field(default=DEFAULT_FASTTEXT_MIN_DOCUMENTS)
    fasttext_model_path: str = Field(default=DEFAULT_FASTTEXT_MODEL_PATH)
    fasttext_quantize: bool = Field(default=DEFAULT_FASTTEXT_QUANTIZE)
    fasttext_quantize_qdim: int = Field(default=DEFAULT_FASTTEXT_QUANTIZE_QDIM)
    fasttext_training_workers: int = Field(default=DEFAULT_FASTTEXT_TRAINING_WORKERS)

    # Concurrency
    feed_workers: int = Field(default=DEFAULT_FEED_WORKERS)
    loader_workers: int = Field(default=DEFAULT_LOADER_WORKERS)

    # Pipeline core
    batch_size: int = Field(default=DEFAULT_BATCH_SIZE)
    load_since: str | None = Field(default=DEFAULT_LOAD_SINCE)
    max_retries: int = Field(default=DEFAULT_MAX_RETRIES)
    apoc_batch_size: int = Field(default=DEFAULT_APOC_BATCH_SIZE)
    similarity_iterate_batch_size: int = Field(
        default=DEFAULT_SIMILARITY_ITERATE_BATCH_SIZE,
    )

    # Communities and clustering
    user_communities: str = Field(default=DEFAULT_USER_COMMUNITIES)
    post_communities: str = Field(default=DEFAULT_POST_COMMUNITIES)
    leiden_resolution: float = Field(default=DEFAULT_LEIDEN_RESOLUTION)
    knn_neighbors: int = Field(default=DEFAULT_KNN_NEIGHBORS)
    similarity_threshold: float = Field(default=DEFAULT_SIMILARITY_THRESHOLD)
    serendipity_probability: float = Field(default=DEFAULT_SERENDIPITY_PROBABILITY)
    interests_ttl_days: int = Field(default=DEFAULT_INTERESTS_TTL_DAYS)
    interests_min_favourites: int = Field(default=DEFAULT_INTERESTS_MIN_FAVOURITES)
    decay_half_life_days: int = Field(default=DEFAULT_DECAY_HALF_LIFE_DAYS)
    likes_weight: float = Field(default=DEFAULT_LIKES_WEIGHT)
    reblogs_weight: float = Field(default=DEFAULT_REBLOGS_WEIGHT)
    replies_weight: float = Field(default=DEFAULT_REPLIES_WEIGHT)
    follows_weight: float = Field(default=DEFAULT_FOLLOWS_WEIGHT)
    mentions_weight: float = Field(default=DEFAULT_MENTIONS_WEIGHT)
    serendipity_limit: int = Field(default=DEFAULT_SERENDIPITY_LIMIT)
    serendipity_score: float = Field(default=DEFAULT_SERENDIPITY_SCORE)
    serendipity_based_on: int = Field(default=DEFAULT_SERENDIPITY_BASED_ON)
    ctr_enabled: bool = Field(default=DEFAULT_CTR_ENABLED)
    ctr_weight: float = Field(default=DEFAULT_CTR_WEIGHT)
    min_ctr: float = Field(default=DEFAULT_MIN_CTR)
    ctr_smoothing: float = Field(default=DEFAULT_CTR_SMOOTHING)
    pagerank_enabled: bool = Field(default=DEFAULT_PAGERANK_ENABLED)
    pagerank_weight: float = Field(default=DEFAULT_PAGERANK_WEIGHT)
    pagerank_damping_factor: float = Field(default=DEFAULT_PAGERANK_DAMPING_FACTOR)
    pagerank_max_iterations: int = Field(default=DEFAULT_PAGERANK_MAX_ITERATIONS)
    community_similarity_enabled: bool = Field(default=DEFAULT_COMMUNITY_SIMILARITY_ENABLED)
    community_similarity_top_k: int = Field(default=DEFAULT_COMMUNITY_SIMILARITY_TOP_K)

    # Embedding filtering
    min_embedding_tokens: int = Field(default=DEFAULT_MIN_EMBEDDING_TOKENS)
    embedding_skip_percentile: float = Field(default=DEFAULT_EMBEDDING_SKIP_PERCENTILE)

    # User activity filtering
    active_user_days: int = Field(default=DEFAULT_ACTIVE_USER_DAYS)
    feed_force_refresh: bool = Field(default=DEFAULT_FEED_FORCE_REFRESH)

    # Language boost (chosen_languages vs UI locale)
    language_match_weight: float = Field(default=DEFAULT_LANGUAGE_MATCH_WEIGHT)
    ui_language_match_weight: float = Field(default=DEFAULT_UI_LANGUAGE_MATCH_WEIGHT)

    # Bookmarks
    bookmark_weight: float = Field(default=DEFAULT_BOOKMARK_WEIGHT)

    # Public timelines
    public_feed_size: int = Field(default=DEFAULT_PUBLIC_FEED_SIZE)
    public_feed_enabled: bool = Field(default=DEFAULT_PUBLIC_FEED_ENABLED)
    public_feed_strategy: str = Field(default=DEFAULT_PUBLIC_FEED_STRATEGY)
    public_timeline_key: str = Field(default=DEFAULT_PUBLIC_TIMELINE_KEY)
    local_timeline_key: str = Field(default=DEFAULT_LOCAL_TIMELINE_KEY)

    # Redis namespace
    redis_namespace: str | None = Field(default=DEFAULT_REDIS_NAMESPACE)

    # Feed generation
    feed_size: int = Field(default=DEFAULT_FEED_SIZE)
    feed_days: int = Field(default=DEFAULT_FEED_DAYS)
    feed_ttl: str = Field(default=DEFAULT_FEED_TTL)
    feed_score_multiplier: int = Field(default=DEFAULT_FEED_SCORE_MULTIPLIER)
    personalized_interest_weight: float = Field(default=DEFAULT_PERSONALIZED_INTEREST_WEIGHT)
    personalized_popularity_weight: float = Field(default=DEFAULT_PERSONALIZED_POPULARITY_WEIGHT)
    personalized_recency_weight: float = Field(default=DEFAULT_PERSONALIZED_RECENCY_WEIGHT)
    cold_start_popularity_weight: float = Field(default=DEFAULT_COLD_START_POPULARITY_WEIGHT)
    cold_start_recency_weight: float = Field(default=DEFAULT_COLD_START_RECENCY_WEIGHT)
    popularity_smoothing: float = Field(default=DEFAULT_POPULARITY_SMOOTHING, gt=0.0)
    recency_smoothing: float = Field(default=DEFAULT_RECENCY_SMOOTHING, gt=0.0)
    recency_numerator: float = Field(default=DEFAULT_RECENCY_NUMERATOR)
    cold_start_fallback: str = Field(default=DEFAULT_COLD_START_FALLBACK)
    cold_start_limit: int = Field(default=DEFAULT_COLD_START_LIMIT)

    # Similarity pruning
    similarity_pruning: str = Field(default=DEFAULT_SIMILARITY_PRUNING)
    prune_after_clustering: bool = Field(default=DEFAULT_PRUNE_AFTER_CLUSTERING)
    prune_similarity_threshold: float = Field(default=DEFAULT_PRUNE_SIMILARITY_THRESHOLD)
    prune_days: int = Field(default=DEFAULT_PRUNE_DAYS)
    leiden_max_levels: int = Field(default=DEFAULT_LEIDEN_MAX_LEVELS)
    leiden_diagnostics: bool = Field(default=DEFAULT_LEIDEN_DIAGNOSTICS_ENABLED)
    singleton_collapse_enabled: bool = Field(default=DEFAULT_SINGLETON_COLLAPSE_ENABLED)
    noise_community_id: int = Field(default=DEFAULT_NOISE_COMMUNITY_ID)
    singleton_collapse_in_transactions_of: int = Field(
        default=DEFAULT_SINGLETON_COLLAPSE_IN_TRANSACTIONS_OF
    )
    similarity_recency_days: int = Field(default=DEFAULT_SIMILARITY_RECENCY_DAYS)
    knn_self_neighbor_offset: int = Field(default=DEFAULT_KNN_SELF_NEIGHBOR_OFFSET)

    # Export settings
    export_max_items: int = Field(default=DEFAULT_EXPORT_MAX_ITEMS)
    text_preview_limit: int = Field(default=DEFAULT_TEXT_PREVIEW_LIMIT)
    community_interest_limit: int = Field(default=DEFAULT_COMMUNITY_INTEREST_LIMIT)
    community_member_sample: int = Field(default=DEFAULT_COMMUNITY_MEMBER_SAMPLE)
    community_sample_limit: int = Field(default=DEFAULT_COMMUNITY_SAMPLE_LIMIT)
    graph_sample_limit: int = Field(default=DEFAULT_GRAPH_SAMPLE_LIMIT)
    feed_score_decimals: int = Field(default=DEFAULT_FEED_SCORE_DECIMALS)

    # Database client tuning
    pg_pool_min_size: int = Field(default=DEFAULT_PG_POOL_MIN_SIZE)
    pg_pool_max_size: int = Field(default=DEFAULT_PG_POOL_MAX_SIZE)
    pg_pool_timeout_seconds: int = Field(default=DEFAULT_PG_POOL_TIMEOUT_SECONDS)
    neo4j_ready_retries: int = Field(default=DEFAULT_NEO4J_READY_RETRIES)
    neo4j_ready_sleep_seconds: int = Field(default=DEFAULT_NEO4J_READY_SLEEP_SECONDS)
    redis_score_tolerance: float = Field(default=DEFAULT_REDIS_SCORE_TOLERANCE)

    # Mastodon integration
    mastodon_public_visibility: int = Field(default=DEFAULT_MASTODON_PUBLIC_VISIBILITY)
    mastodon_account_lookup_limit: int = Field(default=DEFAULT_MASTODON_ACCOUNT_LOOKUP_LIMIT)

    # Pipeline settings
    checkpoint_interval: int = Field(default=DEFAULT_CHECKPOINT_INTERVAL)

    # Logging
    log_level: str = Field(default=DEFAULT_LOG_LEVEL)
    log_file: str = Field(default=DEFAULT_LOG_FILE)
    # CLI progress: auto uses TTY detection (plain when not a terminal, e.g. systemd/journald)
    progress_output: Literal["auto", "rich", "plain"] = Field(default=DEFAULT_PROGRESS_OUTPUT)
    progress_poll_interval_seconds: float = Field(default=DEFAULT_PROGRESS_POLL_INTERVAL_SECONDS)


# Subset of HintGridSettings fields printed by feed inclusion diagnostics (CLI / explain).
FEED_DEBUG_SETTING_FIELD_NAMES: tuple[str, ...] = (
    "feed_size",
    "feed_days",
    "feed_ttl",
    "feed_score_multiplier",
    "feed_score_decimals",
    "personalized_interest_weight",
    "personalized_popularity_weight",
    "personalized_recency_weight",
    "cold_start_popularity_weight",
    "cold_start_recency_weight",
    "cold_start_limit",
    "cold_start_fallback",
    "popularity_smoothing",
    "recency_smoothing",
    "recency_numerator",
    "language_match_weight",
    "ui_language_match_weight",
    "pagerank_enabled",
    "pagerank_weight",
    "pagerank_damping_factor",
    "pagerank_max_iterations",
    "noise_community_id",
    "interests_ttl_days",
    "interests_min_favourites",
    "decay_half_life_days",
    "likes_weight",
    "reblogs_weight",
    "replies_weight",
    "follows_weight",
    "mentions_weight",
    "bookmark_weight",
    "ctr_enabled",
    "ctr_weight",
    "min_ctr",
    "ctr_smoothing",
    "serendipity_probability",
    "serendipity_limit",
    "serendipity_score",
    "serendipity_based_on",
    "community_similarity_enabled",
    "community_similarity_top_k",
    "active_user_days",
    "feed_force_refresh",
    "redis_namespace",
)


def feed_debug_settings_snapshot(
    settings: HintGridSettings,
) -> dict[str, str | int | float | bool | None]:
    """Return selected settings fields for feed inclusion diagnostics (current values only)."""
    out: dict[str, str | int | float | bool | None] = {}
    for name in FEED_DEBUG_SETTING_FIELD_NAMES:
        raw: object = getattr(settings, name)
        if isinstance(raw, (str, int, float, bool)) or raw is None:
            out[name] = raw
        else:
            out[name] = str(raw)
    return out


@dataclass(frozen=True)
class CliOverrides:
    """CLI overrides for settings; any None value is ignored."""

    overrides: dict[str, object]

    def apply(self, settings: HintGridSettings) -> HintGridSettings:
        """Return a new settings object with overrides applied."""
        updates = {key: value for key, value in self.overrides.items() if value is not None}
        if not updates:
            return settings
        return settings.model_copy(update=updates)


# Valid values for enum-like settings
VALID_LLM_PROVIDERS = ("ollama", "openai", "fasttext")
VALID_FEED_TTL_VALUES = ("none", "1h", "6h", "12h", "24h", "48h", "7d")
VALID_COLD_START_FALLBACKS = ("global_top", "recent", "random")
VALID_SIMILARITY_PRUNING = ("none", "aggressive", "partial", "temporal")
VALID_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
VALID_PROGRESS_OUTPUT = ("auto", "rich", "plain")


def validate_settings(settings: HintGridSettings) -> None:
    """Validate all configuration settings at startup.

    Performs comprehensive validation of all settings to fail fast
    with clear error messages before pipeline starts.

    Args:
        settings: HintGrid settings to validate

    Raises:
        ConfigurationError: If any setting is invalid
    """
    from hintgrid.exceptions import ConfigurationError

    errors: list[str] = []

    # === Database connections ===

    # PostgreSQL
    if settings.postgres_port < 1 or settings.postgres_port > 65535:
        errors.append(f"postgres_port must be 1-65535, got {settings.postgres_port}")
    if not settings.postgres_database:
        errors.append("postgres_database cannot be empty")
    if not settings.postgres_user:
        errors.append("postgres_user cannot be empty")
    if settings.pg_pool_min_size < 1:
        errors.append(f"pg_pool_min_size must be >= 1, got {settings.pg_pool_min_size}")
    if settings.pg_pool_max_size < settings.pg_pool_min_size:
        errors.append(
            f"pg_pool_max_size ({settings.pg_pool_max_size}) must be >= "
            f"pg_pool_min_size ({settings.pg_pool_min_size})"
        )
    if settings.pg_pool_timeout_seconds < 1:
        errors.append("pg_pool_timeout_seconds must be >= 1")

    # Neo4j
    if settings.neo4j_port < 1 or settings.neo4j_port > 65535:
        errors.append(f"neo4j_port must be 1-65535, got {settings.neo4j_port}")
    if not settings.neo4j_password:
        errors.append("neo4j_password is required")
    if settings.neo4j_ready_retries < 1:
        errors.append("neo4j_ready_retries must be >= 1")
    if settings.neo4j_ready_sleep_seconds < 0:
        errors.append("neo4j_ready_sleep_seconds must be >= 0")
    if settings.neo4j_worker_label is not None:
        label = settings.neo4j_worker_label
        if not label or not all(ch.isalnum() or ch == "_" for ch in label):
            errors.append(
                "neo4j_worker_label must be alphanumeric or underscore, "
                f"got {settings.neo4j_worker_label}"
            )

    # Redis
    if settings.redis_port < 1 or settings.redis_port > 65535:
        errors.append(f"redis_port must be 1-65535, got {settings.redis_port}")
    if settings.redis_db < 0 or settings.redis_db > 15:
        errors.append(f"redis_db must be 0-15, got {settings.redis_db}")
    if settings.redis_namespace is not None and (
        not settings.redis_namespace
        or not all(ch.isalnum() or ch in (":", "-", "_") for ch in settings.redis_namespace)
    ):
        errors.append(
            f"redis_namespace must be alphanumeric or ':', '-', '_', got {settings.redis_namespace}"
        )

    # === Embedding settings ===

    if settings.llm_dimensions < 1:
        errors.append(f"llm_dimensions must be >= 1, got {settings.llm_dimensions}")
    if settings.llm_dimensions > 4096:
        errors.append(f"llm_dimensions too large: {settings.llm_dimensions} (max 4096)")

    if settings.llm_provider and settings.llm_provider not in VALID_LLM_PROVIDERS:
        errors.append(
            f"Unknown llm_provider: '{settings.llm_provider}'. "
            f"Valid: {', '.join(VALID_LLM_PROVIDERS)}"
        )

    if settings.llm_base_url and not settings.llm_base_url.startswith(("http://", "https://")):
        errors.append("llm_base_url must start with http:// or https://")

    if settings.llm_timeout < 1:
        errors.append(f"llm_timeout must be >= 1, got {settings.llm_timeout}")
    if settings.llm_max_retries < 0:
        errors.append(f"llm_max_retries must be >= 0, got {settings.llm_max_retries}")
    if settings.llm_batch_size < 1:
        errors.append(f"llm_batch_size must be >= 1, got {settings.llm_batch_size}")
    if settings.llm_batch_size > 10_000:
        errors.append(f"llm_batch_size too large: {settings.llm_batch_size} (max 10000)")

    # FastText settings
    if settings.fasttext_vector_size < 16 or settings.fasttext_vector_size > 1024:
        errors.append(
            f"fasttext_vector_size should be 16-1024, got {settings.fasttext_vector_size}"
        )
    if settings.fasttext_window < 1:
        errors.append(f"fasttext_window must be >= 1, got {settings.fasttext_window}")
    if settings.fasttext_min_count < 1:
        errors.append(f"fasttext_min_count must be >= 1, got {settings.fasttext_min_count}")
    if settings.fasttext_max_vocab_size < 1000:
        errors.append(
            f"fasttext_max_vocab_size must be >= 1000, got {settings.fasttext_max_vocab_size}"
        )
    if settings.fasttext_epochs < 1:
        errors.append(f"fasttext_epochs must be >= 1, got {settings.fasttext_epochs}")
    if settings.fasttext_bucket < 1000:
        errors.append(f"fasttext_bucket must be >= 1000, got {settings.fasttext_bucket}")
    if settings.fasttext_min_documents < 1:
        errors.append(f"fasttext_min_documents must be >= 1, got {settings.fasttext_min_documents}")
    if settings.fasttext_quantize_qdim < 10:
        errors.append(
            f"fasttext_quantize_qdim must be >= 10, got {settings.fasttext_quantize_qdim}"
        )
    if settings.fasttext_quantize_qdim > settings.fasttext_vector_size:
        errors.append(
            f"fasttext_quantize_qdim ({settings.fasttext_quantize_qdim}) must be <= "
            f"fasttext_vector_size ({settings.fasttext_vector_size})"
        )
    if settings.fasttext_quantize and (
        settings.fasttext_vector_size % settings.fasttext_quantize_qdim != 0
    ):
        errors.append(
            "fasttext_vector_size must be divisible by fasttext_quantize_qdim when "
            "fasttext_quantize is enabled (product quantization / compress-fasttext). "
            f"Got vector_size={settings.fasttext_vector_size}, "
            f"quantize_qdim={settings.fasttext_quantize_qdim}. "
            "Choose a divisor of vector_size (e.g. 16, 32, 64, or 128 for size 128)."
        )
    if settings.fasttext_training_workers < 0:
        errors.append(
            f"fasttext_training_workers must be >= 0 (0 = auto), got {settings.fasttext_training_workers}"
        )

    # === Concurrency settings ===

    if settings.feed_workers < 1:
        errors.append(f"feed_workers must be >= 1, got {settings.feed_workers}")
    if settings.loader_workers < 1:
        errors.append(f"loader_workers must be >= 1, got {settings.loader_workers}")

    # === Pipeline settings ===

    if settings.batch_size < 1:
        errors.append(f"batch_size must be >= 1, got {settings.batch_size}")
    if settings.batch_size > 100_000:
        errors.append(f"batch_size too large: {settings.batch_size} (max 100000)")
    if settings.max_retries < 0:
        errors.append(f"max_retries must be >= 0, got {settings.max_retries}")
    if settings.checkpoint_interval < 1:
        errors.append(f"checkpoint_interval must be >= 1, got {settings.checkpoint_interval}")
    if settings.apoc_batch_size < 1:
        errors.append(f"apoc_batch_size must be >= 1, got {settings.apoc_batch_size}")
    if settings.apoc_batch_size > 100_000:
        errors.append(f"apoc_batch_size too large: {settings.apoc_batch_size} (max 100000)")
    if settings.similarity_iterate_batch_size < 1:
        errors.append(
            f"similarity_iterate_batch_size must be >= 1, "
            f"got {settings.similarity_iterate_batch_size}"
        )
    if settings.similarity_iterate_batch_size > 100_000:
        errors.append(
            f"similarity_iterate_batch_size too large: {settings.similarity_iterate_batch_size} "
            f"(max 100000)"
        )

    # Clustering
    if settings.leiden_resolution <= 0:
        errors.append(f"leiden_resolution must be > 0, got {settings.leiden_resolution}")
    if settings.leiden_max_levels < 1:
        errors.append(f"leiden_max_levels must be >= 1, got {settings.leiden_max_levels}")
    if settings.noise_community_id == 0:
        errors.append(
            "noise_community_id must not be 0 (reserved for single-cluster fallback "
            "when the interaction/similarity graph has no edges); "
            f"got {settings.noise_community_id}"
        )
    if settings.singleton_collapse_in_transactions_of < 0:
        errors.append(
            "singleton_collapse_in_transactions_of must be >= 0 "
            f"(0 = single transaction), got {settings.singleton_collapse_in_transactions_of}"
        )
    if settings.singleton_collapse_in_transactions_of > 100_000:
        errors.append(
            "singleton_collapse_in_transactions_of too large: "
            f"{settings.singleton_collapse_in_transactions_of} (max 100000)"
        )
    if settings.knn_neighbors < 1:
        errors.append(f"knn_neighbors must be >= 1, got {settings.knn_neighbors}")
    if settings.knn_self_neighbor_offset < 0:
        errors.append(
            f"knn_self_neighbor_offset must be >= 0, got {settings.knn_self_neighbor_offset}"
        )
    if not 0 <= settings.similarity_threshold <= 1:
        errors.append(f"similarity_threshold must be 0-1, got {settings.similarity_threshold}")
    if settings.similarity_recency_days < 1:
        errors.append(
            f"similarity_recency_days must be >= 1, got {settings.similarity_recency_days}"
        )

    # Language boost
    if settings.language_match_weight < 0:
        errors.append(f"language_match_weight must be >= 0, got {settings.language_match_weight}")
    if settings.ui_language_match_weight < 0:
        errors.append(
            f"ui_language_match_weight must be >= 0, got {settings.ui_language_match_weight}"
        )
    if settings.ui_language_match_weight < settings.language_match_weight:
        errors.append(
            "ui_language_match_weight must be >= language_match_weight "
            f"(got ui={settings.ui_language_match_weight}, "
            f"chosen={settings.language_match_weight})"
        )

    # Bookmarks
    if settings.bookmark_weight < 0:
        errors.append(f"bookmark_weight must be >= 0, got {settings.bookmark_weight}")

    # Public timelines
    if settings.public_feed_size < 1:
        errors.append(f"public_feed_size must be >= 1, got {settings.public_feed_size}")
    valid_strategies = ("all_communities", "local_communities")
    if settings.public_feed_strategy not in valid_strategies:
        errors.append(
            f"Invalid public_feed_strategy: '{settings.public_feed_strategy}'. "
            f"Valid: {', '.join(valid_strategies)}"
        )

    # Feed settings
    if settings.feed_size < 1:
        errors.append(f"feed_size must be >= 1, got {settings.feed_size}")
    if settings.feed_days < 1:
        errors.append(f"feed_days must be >= 1, got {settings.feed_days}")
    if settings.feed_ttl not in VALID_FEED_TTL_VALUES:
        errors.append(
            f"Invalid feed_ttl: '{settings.feed_ttl}'. Valid: {', '.join(VALID_FEED_TTL_VALUES)}"
        )
    if settings.feed_score_multiplier < 1:
        errors.append(
            f"feed_score_multiplier must be >= 1 (1 = use calculated score, >1 = boost above Mastodon entries), got {settings.feed_score_multiplier}"
        )
    if settings.feed_score_decimals < 0:
        errors.append(f"feed_score_decimals must be >= 0, got {settings.feed_score_decimals}")

    # Cold start
    if settings.cold_start_fallback not in VALID_COLD_START_FALLBACKS:
        errors.append(
            f"Invalid cold_start_fallback: '{settings.cold_start_fallback}'. "
            f"Valid: {', '.join(VALID_COLD_START_FALLBACKS)}"
        )
    if settings.cold_start_limit < 1:
        errors.append(f"cold_start_limit must be >= 1, got {settings.cold_start_limit}")

    # Similarity pruning
    if settings.similarity_pruning not in VALID_SIMILARITY_PRUNING:
        errors.append(
            f"Invalid similarity_pruning: '{settings.similarity_pruning}'. "
            f"Valid: {', '.join(VALID_SIMILARITY_PRUNING)}"
        )
    if not 0 <= settings.prune_similarity_threshold <= 1:
        errors.append(
            f"prune_similarity_threshold must be 0-1, got {settings.prune_similarity_threshold}"
        )
    if settings.prune_days < 1:
        errors.append(f"prune_days must be >= 1, got {settings.prune_days}")

    # Weights (should be non-negative)
    weight_fields = (
        "likes_weight",
        "reblogs_weight",
        "replies_weight",
        "follows_weight",
        "mentions_weight",
    )
    for weight_name in weight_fields:
        value = getattr(settings, weight_name)
        if value < 0:
            errors.append(f"{weight_name} must be >= 0, got {value}")

    # Scoring weights (should sum to ~1.0)
    personalized_sum = (
        settings.personalized_interest_weight
        + settings.personalized_popularity_weight
        + settings.personalized_recency_weight
    )
    if abs(personalized_sum - 1.0) > 0.01:
        errors.append(
            f"Personalized weights should sum to 1.0, got {personalized_sum:.2f} "
            f"(interest={settings.personalized_interest_weight}, "
            f"popularity={settings.personalized_popularity_weight}, "
            f"recency={settings.personalized_recency_weight})"
        )

    cold_start_sum = settings.cold_start_popularity_weight + settings.cold_start_recency_weight
    if abs(cold_start_sum - 1.0) > 0.01:
        errors.append(
            f"Cold start weights should sum to 1.0, got {cold_start_sum:.2f} "
            f"(popularity={settings.cold_start_popularity_weight}, "
            f"recency={settings.cold_start_recency_weight})"
        )

    # Serendipity
    if not 0 <= settings.serendipity_probability <= 1:
        errors.append(
            f"serendipity_probability must be 0-1, got {settings.serendipity_probability}"
        )
    if settings.serendipity_limit < 0:
        errors.append(f"serendipity_limit must be >= 0, got {settings.serendipity_limit}")
    if not 0 <= settings.serendipity_score <= 1:
        errors.append(f"serendipity_score must be 0-1, got {settings.serendipity_score}")

    # Interests
    if settings.decay_half_life_days < 1:
        errors.append(f"decay_half_life_days must be >= 1, got {settings.decay_half_life_days}")
    if settings.interests_ttl_days < 1:
        errors.append(f"interests_ttl_days must be >= 1, got {settings.interests_ttl_days}")
    if settings.interests_min_favourites < 1:
        errors.append(
            f"interests_min_favourites must be >= 1, got {settings.interests_min_favourites}"
        )

    # CTR settings
    if not 0 <= settings.ctr_weight <= 1:
        errors.append(f"ctr_weight must be 0-1, got {settings.ctr_weight}")
    if settings.min_ctr < 0:
        errors.append(f"min_ctr must be >= 0, got {settings.min_ctr}")
    if settings.ctr_smoothing < 0:
        errors.append(f"ctr_smoothing must be >= 0, got {settings.ctr_smoothing}")

    # PageRank settings
    if settings.pagerank_weight < 0:
        errors.append(f"pagerank_weight must be >= 0, got {settings.pagerank_weight}")
    if not 0 < settings.pagerank_damping_factor <= 1:
        errors.append(
            f"pagerank_damping_factor must be 0-1, got {settings.pagerank_damping_factor}"
        )
    if settings.pagerank_max_iterations < 1:
        errors.append(
            f"pagerank_max_iterations must be >= 1, got {settings.pagerank_max_iterations}"
        )

    # Community similarity settings
    if settings.community_similarity_top_k < 1:
        errors.append(
            f"community_similarity_top_k must be >= 1, got {settings.community_similarity_top_k}"
        )

    # Embedding filtering
    if settings.min_embedding_tokens < 1:
        errors.append(f"min_embedding_tokens must be >= 1, got {settings.min_embedding_tokens}")
    if not 0.0 <= settings.embedding_skip_percentile < 1.0:
        errors.append(
            f"embedding_skip_percentile must be 0-1 (exclusive), "
            f"got {settings.embedding_skip_percentile}"
        )

    # User activity filtering
    if settings.active_user_days < 1:
        errors.append(f"active_user_days must be >= 1, got {settings.active_user_days}")

    # Export settings
    if settings.export_max_items < 1:
        errors.append(f"export_max_items must be >= 1, got {settings.export_max_items}")
    if settings.text_preview_limit < 1:
        errors.append(f"text_preview_limit must be >= 1, got {settings.text_preview_limit}")

    # Logging
    if settings.log_level.upper() not in VALID_LOG_LEVELS:
        errors.append(
            f"Invalid log_level: '{settings.log_level}'. Valid: {', '.join(VALID_LOG_LEVELS)}"
        )
    if settings.progress_output not in VALID_PROGRESS_OUTPUT:
        errors.append(
            f"Invalid progress_output: '{settings.progress_output}'. "
            f"Valid: {', '.join(VALID_PROGRESS_OUTPUT)}"
        )
    if settings.progress_poll_interval_seconds <= 0:
        errors.append(
            f"progress_poll_interval_seconds must be > 0, got {settings.progress_poll_interval_seconds}"
        )
    if settings.progress_poll_interval_seconds > 300:
        errors.append(
            f"progress_poll_interval_seconds must be <= 300, got {settings.progress_poll_interval_seconds}"
        )

    # === Raise if errors ===

    if errors:
        raise ConfigurationError("Invalid configuration:\n" + "\n".join(f"  - {e}" for e in errors))


def build_embedding_signature(settings: HintGridSettings) -> str:
    """Build unique signature for current embedding configuration.

    Format: "provider:model:dimensions"

    Examples:
        - "fasttext:local:128"
        - "openai:text-embedding-3-small:768"
        - "ollama:nomic-embed-text:768"

    Args:
        settings: HintGrid settings

    Returns:
        Signature string identifying the embedding configuration
    """
    provider = settings.llm_provider or "fasttext"
    model = settings.llm_model or "local"

    # For FastText (no base_url), use fasttext_vector_size; for others use llm_dimensions
    if provider == "fasttext" or not settings.llm_base_url:
        dim = settings.fasttext_vector_size
    else:
        dim = settings.llm_dimensions

    return f"{provider}:{model}:{dim}"


def build_similarity_signature(settings: HintGridSettings) -> str:
    """Build unique signature for similarity graph parameters.

    Format: "knn:{knn_neighbors}:threshold:{similarity_threshold}:recency:{similarity_recency_days}"

    When this signature changes between pipeline runs, the entire SIMILAR_TO
    graph is rebuilt from scratch (full mode). When it stays the same, only
    new posts without outgoing SIMILAR_TO edges are processed (incremental mode).

    Args:
        settings: HintGrid settings

    Returns:
        Signature string identifying the similarity configuration
    """
    return (
        f"knn:{settings.knn_neighbors}"
        f":threshold:{settings.similarity_threshold}"
        f":recency:{settings.similarity_recency_days}"
    )
