"""Typer CLI application for HintGrid.

Configuration Priority (highest to lowest):
    1. CLI flags (--postgres-host, etc.)
    2. Environment variables (HINTGRID_POSTGRES_HOST, etc.)
    3. .env file in current directory
    4. .env.local file (for local development, not in git)
    5. Default values

All environment variables use HINTGRID_ prefix.

Example usage:
    # Via CLI flag (highest priority)
    hintgrid run --postgres-host localhost

    # Via environment variable
    export HINTGRID_POSTGRES_HOST=db.example.com
    hintgrid run

    # Via .env file (create in project root)
    # .env:
    #   HINTGRID_POSTGRES_HOST=db.example.com
    #   HINTGRID_POSTGRES_PORT=5432
    #   HINTGRID_NEO4J_PASSWORD=secret
    hintgrid run

Best practices:
    - Add .env to .gitignore (contains secrets)
    - Use .env.example as template (no secrets, committed to git)
    - Use .env.local for personal overrides (not committed)
"""

from __future__ import annotations

from typing import Annotated

import typer

# =============================================================================
# Settings option type aliases with envvar support
# All environment variables use HINTGRID_ prefix to match pydantic-settings
# =============================================================================

# PostgreSQL options
PostgresHostOpt = Annotated[
    str | None,
    typer.Option(
        help="PostgreSQL server hostname for Mastodon database. "
        "Used in data loading step. [default: localhost]",
        envvar="HINTGRID_POSTGRES_HOST",
    ),
]
PostgresPortOpt = Annotated[
    int | None,
    typer.Option(
        help="PostgreSQL server port. [default: 5432]",
        envvar="HINTGRID_POSTGRES_PORT",
    ),
]
PostgresDatabaseOpt = Annotated[
    str | None,
    typer.Option(
        help="PostgreSQL database name containing Mastodon tables. [default: mastodon_production]",
        envvar="HINTGRID_POSTGRES_DATABASE",
    ),
]
PostgresUserOpt = Annotated[
    str | None,
    typer.Option(
        help="PostgreSQL username for authentication. [default: mastodon]",
        envvar="HINTGRID_POSTGRES_USER",
    ),
]
PostgresPasswordOpt = Annotated[
    str | None,
    typer.Option(
        help="PostgreSQL password. Leave empty for peer/trust auth. [default: none]",
        envvar="HINTGRID_POSTGRES_PASSWORD",
    ),
]
PostgresSchemaOpt = Annotated[
    str | None,
    typer.Option(
        help="PostgreSQL schema containing Mastodon tables. [default: public]",
        envvar="HINTGRID_POSTGRES_SCHEMA",
    ),
]
PgPoolMinSizeOpt = Annotated[
    int | None,
    typer.Option(
        help="Minimum connections in PostgreSQL connection pool. [default: 1]",
        envvar="HINTGRID_PG_POOL_MIN_SIZE",
    ),
]
PgPoolMaxSizeOpt = Annotated[
    int | None,
    typer.Option(
        help="Maximum connections in PostgreSQL pool. "
        "Higher values enable more parallel data loading. [default: 5]",
        envvar="HINTGRID_PG_POOL_MAX_SIZE",
    ),
]
PgPoolTimeoutSecondsOpt = Annotated[
    int | None,
    typer.Option(
        help="Timeout in seconds for acquiring a pool connection. [default: 30]",
        envvar="HINTGRID_PG_POOL_TIMEOUT_SECONDS",
    ),
]
MastodonPublicVisibilityOpt = Annotated[
    int | None,
    typer.Option(
        help="Mastodon visibility level filter for public statuses (0=public). "
        "Used in data loading step. [default: 0]",
        envvar="HINTGRID_MASTODON_PUBLIC_VISIBILITY",
    ),
]
MastodonAccountLookupLimitOpt = Annotated[
    int | None,
    typer.Option(
        help="Max accounts per PostgreSQL lookup query. [default: 1]",
        envvar="HINTGRID_MASTODON_ACCOUNT_LOOKUP_LIMIT",
    ),
]

# Neo4j options
Neo4jHostOpt = Annotated[
    str | None,
    typer.Option(
        help="Neo4j server hostname. Graph database for social/recommendation data. "
        "[default: localhost]",
        envvar="HINTGRID_NEO4J_HOST",
    ),
]
Neo4jPortOpt = Annotated[
    int | None,
    typer.Option(
        help="Neo4j Bolt protocol port. [default: 7687]",
        envvar="HINTGRID_NEO4J_PORT",
    ),
]
Neo4jUsernameOpt = Annotated[
    str | None,
    typer.Option(
        help="Neo4j username for authentication. [default: neo4j]",
        envvar="HINTGRID_NEO4J_USERNAME",
    ),
]
Neo4jPasswordOpt = Annotated[
    str | None,
    typer.Option(
        help="Neo4j password for authentication. [default: password]",
        envvar="HINTGRID_NEO4J_PASSWORD",
    ),
]
Neo4jReadyRetriesOpt = Annotated[
    int | None,
    typer.Option(
        help="Number of retries when waiting for Neo4j readiness at startup. [default: 30]",
        envvar="HINTGRID_NEO4J_READY_RETRIES",
    ),
]
Neo4jReadySleepSecondsOpt = Annotated[
    int | None,
    typer.Option(
        help="Sleep duration in seconds between Neo4j readiness checks. [default: 1]",
        envvar="HINTGRID_NEO4J_READY_SLEEP_SECONDS",
    ),
]
Neo4jWorkerLabelOpt = Annotated[
    str | None,
    typer.Option(
        help="Neo4j worker label for multi-worker isolation. "
        "Appended to node labels for data partitioning. [default: none]",
        envvar="HINTGRID_NEO4J_WORKER_LABEL",
    ),
]

# Redis options
RedisHostOpt = Annotated[
    str | None,
    typer.Option(
        help="Redis server hostname. Stores ranked feeds served to Mastodon. [default: localhost]",
        envvar="HINTGRID_REDIS_HOST",
    ),
]
RedisPortOpt = Annotated[
    int | None,
    typer.Option(
        help="Redis server port. [default: 6379]",
        envvar="HINTGRID_REDIS_PORT",
    ),
]
RedisDbOpt = Annotated[
    int | None,
    typer.Option(
        help="Redis database index (0-15). [default: 0]",
        envvar="HINTGRID_REDIS_DB",
    ),
]
RedisPasswordOpt = Annotated[
    str | None,
    typer.Option(
        help="Redis password (leave empty if none required). [default: none]",
        envvar="HINTGRID_REDIS_PASSWORD",
    ),
]
RedisScoreToleranceOpt = Annotated[
    float | None,
    typer.Option(
        help="Score comparison tolerance for feed deduplication in Redis. [default: 1e-06]",
        envvar="HINTGRID_REDIS_SCORE_TOLERANCE",
    ),
]
RedisNamespaceOpt = Annotated[
    str | None,
    typer.Option(
        help="Redis key namespace for Mastodon REDIS_NAMESPACE support. "
        "Prepended to all feed keys. [default: none]",
        envvar="HINTGRID_REDIS_NAMESPACE",
    ),
]

# LLM/Embedding options
LlmProviderOpt = Annotated[
    str | None,
    typer.Option(
        help="Embedding provider. Determines how post text is converted to vectors "
        "for similarity-based clustering. [default: ollama] [values: ollama|openai|fasttext]",
        envvar="HINTGRID_LLM_PROVIDER",
    ),
]
LlmBaseUrlOpt = Annotated[
    str | None,
    typer.Option(
        help="LLM API base URL. When empty, local FastText model is used instead. "
        "[default: none] [example: http://llm:11434]",
        envvar="HINTGRID_LLM_BASE_URL",
    ),
]
LlmModelOpt = Annotated[
    str | None,
    typer.Option(
        help="LLM model name for generating embeddings. [default: nomic-embed-text]",
        envvar="HINTGRID_LLM_MODEL",
    ),
]
LlmDimensionsOpt = Annotated[
    int | None,
    typer.Option(
        help="Embedding vector dimensions for LLM providers (not FastText). [default: 768]",
        envvar="HINTGRID_LLM_DIMENSIONS",
    ),
]
LlmTimeoutOpt = Annotated[
    int | None,
    typer.Option(
        help="LLM API request timeout in seconds. [default: 30]",
        envvar="HINTGRID_LLM_TIMEOUT",
    ),
]
LlmMaxRetriesOpt = Annotated[
    int | None,
    typer.Option(
        help="Max retries for failed LLM API requests. [default: 3]",
        envvar="HINTGRID_LLM_MAX_RETRIES",
    ),
]
LlmBatchSizeOpt = Annotated[
    int | None,
    typer.Option(
        help="Texts per LLM API call. Larger = fewer calls but may exceed provider limits "
        "(OpenAI max 2048). Ignored for FastText. [default: 256]",
        envvar="HINTGRID_LLM_BATCH_SIZE",
    ),
]
LlmApiKeyOpt = Annotated[
    str | None,
    typer.Option(
        help="API key for LLM provider (required for OpenAI). [default: none]",
        envvar="HINTGRID_LLM_API_KEY",
    ),
]

# Embedding filtering options
MinEmbeddingTokensOpt = Annotated[
    int | None,
    typer.Option(
        help="Minimum tokens required for a post to receive embeddings. "
        "Shorter posts are skipped during embedding step. [default: 1]",
        envvar="HINTGRID_MIN_EMBEDDING_TOKENS",
    ),
]
EmbeddingSkipPercentileOpt = Annotated[
    float | None,
    typer.Option(
        help="Skip shortest posts by character length percentile "
        "(0.05 = skip bottom 5%%). Used in embedding step. [default: 0.0]",
        envvar="HINTGRID_EMBEDDING_SKIP_PERCENTILE",
    ),
]

# FastText options
FasttextVectorSizeOpt = Annotated[
    int | None,
    typer.Option(
        help="FastText embedding vector dimensions (16-1024). "
        "Used in post similarity and clustering steps. [default: 128]",
        envvar="HINTGRID_FASTTEXT_VECTOR_SIZE",
    ),
]
FasttextWindowOpt = Annotated[
    int | None,
    typer.Option(
        help="Context window size for FastText training. [default: 3]",
        envvar="HINTGRID_FASTTEXT_WINDOW",
    ),
]
FasttextMinCountOpt = Annotated[
    int | None,
    typer.Option(
        help="Minimum word frequency for FastText vocabulary. "
        "Higher = more aggressive pruning for social media text. [default: 10]",
        envvar="HINTGRID_FASTTEXT_MIN_COUNT",
    ),
]
FasttextMaxVocabSizeOpt = Annotated[
    int | None,
    typer.Option(
        help="Maximum vocabulary size to cap FastText memory usage. [default: 500000]",
        envvar="HINTGRID_FASTTEXT_MAX_VOCAB_SIZE",
    ),
]
FasttextEpochsOpt = Annotated[
    int | None,
    typer.Option(
        help="Training epochs for FastText model. [default: 5]",
        envvar="HINTGRID_FASTTEXT_EPOCHS",
    ),
]
FasttextBucketOpt = Annotated[
    int | None,
    typer.Option(
        help="Hash buckets for FastText subword features. "
        "Lower = less memory usage. [default: 10000]",
        envvar="HINTGRID_FASTTEXT_BUCKET",
    ),
]
FasttextMinDocumentsOpt = Annotated[
    int | None,
    typer.Option(
        help="Minimum documents required before FastText training starts. [default: 100]",
        envvar="HINTGRID_FASTTEXT_MIN_DOCUMENTS",
    ),
]
FasttextModelPathOpt = Annotated[
    str | None,
    typer.Option(
        help="Directory path for FastText model storage. [default: ~/.hintgrid/models]",
        envvar="HINTGRID_FASTTEXT_MODEL_PATH",
    ),
]
FasttextQuantizeOpt = Annotated[
    bool | None,
    typer.Option(
        help="Enable FastText model quantization (10-50x size reduction). [default: true]",
        envvar="HINTGRID_FASTTEXT_QUANTIZE",
    ),
]
FasttextQuantizeQdimOpt = Annotated[
    int | None,
    typer.Option(
        help="PQ subquantizer count (compress-fasttext). Must be <= vector_size and divide "
        "vector_size when --fasttext-quantize is on. [default: 64]",
        envvar="HINTGRID_FASTTEXT_QUANTIZE_QDIM",
    ),
]
FasttextTrainingWorkersOpt = Annotated[
    int | None,
    typer.Option(
        help="CPU threads for FastText training (0 = auto-detect via os.cpu_count()). [default: 0]",
        envvar="HINTGRID_FASTTEXT_TRAINING_WORKERS",
    ),
]

# Batch processing options
BatchSizeOpt = Annotated[
    int | None,
    typer.Option(
        help="Batch size for incremental data loading from PostgreSQL. [default: 10000]",
        envvar="HINTGRID_BATCH_SIZE",
    ),
]
LoadSinceOpt = Annotated[
    str | None,
    typer.Option(
        help="Load data window limiting time-based entities (statuses, favourites, reblogs, "
        "replies) to recent data. Follows/blocks always fully loaded. "
        "[default: none] [example: 30d]",
        envvar="HINTGRID_LOAD_SINCE",
    ),
]
MaxRetriesOpt = Annotated[
    int | None,
    typer.Option(
        help="Global retry count for pipeline step failures. [default: 3]",
        envvar="HINTGRID_MAX_RETRIES",
    ),
]
CheckpointIntervalOpt = Annotated[
    int | None,
    typer.Option(
        help="Records processed between checkpoint saves. "
        "Enables resume from last checkpoint on interrupt. [default: 1000]",
        envvar="HINTGRID_CHECKPOINT_INTERVAL",
    ),
]
ApocBatchSizeOpt = Annotated[
    int | None,
    typer.Option(
        help="Batch size for Neo4j APOC server-side batch operations. [default: 10000]",
        envvar="HINTGRID_APOC_BATCH_SIZE",
    ),
]

# Concurrency options
FeedWorkersOpt = Annotated[
    int | None,
    typer.Option(
        help="ThreadPool workers for parallel feed generation. [default: 1]",
        envvar="HINTGRID_FEED_WORKERS",
    ),
]
LoaderWorkersOpt = Annotated[
    int | None,
    typer.Option(
        help="ThreadPool workers for parallel entity loading from PostgreSQL. [default: 1]",
        envvar="HINTGRID_LOADER_WORKERS",
    ),
]

# Clustering options
UserCommunitiesOpt = Annotated[
    str | None,
    typer.Option(
        help="User community detection strategy. Used in analytics step. "
        "[default: dynamic] [values: dynamic]",
        envvar="HINTGRID_USER_COMMUNITIES",
    ),
]
PostCommunitiesOpt = Annotated[
    str | None,
    typer.Option(
        help="Post community detection strategy. Used in analytics step. "
        "[default: dynamic] [values: dynamic]",
        envvar="HINTGRID_POST_COMMUNITIES",
    ),
]
LeidenResolutionOpt = Annotated[
    float | None,
    typer.Option(
        help="Leiden gamma parameter for community detection. "
        "Lower = fewer, larger communities; 0.1 works well for social graphs. "
        "[default: 0.1] [example: 0.5]",
        envvar="HINTGRID_LEIDEN_RESOLUTION",
    ),
]
LeidenMaxLevelsOpt = Annotated[
    int | None,
    typer.Option(
        help="Maximum hierarchical Leiden levels for multi-level clustering. [default: 10]",
        envvar="HINTGRID_LEIDEN_MAX_LEVELS",
    ),
]
SingletonCollapseEnabledOpt = Annotated[
    bool | None,
    typer.Option(
        help="Merge Leiden singleton clusters into one noise community before BELONGS_TO. [default: true]",
        envvar="HINTGRID_SINGLETON_COLLAPSE_ENABLED",
    ),
]
NoiseCommunityIdOpt = Annotated[
    int | None,
    typer.Option(
        help="Reserved cluster id for singleton noise bucket; must not be 0. [default: -1]",
        envvar="HINTGRID_NOISE_COMMUNITY_ID",
    ),
]
SingletonCollapseInTransactionsOfOpt = Annotated[
    int | None,
    typer.Option(
        help="Singleton collapse: 0 = one Cypher transaction; "
        ">0 = CALL/IN TRANSACTIONS batch size (Neo4j 5+). [default: 100000]",
        envvar="HINTGRID_SINGLETON_COLLAPSE_IN_TRANSACTIONS_OF",
    ),
]
KnnNeighborsOpt = Annotated[
    int | None,
    typer.Option(
        help="KNN neighbors count for post similarity graph construction. "
        "More neighbors = denser graph, better clustering. [default: 10]",
        envvar="HINTGRID_KNN_NEIGHBORS",
    ),
]
KnnSelfNeighborOffsetOpt = Annotated[
    int | None,
    typer.Option(
        help="KNN top_k offset to exclude self-neighbor from results. [default: 1]",
        envvar="HINTGRID_KNN_SELF_NEIGHBOR_OFFSET",
    ),
]
SimilarityThresholdOpt = Annotated[
    float | None,
    typer.Option(
        help="Cosine similarity threshold for creating SIMILAR_TO edges between posts. "
        "Lower = more edges, prevents graph fragmentation. [default: 0.7]",
        envvar="HINTGRID_SIMILARITY_THRESHOLD",
    ),
]
SimilarityRecencyDaysOpt = Annotated[
    int | None,
    typer.Option(
        help="Recency window in days for SIMILAR_TO edge creation. "
        "Only posts within this window are compared. [default: 7]",
        envvar="HINTGRID_SIMILARITY_RECENCY_DAYS",
    ),
]
SimilarityPruningOpt = Annotated[
    str | None,
    typer.Option(
        help="Pruning strategy for SIMILAR_TO edges after clustering. "
        "[default: aggressive] [values: aggressive|partial|temporal|none]",
        envvar="HINTGRID_SIMILARITY_PRUNING",
    ),
]
PruneAfterClusteringOpt = Annotated[
    bool | None,
    typer.Option(
        help="Whether to prune SIMILAR_TO edges after clustering completes. [default: true]",
        envvar="HINTGRID_PRUNE_AFTER_CLUSTERING",
    ),
]
PruneSimilarityThresholdOpt = Annotated[
    float | None,
    typer.Option(
        help="Threshold for partial pruning: edges with similarity below this value "
        "are removed after clustering. [default: 0.9]",
        envvar="HINTGRID_PRUNE_SIMILARITY_THRESHOLD",
    ),
]
PruneDaysOpt = Annotated[
    int | None,
    typer.Option(
        help="Window in days for temporal pruning strategy. "
        "Edges for posts older than this are removed. [default: 30]",
        envvar="HINTGRID_PRUNE_DAYS",
    ),
]

# Feed options
FeedSizeOpt = Annotated[
    int | None,
    typer.Option(
        help="Maximum posts per user feed in feed generation step. [default: 500]",
        envvar="HINTGRID_FEED_SIZE",
    ),
]
FeedDaysOpt = Annotated[
    int | None,
    typer.Option(
        help="Time window in days for candidate posts in feed generation. [default: 7]",
        envvar="HINTGRID_FEED_DAYS",
    ),
]
FeedTtlOpt = Annotated[
    str | None,
    typer.Option(
        help="Feed TTL strategy. Non-none values currently ignored for home feeds "
        "to avoid Mastodon conflicts. [default: none] [values: none|1h|6h|12h|24h|48h|7d]",
        envvar="HINTGRID_FEED_TTL",
    ),
]
FeedScoreMultiplierOpt = Annotated[
    int | None,
    typer.Option(
        help="Score multiplier for feed ranking. Higher values make HintGrid posts "
        "rank above standard Mastodon timeline entries. [default: 2]",
        envvar="HINTGRID_FEED_SCORE_MULTIPLIER",
    ),
]
FeedScoreDecimalsOpt = Annotated[
    int | None,
    typer.Option(
        help="Decimal places for score rounding in feed generation. [default: 4]",
        envvar="HINTGRID_FEED_SCORE_DECIMALS",
    ),
]
ColdStartFallbackOpt = Annotated[
    str | None,
    typer.Option(
        help="Strategy for users with no interaction history. "
        "[default: global_top] [values: global_top|recent|random]",
        envvar="HINTGRID_COLD_START_FALLBACK",
    ),
]
ColdStartLimitOpt = Annotated[
    int | None,
    typer.Option(
        help="Maximum posts for cold start users (no interaction history). [default: 500]",
        envvar="HINTGRID_COLD_START_LIMIT",
    ),
]
SerendipityProbabilityOpt = Annotated[
    float | None,
    typer.Option(
        help="Probability (0-1) of injecting random cross-community posts "
        "for discovery. Used in feed generation. [default: 0.1]",
        envvar="HINTGRID_SERENDIPITY_PROBABILITY",
    ),
]
SerendipityLimitOpt = Annotated[
    int | None,
    typer.Option(
        help="Maximum serendipity (random discovery) posts per pipeline run. [default: 100]",
        envvar="HINTGRID_SERENDIPITY_LIMIT",
    ),
]
SerendipityScoreOpt = Annotated[
    float | None,
    typer.Option(
        help="Base score assigned to serendipity posts (0-1). [default: 0.1]",
        envvar="HINTGRID_SERENDIPITY_SCORE",
    ),
]
SerendipityBasedOnOpt = Annotated[
    int | None,
    typer.Option(
        help="based_on field value for serendipity links in the graph. [default: 0]",
        envvar="HINTGRID_SERENDIPITY_BASED_ON",
    ),
]
DecayHalfLifeDaysOpt = Annotated[
    int | None,
    typer.Option(
        help="Half-life in days for exponential decay of interaction weights. "
        "Older interactions contribute less to interest scores. [default: 14]",
        envvar="HINTGRID_DECAY_HALF_LIFE_DAYS",
    ),
]
InterestsTtlDaysOpt = Annotated[
    int | None,
    typer.Option(
        help="TTL in days for INTERESTED_IN relationships in the graph. "
        "Expired interests are removed during analytics. [default: 30]",
        envvar="HINTGRID_INTERESTS_TTL_DAYS",
    ),
]
InterestsMinFavouritesOpt = Annotated[
    int | None,
    typer.Option(
        help="Minimum favourites required before building user interest profile. [default: 5]",
        envvar="HINTGRID_INTERESTS_MIN_FAVOURITES",
    ),
]
ActiveUserDaysOpt = Annotated[
    int | None,
    typer.Option(
        help="Days of inactivity before a user is considered inactive. "
        "Inactive users skip feed generation to save resources. [default: 90]",
        envvar="HINTGRID_ACTIVE_USER_DAYS",
    ),
]
FeedForceRefreshOpt = Annotated[
    bool | None,
    typer.Option(
        help="Force refresh all user feeds even if recently updated. [default: false]",
        envvar="HINTGRID_FEED_FORCE_REFRESH",
    ),
]
LanguageMatchWeightOpt = Annotated[
    float | None,
    typer.Option(
        help="Weight boost for posts matching chosen_languages (not UI locale). [default: 0.3]",
        envvar="HINTGRID_LANGUAGE_MATCH_WEIGHT",
    ),
]
UiLanguageMatchWeightOpt = Annotated[
    float | None,
    typer.Option(
        help="Weight boost when post language matches user's UI locale "
        "(users.locale). Should be >= language-match-weight. [default: 0.5]",
        envvar="HINTGRID_UI_LANGUAGE_MATCH_WEIGHT",
    ),
]

# Public timeline options
PublicFeedSizeOpt = Annotated[
    int | None,
    typer.Option(
        help="Maximum posts in public timeline feeds (timeline:public). "
        "Mastodon default is 400. [default: 400]",
        envvar="HINTGRID_PUBLIC_FEED_SIZE",
    ),
]
PublicFeedEnabledOpt = Annotated[
    bool | None,
    typer.Option(
        help="Enable filling public timelines (timeline:public, timeline:public:local) "
        "with ranked posts. [default: true]",
        envvar="HINTGRID_PUBLIC_FEED_ENABLED",
    ),
]
PublicFeedStrategyOpt = Annotated[
    str | None,
    typer.Option(
        help="Public feed post selection strategy. "
        "[default: local_communities] [values: all_communities|local_communities]",
        envvar="HINTGRID_PUBLIC_FEED_STRATEGY",
    ),
]
PublicTimelineKeyOpt = Annotated[
    str | None,
    typer.Option(
        help="Redis key pattern for the public timeline. [default: timeline:public]",
        envvar="HINTGRID_PUBLIC_TIMELINE_KEY",
    ),
]
LocalTimelineKeyOpt = Annotated[
    str | None,
    typer.Option(
        help="Redis key pattern for local public timeline. [default: timeline:public:local]",
        envvar="HINTGRID_LOCAL_TIMELINE_KEY",
    ),
]

# Scoring weight options
LikesWeightOpt = Annotated[
    float | None,
    typer.Option(
        help="Weight for FAVORITED edges in interest calculation and INTERACTS_WITH aggregation. "
        "Used in SQL query: count(*) * likes_weight. Higher = likes contribute more to user interests and clustering. "
        "Example: 1.0. [default: 1.0]",
        envvar="HINTGRID_LIKES_WEIGHT",
    ),
]
ReblogsWeightOpt = Annotated[
    float | None,
    typer.Option(
        help="Weight for REBLOGGED edges in interest calculation and INTERACTS_WITH aggregation. "
        "Used in SQL query: count(*) * reblogs_weight. Higher than likes since reblogs are stronger signals. "
        "Based on industry best practices (Twitter Heavy Ranker, Meta): reblogs are 3-10x more valuable than likes. "
        "Example: 3.0. [default: 3.0]",
        envvar="HINTGRID_REBLOGS_WEIGHT",
    ),
]
RepliesWeightOpt = Annotated[
    float | None,
    typer.Option(
        help="Weight for REPLIED edges in interest calculation and INTERACTS_WITH aggregation. "
        "Used in SQL query: count(*) * replies_weight. Highest weight since replies indicate deep engagement. "
        "Based on industry best practices: replies are 10-15x more valuable than likes (Twitter estimates 75x for conversations). "
        "Example: 5.0. [default: 5.0]",
        envvar="HINTGRID_REPLIES_WEIGHT",
    ),
]
BookmarkWeightOpt = Annotated[
    float | None,
    typer.Option(
        help="Weight for bookmarked posts in interest calculation. "
        "Higher than likes since bookmarks indicate strong interest. [default: 2.0]",
        envvar="HINTGRID_BOOKMARK_WEIGHT",
    ),
]
FollowsWeightOpt = Annotated[
    float | None,
    typer.Option(
        help="Weight for FOLLOWS relationships when included in INTERACTS_WITH aggregation. "
        "Used in SQL query: each follow relationship adds follows_weight to total_weight. "
        "Set to 0.0 to exclude FOLLOWS from user clustering. "
        "Based on industry best practices: follows are 20-50x more valuable than likes as they signal long-term interest. "
        "Example: 10.0. [default: 10.0]",
        envvar="HINTGRID_FOLLOWS_WEIGHT",
    ),
]
MentionsWeightOpt = Annotated[
    float | None,
    typer.Option(
        help="Weight for mentions in INTERACTS_WITH aggregation. "
        "Used in SQL query: count(*) * mentions_weight. Controls how mentions contribute to user clustering. "
        "Based on industry best practices: mentions are 10-15x more valuable than likes, similar to replies. "
        "Example: 5.0. [default: 5.0]",
        envvar="HINTGRID_MENTIONS_WEIGHT",
    ),
]
PersonalizedInterestWeightOpt = Annotated[
    float | None,
    typer.Option(
        help="Interest component weight in personalized feed score. "
        "Sum of interest+popularity+recency must equal 1.0. [default: 0.5]",
        envvar="HINTGRID_PERSONALIZED_INTEREST_WEIGHT",
    ),
]
PersonalizedPopularityWeightOpt = Annotated[
    float | None,
    typer.Option(
        help="Popularity component weight in personalized feed score. [default: 0.3]",
        envvar="HINTGRID_PERSONALIZED_POPULARITY_WEIGHT",
    ),
]
PersonalizedRecencyWeightOpt = Annotated[
    float | None,
    typer.Option(
        help="Recency component weight in personalized feed score. [default: 0.2]",
        envvar="HINTGRID_PERSONALIZED_RECENCY_WEIGHT",
    ),
]
ColdStartPopularityWeightOpt = Annotated[
    float | None,
    typer.Option(
        help="Popularity weight for cold start scoring (no user history). "
        "Sum with recency must equal 1.0. [default: 0.7]",
        envvar="HINTGRID_COLD_START_POPULARITY_WEIGHT",
    ),
]
ColdStartRecencyWeightOpt = Annotated[
    float | None,
    typer.Option(
        help="Recency weight for cold start scoring (no user history). [default: 0.3]",
        envvar="HINTGRID_COLD_START_RECENCY_WEIGHT",
    ),
]
PopularitySmoothingOpt = Annotated[
    int | None,
    typer.Option(
        help="Smoothing factor for popularity score calculation. "
        "Prevents zero-division with low interaction counts. [default: 1]",
        envvar="HINTGRID_POPULARITY_SMOOTHING",
    ),
]
RecencySmoothingOpt = Annotated[
    int | None,
    typer.Option(
        help="Smoothing factor for recency score calculation. [default: 1]",
        envvar="HINTGRID_RECENCY_SMOOTHING",
    ),
]
RecencyNumeratorOpt = Annotated[
    float | None,
    typer.Option(
        help="Numerator for recency decay formula: score = numerator / (age + smoothing). "
        "[default: 1.0]",
        envvar="HINTGRID_RECENCY_NUMERATOR",
    ),
]

# CTR options
CtrEnabledOpt = Annotated[
    bool | None,
    typer.Option(
        help="Enable click-through rate scoring. Uses interaction history to boost "
        "posts from authors the user engages with. [default: true]",
        envvar="HINTGRID_CTR_ENABLED",
    ),
]
CtrWeightOpt = Annotated[
    float | None,
    typer.Option(
        help="CTR weight in final scoring formula (0-1). [default: 0.5]",
        envvar="HINTGRID_CTR_WEIGHT",
    ),
]
MinCtrOpt = Annotated[
    float | None,
    typer.Option(
        help="Minimum CTR threshold. Posts below this are not boosted by CTR. [default: 0.0]",
        envvar="HINTGRID_MIN_CTR",
    ),
]
CtrSmoothingOpt = Annotated[
    float | None,
    typer.Option(
        help="CTR smoothing factor (Laplace smoothing) to handle sparse data. [default: 1.0]",
        envvar="HINTGRID_CTR_SMOOTHING",
    ),
]

# PageRank options
PagerankEnabledOpt = Annotated[
    bool | None,
    typer.Option(
        help="Enable PageRank-based author authority scoring in the social graph. "
        "Boosts posts from influential users. [default: true]",
        envvar="HINTGRID_PAGERANK_ENABLED",
    ),
]
PagerankWeightOpt = Annotated[
    float | None,
    typer.Option(
        help="PageRank weight in final feed score. [default: 0.1]",
        envvar="HINTGRID_PAGERANK_WEIGHT",
    ),
]
PagerankDampingFactorOpt = Annotated[
    float | None,
    typer.Option(
        help="PageRank damping factor (0-1). Standard value is 0.85. [default: 0.85]",
        envvar="HINTGRID_PAGERANK_DAMPING_FACTOR",
    ),
]
PagerankMaxIterationsOpt = Annotated[
    int | None,
    typer.Option(
        help="Maximum iterations for PageRank convergence. [default: 20]",
        envvar="HINTGRID_PAGERANK_MAX_ITERATIONS",
    ),
]

# Community similarity options
CommunitySimilarityEnabledOpt = Annotated[
    bool | None,
    typer.Option(
        help="Enable community-based similarity scoring for cross-community "
        "recommendations. [default: true]",
        envvar="HINTGRID_COMMUNITY_SIMILARITY_ENABLED",
    ),
]
CommunitySimilarityTopKOpt = Annotated[
    int | None,
    typer.Option(
        help="Top-K similar communities to consider for cross-community "
        "post recommendations. [default: 5]",
        envvar="HINTGRID_COMMUNITY_SIMILARITY_TOP_K",
    ),
]

# Export options
ExportMaxItemsOpt = Annotated[
    int | None,
    typer.Option(
        help="Maximum items per feed in Markdown export output. [default: 50]",
        envvar="HINTGRID_EXPORT_MAX_ITEMS",
    ),
]
TextPreviewLimitOpt = Annotated[
    int | None,
    typer.Option(
        help="Character limit for text preview in export output. [default: 60]",
        envvar="HINTGRID_TEXT_PREVIEW_LIMIT",
    ),
]
CommunityInterestLimitOpt = Annotated[
    int | None,
    typer.Option(
        help="Max interests displayed per community in export. [default: 30]",
        envvar="HINTGRID_COMMUNITY_INTEREST_LIMIT",
    ),
]
CommunityMemberSampleOpt = Annotated[
    int | None,
    typer.Option(
        help="Sample size for community member list in export. [default: 5]",
        envvar="HINTGRID_COMMUNITY_MEMBER_SAMPLE",
    ),
]
CommunitySampleLimitOpt = Annotated[
    int | None,
    typer.Option(
        help="Max communities shown in export output. [default: 5]",
        envvar="HINTGRID_COMMUNITY_SAMPLE_LIMIT",
    ),
]
GraphSampleLimitOpt = Annotated[
    int | None,
    typer.Option(
        help="Max graph elements in Mermaid diagrams in export. [default: 10]",
        envvar="HINTGRID_GRAPH_SAMPLE_LIMIT",
    ),
]

# Logging options
LogLevelOpt = Annotated[
    str | None,
    typer.Option(
        help="Logging verbosity level. [default: INFO] [values: DEBUG|INFO|WARNING|ERROR|CRITICAL]",
        envvar="HINTGRID_LOG_LEVEL",
    ),
]
LogFileOpt = Annotated[
    str | None,
    typer.Option(
        help="Log file path for persistent logging output. [default: hintgrid.log]",
        envvar="HINTGRID_LOG_FILE",
    ),
]
ProgressPollIntervalSecondsOpt = Annotated[
    float | None,
    typer.Option(
        "--progress-poll-interval-seconds",
        help="Seconds between Neo4j ProgressTracker polls during apoc.periodic.iterate "
        "progress UI. [default: 0.5]",
        envvar="HINTGRID_PROGRESS_POLL_INTERVAL_SECONDS",
    ),
]
VerboseOpt = Annotated[
    bool,
    typer.Option(
        "-v",
        "--verbose",
        help="Enable verbose output: sets log level to DEBUG and shows full "
        "stack traces on errors. CLI-only flag.",
        envvar="HINTGRID_VERBOSE",
    ),
]

# Memory monitoring options
MemoryIntervalOpt = Annotated[
    int,
    typer.Option(
        "--memory-interval",
        help="Memory usage display interval in seconds (0 to disable periodic updates). "
        "[default: 10]",
        envvar="HINTGRID_MEMORY_INTERVAL",
    ),
]

# =============================================================================
# Main Typer app
# =============================================================================

app = typer.Typer(
    name="hintgrid",
    help="HintGrid recommendation pipeline for Mastodon",
    no_args_is_help=True,
)


def _version_callback(value: bool) -> None:
    """Print version and exit."""
    if value:
        from hintgrid import __version__

        typer.echo(f"hintgrid {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            "-V",
            callback=_version_callback,
            is_eager=True,
            help="Show version and exit",
        ),
    ] = False,
) -> None:
    """HintGrid recommendation pipeline for Mastodon."""


def _collect_overrides(**kwargs: object) -> dict[str, object]:
    """Collect non-None CLI overrides into a dict."""
    return {k: v for k, v in kwargs.items() if v is not None}


@app.command()
def run(
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Run without writing to Redis")
    ] = False,
    user_id: Annotated[int | None, typer.Option(help="Process only this user")] = None,
    train: Annotated[
        bool, typer.Option("--train", help="Perform incremental training before pipeline")
    ] = False,
    memory_interval: MemoryIntervalOpt = 10,
    # PostgreSQL
    postgres_host: PostgresHostOpt = None,
    postgres_port: PostgresPortOpt = None,
    postgres_database: PostgresDatabaseOpt = None,
    postgres_user: PostgresUserOpt = None,
    postgres_password: PostgresPasswordOpt = None,
    postgres_schema: PostgresSchemaOpt = None,
    pg_pool_min_size: PgPoolMinSizeOpt = None,
    pg_pool_max_size: PgPoolMaxSizeOpt = None,
    pg_pool_timeout_seconds: PgPoolTimeoutSecondsOpt = None,
    mastodon_public_visibility: MastodonPublicVisibilityOpt = None,
    mastodon_account_lookup_limit: MastodonAccountLookupLimitOpt = None,
    # Neo4j
    neo4j_host: Neo4jHostOpt = None,
    neo4j_port: Neo4jPortOpt = None,
    neo4j_username: Neo4jUsernameOpt = None,
    neo4j_password: Neo4jPasswordOpt = None,
    neo4j_ready_retries: Neo4jReadyRetriesOpt = None,
    neo4j_ready_sleep_seconds: Neo4jReadySleepSecondsOpt = None,
    neo4j_worker_label: Neo4jWorkerLabelOpt = None,
    # Redis
    redis_host: RedisHostOpt = None,
    redis_port: RedisPortOpt = None,
    redis_db: RedisDbOpt = None,
    redis_password: RedisPasswordOpt = None,
    redis_score_tolerance: RedisScoreToleranceOpt = None,
    redis_namespace: RedisNamespaceOpt = None,
    # LLM
    llm_provider: LlmProviderOpt = None,
    llm_base_url: LlmBaseUrlOpt = None,
    llm_model: LlmModelOpt = None,
    llm_dimensions: LlmDimensionsOpt = None,
    llm_timeout: LlmTimeoutOpt = None,
    llm_max_retries: LlmMaxRetriesOpt = None,
    llm_batch_size: LlmBatchSizeOpt = None,
    llm_api_key: LlmApiKeyOpt = None,
    min_embedding_tokens: MinEmbeddingTokensOpt = None,
    embedding_skip_percentile: EmbeddingSkipPercentileOpt = None,
    # FastText
    fasttext_vector_size: FasttextVectorSizeOpt = None,
    fasttext_window: FasttextWindowOpt = None,
    fasttext_min_count: FasttextMinCountOpt = None,
    fasttext_max_vocab_size: FasttextMaxVocabSizeOpt = None,
    fasttext_epochs: FasttextEpochsOpt = None,
    fasttext_bucket: FasttextBucketOpt = None,
    fasttext_min_documents: FasttextMinDocumentsOpt = None,
    fasttext_model_path: FasttextModelPathOpt = None,
    fasttext_quantize: FasttextQuantizeOpt = None,
    fasttext_quantize_qdim: FasttextQuantizeQdimOpt = None,
    fasttext_training_workers: FasttextTrainingWorkersOpt = None,
    # Batch
    batch_size: BatchSizeOpt = None,
    load_since: LoadSinceOpt = None,
    max_retries: MaxRetriesOpt = None,
    checkpoint_interval: CheckpointIntervalOpt = None,
    apoc_batch_size: ApocBatchSizeOpt = None,
    feed_workers: FeedWorkersOpt = None,
    loader_workers: LoaderWorkersOpt = None,
    # Clustering
    user_communities: UserCommunitiesOpt = None,
    post_communities: PostCommunitiesOpt = None,
    leiden_resolution: LeidenResolutionOpt = None,
    leiden_max_levels: LeidenMaxLevelsOpt = None,
    singleton_collapse_enabled: SingletonCollapseEnabledOpt = None,
    noise_community_id: NoiseCommunityIdOpt = None,
    singleton_collapse_in_transactions_of: SingletonCollapseInTransactionsOfOpt = None,
    knn_neighbors: KnnNeighborsOpt = None,
    knn_self_neighbor_offset: KnnSelfNeighborOffsetOpt = None,
    similarity_threshold: SimilarityThresholdOpt = None,
    similarity_recency_days: SimilarityRecencyDaysOpt = None,
    similarity_pruning: SimilarityPruningOpt = None,
    prune_after_clustering: PruneAfterClusteringOpt = None,
    prune_similarity_threshold: PruneSimilarityThresholdOpt = None,
    prune_days: PruneDaysOpt = None,
    # Feed
    feed_size: FeedSizeOpt = None,
    feed_days: FeedDaysOpt = None,
    feed_ttl: FeedTtlOpt = None,
    feed_score_multiplier: FeedScoreMultiplierOpt = None,
    feed_score_decimals: FeedScoreDecimalsOpt = None,
    cold_start_fallback: ColdStartFallbackOpt = None,
    cold_start_limit: ColdStartLimitOpt = None,
    serendipity_probability: SerendipityProbabilityOpt = None,
    serendipity_limit: SerendipityLimitOpt = None,
    serendipity_score: SerendipityScoreOpt = None,
    serendipity_based_on: SerendipityBasedOnOpt = None,
    decay_half_life_days: DecayHalfLifeDaysOpt = None,
    interests_ttl_days: InterestsTtlDaysOpt = None,
    interests_min_favourites: InterestsMinFavouritesOpt = None,
    active_user_days: ActiveUserDaysOpt = None,
    feed_force_refresh: FeedForceRefreshOpt = None,
    language_match_weight: LanguageMatchWeightOpt = None,
    ui_language_match_weight: UiLanguageMatchWeightOpt = None,
    # Scoring
    likes_weight: LikesWeightOpt = None,
    reblogs_weight: ReblogsWeightOpt = None,
    replies_weight: RepliesWeightOpt = None,
    follows_weight: FollowsWeightOpt = None,
    mentions_weight: MentionsWeightOpt = None,
    bookmark_weight: BookmarkWeightOpt = None,
    personalized_interest_weight: PersonalizedInterestWeightOpt = None,
    personalized_popularity_weight: PersonalizedPopularityWeightOpt = None,
    personalized_recency_weight: PersonalizedRecencyWeightOpt = None,
    cold_start_popularity_weight: ColdStartPopularityWeightOpt = None,
    cold_start_recency_weight: ColdStartRecencyWeightOpt = None,
    popularity_smoothing: PopularitySmoothingOpt = None,
    recency_smoothing: RecencySmoothingOpt = None,
    recency_numerator: RecencyNumeratorOpt = None,
    ctr_enabled: CtrEnabledOpt = None,
    ctr_weight: CtrWeightOpt = None,
    min_ctr: MinCtrOpt = None,
    ctr_smoothing: CtrSmoothingOpt = None,
    pagerank_enabled: PagerankEnabledOpt = None,
    pagerank_weight: PagerankWeightOpt = None,
    pagerank_damping_factor: PagerankDampingFactorOpt = None,
    pagerank_max_iterations: PagerankMaxIterationsOpt = None,
    community_similarity_enabled: CommunitySimilarityEnabledOpt = None,
    community_similarity_top_k: CommunitySimilarityTopKOpt = None,
    # Public timelines
    public_feed_size: PublicFeedSizeOpt = None,
    public_feed_enabled: PublicFeedEnabledOpt = None,
    public_feed_strategy: PublicFeedStrategyOpt = None,
    public_timeline_key: PublicTimelineKeyOpt = None,
    local_timeline_key: LocalTimelineKeyOpt = None,
    # Export
    export_max_items: ExportMaxItemsOpt = None,
    text_preview_limit: TextPreviewLimitOpt = None,
    community_interest_limit: CommunityInterestLimitOpt = None,
    community_member_sample: CommunityMemberSampleOpt = None,
    community_sample_limit: CommunitySampleLimitOpt = None,
    graph_sample_limit: GraphSampleLimitOpt = None,
    # Logging
    log_level: LogLevelOpt = None,
    log_file: LogFileOpt = None,
    progress_poll_interval_seconds: ProgressPollIntervalSecondsOpt = None,
    verbose: VerboseOpt = False,
) -> None:
    """Run the full HintGrid pipeline."""
    from hintgrid.cli.runner import execute_run

    overrides = _collect_overrides(
        postgres_host=postgres_host,
        postgres_port=postgres_port,
        postgres_database=postgres_database,
        postgres_user=postgres_user,
        postgres_password=postgres_password,
        postgres_schema=postgres_schema,
        pg_pool_min_size=pg_pool_min_size,
        pg_pool_max_size=pg_pool_max_size,
        pg_pool_timeout_seconds=pg_pool_timeout_seconds,
        mastodon_public_visibility=mastodon_public_visibility,
        mastodon_account_lookup_limit=mastodon_account_lookup_limit,
        neo4j_host=neo4j_host,
        neo4j_port=neo4j_port,
        neo4j_username=neo4j_username,
        neo4j_password=neo4j_password,
        neo4j_ready_retries=neo4j_ready_retries,
        neo4j_ready_sleep_seconds=neo4j_ready_sleep_seconds,
        neo4j_worker_label=neo4j_worker_label,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
        redis_password=redis_password,
        redis_score_tolerance=redis_score_tolerance,
        redis_namespace=redis_namespace,
        llm_provider=llm_provider,
        llm_base_url=llm_base_url,
        llm_model=llm_model,
        llm_dimensions=llm_dimensions,
        llm_timeout=llm_timeout,
        llm_max_retries=llm_max_retries,
        llm_batch_size=llm_batch_size,
        llm_api_key=llm_api_key,
        min_embedding_tokens=min_embedding_tokens,
        embedding_skip_percentile=embedding_skip_percentile,
        fasttext_vector_size=fasttext_vector_size,
        fasttext_window=fasttext_window,
        fasttext_min_count=fasttext_min_count,
        fasttext_max_vocab_size=fasttext_max_vocab_size,
        fasttext_epochs=fasttext_epochs,
        fasttext_bucket=fasttext_bucket,
        fasttext_min_documents=fasttext_min_documents,
        fasttext_model_path=fasttext_model_path,
        fasttext_quantize=fasttext_quantize,
        fasttext_quantize_qdim=fasttext_quantize_qdim,
        fasttext_training_workers=fasttext_training_workers,
        batch_size=batch_size,
        load_since=load_since,
        max_retries=max_retries,
        checkpoint_interval=checkpoint_interval,
        apoc_batch_size=apoc_batch_size,
        feed_workers=feed_workers,
        loader_workers=loader_workers,
        user_communities=user_communities,
        post_communities=post_communities,
        leiden_resolution=leiden_resolution,
        leiden_max_levels=leiden_max_levels,
        singleton_collapse_enabled=singleton_collapse_enabled,
        noise_community_id=noise_community_id,
        singleton_collapse_in_transactions_of=singleton_collapse_in_transactions_of,
        knn_neighbors=knn_neighbors,
        knn_self_neighbor_offset=knn_self_neighbor_offset,
        similarity_threshold=similarity_threshold,
        similarity_recency_days=similarity_recency_days,
        similarity_pruning=similarity_pruning,
        prune_after_clustering=prune_after_clustering,
        prune_similarity_threshold=prune_similarity_threshold,
        prune_days=prune_days,
        feed_size=feed_size,
        feed_days=feed_days,
        feed_ttl=feed_ttl,
        feed_score_multiplier=feed_score_multiplier,
        feed_score_decimals=feed_score_decimals,
        cold_start_fallback=cold_start_fallback,
        cold_start_limit=cold_start_limit,
        serendipity_probability=serendipity_probability,
        serendipity_limit=serendipity_limit,
        serendipity_score=serendipity_score,
        serendipity_based_on=serendipity_based_on,
        decay_half_life_days=decay_half_life_days,
        interests_ttl_days=interests_ttl_days,
        interests_min_favourites=interests_min_favourites,
        active_user_days=active_user_days,
        feed_force_refresh=feed_force_refresh,
        language_match_weight=language_match_weight,
        ui_language_match_weight=ui_language_match_weight,
        likes_weight=likes_weight,
        reblogs_weight=reblogs_weight,
        replies_weight=replies_weight,
        follows_weight=follows_weight,
        mentions_weight=mentions_weight,
        bookmark_weight=bookmark_weight,
        personalized_interest_weight=personalized_interest_weight,
        personalized_popularity_weight=personalized_popularity_weight,
        personalized_recency_weight=personalized_recency_weight,
        cold_start_popularity_weight=cold_start_popularity_weight,
        cold_start_recency_weight=cold_start_recency_weight,
        popularity_smoothing=popularity_smoothing,
        recency_smoothing=recency_smoothing,
        recency_numerator=recency_numerator,
        ctr_enabled=ctr_enabled,
        ctr_weight=ctr_weight,
        min_ctr=min_ctr,
        ctr_smoothing=ctr_smoothing,
        pagerank_enabled=pagerank_enabled,
        pagerank_weight=pagerank_weight,
        pagerank_damping_factor=pagerank_damping_factor,
        pagerank_max_iterations=pagerank_max_iterations,
        community_similarity_enabled=community_similarity_enabled,
        community_similarity_top_k=community_similarity_top_k,
        public_feed_size=public_feed_size,
        public_feed_enabled=public_feed_enabled,
        public_feed_strategy=public_feed_strategy,
        public_timeline_key=public_timeline_key,
        local_timeline_key=local_timeline_key,
        export_max_items=export_max_items,
        text_preview_limit=text_preview_limit,
        community_interest_limit=community_interest_limit,
        community_member_sample=community_member_sample,
        community_sample_limit=community_sample_limit,
        graph_sample_limit=graph_sample_limit,
        log_level=log_level,
        log_file=log_file,
        progress_poll_interval_seconds=progress_poll_interval_seconds,
    )

    exit_code = execute_run(
        overrides=overrides,
        dry_run=dry_run,
        user_id=user_id,
        do_train=train,
        verbose=verbose,
        memory_interval=memory_interval,
    )
    raise typer.Exit(code=exit_code)


@app.command()
def refresh(
    memory_interval: MemoryIntervalOpt = 10,
    # PostgreSQL
    postgres_host: PostgresHostOpt = None,
    postgres_port: PostgresPortOpt = None,
    postgres_database: PostgresDatabaseOpt = None,
    postgres_user: PostgresUserOpt = None,
    postgres_password: PostgresPasswordOpt = None,
    pg_pool_min_size: PgPoolMinSizeOpt = None,
    pg_pool_max_size: PgPoolMaxSizeOpt = None,
    # Neo4j
    neo4j_host: Neo4jHostOpt = None,
    neo4j_port: Neo4jPortOpt = None,
    neo4j_username: Neo4jUsernameOpt = None,
    neo4j_password: Neo4jPasswordOpt = None,
    neo4j_ready_retries: Neo4jReadyRetriesOpt = None,
    neo4j_ready_sleep_seconds: Neo4jReadySleepSecondsOpt = None,
    neo4j_worker_label: Neo4jWorkerLabelOpt = None,
    # Redis
    redis_host: RedisHostOpt = None,
    redis_port: RedisPortOpt = None,
    redis_password: RedisPasswordOpt = None,
    redis_db: RedisDbOpt = None,
    # Interests
    decay_half_life_days: DecayHalfLifeDaysOpt = None,
    interests_ttl_days: InterestsTtlDaysOpt = None,
    interests_min_favourites: InterestsMinFavouritesOpt = None,
    # Scoring
    likes_weight: LikesWeightOpt = None,
    reblogs_weight: ReblogsWeightOpt = None,
    replies_weight: RepliesWeightOpt = None,
    follows_weight: FollowsWeightOpt = None,
    mentions_weight: MentionsWeightOpt = None,
    bookmark_weight: BookmarkWeightOpt = None,
    # CTR
    ctr_enabled: CtrEnabledOpt = None,
    ctr_weight: CtrWeightOpt = None,
    min_ctr: MinCtrOpt = None,
    ctr_smoothing: CtrSmoothingOpt = None,
    # PageRank
    pagerank_enabled: PagerankEnabledOpt = None,
    pagerank_weight: PagerankWeightOpt = None,
    # Community similarity
    community_similarity_enabled: CommunitySimilarityEnabledOpt = None,
    community_similarity_top_k: CommunitySimilarityTopKOpt = None,
    # Language
    language_match_weight: LanguageMatchWeightOpt = None,
    ui_language_match_weight: UiLanguageMatchWeightOpt = None,
    # Logging
    log_level: LogLevelOpt = None,
    log_file: LogFileOpt = None,
    progress_poll_interval_seconds: ProgressPollIntervalSecondsOpt = None,
    verbose: VerboseOpt = False,
) -> None:
    """Lightweight interest refresh (no clustering, just decay + dirty recompute).

    Applies exponential decay to existing interest scores and recomputes
    only communities with new interactions since the last rebuild.
    Falls back to full rebuild if no previous rebuild timestamp exists.
    """
    from hintgrid.cli.runner import execute_refresh

    overrides = _collect_overrides(
        postgres_host=postgres_host,
        postgres_port=postgres_port,
        postgres_database=postgres_database,
        postgres_user=postgres_user,
        postgres_password=postgres_password,
        pg_pool_min_size=pg_pool_min_size,
        pg_pool_max_size=pg_pool_max_size,
        neo4j_host=neo4j_host,
        neo4j_port=neo4j_port,
        neo4j_username=neo4j_username,
        neo4j_password=neo4j_password,
        neo4j_ready_retries=neo4j_ready_retries,
        neo4j_ready_sleep_seconds=neo4j_ready_sleep_seconds,
        neo4j_worker_label=neo4j_worker_label,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_password=redis_password,
        redis_db=redis_db,
        decay_half_life_days=decay_half_life_days,
        interests_ttl_days=interests_ttl_days,
        interests_min_favourites=interests_min_favourites,
        likes_weight=likes_weight,
        reblogs_weight=reblogs_weight,
        replies_weight=replies_weight,
        follows_weight=follows_weight,
        mentions_weight=mentions_weight,
        bookmark_weight=bookmark_weight,
        ctr_enabled=ctr_enabled,
        ctr_weight=ctr_weight,
        min_ctr=min_ctr,
        ctr_smoothing=ctr_smoothing,
        pagerank_enabled=pagerank_enabled,
        pagerank_weight=pagerank_weight,
        community_similarity_enabled=community_similarity_enabled,
        community_similarity_top_k=community_similarity_top_k,
        language_match_weight=language_match_weight,
        ui_language_match_weight=ui_language_match_weight,
        log_level=log_level,
        log_file=log_file,
        progress_poll_interval_seconds=progress_poll_interval_seconds,
    )

    exit_code = execute_refresh(
        overrides=overrides,
        verbose=verbose,
        memory_interval=memory_interval,
    )
    raise typer.Exit(code=exit_code)


@app.command()
def export(
    filename: Annotated[str, typer.Argument(help="Output Markdown filename")],
    user_id: Annotated[int, typer.Option("--user-id", help="User ID to export timeline for")],
    memory_interval: MemoryIntervalOpt = 10,
    # PostgreSQL
    postgres_host: PostgresHostOpt = None,
    postgres_port: PostgresPortOpt = None,
    postgres_database: PostgresDatabaseOpt = None,
    postgres_user: PostgresUserOpt = None,
    postgres_password: PostgresPasswordOpt = None,
    postgres_schema: PostgresSchemaOpt = None,
    pg_pool_min_size: PgPoolMinSizeOpt = None,
    pg_pool_max_size: PgPoolMaxSizeOpt = None,
    pg_pool_timeout_seconds: PgPoolTimeoutSecondsOpt = None,
    mastodon_public_visibility: MastodonPublicVisibilityOpt = None,
    mastodon_account_lookup_limit: MastodonAccountLookupLimitOpt = None,
    # Neo4j
    neo4j_host: Neo4jHostOpt = None,
    neo4j_port: Neo4jPortOpt = None,
    neo4j_username: Neo4jUsernameOpt = None,
    neo4j_password: Neo4jPasswordOpt = None,
    neo4j_ready_retries: Neo4jReadyRetriesOpt = None,
    neo4j_ready_sleep_seconds: Neo4jReadySleepSecondsOpt = None,
    neo4j_worker_label: Neo4jWorkerLabelOpt = None,
    # Redis
    redis_host: RedisHostOpt = None,
    redis_port: RedisPortOpt = None,
    redis_db: RedisDbOpt = None,
    redis_password: RedisPasswordOpt = None,
    redis_score_tolerance: RedisScoreToleranceOpt = None,
    redis_namespace: RedisNamespaceOpt = None,
    # LLM
    llm_provider: LlmProviderOpt = None,
    llm_base_url: LlmBaseUrlOpt = None,
    llm_model: LlmModelOpt = None,
    llm_dimensions: LlmDimensionsOpt = None,
    llm_timeout: LlmTimeoutOpt = None,
    llm_max_retries: LlmMaxRetriesOpt = None,
    llm_batch_size: LlmBatchSizeOpt = None,
    llm_api_key: LlmApiKeyOpt = None,
    min_embedding_tokens: MinEmbeddingTokensOpt = None,
    embedding_skip_percentile: EmbeddingSkipPercentileOpt = None,
    # FastText
    fasttext_vector_size: FasttextVectorSizeOpt = None,
    fasttext_window: FasttextWindowOpt = None,
    fasttext_min_count: FasttextMinCountOpt = None,
    fasttext_max_vocab_size: FasttextMaxVocabSizeOpt = None,
    fasttext_epochs: FasttextEpochsOpt = None,
    fasttext_bucket: FasttextBucketOpt = None,
    fasttext_min_documents: FasttextMinDocumentsOpt = None,
    fasttext_model_path: FasttextModelPathOpt = None,
    fasttext_quantize: FasttextQuantizeOpt = None,
    fasttext_quantize_qdim: FasttextQuantizeQdimOpt = None,
    fasttext_training_workers: FasttextTrainingWorkersOpt = None,
    # Batch
    batch_size: BatchSizeOpt = None,
    load_since: LoadSinceOpt = None,
    max_retries: MaxRetriesOpt = None,
    checkpoint_interval: CheckpointIntervalOpt = None,
    apoc_batch_size: ApocBatchSizeOpt = None,
    feed_workers: FeedWorkersOpt = None,
    loader_workers: LoaderWorkersOpt = None,
    # Clustering
    user_communities: UserCommunitiesOpt = None,
    post_communities: PostCommunitiesOpt = None,
    leiden_resolution: LeidenResolutionOpt = None,
    leiden_max_levels: LeidenMaxLevelsOpt = None,
    singleton_collapse_enabled: SingletonCollapseEnabledOpt = None,
    noise_community_id: NoiseCommunityIdOpt = None,
    singleton_collapse_in_transactions_of: SingletonCollapseInTransactionsOfOpt = None,
    knn_neighbors: KnnNeighborsOpt = None,
    knn_self_neighbor_offset: KnnSelfNeighborOffsetOpt = None,
    similarity_threshold: SimilarityThresholdOpt = None,
    similarity_recency_days: SimilarityRecencyDaysOpt = None,
    similarity_pruning: SimilarityPruningOpt = None,
    prune_after_clustering: PruneAfterClusteringOpt = None,
    prune_similarity_threshold: PruneSimilarityThresholdOpt = None,
    prune_days: PruneDaysOpt = None,
    # Feed
    feed_size: FeedSizeOpt = None,
    feed_days: FeedDaysOpt = None,
    feed_ttl: FeedTtlOpt = None,
    feed_score_multiplier: FeedScoreMultiplierOpt = None,
    feed_score_decimals: FeedScoreDecimalsOpt = None,
    cold_start_fallback: ColdStartFallbackOpt = None,
    cold_start_limit: ColdStartLimitOpt = None,
    serendipity_probability: SerendipityProbabilityOpt = None,
    serendipity_limit: SerendipityLimitOpt = None,
    serendipity_score: SerendipityScoreOpt = None,
    serendipity_based_on: SerendipityBasedOnOpt = None,
    decay_half_life_days: DecayHalfLifeDaysOpt = None,
    interests_ttl_days: InterestsTtlDaysOpt = None,
    interests_min_favourites: InterestsMinFavouritesOpt = None,
    active_user_days: ActiveUserDaysOpt = None,
    feed_force_refresh: FeedForceRefreshOpt = None,
    language_match_weight: LanguageMatchWeightOpt = None,
    ui_language_match_weight: UiLanguageMatchWeightOpt = None,
    # Scoring
    likes_weight: LikesWeightOpt = None,
    reblogs_weight: ReblogsWeightOpt = None,
    replies_weight: RepliesWeightOpt = None,
    follows_weight: FollowsWeightOpt = None,
    mentions_weight: MentionsWeightOpt = None,
    bookmark_weight: BookmarkWeightOpt = None,
    personalized_interest_weight: PersonalizedInterestWeightOpt = None,
    personalized_popularity_weight: PersonalizedPopularityWeightOpt = None,
    personalized_recency_weight: PersonalizedRecencyWeightOpt = None,
    cold_start_popularity_weight: ColdStartPopularityWeightOpt = None,
    cold_start_recency_weight: ColdStartRecencyWeightOpt = None,
    popularity_smoothing: PopularitySmoothingOpt = None,
    recency_smoothing: RecencySmoothingOpt = None,
    recency_numerator: RecencyNumeratorOpt = None,
    ctr_enabled: CtrEnabledOpt = None,
    ctr_weight: CtrWeightOpt = None,
    min_ctr: MinCtrOpt = None,
    ctr_smoothing: CtrSmoothingOpt = None,
    pagerank_enabled: PagerankEnabledOpt = None,
    pagerank_weight: PagerankWeightOpt = None,
    pagerank_damping_factor: PagerankDampingFactorOpt = None,
    pagerank_max_iterations: PagerankMaxIterationsOpt = None,
    community_similarity_enabled: CommunitySimilarityEnabledOpt = None,
    community_similarity_top_k: CommunitySimilarityTopKOpt = None,
    # Public timelines
    public_feed_size: PublicFeedSizeOpt = None,
    public_feed_enabled: PublicFeedEnabledOpt = None,
    public_feed_strategy: PublicFeedStrategyOpt = None,
    public_timeline_key: PublicTimelineKeyOpt = None,
    local_timeline_key: LocalTimelineKeyOpt = None,
    # Export
    export_max_items: ExportMaxItemsOpt = None,
    text_preview_limit: TextPreviewLimitOpt = None,
    community_interest_limit: CommunityInterestLimitOpt = None,
    community_member_sample: CommunityMemberSampleOpt = None,
    community_sample_limit: CommunitySampleLimitOpt = None,
    graph_sample_limit: GraphSampleLimitOpt = None,
    # Logging
    log_level: LogLevelOpt = None,
    log_file: LogFileOpt = None,
    progress_poll_interval_seconds: ProgressPollIntervalSecondsOpt = None,
    verbose: VerboseOpt = False,
) -> None:
    """Export system state to Markdown file."""
    from hintgrid.cli.runner import execute_export

    overrides = _collect_overrides(
        postgres_host=postgres_host,
        postgres_port=postgres_port,
        postgres_database=postgres_database,
        postgres_user=postgres_user,
        postgres_password=postgres_password,
        postgres_schema=postgres_schema,
        pg_pool_min_size=pg_pool_min_size,
        pg_pool_max_size=pg_pool_max_size,
        pg_pool_timeout_seconds=pg_pool_timeout_seconds,
        mastodon_public_visibility=mastodon_public_visibility,
        mastodon_account_lookup_limit=mastodon_account_lookup_limit,
        neo4j_host=neo4j_host,
        neo4j_port=neo4j_port,
        neo4j_username=neo4j_username,
        neo4j_password=neo4j_password,
        neo4j_ready_retries=neo4j_ready_retries,
        neo4j_ready_sleep_seconds=neo4j_ready_sleep_seconds,
        neo4j_worker_label=neo4j_worker_label,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
        redis_password=redis_password,
        redis_score_tolerance=redis_score_tolerance,
        redis_namespace=redis_namespace,
        llm_provider=llm_provider,
        llm_base_url=llm_base_url,
        llm_model=llm_model,
        llm_dimensions=llm_dimensions,
        llm_timeout=llm_timeout,
        llm_max_retries=llm_max_retries,
        llm_batch_size=llm_batch_size,
        llm_api_key=llm_api_key,
        min_embedding_tokens=min_embedding_tokens,
        embedding_skip_percentile=embedding_skip_percentile,
        fasttext_vector_size=fasttext_vector_size,
        fasttext_window=fasttext_window,
        fasttext_min_count=fasttext_min_count,
        fasttext_max_vocab_size=fasttext_max_vocab_size,
        fasttext_epochs=fasttext_epochs,
        fasttext_bucket=fasttext_bucket,
        fasttext_min_documents=fasttext_min_documents,
        fasttext_model_path=fasttext_model_path,
        fasttext_quantize=fasttext_quantize,
        fasttext_quantize_qdim=fasttext_quantize_qdim,
        fasttext_training_workers=fasttext_training_workers,
        batch_size=batch_size,
        load_since=load_since,
        max_retries=max_retries,
        checkpoint_interval=checkpoint_interval,
        apoc_batch_size=apoc_batch_size,
        feed_workers=feed_workers,
        loader_workers=loader_workers,
        user_communities=user_communities,
        post_communities=post_communities,
        leiden_resolution=leiden_resolution,
        leiden_max_levels=leiden_max_levels,
        singleton_collapse_enabled=singleton_collapse_enabled,
        noise_community_id=noise_community_id,
        singleton_collapse_in_transactions_of=singleton_collapse_in_transactions_of,
        knn_neighbors=knn_neighbors,
        knn_self_neighbor_offset=knn_self_neighbor_offset,
        similarity_threshold=similarity_threshold,
        similarity_recency_days=similarity_recency_days,
        similarity_pruning=similarity_pruning,
        prune_after_clustering=prune_after_clustering,
        prune_similarity_threshold=prune_similarity_threshold,
        prune_days=prune_days,
        feed_size=feed_size,
        feed_days=feed_days,
        feed_ttl=feed_ttl,
        feed_score_multiplier=feed_score_multiplier,
        feed_score_decimals=feed_score_decimals,
        cold_start_fallback=cold_start_fallback,
        cold_start_limit=cold_start_limit,
        serendipity_probability=serendipity_probability,
        serendipity_limit=serendipity_limit,
        serendipity_score=serendipity_score,
        serendipity_based_on=serendipity_based_on,
        decay_half_life_days=decay_half_life_days,
        interests_ttl_days=interests_ttl_days,
        interests_min_favourites=interests_min_favourites,
        active_user_days=active_user_days,
        feed_force_refresh=feed_force_refresh,
        language_match_weight=language_match_weight,
        ui_language_match_weight=ui_language_match_weight,
        likes_weight=likes_weight,
        reblogs_weight=reblogs_weight,
        replies_weight=replies_weight,
        follows_weight=follows_weight,
        mentions_weight=mentions_weight,
        bookmark_weight=bookmark_weight,
        personalized_interest_weight=personalized_interest_weight,
        personalized_popularity_weight=personalized_popularity_weight,
        personalized_recency_weight=personalized_recency_weight,
        cold_start_popularity_weight=cold_start_popularity_weight,
        cold_start_recency_weight=cold_start_recency_weight,
        popularity_smoothing=popularity_smoothing,
        recency_smoothing=recency_smoothing,
        recency_numerator=recency_numerator,
        ctr_enabled=ctr_enabled,
        ctr_weight=ctr_weight,
        min_ctr=min_ctr,
        ctr_smoothing=ctr_smoothing,
        pagerank_enabled=pagerank_enabled,
        pagerank_weight=pagerank_weight,
        pagerank_damping_factor=pagerank_damping_factor,
        pagerank_max_iterations=pagerank_max_iterations,
        community_similarity_enabled=community_similarity_enabled,
        community_similarity_top_k=community_similarity_top_k,
        public_feed_size=public_feed_size,
        public_feed_enabled=public_feed_enabled,
        public_feed_strategy=public_feed_strategy,
        public_timeline_key=public_timeline_key,
        local_timeline_key=local_timeline_key,
        export_max_items=export_max_items,
        text_preview_limit=text_preview_limit,
        community_interest_limit=community_interest_limit,
        community_member_sample=community_member_sample,
        community_sample_limit=community_sample_limit,
        graph_sample_limit=graph_sample_limit,
        log_level=log_level,
        log_file=log_file,
        progress_poll_interval_seconds=progress_poll_interval_seconds,
    )

    exit_code = execute_export(
        overrides=overrides,
        filename=filename,
        user_id=user_id,
        verbose=verbose,
        memory_interval=memory_interval,
    )
    raise typer.Exit(code=exit_code)


@app.command()
def train(
    full: Annotated[
        bool, typer.Option("--full", help="Full retraining (clears existing models)")
    ] = False,
    incremental: Annotated[
        bool, typer.Option("--incremental", help="Incremental training")
    ] = False,
    since: Annotated[
        str | None, typer.Option(help="Load data window for training (e.g., 30d)")
    ] = None,
    memory_interval: MemoryIntervalOpt = 10,
    # PostgreSQL
    postgres_host: PostgresHostOpt = None,
    postgres_port: PostgresPortOpt = None,
    postgres_database: PostgresDatabaseOpt = None,
    postgres_user: PostgresUserOpt = None,
    postgres_password: PostgresPasswordOpt = None,
    postgres_schema: PostgresSchemaOpt = None,
    pg_pool_min_size: PgPoolMinSizeOpt = None,
    pg_pool_max_size: PgPoolMaxSizeOpt = None,
    pg_pool_timeout_seconds: PgPoolTimeoutSecondsOpt = None,
    mastodon_public_visibility: MastodonPublicVisibilityOpt = None,
    mastodon_account_lookup_limit: MastodonAccountLookupLimitOpt = None,
    # Neo4j
    neo4j_host: Neo4jHostOpt = None,
    neo4j_port: Neo4jPortOpt = None,
    neo4j_username: Neo4jUsernameOpt = None,
    neo4j_password: Neo4jPasswordOpt = None,
    neo4j_ready_retries: Neo4jReadyRetriesOpt = None,
    neo4j_ready_sleep_seconds: Neo4jReadySleepSecondsOpt = None,
    neo4j_worker_label: Neo4jWorkerLabelOpt = None,
    # Redis
    redis_host: RedisHostOpt = None,
    redis_port: RedisPortOpt = None,
    redis_db: RedisDbOpt = None,
    redis_password: RedisPasswordOpt = None,
    redis_score_tolerance: RedisScoreToleranceOpt = None,
    redis_namespace: RedisNamespaceOpt = None,
    # LLM
    llm_provider: LlmProviderOpt = None,
    llm_base_url: LlmBaseUrlOpt = None,
    llm_model: LlmModelOpt = None,
    llm_dimensions: LlmDimensionsOpt = None,
    llm_timeout: LlmTimeoutOpt = None,
    llm_max_retries: LlmMaxRetriesOpt = None,
    llm_batch_size: LlmBatchSizeOpt = None,
    llm_api_key: LlmApiKeyOpt = None,
    min_embedding_tokens: MinEmbeddingTokensOpt = None,
    embedding_skip_percentile: EmbeddingSkipPercentileOpt = None,
    # FastText
    fasttext_vector_size: FasttextVectorSizeOpt = None,
    fasttext_window: FasttextWindowOpt = None,
    fasttext_min_count: FasttextMinCountOpt = None,
    fasttext_max_vocab_size: FasttextMaxVocabSizeOpt = None,
    fasttext_epochs: FasttextEpochsOpt = None,
    fasttext_bucket: FasttextBucketOpt = None,
    fasttext_min_documents: FasttextMinDocumentsOpt = None,
    fasttext_model_path: FasttextModelPathOpt = None,
    fasttext_quantize: FasttextQuantizeOpt = None,
    fasttext_quantize_qdim: FasttextQuantizeQdimOpt = None,
    fasttext_training_workers: FasttextTrainingWorkersOpt = None,
    # Batch
    batch_size: BatchSizeOpt = None,
    load_since: LoadSinceOpt = None,
    max_retries: MaxRetriesOpt = None,
    checkpoint_interval: CheckpointIntervalOpt = None,
    apoc_batch_size: ApocBatchSizeOpt = None,
    feed_workers: FeedWorkersOpt = None,
    loader_workers: LoaderWorkersOpt = None,
    # Clustering
    user_communities: UserCommunitiesOpt = None,
    post_communities: PostCommunitiesOpt = None,
    leiden_resolution: LeidenResolutionOpt = None,
    leiden_max_levels: LeidenMaxLevelsOpt = None,
    singleton_collapse_enabled: SingletonCollapseEnabledOpt = None,
    noise_community_id: NoiseCommunityIdOpt = None,
    singleton_collapse_in_transactions_of: SingletonCollapseInTransactionsOfOpt = None,
    knn_neighbors: KnnNeighborsOpt = None,
    knn_self_neighbor_offset: KnnSelfNeighborOffsetOpt = None,
    similarity_threshold: SimilarityThresholdOpt = None,
    similarity_recency_days: SimilarityRecencyDaysOpt = None,
    similarity_pruning: SimilarityPruningOpt = None,
    prune_after_clustering: PruneAfterClusteringOpt = None,
    prune_similarity_threshold: PruneSimilarityThresholdOpt = None,
    prune_days: PruneDaysOpt = None,
    # Feed
    feed_size: FeedSizeOpt = None,
    feed_days: FeedDaysOpt = None,
    feed_ttl: FeedTtlOpt = None,
    feed_score_multiplier: FeedScoreMultiplierOpt = None,
    feed_score_decimals: FeedScoreDecimalsOpt = None,
    cold_start_fallback: ColdStartFallbackOpt = None,
    cold_start_limit: ColdStartLimitOpt = None,
    serendipity_probability: SerendipityProbabilityOpt = None,
    serendipity_limit: SerendipityLimitOpt = None,
    serendipity_score: SerendipityScoreOpt = None,
    serendipity_based_on: SerendipityBasedOnOpt = None,
    decay_half_life_days: DecayHalfLifeDaysOpt = None,
    interests_ttl_days: InterestsTtlDaysOpt = None,
    interests_min_favourites: InterestsMinFavouritesOpt = None,
    active_user_days: ActiveUserDaysOpt = None,
    feed_force_refresh: FeedForceRefreshOpt = None,
    language_match_weight: LanguageMatchWeightOpt = None,
    ui_language_match_weight: UiLanguageMatchWeightOpt = None,
    # Scoring
    likes_weight: LikesWeightOpt = None,
    reblogs_weight: ReblogsWeightOpt = None,
    replies_weight: RepliesWeightOpt = None,
    follows_weight: FollowsWeightOpt = None,
    mentions_weight: MentionsWeightOpt = None,
    bookmark_weight: BookmarkWeightOpt = None,
    personalized_interest_weight: PersonalizedInterestWeightOpt = None,
    personalized_popularity_weight: PersonalizedPopularityWeightOpt = None,
    personalized_recency_weight: PersonalizedRecencyWeightOpt = None,
    cold_start_popularity_weight: ColdStartPopularityWeightOpt = None,
    cold_start_recency_weight: ColdStartRecencyWeightOpt = None,
    popularity_smoothing: PopularitySmoothingOpt = None,
    recency_smoothing: RecencySmoothingOpt = None,
    recency_numerator: RecencyNumeratorOpt = None,
    ctr_enabled: CtrEnabledOpt = None,
    ctr_weight: CtrWeightOpt = None,
    min_ctr: MinCtrOpt = None,
    ctr_smoothing: CtrSmoothingOpt = None,
    pagerank_enabled: PagerankEnabledOpt = None,
    pagerank_weight: PagerankWeightOpt = None,
    pagerank_damping_factor: PagerankDampingFactorOpt = None,
    pagerank_max_iterations: PagerankMaxIterationsOpt = None,
    community_similarity_enabled: CommunitySimilarityEnabledOpt = None,
    community_similarity_top_k: CommunitySimilarityTopKOpt = None,
    # Public timelines
    public_feed_size: PublicFeedSizeOpt = None,
    public_feed_enabled: PublicFeedEnabledOpt = None,
    public_feed_strategy: PublicFeedStrategyOpt = None,
    public_timeline_key: PublicTimelineKeyOpt = None,
    local_timeline_key: LocalTimelineKeyOpt = None,
    # Export
    export_max_items: ExportMaxItemsOpt = None,
    text_preview_limit: TextPreviewLimitOpt = None,
    community_interest_limit: CommunityInterestLimitOpt = None,
    community_member_sample: CommunityMemberSampleOpt = None,
    community_sample_limit: CommunitySampleLimitOpt = None,
    graph_sample_limit: GraphSampleLimitOpt = None,
    # Logging
    log_level: LogLevelOpt = None,
    log_file: LogFileOpt = None,
    progress_poll_interval_seconds: ProgressPollIntervalSecondsOpt = None,
    verbose: VerboseOpt = False,
) -> None:
    """Train embedding models."""
    from hintgrid.cli.runner import execute_train

    # Validate mutually exclusive options
    if not full and not incremental:
        typer.echo("Error: Must specify either --full or --incremental", err=True)
        raise typer.Exit(code=2)
    if full and incremental:
        typer.echo("Error: Cannot specify both --full and --incremental", err=True)
        raise typer.Exit(code=2)

    overrides = _collect_overrides(
        postgres_host=postgres_host,
        postgres_port=postgres_port,
        postgres_database=postgres_database,
        postgres_user=postgres_user,
        postgres_password=postgres_password,
        postgres_schema=postgres_schema,
        pg_pool_min_size=pg_pool_min_size,
        pg_pool_max_size=pg_pool_max_size,
        pg_pool_timeout_seconds=pg_pool_timeout_seconds,
        mastodon_public_visibility=mastodon_public_visibility,
        mastodon_account_lookup_limit=mastodon_account_lookup_limit,
        neo4j_host=neo4j_host,
        neo4j_port=neo4j_port,
        neo4j_username=neo4j_username,
        neo4j_password=neo4j_password,
        neo4j_ready_retries=neo4j_ready_retries,
        neo4j_ready_sleep_seconds=neo4j_ready_sleep_seconds,
        neo4j_worker_label=neo4j_worker_label,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
        redis_password=redis_password,
        redis_score_tolerance=redis_score_tolerance,
        redis_namespace=redis_namespace,
        llm_provider=llm_provider,
        llm_base_url=llm_base_url,
        llm_model=llm_model,
        llm_dimensions=llm_dimensions,
        llm_timeout=llm_timeout,
        llm_max_retries=llm_max_retries,
        llm_batch_size=llm_batch_size,
        llm_api_key=llm_api_key,
        min_embedding_tokens=min_embedding_tokens,
        embedding_skip_percentile=embedding_skip_percentile,
        fasttext_vector_size=fasttext_vector_size,
        fasttext_window=fasttext_window,
        fasttext_min_count=fasttext_min_count,
        fasttext_max_vocab_size=fasttext_max_vocab_size,
        fasttext_epochs=fasttext_epochs,
        fasttext_bucket=fasttext_bucket,
        fasttext_min_documents=fasttext_min_documents,
        fasttext_model_path=fasttext_model_path,
        fasttext_quantize=fasttext_quantize,
        fasttext_quantize_qdim=fasttext_quantize_qdim,
        fasttext_training_workers=fasttext_training_workers,
        batch_size=batch_size,
        load_since=load_since,
        max_retries=max_retries,
        checkpoint_interval=checkpoint_interval,
        apoc_batch_size=apoc_batch_size,
        feed_workers=feed_workers,
        loader_workers=loader_workers,
        user_communities=user_communities,
        post_communities=post_communities,
        leiden_resolution=leiden_resolution,
        leiden_max_levels=leiden_max_levels,
        singleton_collapse_enabled=singleton_collapse_enabled,
        noise_community_id=noise_community_id,
        singleton_collapse_in_transactions_of=singleton_collapse_in_transactions_of,
        knn_neighbors=knn_neighbors,
        knn_self_neighbor_offset=knn_self_neighbor_offset,
        similarity_threshold=similarity_threshold,
        similarity_recency_days=similarity_recency_days,
        similarity_pruning=similarity_pruning,
        prune_after_clustering=prune_after_clustering,
        prune_similarity_threshold=prune_similarity_threshold,
        prune_days=prune_days,
        feed_size=feed_size,
        feed_days=feed_days,
        feed_ttl=feed_ttl,
        feed_score_multiplier=feed_score_multiplier,
        feed_score_decimals=feed_score_decimals,
        cold_start_fallback=cold_start_fallback,
        cold_start_limit=cold_start_limit,
        serendipity_probability=serendipity_probability,
        serendipity_limit=serendipity_limit,
        serendipity_score=serendipity_score,
        serendipity_based_on=serendipity_based_on,
        decay_half_life_days=decay_half_life_days,
        interests_ttl_days=interests_ttl_days,
        interests_min_favourites=interests_min_favourites,
        active_user_days=active_user_days,
        feed_force_refresh=feed_force_refresh,
        language_match_weight=language_match_weight,
        ui_language_match_weight=ui_language_match_weight,
        likes_weight=likes_weight,
        reblogs_weight=reblogs_weight,
        replies_weight=replies_weight,
        follows_weight=follows_weight,
        mentions_weight=mentions_weight,
        bookmark_weight=bookmark_weight,
        personalized_interest_weight=personalized_interest_weight,
        personalized_popularity_weight=personalized_popularity_weight,
        personalized_recency_weight=personalized_recency_weight,
        cold_start_popularity_weight=cold_start_popularity_weight,
        cold_start_recency_weight=cold_start_recency_weight,
        popularity_smoothing=popularity_smoothing,
        recency_smoothing=recency_smoothing,
        recency_numerator=recency_numerator,
        ctr_enabled=ctr_enabled,
        ctr_weight=ctr_weight,
        min_ctr=min_ctr,
        ctr_smoothing=ctr_smoothing,
        pagerank_enabled=pagerank_enabled,
        pagerank_weight=pagerank_weight,
        pagerank_damping_factor=pagerank_damping_factor,
        pagerank_max_iterations=pagerank_max_iterations,
        community_similarity_enabled=community_similarity_enabled,
        community_similarity_top_k=community_similarity_top_k,
        public_feed_size=public_feed_size,
        public_feed_enabled=public_feed_enabled,
        public_feed_strategy=public_feed_strategy,
        public_timeline_key=public_timeline_key,
        local_timeline_key=local_timeline_key,
        export_max_items=export_max_items,
        text_preview_limit=text_preview_limit,
        community_interest_limit=community_interest_limit,
        community_member_sample=community_member_sample,
        community_sample_limit=community_sample_limit,
        graph_sample_limit=graph_sample_limit,
        log_level=log_level,
        log_file=log_file,
        progress_poll_interval_seconds=progress_poll_interval_seconds,
    )

    exit_code = execute_train(
        overrides=overrides,
        full=full,
        since=since,
        verbose=verbose,
        memory_interval=memory_interval,
    )
    raise typer.Exit(code=exit_code)


@app.command()
def clean(
    graph: Annotated[bool, typer.Option("--graph", help="Clean Neo4j graph data only")] = False,
    redis_flag: Annotated[bool, typer.Option("--redis", help="Clean Redis feed data only")] = False,
    models: Annotated[
        bool, typer.Option("--models", help="Clean model files on disk only")
    ] = False,
    embeddings: Annotated[
        bool, typer.Option("--embeddings", help="Clear post embeddings (cascades to similarity)")
    ] = False,
    clusters: Annotated[
        bool,
        typer.Option(
            "--clusters",
            help="Clear user and post clusters (cascades to interests and recommendations)",
        ),
    ] = False,
    similarity: Annotated[
        bool,
        typer.Option(
            "--similarity",
            help="Clear SIMILAR_TO relationships (cascades to post clusters and interests)",
        ),
    ] = False,
    interests: Annotated[
        bool, typer.Option("--interests", help="Clear INTERESTED_IN relationships")
    ] = False,
    interactions: Annotated[
        bool,
        typer.Option("--interactions", help="Clear INTERACTS_WITH relationships between users"),
    ] = False,
    recommendations: Annotated[
        bool, typer.Option("--recommendations", help="Clear WAS_RECOMMENDED relationships")
    ] = False,
    fasttext_state: Annotated[
        bool, typer.Option("--fasttext-state", help="Clear FastTextState node from graph")
    ] = False,
    memory_interval: MemoryIntervalOpt = 10,
    # PostgreSQL
    postgres_host: PostgresHostOpt = None,
    postgres_port: PostgresPortOpt = None,
    postgres_database: PostgresDatabaseOpt = None,
    postgres_user: PostgresUserOpt = None,
    postgres_password: PostgresPasswordOpt = None,
    postgres_schema: PostgresSchemaOpt = None,
    pg_pool_min_size: PgPoolMinSizeOpt = None,
    pg_pool_max_size: PgPoolMaxSizeOpt = None,
    pg_pool_timeout_seconds: PgPoolTimeoutSecondsOpt = None,
    mastodon_public_visibility: MastodonPublicVisibilityOpt = None,
    mastodon_account_lookup_limit: MastodonAccountLookupLimitOpt = None,
    # Neo4j
    neo4j_host: Neo4jHostOpt = None,
    neo4j_port: Neo4jPortOpt = None,
    neo4j_username: Neo4jUsernameOpt = None,
    neo4j_password: Neo4jPasswordOpt = None,
    neo4j_ready_retries: Neo4jReadyRetriesOpt = None,
    neo4j_ready_sleep_seconds: Neo4jReadySleepSecondsOpt = None,
    neo4j_worker_label: Neo4jWorkerLabelOpt = None,
    # Redis
    redis_host: RedisHostOpt = None,
    redis_port: RedisPortOpt = None,
    redis_db: RedisDbOpt = None,
    redis_password: RedisPasswordOpt = None,
    redis_score_tolerance: RedisScoreToleranceOpt = None,
    redis_namespace: RedisNamespaceOpt = None,
    # LLM
    llm_provider: LlmProviderOpt = None,
    llm_base_url: LlmBaseUrlOpt = None,
    llm_model: LlmModelOpt = None,
    llm_dimensions: LlmDimensionsOpt = None,
    llm_timeout: LlmTimeoutOpt = None,
    llm_max_retries: LlmMaxRetriesOpt = None,
    llm_batch_size: LlmBatchSizeOpt = None,
    llm_api_key: LlmApiKeyOpt = None,
    min_embedding_tokens: MinEmbeddingTokensOpt = None,
    embedding_skip_percentile: EmbeddingSkipPercentileOpt = None,
    # FastText
    fasttext_vector_size: FasttextVectorSizeOpt = None,
    fasttext_window: FasttextWindowOpt = None,
    fasttext_min_count: FasttextMinCountOpt = None,
    fasttext_max_vocab_size: FasttextMaxVocabSizeOpt = None,
    fasttext_epochs: FasttextEpochsOpt = None,
    fasttext_bucket: FasttextBucketOpt = None,
    fasttext_min_documents: FasttextMinDocumentsOpt = None,
    fasttext_model_path: FasttextModelPathOpt = None,
    fasttext_quantize: FasttextQuantizeOpt = None,
    fasttext_quantize_qdim: FasttextQuantizeQdimOpt = None,
    fasttext_training_workers: FasttextTrainingWorkersOpt = None,
    # Batch
    batch_size: BatchSizeOpt = None,
    load_since: LoadSinceOpt = None,
    max_retries: MaxRetriesOpt = None,
    checkpoint_interval: CheckpointIntervalOpt = None,
    apoc_batch_size: ApocBatchSizeOpt = None,
    feed_workers: FeedWorkersOpt = None,
    loader_workers: LoaderWorkersOpt = None,
    # Clustering
    user_communities: UserCommunitiesOpt = None,
    post_communities: PostCommunitiesOpt = None,
    leiden_resolution: LeidenResolutionOpt = None,
    leiden_max_levels: LeidenMaxLevelsOpt = None,
    singleton_collapse_enabled: SingletonCollapseEnabledOpt = None,
    noise_community_id: NoiseCommunityIdOpt = None,
    singleton_collapse_in_transactions_of: SingletonCollapseInTransactionsOfOpt = None,
    knn_neighbors: KnnNeighborsOpt = None,
    knn_self_neighbor_offset: KnnSelfNeighborOffsetOpt = None,
    similarity_threshold: SimilarityThresholdOpt = None,
    similarity_recency_days: SimilarityRecencyDaysOpt = None,
    similarity_pruning: SimilarityPruningOpt = None,
    prune_after_clustering: PruneAfterClusteringOpt = None,
    prune_similarity_threshold: PruneSimilarityThresholdOpt = None,
    prune_days: PruneDaysOpt = None,
    # Feed
    feed_size: FeedSizeOpt = None,
    feed_days: FeedDaysOpt = None,
    feed_ttl: FeedTtlOpt = None,
    feed_score_multiplier: FeedScoreMultiplierOpt = None,
    feed_score_decimals: FeedScoreDecimalsOpt = None,
    cold_start_fallback: ColdStartFallbackOpt = None,
    cold_start_limit: ColdStartLimitOpt = None,
    serendipity_probability: SerendipityProbabilityOpt = None,
    serendipity_limit: SerendipityLimitOpt = None,
    serendipity_score: SerendipityScoreOpt = None,
    serendipity_based_on: SerendipityBasedOnOpt = None,
    decay_half_life_days: DecayHalfLifeDaysOpt = None,
    interests_ttl_days: InterestsTtlDaysOpt = None,
    interests_min_favourites: InterestsMinFavouritesOpt = None,
    active_user_days: ActiveUserDaysOpt = None,
    feed_force_refresh: FeedForceRefreshOpt = None,
    language_match_weight: LanguageMatchWeightOpt = None,
    ui_language_match_weight: UiLanguageMatchWeightOpt = None,
    # Scoring
    likes_weight: LikesWeightOpt = None,
    reblogs_weight: ReblogsWeightOpt = None,
    replies_weight: RepliesWeightOpt = None,
    follows_weight: FollowsWeightOpt = None,
    mentions_weight: MentionsWeightOpt = None,
    bookmark_weight: BookmarkWeightOpt = None,
    personalized_interest_weight: PersonalizedInterestWeightOpt = None,
    personalized_popularity_weight: PersonalizedPopularityWeightOpt = None,
    personalized_recency_weight: PersonalizedRecencyWeightOpt = None,
    cold_start_popularity_weight: ColdStartPopularityWeightOpt = None,
    cold_start_recency_weight: ColdStartRecencyWeightOpt = None,
    popularity_smoothing: PopularitySmoothingOpt = None,
    recency_smoothing: RecencySmoothingOpt = None,
    recency_numerator: RecencyNumeratorOpt = None,
    ctr_enabled: CtrEnabledOpt = None,
    ctr_weight: CtrWeightOpt = None,
    min_ctr: MinCtrOpt = None,
    ctr_smoothing: CtrSmoothingOpt = None,
    pagerank_enabled: PagerankEnabledOpt = None,
    pagerank_weight: PagerankWeightOpt = None,
    pagerank_damping_factor: PagerankDampingFactorOpt = None,
    pagerank_max_iterations: PagerankMaxIterationsOpt = None,
    community_similarity_enabled: CommunitySimilarityEnabledOpt = None,
    community_similarity_top_k: CommunitySimilarityTopKOpt = None,
    # Public timelines
    public_feed_size: PublicFeedSizeOpt = None,
    public_feed_enabled: PublicFeedEnabledOpt = None,
    public_feed_strategy: PublicFeedStrategyOpt = None,
    public_timeline_key: PublicTimelineKeyOpt = None,
    local_timeline_key: LocalTimelineKeyOpt = None,
    # Export
    export_max_items: ExportMaxItemsOpt = None,
    text_preview_limit: TextPreviewLimitOpt = None,
    community_interest_limit: CommunityInterestLimitOpt = None,
    community_member_sample: CommunityMemberSampleOpt = None,
    community_sample_limit: CommunitySampleLimitOpt = None,
    graph_sample_limit: GraphSampleLimitOpt = None,
    # Logging
    log_level: LogLevelOpt = None,
    log_file: LogFileOpt = None,
    progress_poll_interval_seconds: ProgressPollIntervalSecondsOpt = None,
    verbose: VerboseOpt = False,
) -> None:
    """Delete data from Neo4j, Redis, and/or model files.

    When no target flags (--graph, --redis, --models) are specified,
    all data is cleaned. When one or more flags are specified, only
    the selected targets are cleaned.
    """
    from hintgrid.cli.runner import execute_clean

    overrides = _collect_overrides(
        postgres_host=postgres_host,
        postgres_port=postgres_port,
        postgres_database=postgres_database,
        postgres_user=postgres_user,
        postgres_password=postgres_password,
        postgres_schema=postgres_schema,
        pg_pool_min_size=pg_pool_min_size,
        pg_pool_max_size=pg_pool_max_size,
        pg_pool_timeout_seconds=pg_pool_timeout_seconds,
        mastodon_public_visibility=mastodon_public_visibility,
        mastodon_account_lookup_limit=mastodon_account_lookup_limit,
        neo4j_host=neo4j_host,
        neo4j_port=neo4j_port,
        neo4j_username=neo4j_username,
        neo4j_password=neo4j_password,
        neo4j_ready_retries=neo4j_ready_retries,
        neo4j_ready_sleep_seconds=neo4j_ready_sleep_seconds,
        neo4j_worker_label=neo4j_worker_label,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
        redis_password=redis_password,
        redis_score_tolerance=redis_score_tolerance,
        redis_namespace=redis_namespace,
        llm_provider=llm_provider,
        llm_base_url=llm_base_url,
        llm_model=llm_model,
        llm_dimensions=llm_dimensions,
        llm_timeout=llm_timeout,
        llm_max_retries=llm_max_retries,
        llm_batch_size=llm_batch_size,
        llm_api_key=llm_api_key,
        min_embedding_tokens=min_embedding_tokens,
        embedding_skip_percentile=embedding_skip_percentile,
        fasttext_vector_size=fasttext_vector_size,
        fasttext_window=fasttext_window,
        fasttext_min_count=fasttext_min_count,
        fasttext_max_vocab_size=fasttext_max_vocab_size,
        fasttext_epochs=fasttext_epochs,
        fasttext_bucket=fasttext_bucket,
        fasttext_min_documents=fasttext_min_documents,
        fasttext_model_path=fasttext_model_path,
        fasttext_quantize=fasttext_quantize,
        fasttext_quantize_qdim=fasttext_quantize_qdim,
        fasttext_training_workers=fasttext_training_workers,
        batch_size=batch_size,
        load_since=load_since,
        max_retries=max_retries,
        checkpoint_interval=checkpoint_interval,
        apoc_batch_size=apoc_batch_size,
        feed_workers=feed_workers,
        loader_workers=loader_workers,
        user_communities=user_communities,
        post_communities=post_communities,
        leiden_resolution=leiden_resolution,
        leiden_max_levels=leiden_max_levels,
        singleton_collapse_enabled=singleton_collapse_enabled,
        noise_community_id=noise_community_id,
        singleton_collapse_in_transactions_of=singleton_collapse_in_transactions_of,
        knn_neighbors=knn_neighbors,
        knn_self_neighbor_offset=knn_self_neighbor_offset,
        similarity_threshold=similarity_threshold,
        similarity_recency_days=similarity_recency_days,
        similarity_pruning=similarity_pruning,
        prune_after_clustering=prune_after_clustering,
        prune_similarity_threshold=prune_similarity_threshold,
        prune_days=prune_days,
        feed_size=feed_size,
        feed_days=feed_days,
        feed_ttl=feed_ttl,
        feed_score_multiplier=feed_score_multiplier,
        feed_score_decimals=feed_score_decimals,
        cold_start_fallback=cold_start_fallback,
        cold_start_limit=cold_start_limit,
        serendipity_probability=serendipity_probability,
        serendipity_limit=serendipity_limit,
        serendipity_score=serendipity_score,
        serendipity_based_on=serendipity_based_on,
        decay_half_life_days=decay_half_life_days,
        interests_ttl_days=interests_ttl_days,
        interests_min_favourites=interests_min_favourites,
        active_user_days=active_user_days,
        feed_force_refresh=feed_force_refresh,
        language_match_weight=language_match_weight,
        ui_language_match_weight=ui_language_match_weight,
        likes_weight=likes_weight,
        reblogs_weight=reblogs_weight,
        replies_weight=replies_weight,
        follows_weight=follows_weight,
        mentions_weight=mentions_weight,
        bookmark_weight=bookmark_weight,
        personalized_interest_weight=personalized_interest_weight,
        personalized_popularity_weight=personalized_popularity_weight,
        personalized_recency_weight=personalized_recency_weight,
        cold_start_popularity_weight=cold_start_popularity_weight,
        cold_start_recency_weight=cold_start_recency_weight,
        popularity_smoothing=popularity_smoothing,
        recency_smoothing=recency_smoothing,
        recency_numerator=recency_numerator,
        ctr_enabled=ctr_enabled,
        ctr_weight=ctr_weight,
        min_ctr=min_ctr,
        ctr_smoothing=ctr_smoothing,
        pagerank_enabled=pagerank_enabled,
        pagerank_weight=pagerank_weight,
        pagerank_damping_factor=pagerank_damping_factor,
        pagerank_max_iterations=pagerank_max_iterations,
        community_similarity_enabled=community_similarity_enabled,
        community_similarity_top_k=community_similarity_top_k,
        public_feed_size=public_feed_size,
        public_feed_enabled=public_feed_enabled,
        public_feed_strategy=public_feed_strategy,
        public_timeline_key=public_timeline_key,
        local_timeline_key=local_timeline_key,
        export_max_items=export_max_items,
        text_preview_limit=text_preview_limit,
        community_interest_limit=community_interest_limit,
        community_member_sample=community_member_sample,
        community_sample_limit=community_sample_limit,
        graph_sample_limit=graph_sample_limit,
        log_level=log_level,
        log_file=log_file,
        progress_poll_interval_seconds=progress_poll_interval_seconds,
    )

    exit_code = execute_clean(
        overrides=overrides,
        verbose=verbose,
        memory_interval=memory_interval,
        graph=graph,
        redis=redis_flag,
        models=models,
        embeddings=embeddings,
        clusters=clusters,
        similarity=similarity,
        interests=interests,
        interactions=interactions,
        recommendations=recommendations,
        fasttext_state=fasttext_state,
    )
    raise typer.Exit(code=exit_code)


@app.command("get-user-info")
def get_user_info_cmd(
    handle: Annotated[str, typer.Argument(help="Mastodon handle (@user or @user@domain)")],
    memory_interval: MemoryIntervalOpt = 10,
    # PostgreSQL
    postgres_host: PostgresHostOpt = None,
    postgres_port: PostgresPortOpt = None,
    postgres_database: PostgresDatabaseOpt = None,
    postgres_user: PostgresUserOpt = None,
    postgres_password: PostgresPasswordOpt = None,
    postgres_schema: PostgresSchemaOpt = None,
    pg_pool_min_size: PgPoolMinSizeOpt = None,
    pg_pool_max_size: PgPoolMaxSizeOpt = None,
    pg_pool_timeout_seconds: PgPoolTimeoutSecondsOpt = None,
    mastodon_public_visibility: MastodonPublicVisibilityOpt = None,
    mastodon_account_lookup_limit: MastodonAccountLookupLimitOpt = None,
    # Neo4j
    neo4j_host: Neo4jHostOpt = None,
    neo4j_port: Neo4jPortOpt = None,
    neo4j_username: Neo4jUsernameOpt = None,
    neo4j_password: Neo4jPasswordOpt = None,
    neo4j_ready_retries: Neo4jReadyRetriesOpt = None,
    neo4j_ready_sleep_seconds: Neo4jReadySleepSecondsOpt = None,
    neo4j_worker_label: Neo4jWorkerLabelOpt = None,
    # Redis
    redis_host: RedisHostOpt = None,
    redis_port: RedisPortOpt = None,
    redis_db: RedisDbOpt = None,
    redis_password: RedisPasswordOpt = None,
    redis_score_tolerance: RedisScoreToleranceOpt = None,
    redis_namespace: RedisNamespaceOpt = None,
    # LLM
    llm_provider: LlmProviderOpt = None,
    llm_base_url: LlmBaseUrlOpt = None,
    llm_model: LlmModelOpt = None,
    llm_dimensions: LlmDimensionsOpt = None,
    llm_timeout: LlmTimeoutOpt = None,
    llm_max_retries: LlmMaxRetriesOpt = None,
    llm_batch_size: LlmBatchSizeOpt = None,
    llm_api_key: LlmApiKeyOpt = None,
    min_embedding_tokens: MinEmbeddingTokensOpt = None,
    embedding_skip_percentile: EmbeddingSkipPercentileOpt = None,
    # FastText
    fasttext_vector_size: FasttextVectorSizeOpt = None,
    fasttext_window: FasttextWindowOpt = None,
    fasttext_min_count: FasttextMinCountOpt = None,
    fasttext_max_vocab_size: FasttextMaxVocabSizeOpt = None,
    fasttext_epochs: FasttextEpochsOpt = None,
    fasttext_bucket: FasttextBucketOpt = None,
    fasttext_min_documents: FasttextMinDocumentsOpt = None,
    fasttext_model_path: FasttextModelPathOpt = None,
    fasttext_quantize: FasttextQuantizeOpt = None,
    fasttext_quantize_qdim: FasttextQuantizeQdimOpt = None,
    fasttext_training_workers: FasttextTrainingWorkersOpt = None,
    # Batch
    batch_size: BatchSizeOpt = None,
    load_since: LoadSinceOpt = None,
    max_retries: MaxRetriesOpt = None,
    checkpoint_interval: CheckpointIntervalOpt = None,
    apoc_batch_size: ApocBatchSizeOpt = None,
    feed_workers: FeedWorkersOpt = None,
    loader_workers: LoaderWorkersOpt = None,
    # Clustering
    user_communities: UserCommunitiesOpt = None,
    post_communities: PostCommunitiesOpt = None,
    leiden_resolution: LeidenResolutionOpt = None,
    leiden_max_levels: LeidenMaxLevelsOpt = None,
    singleton_collapse_enabled: SingletonCollapseEnabledOpt = None,
    noise_community_id: NoiseCommunityIdOpt = None,
    singleton_collapse_in_transactions_of: SingletonCollapseInTransactionsOfOpt = None,
    knn_neighbors: KnnNeighborsOpt = None,
    knn_self_neighbor_offset: KnnSelfNeighborOffsetOpt = None,
    similarity_threshold: SimilarityThresholdOpt = None,
    similarity_recency_days: SimilarityRecencyDaysOpt = None,
    similarity_pruning: SimilarityPruningOpt = None,
    prune_after_clustering: PruneAfterClusteringOpt = None,
    prune_similarity_threshold: PruneSimilarityThresholdOpt = None,
    prune_days: PruneDaysOpt = None,
    # Feed
    feed_size: FeedSizeOpt = None,
    feed_days: FeedDaysOpt = None,
    feed_ttl: FeedTtlOpt = None,
    feed_score_multiplier: FeedScoreMultiplierOpt = None,
    feed_score_decimals: FeedScoreDecimalsOpt = None,
    cold_start_fallback: ColdStartFallbackOpt = None,
    cold_start_limit: ColdStartLimitOpt = None,
    serendipity_probability: SerendipityProbabilityOpt = None,
    serendipity_limit: SerendipityLimitOpt = None,
    serendipity_score: SerendipityScoreOpt = None,
    serendipity_based_on: SerendipityBasedOnOpt = None,
    decay_half_life_days: DecayHalfLifeDaysOpt = None,
    interests_ttl_days: InterestsTtlDaysOpt = None,
    interests_min_favourites: InterestsMinFavouritesOpt = None,
    active_user_days: ActiveUserDaysOpt = None,
    feed_force_refresh: FeedForceRefreshOpt = None,
    language_match_weight: LanguageMatchWeightOpt = None,
    ui_language_match_weight: UiLanguageMatchWeightOpt = None,
    # Scoring
    likes_weight: LikesWeightOpt = None,
    reblogs_weight: ReblogsWeightOpt = None,
    replies_weight: RepliesWeightOpt = None,
    follows_weight: FollowsWeightOpt = None,
    mentions_weight: MentionsWeightOpt = None,
    bookmark_weight: BookmarkWeightOpt = None,
    personalized_interest_weight: PersonalizedInterestWeightOpt = None,
    personalized_popularity_weight: PersonalizedPopularityWeightOpt = None,
    personalized_recency_weight: PersonalizedRecencyWeightOpt = None,
    cold_start_popularity_weight: ColdStartPopularityWeightOpt = None,
    cold_start_recency_weight: ColdStartRecencyWeightOpt = None,
    popularity_smoothing: PopularitySmoothingOpt = None,
    recency_smoothing: RecencySmoothingOpt = None,
    recency_numerator: RecencyNumeratorOpt = None,
    ctr_enabled: CtrEnabledOpt = None,
    ctr_weight: CtrWeightOpt = None,
    min_ctr: MinCtrOpt = None,
    ctr_smoothing: CtrSmoothingOpt = None,
    pagerank_enabled: PagerankEnabledOpt = None,
    pagerank_weight: PagerankWeightOpt = None,
    pagerank_damping_factor: PagerankDampingFactorOpt = None,
    pagerank_max_iterations: PagerankMaxIterationsOpt = None,
    community_similarity_enabled: CommunitySimilarityEnabledOpt = None,
    community_similarity_top_k: CommunitySimilarityTopKOpt = None,
    # Public timelines
    public_feed_size: PublicFeedSizeOpt = None,
    public_feed_enabled: PublicFeedEnabledOpt = None,
    public_feed_strategy: PublicFeedStrategyOpt = None,
    public_timeline_key: PublicTimelineKeyOpt = None,
    local_timeline_key: LocalTimelineKeyOpt = None,
    # Export
    export_max_items: ExportMaxItemsOpt = None,
    text_preview_limit: TextPreviewLimitOpt = None,
    community_interest_limit: CommunityInterestLimitOpt = None,
    community_member_sample: CommunityMemberSampleOpt = None,
    community_sample_limit: CommunitySampleLimitOpt = None,
    graph_sample_limit: GraphSampleLimitOpt = None,
    # Logging
    log_level: LogLevelOpt = None,
    log_file: LogFileOpt = None,
    progress_poll_interval_seconds: ProgressPollIntervalSecondsOpt = None,
    verbose: VerboseOpt = False,
) -> None:
    """Get detailed information about a Mastodon user."""
    from hintgrid.cli.runner import execute_get_user_info

    overrides = _collect_overrides(
        postgres_host=postgres_host,
        postgres_port=postgres_port,
        postgres_database=postgres_database,
        postgres_user=postgres_user,
        postgres_password=postgres_password,
        postgres_schema=postgres_schema,
        pg_pool_min_size=pg_pool_min_size,
        pg_pool_max_size=pg_pool_max_size,
        pg_pool_timeout_seconds=pg_pool_timeout_seconds,
        mastodon_public_visibility=mastodon_public_visibility,
        mastodon_account_lookup_limit=mastodon_account_lookup_limit,
        neo4j_host=neo4j_host,
        neo4j_port=neo4j_port,
        neo4j_username=neo4j_username,
        neo4j_password=neo4j_password,
        neo4j_ready_retries=neo4j_ready_retries,
        neo4j_ready_sleep_seconds=neo4j_ready_sleep_seconds,
        neo4j_worker_label=neo4j_worker_label,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
        redis_password=redis_password,
        redis_score_tolerance=redis_score_tolerance,
        redis_namespace=redis_namespace,
        llm_provider=llm_provider,
        llm_base_url=llm_base_url,
        llm_model=llm_model,
        llm_dimensions=llm_dimensions,
        llm_timeout=llm_timeout,
        llm_max_retries=llm_max_retries,
        llm_batch_size=llm_batch_size,
        llm_api_key=llm_api_key,
        min_embedding_tokens=min_embedding_tokens,
        embedding_skip_percentile=embedding_skip_percentile,
        fasttext_vector_size=fasttext_vector_size,
        fasttext_window=fasttext_window,
        fasttext_min_count=fasttext_min_count,
        fasttext_max_vocab_size=fasttext_max_vocab_size,
        fasttext_epochs=fasttext_epochs,
        fasttext_bucket=fasttext_bucket,
        fasttext_min_documents=fasttext_min_documents,
        fasttext_model_path=fasttext_model_path,
        fasttext_quantize=fasttext_quantize,
        fasttext_quantize_qdim=fasttext_quantize_qdim,
        fasttext_training_workers=fasttext_training_workers,
        batch_size=batch_size,
        load_since=load_since,
        max_retries=max_retries,
        checkpoint_interval=checkpoint_interval,
        apoc_batch_size=apoc_batch_size,
        feed_workers=feed_workers,
        loader_workers=loader_workers,
        user_communities=user_communities,
        post_communities=post_communities,
        leiden_resolution=leiden_resolution,
        leiden_max_levels=leiden_max_levels,
        singleton_collapse_enabled=singleton_collapse_enabled,
        noise_community_id=noise_community_id,
        singleton_collapse_in_transactions_of=singleton_collapse_in_transactions_of,
        knn_neighbors=knn_neighbors,
        knn_self_neighbor_offset=knn_self_neighbor_offset,
        similarity_threshold=similarity_threshold,
        similarity_recency_days=similarity_recency_days,
        similarity_pruning=similarity_pruning,
        prune_after_clustering=prune_after_clustering,
        prune_similarity_threshold=prune_similarity_threshold,
        prune_days=prune_days,
        feed_size=feed_size,
        feed_days=feed_days,
        feed_ttl=feed_ttl,
        feed_score_multiplier=feed_score_multiplier,
        feed_score_decimals=feed_score_decimals,
        cold_start_fallback=cold_start_fallback,
        cold_start_limit=cold_start_limit,
        serendipity_probability=serendipity_probability,
        serendipity_limit=serendipity_limit,
        serendipity_score=serendipity_score,
        serendipity_based_on=serendipity_based_on,
        decay_half_life_days=decay_half_life_days,
        interests_ttl_days=interests_ttl_days,
        interests_min_favourites=interests_min_favourites,
        active_user_days=active_user_days,
        feed_force_refresh=feed_force_refresh,
        language_match_weight=language_match_weight,
        ui_language_match_weight=ui_language_match_weight,
        likes_weight=likes_weight,
        reblogs_weight=reblogs_weight,
        replies_weight=replies_weight,
        follows_weight=follows_weight,
        mentions_weight=mentions_weight,
        bookmark_weight=bookmark_weight,
        personalized_interest_weight=personalized_interest_weight,
        personalized_popularity_weight=personalized_popularity_weight,
        personalized_recency_weight=personalized_recency_weight,
        cold_start_popularity_weight=cold_start_popularity_weight,
        cold_start_recency_weight=cold_start_recency_weight,
        popularity_smoothing=popularity_smoothing,
        recency_smoothing=recency_smoothing,
        recency_numerator=recency_numerator,
        ctr_enabled=ctr_enabled,
        ctr_weight=ctr_weight,
        min_ctr=min_ctr,
        ctr_smoothing=ctr_smoothing,
        pagerank_enabled=pagerank_enabled,
        pagerank_weight=pagerank_weight,
        pagerank_damping_factor=pagerank_damping_factor,
        pagerank_max_iterations=pagerank_max_iterations,
        community_similarity_enabled=community_similarity_enabled,
        community_similarity_top_k=community_similarity_top_k,
        public_feed_size=public_feed_size,
        public_feed_enabled=public_feed_enabled,
        public_feed_strategy=public_feed_strategy,
        public_timeline_key=public_timeline_key,
        local_timeline_key=local_timeline_key,
        export_max_items=export_max_items,
        text_preview_limit=text_preview_limit,
        community_interest_limit=community_interest_limit,
        community_member_sample=community_member_sample,
        community_sample_limit=community_sample_limit,
        graph_sample_limit=graph_sample_limit,
        log_level=log_level,
        log_file=log_file,
        progress_poll_interval_seconds=progress_poll_interval_seconds,
    )

    exit_code = execute_get_user_info(overrides=overrides, handle=handle, verbose=verbose)
    raise typer.Exit(code=exit_code)


@app.command("get-post-info")
def get_post_info_cmd(
    post_ref: Annotated[str, typer.Argument(help="Post URL, public id from URL, or internal status id")],
    memory_interval: MemoryIntervalOpt = 10,
    # PostgreSQL
    postgres_host: PostgresHostOpt = None,
    postgres_port: PostgresPortOpt = None,
    postgres_database: PostgresDatabaseOpt = None,
    postgres_user: PostgresUserOpt = None,
    postgres_password: PostgresPasswordOpt = None,
    postgres_schema: PostgresSchemaOpt = None,
    pg_pool_min_size: PgPoolMinSizeOpt = None,
    pg_pool_max_size: PgPoolMaxSizeOpt = None,
    pg_pool_timeout_seconds: PgPoolTimeoutSecondsOpt = None,
    mastodon_public_visibility: MastodonPublicVisibilityOpt = None,
    mastodon_account_lookup_limit: MastodonAccountLookupLimitOpt = None,
    # Neo4j
    neo4j_host: Neo4jHostOpt = None,
    neo4j_port: Neo4jPortOpt = None,
    neo4j_username: Neo4jUsernameOpt = None,
    neo4j_password: Neo4jPasswordOpt = None,
    neo4j_ready_retries: Neo4jReadyRetriesOpt = None,
    neo4j_ready_sleep_seconds: Neo4jReadySleepSecondsOpt = None,
    neo4j_worker_label: Neo4jWorkerLabelOpt = None,
    # Redis
    redis_host: RedisHostOpt = None,
    redis_port: RedisPortOpt = None,
    redis_db: RedisDbOpt = None,
    redis_password: RedisPasswordOpt = None,
    redis_score_tolerance: RedisScoreToleranceOpt = None,
    redis_namespace: RedisNamespaceOpt = None,
    # LLM
    llm_provider: LlmProviderOpt = None,
    llm_base_url: LlmBaseUrlOpt = None,
    llm_model: LlmModelOpt = None,
    llm_dimensions: LlmDimensionsOpt = None,
    llm_timeout: LlmTimeoutOpt = None,
    llm_max_retries: LlmMaxRetriesOpt = None,
    llm_batch_size: LlmBatchSizeOpt = None,
    llm_api_key: LlmApiKeyOpt = None,
    min_embedding_tokens: MinEmbeddingTokensOpt = None,
    embedding_skip_percentile: EmbeddingSkipPercentileOpt = None,
    # FastText
    fasttext_vector_size: FasttextVectorSizeOpt = None,
    fasttext_window: FasttextWindowOpt = None,
    fasttext_min_count: FasttextMinCountOpt = None,
    fasttext_max_vocab_size: FasttextMaxVocabSizeOpt = None,
    fasttext_epochs: FasttextEpochsOpt = None,
    fasttext_bucket: FasttextBucketOpt = None,
    fasttext_min_documents: FasttextMinDocumentsOpt = None,
    fasttext_model_path: FasttextModelPathOpt = None,
    fasttext_quantize: FasttextQuantizeOpt = None,
    fasttext_quantize_qdim: FasttextQuantizeQdimOpt = None,
    fasttext_training_workers: FasttextTrainingWorkersOpt = None,
    # Batch
    batch_size: BatchSizeOpt = None,
    load_since: LoadSinceOpt = None,
    max_retries: MaxRetriesOpt = None,
    checkpoint_interval: CheckpointIntervalOpt = None,
    apoc_batch_size: ApocBatchSizeOpt = None,
    feed_workers: FeedWorkersOpt = None,
    loader_workers: LoaderWorkersOpt = None,
    # Clustering
    user_communities: UserCommunitiesOpt = None,
    post_communities: PostCommunitiesOpt = None,
    leiden_resolution: LeidenResolutionOpt = None,
    leiden_max_levels: LeidenMaxLevelsOpt = None,
    singleton_collapse_enabled: SingletonCollapseEnabledOpt = None,
    noise_community_id: NoiseCommunityIdOpt = None,
    singleton_collapse_in_transactions_of: SingletonCollapseInTransactionsOfOpt = None,
    knn_neighbors: KnnNeighborsOpt = None,
    knn_self_neighbor_offset: KnnSelfNeighborOffsetOpt = None,
    similarity_threshold: SimilarityThresholdOpt = None,
    similarity_recency_days: SimilarityRecencyDaysOpt = None,
    similarity_pruning: SimilarityPruningOpt = None,
    prune_after_clustering: PruneAfterClusteringOpt = None,
    prune_similarity_threshold: PruneSimilarityThresholdOpt = None,
    prune_days: PruneDaysOpt = None,
    # Feed
    feed_size: FeedSizeOpt = None,
    feed_days: FeedDaysOpt = None,
    feed_ttl: FeedTtlOpt = None,
    feed_score_multiplier: FeedScoreMultiplierOpt = None,
    feed_score_decimals: FeedScoreDecimalsOpt = None,
    cold_start_fallback: ColdStartFallbackOpt = None,
    cold_start_limit: ColdStartLimitOpt = None,
    serendipity_probability: SerendipityProbabilityOpt = None,
    serendipity_limit: SerendipityLimitOpt = None,
    serendipity_score: SerendipityScoreOpt = None,
    serendipity_based_on: SerendipityBasedOnOpt = None,
    decay_half_life_days: DecayHalfLifeDaysOpt = None,
    interests_ttl_days: InterestsTtlDaysOpt = None,
    interests_min_favourites: InterestsMinFavouritesOpt = None,
    active_user_days: ActiveUserDaysOpt = None,
    feed_force_refresh: FeedForceRefreshOpt = None,
    language_match_weight: LanguageMatchWeightOpt = None,
    ui_language_match_weight: UiLanguageMatchWeightOpt = None,
    # Scoring
    likes_weight: LikesWeightOpt = None,
    reblogs_weight: ReblogsWeightOpt = None,
    replies_weight: RepliesWeightOpt = None,
    follows_weight: FollowsWeightOpt = None,
    mentions_weight: MentionsWeightOpt = None,
    bookmark_weight: BookmarkWeightOpt = None,
    personalized_interest_weight: PersonalizedInterestWeightOpt = None,
    personalized_popularity_weight: PersonalizedPopularityWeightOpt = None,
    personalized_recency_weight: PersonalizedRecencyWeightOpt = None,
    cold_start_popularity_weight: ColdStartPopularityWeightOpt = None,
    cold_start_recency_weight: ColdStartRecencyWeightOpt = None,
    popularity_smoothing: PopularitySmoothingOpt = None,
    recency_smoothing: RecencySmoothingOpt = None,
    recency_numerator: RecencyNumeratorOpt = None,
    ctr_enabled: CtrEnabledOpt = None,
    ctr_weight: CtrWeightOpt = None,
    min_ctr: MinCtrOpt = None,
    ctr_smoothing: CtrSmoothingOpt = None,
    pagerank_enabled: PagerankEnabledOpt = None,
    pagerank_weight: PagerankWeightOpt = None,
    pagerank_damping_factor: PagerankDampingFactorOpt = None,
    pagerank_max_iterations: PagerankMaxIterationsOpt = None,
    community_similarity_enabled: CommunitySimilarityEnabledOpt = None,
    community_similarity_top_k: CommunitySimilarityTopKOpt = None,
    # Public timelines
    public_feed_size: PublicFeedSizeOpt = None,
    public_feed_enabled: PublicFeedEnabledOpt = None,
    public_feed_strategy: PublicFeedStrategyOpt = None,
    public_timeline_key: PublicTimelineKeyOpt = None,
    local_timeline_key: LocalTimelineKeyOpt = None,
    # Export
    export_max_items: ExportMaxItemsOpt = None,
    text_preview_limit: TextPreviewLimitOpt = None,
    community_interest_limit: CommunityInterestLimitOpt = None,
    community_member_sample: CommunityMemberSampleOpt = None,
    community_sample_limit: CommunitySampleLimitOpt = None,
    graph_sample_limit: GraphSampleLimitOpt = None,
    # Logging
    log_level: LogLevelOpt = None,
    log_file: LogFileOpt = None,
    progress_poll_interval_seconds: ProgressPollIntervalSecondsOpt = None,
    verbose: VerboseOpt = False,
) -> None:
    """Get detailed information about a Mastodon post."""
    from hintgrid.cli.runner import execute_get_post_info

    overrides = _collect_overrides(
        postgres_host=postgres_host,
        postgres_port=postgres_port,
        postgres_database=postgres_database,
        postgres_user=postgres_user,
        postgres_password=postgres_password,
        postgres_schema=postgres_schema,
        pg_pool_min_size=pg_pool_min_size,
        pg_pool_max_size=pg_pool_max_size,
        pg_pool_timeout_seconds=pg_pool_timeout_seconds,
        mastodon_public_visibility=mastodon_public_visibility,
        mastodon_account_lookup_limit=mastodon_account_lookup_limit,
        neo4j_host=neo4j_host,
        neo4j_port=neo4j_port,
        neo4j_username=neo4j_username,
        neo4j_password=neo4j_password,
        neo4j_ready_retries=neo4j_ready_retries,
        neo4j_ready_sleep_seconds=neo4j_ready_sleep_seconds,
        neo4j_worker_label=neo4j_worker_label,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
        redis_password=redis_password,
        redis_score_tolerance=redis_score_tolerance,
        redis_namespace=redis_namespace,
        llm_provider=llm_provider,
        llm_base_url=llm_base_url,
        llm_model=llm_model,
        llm_dimensions=llm_dimensions,
        llm_timeout=llm_timeout,
        llm_max_retries=llm_max_retries,
        llm_batch_size=llm_batch_size,
        llm_api_key=llm_api_key,
        min_embedding_tokens=min_embedding_tokens,
        embedding_skip_percentile=embedding_skip_percentile,
        fasttext_vector_size=fasttext_vector_size,
        fasttext_window=fasttext_window,
        fasttext_min_count=fasttext_min_count,
        fasttext_max_vocab_size=fasttext_max_vocab_size,
        fasttext_epochs=fasttext_epochs,
        fasttext_bucket=fasttext_bucket,
        fasttext_min_documents=fasttext_min_documents,
        fasttext_model_path=fasttext_model_path,
        fasttext_quantize=fasttext_quantize,
        fasttext_quantize_qdim=fasttext_quantize_qdim,
        fasttext_training_workers=fasttext_training_workers,
        batch_size=batch_size,
        load_since=load_since,
        max_retries=max_retries,
        checkpoint_interval=checkpoint_interval,
        apoc_batch_size=apoc_batch_size,
        feed_workers=feed_workers,
        loader_workers=loader_workers,
        user_communities=user_communities,
        post_communities=post_communities,
        leiden_resolution=leiden_resolution,
        leiden_max_levels=leiden_max_levels,
        singleton_collapse_enabled=singleton_collapse_enabled,
        noise_community_id=noise_community_id,
        singleton_collapse_in_transactions_of=singleton_collapse_in_transactions_of,
        knn_neighbors=knn_neighbors,
        knn_self_neighbor_offset=knn_self_neighbor_offset,
        similarity_threshold=similarity_threshold,
        similarity_recency_days=similarity_recency_days,
        similarity_pruning=similarity_pruning,
        prune_after_clustering=prune_after_clustering,
        prune_similarity_threshold=prune_similarity_threshold,
        prune_days=prune_days,
        feed_size=feed_size,
        feed_days=feed_days,
        feed_ttl=feed_ttl,
        feed_score_multiplier=feed_score_multiplier,
        feed_score_decimals=feed_score_decimals,
        cold_start_fallback=cold_start_fallback,
        cold_start_limit=cold_start_limit,
        serendipity_probability=serendipity_probability,
        serendipity_limit=serendipity_limit,
        serendipity_score=serendipity_score,
        serendipity_based_on=serendipity_based_on,
        decay_half_life_days=decay_half_life_days,
        interests_ttl_days=interests_ttl_days,
        interests_min_favourites=interests_min_favourites,
        active_user_days=active_user_days,
        feed_force_refresh=feed_force_refresh,
        language_match_weight=language_match_weight,
        ui_language_match_weight=ui_language_match_weight,
        likes_weight=likes_weight,
        reblogs_weight=reblogs_weight,
        replies_weight=replies_weight,
        follows_weight=follows_weight,
        mentions_weight=mentions_weight,
        bookmark_weight=bookmark_weight,
        personalized_interest_weight=personalized_interest_weight,
        personalized_popularity_weight=personalized_popularity_weight,
        personalized_recency_weight=personalized_recency_weight,
        cold_start_popularity_weight=cold_start_popularity_weight,
        cold_start_recency_weight=cold_start_recency_weight,
        popularity_smoothing=popularity_smoothing,
        recency_smoothing=recency_smoothing,
        recency_numerator=recency_numerator,
        ctr_enabled=ctr_enabled,
        ctr_weight=ctr_weight,
        min_ctr=min_ctr,
        ctr_smoothing=ctr_smoothing,
        pagerank_enabled=pagerank_enabled,
        pagerank_weight=pagerank_weight,
        pagerank_damping_factor=pagerank_damping_factor,
        pagerank_max_iterations=pagerank_max_iterations,
        community_similarity_enabled=community_similarity_enabled,
        community_similarity_top_k=community_similarity_top_k,
        public_feed_size=public_feed_size,
        public_feed_enabled=public_feed_enabled,
        public_feed_strategy=public_feed_strategy,
        public_timeline_key=public_timeline_key,
        local_timeline_key=local_timeline_key,
        export_max_items=export_max_items,
        text_preview_limit=text_preview_limit,
        community_interest_limit=community_interest_limit,
        community_member_sample=community_member_sample,
        community_sample_limit=community_sample_limit,
        graph_sample_limit=graph_sample_limit,
        log_level=log_level,
        log_file=log_file,
        progress_poll_interval_seconds=progress_poll_interval_seconds,
    )

    exit_code = execute_get_post_info(overrides=overrides, post_ref=post_ref, verbose=verbose)
    raise typer.Exit(code=exit_code)


@app.command()
def validate(
    memory_interval: MemoryIntervalOpt = 10,
    # PostgreSQL
    postgres_host: PostgresHostOpt = None,
    postgres_port: PostgresPortOpt = None,
    postgres_database: PostgresDatabaseOpt = None,
    postgres_user: PostgresUserOpt = None,
    postgres_password: PostgresPasswordOpt = None,
    postgres_schema: PostgresSchemaOpt = None,
    pg_pool_min_size: PgPoolMinSizeOpt = None,
    pg_pool_max_size: PgPoolMaxSizeOpt = None,
    pg_pool_timeout_seconds: PgPoolTimeoutSecondsOpt = None,
    mastodon_public_visibility: MastodonPublicVisibilityOpt = None,
    mastodon_account_lookup_limit: MastodonAccountLookupLimitOpt = None,
    # Neo4j
    neo4j_host: Neo4jHostOpt = None,
    neo4j_port: Neo4jPortOpt = None,
    neo4j_username: Neo4jUsernameOpt = None,
    neo4j_password: Neo4jPasswordOpt = None,
    neo4j_ready_retries: Neo4jReadyRetriesOpt = None,
    neo4j_ready_sleep_seconds: Neo4jReadySleepSecondsOpt = None,
    neo4j_worker_label: Neo4jWorkerLabelOpt = None,
    # Redis
    redis_host: RedisHostOpt = None,
    redis_port: RedisPortOpt = None,
    redis_db: RedisDbOpt = None,
    redis_password: RedisPasswordOpt = None,
    redis_score_tolerance: RedisScoreToleranceOpt = None,
    redis_namespace: RedisNamespaceOpt = None,
    # LLM
    llm_provider: LlmProviderOpt = None,
    llm_base_url: LlmBaseUrlOpt = None,
    llm_model: LlmModelOpt = None,
    llm_dimensions: LlmDimensionsOpt = None,
    llm_timeout: LlmTimeoutOpt = None,
    llm_max_retries: LlmMaxRetriesOpt = None,
    llm_batch_size: LlmBatchSizeOpt = None,
    llm_api_key: LlmApiKeyOpt = None,
    min_embedding_tokens: MinEmbeddingTokensOpt = None,
    embedding_skip_percentile: EmbeddingSkipPercentileOpt = None,
    # FastText
    fasttext_vector_size: FasttextVectorSizeOpt = None,
    fasttext_window: FasttextWindowOpt = None,
    fasttext_min_count: FasttextMinCountOpt = None,
    fasttext_max_vocab_size: FasttextMaxVocabSizeOpt = None,
    fasttext_epochs: FasttextEpochsOpt = None,
    fasttext_bucket: FasttextBucketOpt = None,
    fasttext_min_documents: FasttextMinDocumentsOpt = None,
    fasttext_model_path: FasttextModelPathOpt = None,
    fasttext_quantize: FasttextQuantizeOpt = None,
    fasttext_quantize_qdim: FasttextQuantizeQdimOpt = None,
    fasttext_training_workers: FasttextTrainingWorkersOpt = None,
    # Batch
    batch_size: BatchSizeOpt = None,
    load_since: LoadSinceOpt = None,
    max_retries: MaxRetriesOpt = None,
    checkpoint_interval: CheckpointIntervalOpt = None,
    apoc_batch_size: ApocBatchSizeOpt = None,
    feed_workers: FeedWorkersOpt = None,
    loader_workers: LoaderWorkersOpt = None,
    # Clustering
    user_communities: UserCommunitiesOpt = None,
    post_communities: PostCommunitiesOpt = None,
    leiden_resolution: LeidenResolutionOpt = None,
    leiden_max_levels: LeidenMaxLevelsOpt = None,
    singleton_collapse_enabled: SingletonCollapseEnabledOpt = None,
    noise_community_id: NoiseCommunityIdOpt = None,
    singleton_collapse_in_transactions_of: SingletonCollapseInTransactionsOfOpt = None,
    knn_neighbors: KnnNeighborsOpt = None,
    knn_self_neighbor_offset: KnnSelfNeighborOffsetOpt = None,
    similarity_threshold: SimilarityThresholdOpt = None,
    similarity_recency_days: SimilarityRecencyDaysOpt = None,
    similarity_pruning: SimilarityPruningOpt = None,
    prune_after_clustering: PruneAfterClusteringOpt = None,
    prune_similarity_threshold: PruneSimilarityThresholdOpt = None,
    prune_days: PruneDaysOpt = None,
    # Feed
    feed_size: FeedSizeOpt = None,
    feed_days: FeedDaysOpt = None,
    feed_ttl: FeedTtlOpt = None,
    feed_score_multiplier: FeedScoreMultiplierOpt = None,
    feed_score_decimals: FeedScoreDecimalsOpt = None,
    cold_start_fallback: ColdStartFallbackOpt = None,
    cold_start_limit: ColdStartLimitOpt = None,
    serendipity_probability: SerendipityProbabilityOpt = None,
    serendipity_limit: SerendipityLimitOpt = None,
    serendipity_score: SerendipityScoreOpt = None,
    serendipity_based_on: SerendipityBasedOnOpt = None,
    decay_half_life_days: DecayHalfLifeDaysOpt = None,
    interests_ttl_days: InterestsTtlDaysOpt = None,
    interests_min_favourites: InterestsMinFavouritesOpt = None,
    active_user_days: ActiveUserDaysOpt = None,
    feed_force_refresh: FeedForceRefreshOpt = None,
    language_match_weight: LanguageMatchWeightOpt = None,
    ui_language_match_weight: UiLanguageMatchWeightOpt = None,
    # Scoring
    likes_weight: LikesWeightOpt = None,
    reblogs_weight: ReblogsWeightOpt = None,
    replies_weight: RepliesWeightOpt = None,
    follows_weight: FollowsWeightOpt = None,
    mentions_weight: MentionsWeightOpt = None,
    bookmark_weight: BookmarkWeightOpt = None,
    personalized_interest_weight: PersonalizedInterestWeightOpt = None,
    personalized_popularity_weight: PersonalizedPopularityWeightOpt = None,
    personalized_recency_weight: PersonalizedRecencyWeightOpt = None,
    cold_start_popularity_weight: ColdStartPopularityWeightOpt = None,
    cold_start_recency_weight: ColdStartRecencyWeightOpt = None,
    popularity_smoothing: PopularitySmoothingOpt = None,
    recency_smoothing: RecencySmoothingOpt = None,
    recency_numerator: RecencyNumeratorOpt = None,
    ctr_enabled: CtrEnabledOpt = None,
    ctr_weight: CtrWeightOpt = None,
    min_ctr: MinCtrOpt = None,
    ctr_smoothing: CtrSmoothingOpt = None,
    pagerank_enabled: PagerankEnabledOpt = None,
    pagerank_weight: PagerankWeightOpt = None,
    pagerank_damping_factor: PagerankDampingFactorOpt = None,
    pagerank_max_iterations: PagerankMaxIterationsOpt = None,
    community_similarity_enabled: CommunitySimilarityEnabledOpt = None,
    community_similarity_top_k: CommunitySimilarityTopKOpt = None,
    # Public timelines
    public_feed_size: PublicFeedSizeOpt = None,
    public_feed_enabled: PublicFeedEnabledOpt = None,
    public_feed_strategy: PublicFeedStrategyOpt = None,
    public_timeline_key: PublicTimelineKeyOpt = None,
    local_timeline_key: LocalTimelineKeyOpt = None,
    # Export
    export_max_items: ExportMaxItemsOpt = None,
    text_preview_limit: TextPreviewLimitOpt = None,
    community_interest_limit: CommunityInterestLimitOpt = None,
    community_member_sample: CommunityMemberSampleOpt = None,
    community_sample_limit: CommunitySampleLimitOpt = None,
    graph_sample_limit: GraphSampleLimitOpt = None,
    # Logging
    log_level: LogLevelOpt = None,
    log_file: LogFileOpt = None,
    progress_poll_interval_seconds: ProgressPollIntervalSecondsOpt = None,
    verbose: VerboseOpt = False,
) -> None:
    """Validate configuration and show settings summary."""
    from hintgrid.cli.runner import execute_validate

    overrides = _collect_overrides(
        postgres_host=postgres_host,
        postgres_port=postgres_port,
        postgres_database=postgres_database,
        postgres_user=postgres_user,
        postgres_password=postgres_password,
        postgres_schema=postgres_schema,
        pg_pool_min_size=pg_pool_min_size,
        pg_pool_max_size=pg_pool_max_size,
        pg_pool_timeout_seconds=pg_pool_timeout_seconds,
        mastodon_public_visibility=mastodon_public_visibility,
        mastodon_account_lookup_limit=mastodon_account_lookup_limit,
        neo4j_host=neo4j_host,
        neo4j_port=neo4j_port,
        neo4j_username=neo4j_username,
        neo4j_password=neo4j_password,
        neo4j_ready_retries=neo4j_ready_retries,
        neo4j_ready_sleep_seconds=neo4j_ready_sleep_seconds,
        neo4j_worker_label=neo4j_worker_label,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
        redis_password=redis_password,
        redis_score_tolerance=redis_score_tolerance,
        redis_namespace=redis_namespace,
        llm_provider=llm_provider,
        llm_base_url=llm_base_url,
        llm_model=llm_model,
        llm_dimensions=llm_dimensions,
        llm_timeout=llm_timeout,
        llm_max_retries=llm_max_retries,
        llm_batch_size=llm_batch_size,
        llm_api_key=llm_api_key,
        min_embedding_tokens=min_embedding_tokens,
        embedding_skip_percentile=embedding_skip_percentile,
        fasttext_vector_size=fasttext_vector_size,
        fasttext_window=fasttext_window,
        fasttext_min_count=fasttext_min_count,
        fasttext_max_vocab_size=fasttext_max_vocab_size,
        fasttext_epochs=fasttext_epochs,
        fasttext_bucket=fasttext_bucket,
        fasttext_min_documents=fasttext_min_documents,
        fasttext_model_path=fasttext_model_path,
        fasttext_quantize=fasttext_quantize,
        fasttext_quantize_qdim=fasttext_quantize_qdim,
        fasttext_training_workers=fasttext_training_workers,
        batch_size=batch_size,
        load_since=load_since,
        max_retries=max_retries,
        checkpoint_interval=checkpoint_interval,
        apoc_batch_size=apoc_batch_size,
        feed_workers=feed_workers,
        loader_workers=loader_workers,
        user_communities=user_communities,
        post_communities=post_communities,
        leiden_resolution=leiden_resolution,
        leiden_max_levels=leiden_max_levels,
        singleton_collapse_enabled=singleton_collapse_enabled,
        noise_community_id=noise_community_id,
        singleton_collapse_in_transactions_of=singleton_collapse_in_transactions_of,
        knn_neighbors=knn_neighbors,
        knn_self_neighbor_offset=knn_self_neighbor_offset,
        similarity_threshold=similarity_threshold,
        similarity_recency_days=similarity_recency_days,
        similarity_pruning=similarity_pruning,
        prune_after_clustering=prune_after_clustering,
        prune_similarity_threshold=prune_similarity_threshold,
        prune_days=prune_days,
        feed_size=feed_size,
        feed_days=feed_days,
        feed_ttl=feed_ttl,
        feed_score_multiplier=feed_score_multiplier,
        feed_score_decimals=feed_score_decimals,
        cold_start_fallback=cold_start_fallback,
        cold_start_limit=cold_start_limit,
        serendipity_probability=serendipity_probability,
        serendipity_limit=serendipity_limit,
        serendipity_score=serendipity_score,
        serendipity_based_on=serendipity_based_on,
        decay_half_life_days=decay_half_life_days,
        interests_ttl_days=interests_ttl_days,
        interests_min_favourites=interests_min_favourites,
        active_user_days=active_user_days,
        feed_force_refresh=feed_force_refresh,
        language_match_weight=language_match_weight,
        ui_language_match_weight=ui_language_match_weight,
        likes_weight=likes_weight,
        reblogs_weight=reblogs_weight,
        replies_weight=replies_weight,
        follows_weight=follows_weight,
        mentions_weight=mentions_weight,
        bookmark_weight=bookmark_weight,
        personalized_interest_weight=personalized_interest_weight,
        personalized_popularity_weight=personalized_popularity_weight,
        personalized_recency_weight=personalized_recency_weight,
        cold_start_popularity_weight=cold_start_popularity_weight,
        cold_start_recency_weight=cold_start_recency_weight,
        popularity_smoothing=popularity_smoothing,
        recency_smoothing=recency_smoothing,
        recency_numerator=recency_numerator,
        ctr_enabled=ctr_enabled,
        ctr_weight=ctr_weight,
        min_ctr=min_ctr,
        ctr_smoothing=ctr_smoothing,
        pagerank_enabled=pagerank_enabled,
        pagerank_weight=pagerank_weight,
        pagerank_damping_factor=pagerank_damping_factor,
        pagerank_max_iterations=pagerank_max_iterations,
        community_similarity_enabled=community_similarity_enabled,
        community_similarity_top_k=community_similarity_top_k,
        public_feed_size=public_feed_size,
        public_feed_enabled=public_feed_enabled,
        public_feed_strategy=public_feed_strategy,
        public_timeline_key=public_timeline_key,
        local_timeline_key=local_timeline_key,
        export_max_items=export_max_items,
        text_preview_limit=text_preview_limit,
        community_interest_limit=community_interest_limit,
        community_member_sample=community_member_sample,
        community_sample_limit=community_sample_limit,
        graph_sample_limit=graph_sample_limit,
        log_level=log_level,
        log_file=log_file,
        progress_poll_interval_seconds=progress_poll_interval_seconds,
    )

    exit_code = execute_validate(
        overrides=overrides, verbose=verbose, memory_interval=memory_interval
    )
    raise typer.Exit(code=exit_code)


@app.command("model-export")
def model_export(
    output: Annotated[
        str,
        typer.Argument(help="Output path for the .tar.gz bundle"),
    ],
    mode: Annotated[
        str,
        typer.Option(
            "--mode",
            help="Bundle mode: 'inference' (minimal) or 'full' (for retraining)",
        ),
    ] = "inference",
    memory_interval: MemoryIntervalOpt = 10,
    # Neo4j (needed for reading FastTextState)
    neo4j_host: Neo4jHostOpt = None,
    neo4j_port: Neo4jPortOpt = None,
    neo4j_username: Neo4jUsernameOpt = None,
    neo4j_password: Neo4jPasswordOpt = None,
    neo4j_ready_retries: Neo4jReadyRetriesOpt = None,
    neo4j_ready_sleep_seconds: Neo4jReadySleepSecondsOpt = None,
    neo4j_worker_label: Neo4jWorkerLabelOpt = None,
    # Logging
    log_level: LogLevelOpt = None,
    log_file: LogFileOpt = None,
    progress_poll_interval_seconds: ProgressPollIntervalSecondsOpt = None,
    verbose: VerboseOpt = False,
) -> None:
    """Export pretrained models as a single .tar.gz bundle."""
    from hintgrid.cli.runner import execute_model_export

    if mode not in ("inference", "full"):
        typer.echo(
            f"Error: Invalid mode '{mode}'. Must be 'inference' or 'full'",
            err=True,
        )
        raise typer.Exit(code=2)

    overrides = _collect_overrides(
        neo4j_host=neo4j_host,
        neo4j_port=neo4j_port,
        neo4j_username=neo4j_username,
        neo4j_password=neo4j_password,
        neo4j_ready_retries=neo4j_ready_retries,
        neo4j_ready_sleep_seconds=neo4j_ready_sleep_seconds,
        neo4j_worker_label=neo4j_worker_label,
        log_level=log_level,
        log_file=log_file,
        progress_poll_interval_seconds=progress_poll_interval_seconds,
    )

    exit_code = execute_model_export(
        overrides=overrides,
        output_path=output,
        mode=mode,
        verbose=verbose,
        memory_interval=memory_interval,
    )
    raise typer.Exit(code=exit_code)


@app.command("model-import")
def model_import(
    archive: Annotated[
        str,
        typer.Argument(help="Path to the .tar.gz model bundle"),
    ],
    force: Annotated[
        bool,
        typer.Option("--force", help="Overwrite existing model files"),
    ] = False,
    memory_interval: MemoryIntervalOpt = 10,
    # Neo4j (needed for updating FastTextState)
    neo4j_host: Neo4jHostOpt = None,
    neo4j_port: Neo4jPortOpt = None,
    neo4j_username: Neo4jUsernameOpt = None,
    neo4j_password: Neo4jPasswordOpt = None,
    neo4j_ready_retries: Neo4jReadyRetriesOpt = None,
    neo4j_ready_sleep_seconds: Neo4jReadySleepSecondsOpt = None,
    neo4j_worker_label: Neo4jWorkerLabelOpt = None,
    # Logging
    log_level: LogLevelOpt = None,
    log_file: LogFileOpt = None,
    progress_poll_interval_seconds: ProgressPollIntervalSecondsOpt = None,
    verbose: VerboseOpt = False,
) -> None:
    """Import a pretrained model bundle from a .tar.gz archive."""
    from hintgrid.cli.runner import execute_model_import

    overrides = _collect_overrides(
        neo4j_host=neo4j_host,
        neo4j_port=neo4j_port,
        neo4j_username=neo4j_username,
        neo4j_password=neo4j_password,
        neo4j_ready_retries=neo4j_ready_retries,
        neo4j_ready_sleep_seconds=neo4j_ready_sleep_seconds,
        neo4j_worker_label=neo4j_worker_label,
        log_level=log_level,
        log_file=log_file,
        progress_poll_interval_seconds=progress_poll_interval_seconds,
    )

    exit_code = execute_model_import(
        overrides=overrides,
        archive_path=archive,
        force=force,
        verbose=verbose,
        memory_interval=memory_interval,
    )
    raise typer.Exit(code=exit_code)


@app.command()
def reindex(
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Show what would be reindexed without changes")
    ] = False,
    memory_interval: MemoryIntervalOpt = 10,
    # PostgreSQL
    postgres_host: PostgresHostOpt = None,
    postgres_port: PostgresPortOpt = None,
    postgres_database: PostgresDatabaseOpt = None,
    postgres_user: PostgresUserOpt = None,
    postgres_password: PostgresPasswordOpt = None,
    postgres_schema: PostgresSchemaOpt = None,
    pg_pool_min_size: PgPoolMinSizeOpt = None,
    pg_pool_max_size: PgPoolMaxSizeOpt = None,
    pg_pool_timeout_seconds: PgPoolTimeoutSecondsOpt = None,
    mastodon_public_visibility: MastodonPublicVisibilityOpt = None,
    mastodon_account_lookup_limit: MastodonAccountLookupLimitOpt = None,
    # Neo4j
    neo4j_host: Neo4jHostOpt = None,
    neo4j_port: Neo4jPortOpt = None,
    neo4j_username: Neo4jUsernameOpt = None,
    neo4j_password: Neo4jPasswordOpt = None,
    neo4j_ready_retries: Neo4jReadyRetriesOpt = None,
    neo4j_ready_sleep_seconds: Neo4jReadySleepSecondsOpt = None,
    neo4j_worker_label: Neo4jWorkerLabelOpt = None,
    # Redis
    redis_host: RedisHostOpt = None,
    redis_port: RedisPortOpt = None,
    redis_db: RedisDbOpt = None,
    redis_password: RedisPasswordOpt = None,
    redis_score_tolerance: RedisScoreToleranceOpt = None,
    redis_namespace: RedisNamespaceOpt = None,
    # LLM
    llm_provider: LlmProviderOpt = None,
    llm_base_url: LlmBaseUrlOpt = None,
    llm_model: LlmModelOpt = None,
    llm_dimensions: LlmDimensionsOpt = None,
    llm_timeout: LlmTimeoutOpt = None,
    llm_max_retries: LlmMaxRetriesOpt = None,
    llm_batch_size: LlmBatchSizeOpt = None,
    llm_api_key: LlmApiKeyOpt = None,
    min_embedding_tokens: MinEmbeddingTokensOpt = None,
    embedding_skip_percentile: EmbeddingSkipPercentileOpt = None,
    # FastText
    fasttext_vector_size: FasttextVectorSizeOpt = None,
    fasttext_window: FasttextWindowOpt = None,
    fasttext_min_count: FasttextMinCountOpt = None,
    fasttext_max_vocab_size: FasttextMaxVocabSizeOpt = None,
    fasttext_epochs: FasttextEpochsOpt = None,
    fasttext_bucket: FasttextBucketOpt = None,
    fasttext_min_documents: FasttextMinDocumentsOpt = None,
    fasttext_model_path: FasttextModelPathOpt = None,
    fasttext_quantize: FasttextQuantizeOpt = None,
    fasttext_quantize_qdim: FasttextQuantizeQdimOpt = None,
    fasttext_training_workers: FasttextTrainingWorkersOpt = None,
    # Batch
    batch_size: BatchSizeOpt = None,
    load_since: LoadSinceOpt = None,
    max_retries: MaxRetriesOpt = None,
    checkpoint_interval: CheckpointIntervalOpt = None,
    apoc_batch_size: ApocBatchSizeOpt = None,
    feed_workers: FeedWorkersOpt = None,
    loader_workers: LoaderWorkersOpt = None,
    # Clustering
    user_communities: UserCommunitiesOpt = None,
    post_communities: PostCommunitiesOpt = None,
    leiden_resolution: LeidenResolutionOpt = None,
    leiden_max_levels: LeidenMaxLevelsOpt = None,
    singleton_collapse_enabled: SingletonCollapseEnabledOpt = None,
    noise_community_id: NoiseCommunityIdOpt = None,
    singleton_collapse_in_transactions_of: SingletonCollapseInTransactionsOfOpt = None,
    knn_neighbors: KnnNeighborsOpt = None,
    knn_self_neighbor_offset: KnnSelfNeighborOffsetOpt = None,
    similarity_threshold: SimilarityThresholdOpt = None,
    similarity_recency_days: SimilarityRecencyDaysOpt = None,
    similarity_pruning: SimilarityPruningOpt = None,
    prune_after_clustering: PruneAfterClusteringOpt = None,
    prune_similarity_threshold: PruneSimilarityThresholdOpt = None,
    prune_days: PruneDaysOpt = None,
    # Feed
    feed_size: FeedSizeOpt = None,
    feed_days: FeedDaysOpt = None,
    feed_ttl: FeedTtlOpt = None,
    feed_score_multiplier: FeedScoreMultiplierOpt = None,
    feed_score_decimals: FeedScoreDecimalsOpt = None,
    cold_start_fallback: ColdStartFallbackOpt = None,
    cold_start_limit: ColdStartLimitOpt = None,
    serendipity_probability: SerendipityProbabilityOpt = None,
    serendipity_limit: SerendipityLimitOpt = None,
    serendipity_score: SerendipityScoreOpt = None,
    serendipity_based_on: SerendipityBasedOnOpt = None,
    decay_half_life_days: DecayHalfLifeDaysOpt = None,
    interests_ttl_days: InterestsTtlDaysOpt = None,
    interests_min_favourites: InterestsMinFavouritesOpt = None,
    active_user_days: ActiveUserDaysOpt = None,
    feed_force_refresh: FeedForceRefreshOpt = None,
    language_match_weight: LanguageMatchWeightOpt = None,
    ui_language_match_weight: UiLanguageMatchWeightOpt = None,
    # Scoring
    likes_weight: LikesWeightOpt = None,
    reblogs_weight: ReblogsWeightOpt = None,
    replies_weight: RepliesWeightOpt = None,
    follows_weight: FollowsWeightOpt = None,
    mentions_weight: MentionsWeightOpt = None,
    bookmark_weight: BookmarkWeightOpt = None,
    personalized_interest_weight: PersonalizedInterestWeightOpt = None,
    personalized_popularity_weight: PersonalizedPopularityWeightOpt = None,
    personalized_recency_weight: PersonalizedRecencyWeightOpt = None,
    cold_start_popularity_weight: ColdStartPopularityWeightOpt = None,
    cold_start_recency_weight: ColdStartRecencyWeightOpt = None,
    popularity_smoothing: PopularitySmoothingOpt = None,
    recency_smoothing: RecencySmoothingOpt = None,
    recency_numerator: RecencyNumeratorOpt = None,
    ctr_enabled: CtrEnabledOpt = None,
    ctr_weight: CtrWeightOpt = None,
    min_ctr: MinCtrOpt = None,
    ctr_smoothing: CtrSmoothingOpt = None,
    pagerank_enabled: PagerankEnabledOpt = None,
    pagerank_weight: PagerankWeightOpt = None,
    pagerank_damping_factor: PagerankDampingFactorOpt = None,
    pagerank_max_iterations: PagerankMaxIterationsOpt = None,
    community_similarity_enabled: CommunitySimilarityEnabledOpt = None,
    community_similarity_top_k: CommunitySimilarityTopKOpt = None,
    # Public timelines
    public_feed_size: PublicFeedSizeOpt = None,
    public_feed_enabled: PublicFeedEnabledOpt = None,
    public_feed_strategy: PublicFeedStrategyOpt = None,
    public_timeline_key: PublicTimelineKeyOpt = None,
    local_timeline_key: LocalTimelineKeyOpt = None,
    # Export
    export_max_items: ExportMaxItemsOpt = None,
    text_preview_limit: TextPreviewLimitOpt = None,
    community_interest_limit: CommunityInterestLimitOpt = None,
    community_member_sample: CommunityMemberSampleOpt = None,
    community_sample_limit: CommunitySampleLimitOpt = None,
    graph_sample_limit: GraphSampleLimitOpt = None,
    # Logging
    log_level: LogLevelOpt = None,
    log_file: LogFileOpt = None,
    progress_poll_interval_seconds: ProgressPollIntervalSecondsOpt = None,
    verbose: VerboseOpt = False,
) -> None:
    """Force re-indexing of all embeddings."""
    from hintgrid.cli.runner import execute_reindex

    overrides = _collect_overrides(
        postgres_host=postgres_host,
        postgres_port=postgres_port,
        postgres_database=postgres_database,
        postgres_user=postgres_user,
        postgres_password=postgres_password,
        postgres_schema=postgres_schema,
        pg_pool_min_size=pg_pool_min_size,
        pg_pool_max_size=pg_pool_max_size,
        pg_pool_timeout_seconds=pg_pool_timeout_seconds,
        mastodon_public_visibility=mastodon_public_visibility,
        mastodon_account_lookup_limit=mastodon_account_lookup_limit,
        neo4j_host=neo4j_host,
        neo4j_port=neo4j_port,
        neo4j_username=neo4j_username,
        neo4j_password=neo4j_password,
        neo4j_ready_retries=neo4j_ready_retries,
        neo4j_ready_sleep_seconds=neo4j_ready_sleep_seconds,
        neo4j_worker_label=neo4j_worker_label,
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
        redis_password=redis_password,
        redis_score_tolerance=redis_score_tolerance,
        redis_namespace=redis_namespace,
        llm_provider=llm_provider,
        llm_base_url=llm_base_url,
        llm_model=llm_model,
        llm_dimensions=llm_dimensions,
        llm_timeout=llm_timeout,
        llm_max_retries=llm_max_retries,
        llm_batch_size=llm_batch_size,
        llm_api_key=llm_api_key,
        min_embedding_tokens=min_embedding_tokens,
        embedding_skip_percentile=embedding_skip_percentile,
        fasttext_vector_size=fasttext_vector_size,
        fasttext_window=fasttext_window,
        fasttext_min_count=fasttext_min_count,
        fasttext_max_vocab_size=fasttext_max_vocab_size,
        fasttext_epochs=fasttext_epochs,
        fasttext_bucket=fasttext_bucket,
        fasttext_min_documents=fasttext_min_documents,
        fasttext_model_path=fasttext_model_path,
        fasttext_quantize=fasttext_quantize,
        fasttext_quantize_qdim=fasttext_quantize_qdim,
        fasttext_training_workers=fasttext_training_workers,
        batch_size=batch_size,
        load_since=load_since,
        max_retries=max_retries,
        checkpoint_interval=checkpoint_interval,
        apoc_batch_size=apoc_batch_size,
        feed_workers=feed_workers,
        loader_workers=loader_workers,
        user_communities=user_communities,
        post_communities=post_communities,
        leiden_resolution=leiden_resolution,
        leiden_max_levels=leiden_max_levels,
        singleton_collapse_enabled=singleton_collapse_enabled,
        noise_community_id=noise_community_id,
        singleton_collapse_in_transactions_of=singleton_collapse_in_transactions_of,
        knn_neighbors=knn_neighbors,
        knn_self_neighbor_offset=knn_self_neighbor_offset,
        similarity_threshold=similarity_threshold,
        similarity_recency_days=similarity_recency_days,
        similarity_pruning=similarity_pruning,
        prune_after_clustering=prune_after_clustering,
        prune_similarity_threshold=prune_similarity_threshold,
        prune_days=prune_days,
        feed_size=feed_size,
        feed_days=feed_days,
        feed_ttl=feed_ttl,
        feed_score_multiplier=feed_score_multiplier,
        feed_score_decimals=feed_score_decimals,
        cold_start_fallback=cold_start_fallback,
        cold_start_limit=cold_start_limit,
        serendipity_probability=serendipity_probability,
        serendipity_limit=serendipity_limit,
        serendipity_score=serendipity_score,
        serendipity_based_on=serendipity_based_on,
        decay_half_life_days=decay_half_life_days,
        interests_ttl_days=interests_ttl_days,
        interests_min_favourites=interests_min_favourites,
        active_user_days=active_user_days,
        feed_force_refresh=feed_force_refresh,
        language_match_weight=language_match_weight,
        ui_language_match_weight=ui_language_match_weight,
        likes_weight=likes_weight,
        reblogs_weight=reblogs_weight,
        replies_weight=replies_weight,
        follows_weight=follows_weight,
        mentions_weight=mentions_weight,
        bookmark_weight=bookmark_weight,
        personalized_interest_weight=personalized_interest_weight,
        personalized_popularity_weight=personalized_popularity_weight,
        personalized_recency_weight=personalized_recency_weight,
        cold_start_popularity_weight=cold_start_popularity_weight,
        cold_start_recency_weight=cold_start_recency_weight,
        popularity_smoothing=popularity_smoothing,
        recency_smoothing=recency_smoothing,
        recency_numerator=recency_numerator,
        ctr_enabled=ctr_enabled,
        ctr_weight=ctr_weight,
        min_ctr=min_ctr,
        ctr_smoothing=ctr_smoothing,
        pagerank_enabled=pagerank_enabled,
        pagerank_weight=pagerank_weight,
        pagerank_damping_factor=pagerank_damping_factor,
        pagerank_max_iterations=pagerank_max_iterations,
        community_similarity_enabled=community_similarity_enabled,
        community_similarity_top_k=community_similarity_top_k,
        public_feed_size=public_feed_size,
        public_feed_enabled=public_feed_enabled,
        public_feed_strategy=public_feed_strategy,
        public_timeline_key=public_timeline_key,
        local_timeline_key=local_timeline_key,
        export_max_items=export_max_items,
        text_preview_limit=text_preview_limit,
        community_interest_limit=community_interest_limit,
        community_member_sample=community_member_sample,
        community_sample_limit=community_sample_limit,
        graph_sample_limit=graph_sample_limit,
        log_level=log_level,
        log_file=log_file,
        progress_poll_interval_seconds=progress_poll_interval_seconds,
    )

    exit_code = execute_reindex(
        overrides=overrides, dry_run=dry_run, verbose=verbose, memory_interval=memory_interval
    )
    raise typer.Exit(code=exit_code)
