# HintGrid - Community-Based Recommendation System

> 🌐 English | **[Русская версия](README.ru.md)** · [Server install (systemd)](INSTALL.md)

HintGrid is a personalized recommendation system for Mastodon based on community analysis. It is implemented as a Python utility for incremental batch processing.

## Key features

- Community-first approach: recommendations through links between communities, not individual users.
- Two embedding strategies: graph communities and content topics.
- Incremental loading and idempotent operations.
- Built-in state management and recovery after failures.
- Structured logging and pipeline observability.
- Clear error messages with remediation hints.

## Documentation

Full reference documentation is in [docs/REFERENCE.ru.md](docs/REFERENCE.ru.md).

## Installation and usage

- Requires Python >=3.11 and a virtual environment `.venv` at the project root.
- Install the package and dependencies with pip in the activated environment.
- Create a `.env` file from `env.example` and fill in connection parameters.
- Run via the `hintgrid` CLI. Modes include full pipeline, dry-run (no Redis writes), user state export, graph cleanup, and user id lookup.
- The `run` command accepts an optional `--user-id` to process a single user. The `export` command requires `--user-id`.

## Installing on a Mastodon server (systemd)

Step-by-step guide: PostgreSQL read-only role, Neo4j in Docker, `.env`, systemd units, paths under `/opt/hintgrid` — see **[INSTALL.md](INSTALL.md)** and **[INSTALL.ru.md](INSTALL.ru.md)** (Russian).

## CLI parameters (overview)

Below are the main CLI parameters and their purpose. The full list of environment variables, defaults, types, and examples is in `env.example`.

Connection parameters:

- postgres host/port/database/user/password — PostgreSQL (read-only to Mastodon DB).
- neo4j host/port/username/password — Neo4j.
- redis host/port/db/password — Redis.

LLM / embedding parameters:

- llm provider/base url/model/dimensions/timeout/max retries/api key — provider and embedding settings.

Pipeline parameters:

- batch size — incremental load batch size.
- load since — time window for data load (e.g. `30d` for the last 30 days).
- max retries — global retries on failure.
- checkpoint interval — checkpoint interval during processing.

Community and clustering parameters:

- user/post communities — community construction strategy.
- leiden resolution/max levels — Leiden clustering parameters.
- knn neighbors/self neighbor offset — KNN parameters for the similarity graph.
- similarity threshold/recency days — SIMILAR_TO parameters.
- similarity pruning/prune after clustering/prune threshold/prune days — pruning strategy.

Interest and serendipity parameters:

- interests ttl/min favourites — TTL and minimum stats for interests.
- likes/reblogs/replies weight — weights for INTERESTED_IN.
- serendipity probability/limit/score/based_on — serendipity parameters.

Feed parameters:

- feed size/days/ttl/score multiplier/score decimals — base delivery settings.
- personalized interest/popularity/recency weight — personalized scoring weights.
- cold start popularity/recency weight/fallback/limit — cold start parameters.
- popularity/recency smoothing/numerator — smoothing and formula parameters.

Export parameters:

- export max items/text preview limit/community limits/graph sample limit — export and sampling limits.

DB client and integration parameters:

- pg pool min/max size/timeout — PostgreSQL pool.
- neo4j readiness retries/sleep — Neo4j readiness wait.
- redis score tolerance — score comparison tolerance in cleanup.
- mastodon public visibility/account lookup limit — Mastodon integration.

Logging and debugging:

- log level/log file — logging level and file.
- `-v`, `--verbose` — verbose output with full stack traces on errors.

## Database setup

### PostgreSQL (read-only user for Mastodon DB)

HintGrid only needs read access to PostgreSQL. Create a dedicated user with minimal privileges:

```bash
# Connect to PostgreSQL as superuser
sudo -u postgres psql

# Create user with password
CREATE USER hintgrid WITH PASSWORD 'your_secure_password';

# Grant connect to Mastodon database
GRANT CONNECT ON DATABASE mastodon_production TO hintgrid;

# Switch to Mastodon database
\c mastodon_production

# Schema usage
GRANT USAGE ON SCHEMA public TO hintgrid;

# SELECT on all existing tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO hintgrid;

# Default privileges for new tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO hintgrid;

# Quit
\q
```

Connection check:

```bash
psql -h localhost -U hintgrid -d mastodon_production -c "SELECT COUNT(*) FROM accounts;"
```

### Redis (optional password)

If Redis requires authentication, configure a password:

```bash
# Edit Redis configuration
sudo nano /etc/redis/redis.conf

# Add or uncomment:
# requirepass your_redis_password

# Restart Redis
sudo systemctl restart redis
```

Connection check:

```bash
# Without password
redis-cli ping

# With password
redis-cli -a your_redis_password ping
```

To isolate HintGrid, use a separate Redis database (`--redis-db`):

```bash
# HintGrid uses DB 1, Mastodon stays on DB 0
hintgrid run --redis-db 1
```

## CLI usage examples

Full run (all parameters, writes to Redis):

```bash
hintgrid run \
  --postgres-host localhost \
  --postgres-port 5432 \
  --postgres-database mastodon_production \
  --postgres-user hintgrid \
  --postgres-password "your_secure_password" \
  --neo4j-host localhost \
  --neo4j-port 7687 \
  --neo4j-username neo4j \
  --neo4j-password password \
  --redis-host localhost \
  --redis-port 6379 \
  --redis-db 1 \
  --redis-password "your_redis_password" \
  --llm-provider ollama \
  --llm-base-url http://localhost:11434 \
  --llm-model nomic-embed-text \
  --llm-dimensions 768 \
  --llm-timeout 30 \
  --llm-max-retries 3 \
  --llm-api-key "" \
  --batch-size 10000 \
  --max-retries 3 \
  --checkpoint-interval 1000 \
  --user-communities dynamic \
  --post-communities dynamic \
  --leiden-resolution 1.0 \
  --leiden-max-levels 10 \
  --knn-neighbors 5 \
  --knn-self-neighbor-offset 1 \
  --similarity-threshold 0.85 \
  --similarity-recency-days 7 \
  --similarity-pruning aggressive \
  --prune-after-clustering \
  --prune-similarity-threshold 0.9 \
  --prune-days 30 \
  --interests-ttl-days 30 \
  --interests-min-favourites 5 \
  --likes-weight 1.0 \
  --reblogs-weight 1.5 \
  --replies-weight 3.0 \
  --serendipity-probability 0.1 \
  --serendipity-limit 100 \
  --serendipity-score 0.1 \
  --serendipity-based-on 0 \
  --feed-size 500 \
  --feed-days 7 \
  --feed-ttl none \
  --feed-score-multiplier 2 \
  --feed-score-decimals 4 \
  --personalized-interest-weight 0.5 \
  --personalized-popularity-weight 0.3 \
  --personalized-recency-weight 0.2 \
  --cold-start-popularity-weight 0.7 \
  --cold-start-recency-weight 0.3 \
  --popularity-smoothing 1 \
  --recency-smoothing 1 \
  --recency-numerator 1.0 \
  --cold-start-fallback global_top \
  --cold-start-limit 500 \
  --export-max-items 50 \
  --text-preview-limit 60 \
  --community-interest-limit 30 \
  --community-member-sample 5 \
  --community-sample-limit 5 \
  --graph-sample-limit 10 \
  --pg-pool-min-size 1 \
  --pg-pool-max-size 5 \
  --pg-pool-timeout-seconds 30 \
  --neo4j-ready-retries 30 \
  --neo4j-ready-sleep-seconds 1 \
  --redis-score-tolerance 1e-06 \
  --mastodon-public-visibility 0 \
  --mastodon-account-lookup-limit 1 \
  --log-level INFO \
  --log-file hintgrid.log
```

Dry-run (no Redis writes):

```bash
hintgrid run --dry-run
```

Single user:

```bash
hintgrid run --user-id 101
```

Load data for the last 30 days (speeds up first run):

```bash
hintgrid run --load-since 30d
```

Combine with dry-run for a quick check:

```bash
hintgrid run --dry-run --load-since 7d
```

Verbose output for debugging (full stack traces on errors):

```bash
hintgrid run --verbose
hintgrid run -v --dry-run
```

Export user state (`--user-id` is required):

```bash
hintgrid export user_101_state.md --user-id 101
```

Clean graph and HintGrid keys in Redis:

```bash
hintgrid clean
```

Resolve user id from address:

```bash
hintgrid get-user-info @username@mastodon.social
```

## Test mode (full scenarios)

Full scenarios for testing and experiments without affecting production data.

### Single-user testing

Full cycle: compute without Redis writes + export:

```bash
# Step 1: User info (including user id)
hintgrid get-user-info @username@mastodon.social

# Step 2: Dry-run — all computation without Redis writes
hintgrid run --dry-run --user-id 101

# Step 3: Export — compare current (Redis) and new (Neo4j) timelines
hintgrid export user_101_state.md --user-id 101
```

Result: `user_101_state.md` contains:

- **Redis Timeline** — current feed in Redis (before changes)
- **Neo4j Timeline** — feed that would be written

### All users

Full cycle for everyone without Redis writes:

```bash
# Step 1: Dry-run — all users, no writes
hintgrid run --dry-run

# Step 2: Export specific users for inspection
hintgrid export user_101_state.md --user-id 101
hintgrid export user_202_state.md --user-id 202
```

### Incremental runs

After the first full run, later runs only process new data:

```bash
# First run — full data from scratch
hintgrid run --dry-run

# Second run — only new posts, likes, follows
# (cursor state is stored in Neo4j AppState)
hintgrid run --dry-run

# Check result for a user
hintgrid export user_101_state.md --user-id 101
```

### Cleanup for repeated experiments

Full reset for a clean experiment:

```bash
# Clear Neo4j graph and HintGrid Redis keys
hintgrid clean

# Run from scratch
hintgrid run --dry-run

# Export result
hintgrid export user_101_state.md --user-id 101
```

### Loading only recent data

To speed the first run or limit the analysis window:

```bash
# Last 30 days only
hintgrid run --load-since 30d

# Last week (fast first run)
hintgrid run --load-since 7d

# With dry-run
hintgrid run --dry-run --load-since 14d

# Export after analyzing recent data
hintgrid export fresh_data_result.md --user-id 101
```

**Important:** On each run with `--load-since`, the window is recomputed from the current time. Incremental state for statuses, favourites, reblogs, and replies is ignored. Follows, blocks, and mutes are always loaded fully for a consistent subscription graph.

### Full experiment cycle

End-to-end scenario for tuning parameters:

```bash
# 1. Clear previous data
hintgrid clean

# 2. Run with experimental parameters (no Redis writes)
hintgrid run --dry-run \
  --leiden-resolution 1.5 \
  --similarity-threshold 0.9 \
  --feed-size 100

# 3. Export for analysis
hintgrid export experiment_v1.md --user-id 101

# 4. Clear and repeat with other parameters
hintgrid clean

hintgrid run --dry-run \
  --leiden-resolution 0.8 \
  --similarity-threshold 0.75 \
  --feed-size 100

hintgrid export experiment_v2.md --user-id 101

# 5. Compare experiment_v1.md and experiment_v2.md
diff experiment_v1.md experiment_v2.md
```

## Processing architecture

1. Incremental load from PostgreSQL.
2. Content vectorization and writes to Neo4j.
3. Community clustering via Neo4j GDS.
4. Personalized feeds written to Redis.

## Data model (brief)

- User, Post, UserCommunity, PostCommunity.
- Relationships: FOLLOWS, FAVORITED, WROTE, BELONGS_TO, INTERESTED_IN, WAS_RECOMMENDED.

## Infrastructure requirements

- PostgreSQL 14+ (Mastodon DB, read-only).
- Neo4j 5+ with GDS.
- Redis 7+ for feeds.
- LLM / embeddings provider (built-in TF-IDF by default; optional Ollama/OpenAI).

## Logging and error handling

Logs go to a file (full format with timestamp) and to the console (compact, colorized).

**Output format:**

- Console: `LEVEL: message` (color in terminal)
- File: `2026-02-04 20:00:00 INFO hintgrid.app - message`

**Error handling:**

- DB connection errors show clear messages with hints
- Stack traces hidden by default for cleaner output
- `--verbose` enables full stack traces for debugging

**Example error messages:**

```
ERROR: Cannot connect to Neo4j at localhost:7687
Hint: Check that Neo4j is running and accessible. Verify HINTGRID_NEO4J_HOST and HINTGRID_NEO4J_PORT settings.
```

```
ERROR: Cannot connect to PostgreSQL at localhost:5432/mastodon_production
Hint: Authentication failed. Verify HINTGRID_POSTGRES_USER and HINTGRID_POSTGRES_PASSWORD settings.
```

## Status

Production-ready for instances up to ~100k users.
