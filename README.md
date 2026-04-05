# HintGrid - Community-Based Recommendation System

> 🌐 English | **[Русская версия](README.ru.md)**

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

Below is a full scenario for **the same machine** that runs Mastodon: shared PostgreSQL and Redis; **Neo4j** can run in Docker using [deploy/docker-compose.neo4j.yml](deploy/docker-compose.neo4j.yml) (step 3) or on another host (see [docs/REFERENCE.ru.md](docs/REFERENCE.ru.md)). Paths and username match the examples in the repository (`deploy/systemd/`).

### 1. System packages and user

HintGrid requires **Python 3.11+** (`requires-python` in `pyproject.toml`). Debian/Ubuntu repos **often lack** a separate `python3.11` package: on some distributions **`python3`** is already ≥ 3.11; on others `python3` is older — then install a newer Python separately (see below).

```bash
sudo apt update
sudo apt install -y python3 python3-venv
python3 --version
```

In the commands below **`python3`** is used; if your binary is different (e.g. `python3.12`), substitute it for `python3`.

```bash
sudo useradd -r -m -d /opt/hintgrid -s /usr/sbin/nologin hintgrid 2>/dev/null || true
sudo mkdir -p /opt/hintgrid
sudo chown hintgrid:hintgrid /opt/hintgrid
```

### 2. PostgreSQL: read-only user (Mastodon database)

Run **once** as the database superuser (before filling `HINTGRID_POSTGRES_*` in `.env`). The database name is shown as `mastodon_production` — use yours.

```bash
sudo -u postgres psql
```

```sql
CREATE USER hintgrid WITH PASSWORD 'your_secure_password';
GRANT CONNECT ON DATABASE mastodon_production TO hintgrid;
\c mastodon_production
GRANT USAGE ON SCHEMA public TO hintgrid;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO hintgrid;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO hintgrid;
\q
```

Verification:

```bash
psql -h localhost -U hintgrid -d mastodon_production -c "SELECT COUNT(*) FROM accounts;"
```

More on privileges and options in [Database setup](#database-setup) below.

### 3. Neo4j in Docker on the same host

A **Neo4j-only** compose file (GDS + APOC): [deploy/docker-compose.neo4j.yml](deploy/docker-compose.neo4j.yml). Data is stored on the host under `data/`, `logs/`, `import/`, and `plugins/` next to the compose file.

Install Docker and **Compose v2** if needed, then (as `hintgrid`, or use `root` and fix ownership). The **package name** for the `docker compose` plugin **varies by distro**:

```bash
sudo apt update
sudo apt install -y docker.io
sudo systemctl enable --now docker
# Ubuntu 25.04 (Plucky) and some releases: package is docker-compose-v2
sudo apt install -y docker-compose-v2
# Debian and many Ubuntu images: docker-compose-plugin is common (use apt search docker-compose if unsure)
# sudo apt install -y docker-compose-plugin
sudo usermod -aG docker hintgrid
# Log out and back in, or run newgrp docker, for the docker group to apply.
```

Check: `docker compose version`. If `docker compose` is missing but the legacy `docker-compose` (v1) package is installed, use `docker-compose -f ...` instead of `docker compose -f ...` below.

Stack directory and volumes:

```bash
sudo install -d -o hintgrid -g hintgrid /opt/hintgrid/neo4j/{data,logs,import,plugins}
# If the repo was cloned earlier, pull updates (including deploy/docker-compose.neo4j.yml):
sudo -u hintgrid git -C /opt/hintgrid/hintgrid pull
# If you did not clone the repo: copy deploy/docker-compose.neo4j.yml from a machine that has up-to-date sources.
sudo -u hintgrid cp /opt/hintgrid/hintgrid/deploy/docker-compose.neo4j.yml /opt/hintgrid/neo4j/docker-compose.neo4j.yml
sudo -u hintgrid nano /opt/hintgrid/neo4j/docker-compose.neo4j.yml
# Set password: NEO4J_AUTH=neo4j/YOUR_STRONG_PASSWORD
```

Start and verify:

```bash
cd /opt/hintgrid/neo4j
docker compose -f docker-compose.neo4j.yml up -d
docker compose -f docker-compose.neo4j.yml ps
curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:7474
```

In HintGrid **`.env`**: `HINTGRID_NEO4J_HOST=localhost`, `HINTGRID_NEO4J_PORT=7687`, `HINTGRID_NEO4J_USERNAME=neo4j`, `HINTGRID_NEO4J_PASSWORD=` — same password as in `NEO4J_AUTH`. Ensure **7474** and **7687** are not used by other services; change `ports:` in the compose file if they conflict.

After reboot, the container with `restart: unless-stopped` comes back with Docker.

### 4. Virtual environment and HintGrid package

From the source tree or a built wheel:

```bash
sudo -u hintgrid python3 -m venv /opt/hintgrid/venv
sudo -u hintgrid /opt/hintgrid/venv/bin/pip install -U pip

# Option A: install from a wheel (e.g. after python -m build)
sudo -u hintgrid /opt/hintgrid/venv/bin/pip install /path/to/hintgrid-*.whl

# Option B: from a repository clone
# sudo -u hintgrid /opt/hintgrid/venv/bin/pip install /path/to/hintgrid/
```

### 5. Environment configuration

```bash
sudo -u hintgrid cp /path/to/hintgrid/env.example /opt/hintgrid/.env
sudo -u hintgrid nano /opt/hintgrid/.env
sudo chmod 600 /opt/hintgrid/.env
```

Fill in PostgreSQL (read-only Mastodon user), Neo4j, and Redis. For **running alongside Mastodon**:

- **`HINTGRID_REDIS_DB`** — the same logical Redis database number Mastodon uses (often `0`), otherwise the feed is written “beside” the instance.
- **`HINTGRID_REDIS_NAMESPACE`** — if Mastodon sets `REDIS_NAMESPACE`, set the same value here.

### 6. systemd unit files

Examples in the repo: [deploy/systemd/hintgrid-run.service](deploy/systemd/hintgrid-run.service) and [deploy/systemd/hintgrid-run.timer](deploy/systemd/hintgrid-run.timer) (`Nice=10`, `IOSchedulingClass=best-effort`, `TimeoutStartSec=infinity` for long pipelines). Default timer: **first run** after `OnBootSec`, then **`OnUnitInactiveSec=10min`** — next run 10 minutes **after the previous job finished** (good for long batches). While `ExecStart` is running, systemd **does not start a second instance** of the same unit (`Type=oneshot`). A `flock` lock file is unnecessary if the only entry point is this service and its timer (for cron/other units hitting the same pipeline, a separate lock may still make sense). An alternative “every 10 minutes on the clock” is the commented `OnCalendar=*-*-* *:0/10:00` in the timer.

Copy them to the server and enable the timer:

```bash
sudo cp /path/to/hintgrid/deploy/systemd/hintgrid-run.service /etc/systemd/system/
sudo cp /path/to/hintgrid/deploy/systemd/hintgrid-run.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now hintgrid-run.timer
```

Edit `User=`, `Group=`, `WorkingDirectory=`, `ExecStart=`, and the `After=` block if needed (e.g. add `postgresql.service` and `redis-server.service` when those run on the same host).

### 7. Manual run and checks

Run the pipeline **without blocking** the terminal (long run):

```bash
sudo systemctl start --no-block hintgrid-run.service
```

Check timer and logs:

```bash
systemctl status hintgrid-run.timer
systemctl list-timers --all | grep hintgrid
journalctl -u hintgrid-run.service -e
journalctl -u hintgrid-run.service -f
```

You need not wait for the timer for a one-off: `sudo systemctl start --no-block hintgrid-run.service`.

### 8. Stopping the timer and service (reference)

Stop the **timer** (no new scheduled runs; a running `hintgrid run` is **not** cancelled by this alone):

```bash
sudo systemctl stop hintgrid-run.timer
```

Disable **timer autostart on boot** (timer stays stopped after reboot until you `enable` again):

```bash
sudo systemctl disable hintgrid-run.timer
```

Stop the **current** pipeline run if `hintgrid-run.service` is still `activating` / `running`:

```bash
sudo systemctl stop hintgrid-run.service
```

Full “no schedule and no start on boot”: `stop` + `disable` for the timer; optionally `stop` the service.

### 9. Updating the package after deploy

After installing a new wheel into the same venv (`pip install --force-reinstall ...`), restarting the timer is usually **not** required — the next run picks up the code. If needed: `sudo systemctl restart hintgrid-run.timer`.

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
