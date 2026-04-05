# Installing HintGrid on a Mastodon server (systemd)

> 🌐 English | **[Русская версия](INSTALL.ru.md)**

**Back up first.** Before install, upgrades, editing `.env`, wiping data, or any production experiment, take **backups** per your operations policy: PostgreSQL (Mastodon database), Redis (snapshot / `BGSAVE` / hoster workflow), Neo4j data (Docker volumes or host paths from compose), and the HintGrid tree (including `.env`, optional venv, FastText model files). Exact dump commands are not listed here—they depend on OS and deployment. **Without a current backup, recovery may be impossible.**

Below is a full scenario for **the same machine** that runs Mastodon: shared PostgreSQL and Redis; **Neo4j** can run in Docker using [deploy/docker-compose.neo4j.yml](deploy/docker-compose.neo4j.yml) (step 3) or on another host (see [docs/REFERENCE.ru.md](docs/REFERENCE.ru.md)). Paths and username match the examples in the repository (`deploy/systemd/`).

### First run, load, and later runs

The first successful `hintgrid run` against an **empty or nearly empty** Neo4j graph usually **takes much longer** than later runs and places **heavy load** on CPU, disk, PostgreSQL, and Neo4j. That follows from how the pipeline is implemented:

- **Incremental loading from PostgreSQL** uses cursors stored in Neo4j (`PipelineState`: `last_status_id`, interactions, stats, etc.). While cursors are still at their initial values, the pipeline pulls **the full historical volume** allowed by settings (unlike later runs, which mostly fetch **new** rows after the saved ids). The `load_since` option narrows the time window and reduces first-run work.
- **Local FastText embeddings** (the default when no external LLM is configured): if no trained model exists, the embedding path performs **full FastText training** over a streamed corpus from PostgreSQL before vectors are computed — a long, CPU- and I/O-intensive phase.
- **Neo4j GDS analytics** (user/post clustering, PageRank, similarity graph, etc.) run over the **graph built in that run**; on the first run the graph is largest, so this phase costs more than when only deltas were added since the previous run.
- **Personalized feed generation** defaults to users whose graph state changed (“dirty”); the **first** run may need to process **all** active users, which is slower and stresses Neo4j and Redis more.

**Later** runs are typically **shorter and lighter**: cursor-based incremental loads, an existing embedding model (no full retrain while settings stay the same), and fewer feed updates — unless you force a full refresh (`feed_force_refresh` or similar) or change embedding settings (which can trigger migration and re-embedding).

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

Clone the repository **as the same user** (needs `git`; replace the URL with yours — HTTPS or SSH for a private repo):

```bash
sudo apt install -y git
sudo -u hintgrid git clone https://github.com/OWNER/hintgrid.git /opt/hintgrid/hintgrid
```

The **`/opt/hintgrid/hintgrid`** directory holds the sources; the virtualenv below goes to **`/opt/hintgrid/venv`**, separate from the clone. If you install **only a wheel** and do not need the repo on the server, skip this block (then substitute paths to local files manually in later steps).

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

The `.env` values (`HINTGRID_POSTGRES_USER`, `HINTGRID_POSTGRES_PASSWORD`) must **exactly** match the `CREATE USER` name and password. A typo in the username (e.g. `hintgtid` instead of `hintgrid`) yields `FATAL: password authentication failed for user "…"` because PostgreSQL authenticates a different role.

Verification:

```bash
psql -h localhost -U hintgrid -d mastodon_production -c "SELECT COUNT(*) FROM accounts;"
```

More on privileges and options in [Database setup](README.md#database-setup) in the README.

### 3. Neo4j in Docker on the same host

**Memory.** Neo4j needs a large amount of RAM: for a typical instance with roughly **2.5 million posts** in the graph, plan for about **8 GB of RAM** per Neo4j instance (plus headroom for GDS spikes). Size the host or container limits accordingly.

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
# If neo4j was ever created as root, fix ownership (otherwise cp as hintgrid fails with Permission denied):
sudo chown -R hintgrid:hintgrid /opt/hintgrid/neo4j
# If the repo was cloned earlier, pull updates (including deploy/docker-compose.neo4j.yml):
sudo -u hintgrid git -C /opt/hintgrid/hintgrid pull
# If you did not clone the repo: copy deploy/docker-compose.neo4j.yml from a machine that has up-to-date sources.
sudo cp /opt/hintgrid/hintgrid/deploy/docker-compose.neo4j.yml /opt/hintgrid/neo4j/docker-compose.neo4j.yml
sudo chown hintgrid:hintgrid /opt/hintgrid/neo4j/docker-compose.neo4j.yml
sudo -u hintgrid nano /opt/hintgrid/neo4j/docker-compose.neo4j.yml
# Set password: NEO4J_AUTH=neo4j/YOUR_STRONG_PASSWORD
```

Start and verify (run as an **administrator** with `sudo` — SSH as root or a user with sudo. **Do not** rely on logging in interactively as `hintgrid`):

The `hintgrid` account uses **`/usr/sbin/nologin`**, so it is not meant for password login; run its commands as **`sudo -u hintgrid …`**. Docker access for `hintgrid` requires the **`docker`** group (`usermod -aG docker hintgrid`) and a new login session (or `newgrp docker`).

```bash
sudo -u hintgrid bash -lc 'cd /opt/hintgrid/neo4j && docker compose -f docker-compose.neo4j.yml up -d'
sudo -u hintgrid bash -lc 'cd /opt/hintgrid/neo4j && docker compose -f docker-compose.neo4j.yml ps'
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:7474
```

Running **`docker compose` as root** also works; using **`sudo -u hintgrid`** keeps ownership and group usage consistent.

In HintGrid **`.env`**: `HINTGRID_NEO4J_HOST=localhost`, `HINTGRID_NEO4J_PORT=7687`, `HINTGRID_NEO4J_USERNAME=neo4j`, `HINTGRID_NEO4J_PASSWORD=` — same password as in `NEO4J_AUTH`. Ensure **7474** and **7687** are not used by other services; change `ports:` in the compose file if they conflict.

After reboot, the container with `restart: unless-stopped` comes back with Docker.

### 4. Virtual environment and HintGrid package

From the source tree or a built wheel:

```bash
sudo -u hintgrid python3 -m venv /opt/hintgrid/venv
sudo -u hintgrid /opt/hintgrid/venv/bin/pip install -U pip

# Option A: install from a wheel (e.g. after python -m build)
sudo -u hintgrid /opt/hintgrid/venv/bin/pip install /path/to/hintgrid-*.whl

# Option B: from a repository clone (see step 1)
sudo -u hintgrid /opt/hintgrid/venv/bin/pip install /opt/hintgrid/hintgrid/
```

### 5. Environment configuration

```bash
# If you did not clone the repo: set the path to env.example manually.
sudo -u hintgrid cp /opt/hintgrid/hintgrid/env.example /opt/hintgrid/.env
sudo -u hintgrid nano /opt/hintgrid/.env
sudo chmod 600 /opt/hintgrid/.env
```

#### Matching usernames and passwords to each service

Values in `/opt/hintgrid/.env` must match what is actually configured in each service — otherwise authentication fails (and Neo4j may temporarily rate-limit after many wrong attempts: `AuthenticationRateLimit`).

| Service | Where the source of truth is | What must match in HintGrid `.env` |
|---------|------------------------------|-------------------------------------|
| **PostgreSQL** | `CREATE USER` / `ALTER USER`, database name in the cluster | `HINTGRID_POSTGRES_USER` and `HINTGRID_POSTGRES_PASSWORD` — same role name and password; `HINTGRID_POSTGRES_DATABASE` — Mastodon database name. |
| **Neo4j** | `NEO4J_AUTH=neo4j/password` in `docker-compose.neo4j.yml` (or password changed later in Neo4j) | `HINTGRID_NEO4J_USERNAME` — Bolt user (commonly `neo4j`); `HINTGRID_NEO4J_PASSWORD` — same as the password in `NEO4J_AUTH` (after `/`) or the current Neo4j password. |
| **Redis** | `requirepass` in Redis config (if enabled) | `HINTGRID_REDIS_PASSWORD` — same password, or empty if Redis has no password. |

**Do not confuse:** the Unix account `hintgrid` (paths, systemd), the PostgreSQL role (named `hintgrid` in the examples), and the Neo4j built-in user `neo4j` are **different** things; similar names in the docs are for readability, not shared passwords across services.

Fill in PostgreSQL (read-only Mastodon user), Neo4j, and Redis. For **running alongside Mastodon**:

- **`HINTGRID_REDIS_DB`** — the same logical Redis database number Mastodon uses (often `0`), otherwise the feed is written “beside” the instance.
- **`HINTGRID_REDIS_NAMESPACE`** — if Mastodon sets `REDIS_NAMESPACE`, set the same value here.

### 6. systemd unit files

Examples in the repo: [deploy/systemd/hintgrid-run.service](deploy/systemd/hintgrid-run.service) and [deploy/systemd/hintgrid-run.timer](deploy/systemd/hintgrid-run.timer) (`Nice=10`, `IOSchedulingClass=best-effort`, `TimeoutStartSec=infinity` for long pipelines). Default timer: **first run** after `OnBootSec`, then **`OnUnitInactiveSec=10min`** — next run 10 minutes **after the previous job finished** (good for long batches). While `ExecStart` is running, systemd **does not start a second instance** of the same unit (`Type=oneshot`). A `flock` lock file is unnecessary if the only entry point is this service and its timer (for cron/other units hitting the same pipeline, a separate lock may still make sense). An alternative “every 10 minutes on the clock” is the commented `OnCalendar=*-*-* *:0/10:00` in the timer.

Copy them to the server and enable the timer:

```bash
sudo cp /opt/hintgrid/hintgrid/deploy/systemd/hintgrid-run.service /etc/systemd/system/
sudo cp /opt/hintgrid/hintgrid/deploy/systemd/hintgrid-run.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now hintgrid-run.timer
```

[deploy/systemd/hintgrid-run.service](deploy/systemd/hintgrid-run.service) is configured for **Mastodon on the same machine**: **`After=`** and **`Wants=`** include **`postgresql.service`** and **`redis-server.service`** (same host; `.env` typically uses `localhost`), **`docker.service`** (Neo4j container), **`mastodon-web.service`**, **`mastodon-sidekiq.service`**. Edit only for non-standard paths, a different Unix user, a different Redis unit name (`redis.service` vs `redis-server.service`), or a versioned PostgreSQL unit only (e.g. `postgresql@16-main.service` instead of `postgresql.service`).

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

### 9. Updating HintGrid

Before updating production, take **backups** and stop the timer/current run if needed (see [section 8](#8-stopping-the-timer-and-service-reference) and the warning at the top). Below are typical commands; paths and user match steps 1–6 (`/opt/hintgrid`, user `hintgrid`).

#### 9.1. Update from a cloned repository

Fetch changes and reinstall the package into the same venv from the clone:

```bash
sudo -u hintgrid git -C /opt/hintgrid/hintgrid fetch origin
sudo -u hintgrid git -C /opt/hintgrid/hintgrid pull --ff-only
sudo -u hintgrid /opt/hintgrid/venv/bin/pip install -U pip
sudo -u hintgrid /opt/hintgrid/venv/bin/pip install --upgrade /opt/hintgrid/hintgrid/
```

Check the installed package version:

```bash
sudo -u hintgrid /opt/hintgrid/venv/bin/pip show hintgrid
```

If `pull` cannot fast-forward (local edits in the clone), resolve git separately or use a wheel install (9.2).

#### 9.2. Update from a wheel

Copy the new `.whl` to the server, then:

```bash
sudo -u hintgrid /opt/hintgrid/venv/bin/pip install --force-reinstall /path/to/hintgrid-*.whl
```

`--force-reinstall` is useful when the version metadata did not change but the code did (as in [deploy.sh](deploy.sh)). If `pyproject.toml` dependencies changed, install without `--no-deps` or upgrade dependencies explicitly.

#### 9.3. Config and unit files after an update

- **`.env`:** diff fresh [env.example](env.example) against `/opt/hintgrid/.env` and add **new** variables and comments; do not overwrite working values without cause. File permissions: `chmod 600 /opt/hintgrid/.env`.
- **Neo4j in Docker:** if [deploy/docker-compose.neo4j.yml](deploy/docker-compose.neo4j.yml) changed upstream, copy it into the stack directory (as in step 3), verify `NEO4J_AUTH` and images, then under a user with Docker access:

```bash
sudo -u hintgrid bash -lc 'cd /opt/hintgrid/neo4j && docker compose -f docker-compose.neo4j.yml pull && docker compose -f docker-compose.neo4j.yml up -d'
```

- **systemd:** if [deploy/systemd/hintgrid-run.service](deploy/systemd/hintgrid-run.service) or [hintgrid-run.timer](deploy/systemd/hintgrid-run.timer) changed, copy them to `/etc/systemd/system/`, then:

```bash
sudo systemctl daemon-reload
sudo systemctl restart hintgrid-run.timer
```

#### 9.4. Timer restart and verification

After **only** upgrading the package in the venv, restarting the timer is usually **not** required — the next `hintgrid run` picks up the code. Restart the timer after unit file changes (9.3) or if you need to reset scheduler state:

```bash
sudo systemctl restart hintgrid-run.timer
systemctl status hintgrid-run.timer
```

### 10. Clearing HintGrid recommendations in Redis (restore Mastodon behavior)

If timelines misbehave because of HintGrid entries in Redis, the normal approach is to **remove only HintGrid members from the sorted sets (ZSETs)** and keep native Mastodon rows (score equals post id; HintGrid uses rank-based scores with a multiplier).

Recommendations are stored in **two** places: per-user home feeds — `feed:home:<user_id>`; instance public timelines — keys such as `timeline:public` / `timeline:public:local` (see [docs/REFERENCE.ru.md — Redis](docs/REFERENCE.ru.md#сводка-где-лежат-рекомендации); Russian reference).

#### Recommended: `hintgrid clean --redis`

**Project-supported method** — the HintGrid CLI (same config as the pipeline — run from the directory that contains `.env`):

```bash
sudo systemctl stop hintgrid-run.timer
sudo systemctl stop hintgrid-run.service
sudo -u hintgrid bash -lc 'cd /opt/hintgrid && /opt/hintgrid/venv/bin/hintgrid clean --redis'
```

**`hintgrid clean --redis`** removes **ZSET members only** (not whole keys): for each **local** user from the Neo4j graph it strips entries in `feed:home:<user_id>` whose score does not match the post id (within `HINTGRID_REDIS_SCORE_TOLERANCE`); if public feeds are enabled, it does the same for the public timeline keys (`HINTGRID_PUBLIC_TIMELINE_KEY`, `HINTGRID_LOCAL_TIMELINE_KEY`) with `HINTGRID_REDIS_NAMESPACE` applied. **Neo4j and on-disk models are untouched.** Neo4j access is required to list users, same as a normal `hintgrid run`.

Integration tests: `tests/integration/cli/test_clean.py` (`test_cli_clean_redis_only`), full `hintgrid clean` checks in `tests/integration/cli/test_basic.py` (`test_cli_clean_removes_hintgrid_entries`).

#### Manual: selective `ZSCAN` / `ZREM`

You can mirror the code’s logic by hand: for each relevant key, `ZSCAN` and `ZREM` only members where the score does not match the post id (within tolerance). Labor-intensive but does not delete whole keys. These flows are **not** covered by HintGrid’s automated tests.

#### Not recommended: deleting entire feed keys with `redis-cli`

The following is **not verified** by the HintGrid team, is **dangerous**, and **may break** your instance: deleting a whole key drops the entire ZSET, including native Mastodon rows in Redis; Mastodon’s behavior afterward **depends on version and load**. Use **only** if you understand the risk and have a **backup** (see the warning at the top of this document). Prefer **`hintgrid clean --redis`** first.

- Public timelines (adjust prefix from `HINTGRID_REDIS_NAMESPACE`, DB index from `HINTGRID_REDIS_DB`):

```bash
redis-cli -n 0 DEL 'cache:timeline:public' 'cache:timeline:public:local'
```

- Home feeds — list keys, then **`DEL` each key** (destructive):

```bash
redis-cli -n 0 --scan --pattern 'feed:home:*'
# then per key: redis-cli -n 0 DEL '<key>'
```

- **`FLUSHDB`** on a Redis DB index wipes **everything** in that logical database; on a typical Mastodon + HintGrid host they often share one DB with other cache data — **high risk**; **not recommended** in HintGrid docs.

If **`HINTGRID_FEED_SCORE_MULTIPLIER`** is `1`, selective removal in code does nothing. First set the multiplier to **at least 2** (as in `env.example`), rerun the pipeline if needed, then **`hintgrid clean --redis`**. Deleting keys manually is a last resort with the warnings above.
