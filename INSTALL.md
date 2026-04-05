# Installing HintGrid on a Mastodon server (systemd)

> 🌐 English | **[Русская версия](INSTALL.ru.md)**

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

### 9. Updating the package after deploy

After installing a new wheel into the same venv (`pip install --force-reinstall ...`), restarting the timer is usually **not** required — the next run picks up the code. If needed: `sudo systemctl restart hintgrid-run.timer`.
