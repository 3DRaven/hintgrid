# Установка HintGrid на сервере Mastodon (systemd)

> 🌐 **[English](INSTALL.md)** | Русская версия

**Резервное копирование.** Перед установкой, обновлением, правкой `.env`, очисткой данных или любыми экспериментами на production сделайте **бэкапы** по правилам вашей инфраструктуры: PostgreSQL (база Mastodon), Redis (снимок или `BGSAVE` / политика хостинга), данные Neo4j (тома Docker или каталоги из compose), каталог HintGrid (включая `.env`, venv при необходимости, файлы моделей FastText). Конкретные команды дампа здесь не приводятся — зависят от ОС и способа деплоя. **Без актуального бэкапа откат после ошибки может быть невозможен.**

Ниже — полный сценарий для **той же машины**, где работает Mastodon: общий PostgreSQL и Redis; **Neo4j** можно поднять в Docker по [deploy/docker-compose.neo4j.yml](deploy/docker-compose.neo4j.yml) (шаг 3) или на отдельном хосте (см. [docs/REFERENCE.ru.md](docs/REFERENCE.ru.md)). Пути и имя пользователя совпадают с примерами в репозитории (`deploy/systemd/`).

### Первый запуск, нагрузка и последующие прогоны

Первый успешный `hintgrid run` по «пустому» или почти пустому графу в Neo4j обычно **занимает гораздо больше времени**, чем следующие запуски, и создаёт **высокую нагрузку** на CPU, диск и на сервисы PostgreSQL и Neo4j. Это следует из устройства пайплайна в коде:

- **Инкрементальная загрузка из PostgreSQL** опирается на сохранённые в Neo4j курсоры (`PipelineState`: `last_status_id`, взаимодействия, статистика и т.д.). Пока курсоры в начальном состоянии, в граф попадает **весь объём** исторических данных в рамках настроек (в отличие от следующих прогонов, где из Postgres выбираются в основном **новые** записи после сохранённых id). Опция `load_since` ограничивает окно по времени и уменьшает объём первой загрузки.
- **Локальные эмбеддинги FastText** (режим по умолчанию, если не задан внешний провайдер через LLM): при отсутствии готовой модели пайплайн при первом обращении к эмбеддингам выполняет **полное обучение FastText** по потоку текстов из PostgreSQL, затем уже векторизует посты — отдельная длительная фаза с нагрузкой на CPU и чтение БД.
- **Аналитика в Neo4j (GDS)**: кластеризация пользователей и постов, PageRank, граф схожести и связанные шаги выполняются по **уже построенному** графу; при первом прогоне объём узлов и рёбер максимален, поэтому фаза тяжелее, чем когда в граф добавляется только прирост с прошлого запуска.
- **Генерация персональных лент** после первого прогона по умолчанию ориентирована на пользователей с изменившимся состоянием («грязные»); первый раз может потребоваться обойти **всех** активных пользователей, что дольше и заметнее по нагрузке на Neo4j и Redis.

**Последующие** запуски обычно **короче и слабее нагружают** сервер: догрузка по курсорам, уже обученная модель эмбеддингов (без полного переобучения при неизменных настройках), меньший объём пересчёта лент — при условии, что не включён принудительный пересчёт (`feed_force_refresh` и аналогичные сценарии) и не менялась конфигурация эмбеддингов (иначе возможна миграция и повторная обработка постов).

### 1. Системные пакеты и пользователь

HintGrid требует **Python 3.11+** (`requires-python` в `pyproject.toml`). В репозиториях Debian/Ubuntu **часто нет** отдельного пакета `python3.11`: на одних дистрибутивах достаточно **`python3`** (он уже ≥ 3.11), на других `python3` старый — тогда Python новее ставят отдельно (см. ниже).

```bash
sudo apt update
sudo apt install -y python3 python3-venv
python3 --version
```

Дальше в командах используется **`python3`**: если у вас другой бинарь (например `python3.12`), подставьте его вместо `python3`.

```bash
sudo useradd -r -m -d /opt/hintgrid -s /usr/sbin/nologin hintgrid 2>/dev/null || true
sudo mkdir -p /opt/hintgrid
sudo chown hintgrid:hintgrid /opt/hintgrid
```

Клонирование репозитория **под тем же пользователем** (нужен `git`; URL замените на свой — HTTPS или SSH для приватного репозитория):

```bash
sudo apt install -y git
sudo -u hintgrid git clone https://github.com/OWNER/hintgrid.git /opt/hintgrid/hintgrid
```

Каталог **`/opt/hintgrid/hintgrid`** — исходники; виртуальное окружение ниже кладётся в **`/opt/hintgrid/venv`**, отдельно от клона. Если ставите только **wheel** и репозиторий на сервере не нужен, этот блок можно пропустить (тогда в следующих шагах подставляйте пути к локальным файлам вручную).

### 2. PostgreSQL: пользователь только на чтение (база Mastodon)

Выполняется **один раз** под суперпользователем БД (до заполнения `HINTGRID_POSTGRES_*` в `.env`). Имя БД приведено как `mastodon_production` — подставьте своё.

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

Имя в `.env` (`HINTGRID_POSTGRES_USER`) и пароль (`HINTGRID_POSTGRES_PASSWORD`) должны **точно** совпадать с `CREATE USER` и заданным паролем. Опечатка в имени (например, `hintgtid` вместо `hintgrid`) даёт в логах `FATAL: password authentication failed for user "…"` — PostgreSQL при этом проверяет другого пользователя.

Проверка:

```bash
psql -h localhost -U hintgrid -d mastodon_production -c "SELECT COUNT(*) FROM accounts;"
```

Подробнее о правах и вариантах — в разделе [Подготовка баз данных](README.ru.md#подготовка-баз-данных) в README.

### 3. Neo4j в Docker на том же хосте

Отдельный compose-файл **только с Neo4j** (GDS + APOC): [deploy/docker-compose.neo4j.yml](deploy/docker-compose.neo4j.yml). Данные на диске хоста в каталоге `neo4j/` рядом с файлом compose.

Установите Docker и **Compose v2** при необходимости, затем под пользователем `hintgrid` (или от root: смените владельца каталогов). Имя пакета с плагином `docker compose` **зависит от дистрибутива**:

```bash
sudo apt update
sudo apt install -y docker.io
sudo systemctl enable --now docker
# Ubuntu 25.04 (Plucky) и ряд выпусков: пакет называется docker-compose-v2
sudo apt install -y docker-compose-v2
# Debian и многие Ubuntu: часто доступен docker-compose-plugin (если нет — см. apt search docker-compose)
# sudo apt install -y docker-compose-plugin
sudo usermod -aG docker hintgrid
# Перелогиньтесь или newgrp docker, чтобы группа docker применилась.
```

Проверка: `docker compose version`. Если команды `docker compose` нет, но установлен классический пакет `docker-compose` (v1), используйте `docker-compose -f ...` вместо `docker compose -f ...` в командах ниже.

Каталог для стека и тома (пути относительно `docker compose`):

```bash
sudo install -d -o hintgrid -g hintgrid /opt/hintgrid/neo4j/{data,logs,import,plugins}
# Если каталог neo4j когда-то создавался от root, исправьте владельца (иначе cp от hintgrid даст Permission denied):
sudo chown -R hintgrid:hintgrid /opt/hintgrid/neo4j
# Если клонировали репозиторий раньше, подтяните файлы (в т.ч. deploy/docker-compose.neo4j.yml):
sudo -u hintgrid git -C /opt/hintgrid/hintgrid pull
# Если репозиторий не клонировали: скопируйте deploy/docker-compose.neo4j.yml с машины, где есть актуальные исходники.
sudo cp /opt/hintgrid/hintgrid/deploy/docker-compose.neo4j.yml /opt/hintgrid/neo4j/docker-compose.neo4j.yml
sudo chown hintgrid:hintgrid /opt/hintgrid/neo4j/docker-compose.neo4j.yml
sudo -u hintgrid nano /opt/hintgrid/neo4j/docker-compose.neo4j.yml
# Задайте пароль: NEO4J_AUTH=neo4j/ВАШ_НАДЁЖНЫЙ_ПАРОЛЬ
```

Запуск и проверка (выполняйте **под администратором** с `sudo`, сессия SSH как root или обычный пользователь с правами sudo — **не** интерактивный вход под `hintgrid`):

Учётная запись `hintgrid` с оболочкой **`/usr/sbin/nologin`** не предназначена для входа в систему с паролем; команды от её имени задаются так: **`sudo -u hintgrid …`**. Доступ к сокету Docker у `hintgrid` есть после **`usermod -aG docker hintgrid`** и перелогина (или `newgrp docker`).

```bash
sudo -u hintgrid bash -lc 'cd /opt/hintgrid/neo4j && docker compose -f docker-compose.neo4j.yml up -d'
sudo -u hintgrid bash -lc 'cd /opt/hintgrid/neo4j && docker compose -f docker-compose.neo4j.yml ps'
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:7474
```

Запуск **`docker compose` от root** тоже возможен, но тогда единообразие с владельцем файлов и группой `docker` лучше сохранять через **`sudo -u hintgrid`**.

В **`.env` HintGrid** укажите: `HINTGRID_NEO4J_HOST=localhost`, `HINTGRID_NEO4J_PORT=7687`, `HINTGRID_NEO4J_USERNAME=neo4j`, `HINTGRID_NEO4J_PASSWORD=` — тот же пароль, что в `NEO4J_AUTH`. Порты **7474** и **7687** не должны быть заняты другими сервисами; при конфликте измените проброс `ports:` в compose.

После перезагрузки сервера контейнер с `restart: unless-stopped` поднимется вместе с Docker.

### 4. Виртуальное окружение и пакет HintGrid

Из каталога с исходниками или с собранным wheel:

```bash
sudo -u hintgrid python3 -m venv /opt/hintgrid/venv
sudo -u hintgrid /opt/hintgrid/venv/bin/pip install -U pip

# Вариант A: установка из wheel (например после python -m build на другой машине)
sudo -u hintgrid /opt/hintgrid/venv/bin/pip install /path/to/hintgrid-*.whl

# Вариант B: из клона репозитория (см. шаг 1)
sudo -u hintgrid /opt/hintgrid/venv/bin/pip install /opt/hintgrid/hintgrid/
```

### 5. Конфигурация окружения

```bash
# Если репозиторий не клонировали: укажите путь к env.example вручную.
sudo -u hintgrid cp /opt/hintgrid/hintgrid/env.example /opt/hintgrid/.env
sudo -u hintgrid nano /opt/hintgrid/.env
sudo chmod 600 /opt/hintgrid/.env
```

#### Согласование имён и паролей с сервисами

Переменные в `/opt/hintgrid/.env` должны совпадать с тем, что реально настроено в каждом сервисе — иначе будут ошибки аутентификации (а для Neo4j при многократных неверных попытках — временная блокировка `AuthenticationRateLimit`).

| Сервис | Где задаётся «эталон» | Что в `.env` HintGrid должно совпадать |
|--------|------------------------|----------------------------------------|
| **PostgreSQL** | `CREATE USER` / `ALTER USER`, имя БД в кластере | `HINTGRID_POSTGRES_USER` и `HINTGRID_POSTGRES_PASSWORD` — с именем роли и паролем в PostgreSQL; `HINTGRID_POSTGRES_DATABASE` — с именем базы Mastodon. |
| **Neo4j** | `NEO4J_AUTH=neo4j/пароль` в `docker-compose.neo4j.yml` (или пароль после смены в Neo4j) | `HINTGRID_NEO4J_USERNAME` — с пользователем Bolt (в образе обычно `neo4j`); `HINTGRID_NEO4J_PASSWORD` — с паролем из `NEO4J_AUTH` (часть после `/`) или с актуальным паролем в графе. |
| **Redis** | `requirepass` в конфиге Redis (если включён) | `HINTGRID_REDIS_PASSWORD` — тот же пароль, или пусто, если пароля нет. |

**Не путать:** Unix-пользователь `hintgrid` (каталог `/opt/hintgrid`, systemd), роль PostgreSQL (в примерах тоже `hintgrid`) и встроенный пользователь Neo4j `neo4j` — это **разные** сущности; совпадение имён в инструкции удобно для памяти, но не означает автоматического совпадения паролей между сервисами.

Заполните подключения к PostgreSQL (read-only пользователь Mastodon), Neo4j и Redis. Для работы **в одной связке с Mastodon** важно:

- **`HINTGRID_REDIS_DB`** — тот же логический номер базы Redis, который использует Mastodon (часто `0`), иначе лента пишется «мимо» инстанса.
- **`HINTGRID_REDIS_NAMESPACE`** — если в Mastodon задан `REDIS_NAMESPACE`, задайте то же значение.

### 6. Unit-файлы systemd

В репозитории лежат примеры: [deploy/systemd/hintgrid-run.service](deploy/systemd/hintgrid-run.service) и [deploy/systemd/hintgrid-run.timer](deploy/systemd/hintgrid-run.timer) (`Nice=10`, `IOSchedulingClass=best-effort`, `TimeoutStartSec=infinity` для длительного пайплайна). Таймер по умолчанию: **первый прогон** через `OnBootSec`, далее **`OnUnitInactiveSec=10min`** — следующий запуск через 10 минут **после завершения** предыдущего (удобно для длинных batch). Пока `ExecStart` ещё выполняется, systemd **не запускает второй экземпляр** того же unit (`Type=oneshot`). Файл блокировки `flock` не нужен, если единственная точка входа — этот сервис и его timer (для запусков из cron/другого unit к тому же пайплайну блокировка может быть уместна отдельно). Альтернатива «каждые 10 минут по часам» — закомментированный в таймере вариант `OnCalendar=*-*-* *:0/10:00`.

Скопируйте их на сервер и включите таймер:

```bash
sudo cp /opt/hintgrid/hintgrid/deploy/systemd/hintgrid-run.service /etc/systemd/system/
sudo cp /opt/hintgrid/hintgrid/deploy/systemd/hintgrid-run.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now hintgrid-run.timer
```

В [deploy/systemd/hintgrid-run.service](deploy/systemd/hintgrid-run.service) для установки **на одном хосте с Mastodon** заданы **`After=`** и **`Wants=`**: **`postgresql.service`** и **`redis-server.service`** (та же машина, в `.env` обычно `localhost`), **`docker.service`** (Neo4j в контейнере), **`mastodon-web.service`**, **`mastodon-sidekiq.service`**. Менять unit нужно только при нестандартных путях (`/opt/hintgrid`), другом пользователе, другом имени юнита Redis (`redis.service` вместо `redis-server.service`) или если у PostgreSQL только шаблонный юнит (например `postgresql@16-main.service` вместо `postgresql.service`).

### 7. Ручной запуск и проверка

Запуск пайплайна **без блокировки** терминала (долгий прогон):

```bash
sudo systemctl start --no-block hintgrid-run.service
```

Проверка таймера и логов:

```bash
systemctl status hintgrid-run.timer
systemctl list-timers --all | grep hintgrid
journalctl -u hintgrid-run.service -e
journalctl -u hintgrid-run.service -f
```

Разовый запуск по таймеру можно не ждать: `sudo systemctl start --no-block hintgrid-run.service`.

### 8. Остановка таймера и сервиса (справка)

Остановить **таймер** (новые срабатывания по расписанию не планируются; уже запущенный `hintgrid run` **не** отменяется только этой командой):

```bash
sudo systemctl stop hintgrid-run.timer
```

Отключить **автозапуск таймера при загрузке** ОС (таймер останется остановленным после ребута, пока снова не сделаете `enable`):

```bash
sudo systemctl disable hintgrid-run.timer
```

Прервать **текущий** прогон пайплайна, если `hintgrid-run.service` ещё в состоянии `activating` / `running`:

```bash
sudo systemctl stop hintgrid-run.service
```

Полная остановка «по расписанию не звать и не стартовать при boot»: `stop` + `disable` для таймера; при необходимости отдельно `stop` для сервиса.

### 9. Обновление пакета после деплоя

После установки нового wheel в тот же venv (`pip install --force-reinstall ...`) перезапуск таймера обычно **не** обязателен — следующий запуск подхватит код. При необходимости: `sudo systemctl restart hintgrid-run.timer`.

### 10. Очистка рекомендаций HintGrid в Redis (восстановление работы Mastodon)

Если из‑за записей HintGrid в Redis «зависла» или некорректно ведёт себя выдача лент, в норме нужно **удалить из ZSET только элементы‑рекомендации HintGrid**, сохранив нативные записи Mastodon (score совпадает с id поста; у HintGrid — ранговый score с множителем).

#### Рекомендуемо: `hintgrid clean --redis`

**Поддерживаемый проектом способ** — утилита HintGrid (те же настройки, что у пайплайна: `cd` в каталог с `.env`):

```bash
sudo systemctl stop hintgrid-run.timer
sudo systemctl stop hintgrid-run.service
sudo -u hintgrid bash -lc 'cd /opt/hintgrid && /opt/hintgrid/venv/bin/hintgrid clean --redis'
```

**`hintgrid clean --redis`** удаляет **только члены ZSET** (не ключи целиком): для каждого **локального** пользователя из графа Neo4j — в `feed:home:<user_id>` убираются элементы, у которых score не совпадает с id поста (в пределах `HINTGRID_REDIS_SCORE_TOLERANCE`); при включённых публичных лентах то же для ключей публичных таймлайнов (`HINTGRID_PUBLIC_TIMELINE_KEY`, `HINTGRID_LOCAL_TIMELINE_KEY`) с учётом `HINTGRID_REDIS_NAMESPACE`. **Neo4j и файлы моделей не трогаются.** Нужен доступ к Neo4j — как при обычном `hintgrid run`.

Интеграционные тесты: `tests/integration/cli/test_clean.py` (`test_cli_clean_redis_only`), полный `hintgrid clean` — `tests/integration/cli/test_basic.py` (`test_cli_clean_removes_hintgrid_entries`).

#### Вручную: выборочный `ZSCAN` / `ZREM`

Ту же логику, что в коде, можно воспроизвести вручную: для нужного ключа обход `ZSCAN` и `ZREM` только тех members, где score не равен id поста (с учётом допуска). Это **трудоёмко**, зато не удаляет ключи целиком. Сценарии не являются частью автоматических тестов HintGrid.

#### Не рекомендуется: полное удаление ключей лент через `redis-cli`

Ниже — **не проверено** командой HintGrid, **опасно** для работы инстанса: удаляется весь ZSET (включая нативные записи Mastodon в Redis), поведение Mastodon после этого **зависит от версии и нагрузки** и **может нарушить** выдачу лент до восстановления кэша. Используйте **только** при понимании риска и при наличии **бэкапа** (см. предупреждение в начале документа). Предпочтительно сначала **`hintgrid clean --redis`**.

- Публичные ленты (подставьте префикс из `HINTGRID_REDIS_NAMESPACE`, номер БД — `HINTGRID_REDIS_DB`):

```bash
redis-cli -n 0 DEL 'cache:timeline:public' 'cache:timeline:public:local'
```

- Домашние ленты — просмотр ключей и удаление **каждого** ключа целиком (разрушительно):

```bash
redis-cli -n 0 --scan --pattern 'feed:home:*'
# затем по одному: redis-cli -n 0 DEL '<ключ>'
```

- **`FLUSHDB`** по индексу БД Redis — уничтожает **всё** в этой логической базе; на типичной установке Mastodon и HintGrid делят одну БД с остальным кэшем — **крайне рискованно**, в документации HintGrid **не рекомендуется**.

Если **`HINTGRID_FEED_SCORE_MULTIPLIER`** равен `1`, селективное удаление в коде не срабатывает. Сначала задайте множитель **не меньше 2** (как в `env.example`), при необходимости перезапустите пайплайн и снова **`hintgrid clean --redis`**. Удаление ключей вручную — только как крайняя мера с предупреждениями выше.
