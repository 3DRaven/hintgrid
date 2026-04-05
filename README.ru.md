# HintGrid - Community-Based Recommendation System

> 🌐 **[English version](README.en.md)** | Русская версия

HintGrid — персонализированная рекомендательная система для Mastodon, основанная на анализе сообществ. Реализована как Python-утилита для инкрементальной batch-обработки данных.

## Ключевые особенности

- Community-first подход: рекомендации через связи между сообществами, а не отдельными пользователями.
- Две стратегии эмбеддингов: графовые сообщества и контентные топики.
- Инкрементальная загрузка и идемпотентность операций.
- Встроенный state management и восстановление после ошибок.
- Structured logging и наблюдаемость пайплайна.
- Понятные сообщения об ошибках с подсказками по исправлению.

## Документация

Полная справочная документация находится в [docs/REFERENCE.ru.md](docs/REFERENCE.ru.md).

## Установка и запуск

- Требуется Python >=3.11 и виртуальное окружение .venv в корне проекта.
- Установите пакет и зависимости через pip в активированном окружении.
- Создайте файл .env на основе env.example и заполните параметры подключения.
- Запуск выполняется через CLI утилиту hintgrid. Доступны режимы полного цикла, dry-run без записи в Redis, экспорт состояния пользователя, очистка графа и получение user id.
- Команда `run` принимает опциональный `--user-id` для обработки одного пользователя. Команда `export` требует обязательный `--user-id`.

## Установка на сервере Mastodon (systemd)

Ниже — полный сценарий для **той же машины**, где работает Mastodon: общий PostgreSQL и Redis; Neo4j поднимается отдельно (см. [docs/REFERENCE.ru.md](docs/REFERENCE.ru.md)). Пути и имя пользователя совпадают с примерами в репозитории (`deploy/systemd/`).

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

Проверка:

```bash
psql -h localhost -U hintgrid -d mastodon_production -c "SELECT COUNT(*) FROM accounts;"
```

Подробнее о правах и вариантах — в разделе [Подготовка баз данных](#подготовка-баз-данных) ниже.

### 3. Виртуальное окружение и пакет HintGrid

Из каталога с исходниками или с собранным wheel:

```bash
sudo -u hintgrid python3 -m venv /opt/hintgrid/venv
sudo -u hintgrid /opt/hintgrid/venv/bin/pip install -U pip

# Вариант A: установка из wheel (например после python -m build)
sudo -u hintgrid /opt/hintgrid/venv/bin/pip install /path/to/hintgrid-*.whl

# Вариант B: из клона репозитория
# sudo -u hintgrid /opt/hintgrid/venv/bin/pip install /path/to/hintgrid/
```

### 4. Конфигурация окружения

```bash
sudo -u hintgrid cp /path/to/hintgrid/env.example /opt/hintgrid/.env
sudo -u hintgrid nano /opt/hintgrid/.env
sudo chmod 600 /opt/hintgrid/.env
```

Заполните подключения к PostgreSQL (read-only пользователь Mastodon), Neo4j и Redis. Для работы **в одной связке с Mastodon** важно:

- **`HINTGRID_REDIS_DB`** — тот же логический номер базы Redis, который использует Mastodon (часто `0`), иначе лента пишется «мимо» инстанса.
- **`HINTGRID_REDIS_NAMESPACE`** — если в Mastodon задан `REDIS_NAMESPACE`, задайте то же значение.

### 5. Unit-файлы systemd

В репозитории лежат примеры: [deploy/systemd/hintgrid-run.service](deploy/systemd/hintgrid-run.service) и [deploy/systemd/hintgrid-run.timer](deploy/systemd/hintgrid-run.timer) (`Nice=10`, `IOSchedulingClass=best-effort`, `TimeoutStartSec=infinity` для длительного пайплайна). Таймер по умолчанию: **первый прогон** через `OnBootSec`, далее **`OnUnitInactiveSec=10min`** — следующий запуск через 10 минут **после завершения** предыдущего (удобно для длинных batch). Пока `ExecStart` ещё выполняется, systemd **не запускает второй экземпляр** того же unit (`Type=oneshot`). Файл блокировки `flock` не нужен, если единственная точка входа — этот сервис и его timer (для запусков из cron/другого unit к тому же пайплайну блокировка может быть уместна отдельно). Альтернатива «каждые 10 минут по часам» — закомментированный в таймере вариант `OnCalendar=*-*-* *:0/10:00`.

Скопируйте их на сервер и включите таймер:

```bash
sudo cp /path/to/hintgrid/deploy/systemd/hintgrid-run.service /etc/systemd/system/
sudo cp /path/to/hintgrid/deploy/systemd/hintgrid-run.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now hintgrid-run.timer
```

При необходимости отредактируйте `User=`, `Group=`, `WorkingDirectory=`, `ExecStart=` и блок `After=` (например добавьте `postgresql.service` и `redis-server.service`, если сервисы на этом хосте).

### 6. Ручной запуск и проверка

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

### 7. Остановка таймера и сервиса (справка)

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

### 8. Обновление пакета после деплоя

После установки нового wheel в тот же venv (`pip install --force-reinstall ...`) перезапуск таймера обычно **не** обязателен — следующий запуск подхватит код. При необходимости: `sudo systemctl restart hintgrid-run.timer`.

## CLI параметры (описания)

Ниже перечислены основные параметры CLI и их назначение. Полный список переменных окружения, значения по умолчанию, типы и примеры доступны в `env.example`.

Параметры подключения:
- postgres host/port/database/user/password — подключение к PostgreSQL (read-only к Mastodon DB).
- neo4j host/port/username/password — подключение к Neo4j.
- redis host/port/db/password — подключение к Redis.

Параметры LLM/эмбеддингов:
- llm provider/base url/model/dimensions/timeout/max retries/api key — настройки провайдера и параметров эмбеддингов.

Параметры пайплайна:
- batch size — размер батча инкрементальной загрузки.
- load since — окно загрузки данных по времени (например, 30d для последних 30 дней).
- max retries — число глобальных повторов при ошибках.
- checkpoint interval — интервал чекпойнтов в процессе обработки.

Параметры сообществ и кластеризации:
- user/post communities — стратегия построения сообществ.
- leiden resolution/max levels — параметры Leiden для кластеризации.
- knn neighbors/self neighbor offset — параметры KNN для similarity графа.
- similarity threshold/recency days — параметры SIMILAR_TO.
- similarity pruning/prune after clustering/prune threshold/prune days — параметры pruning стратегии.

Параметры интересов и серендипити:
- interests ttl/min favourites — TTL и минимальная статистика для построения интересов.
- likes/reblogs/replies weight — веса для расчёта INTERESTED_IN.
- serendipity probability/limit/score/based_on — параметры серендипити.

Параметры ленты:
- feed size/days/ttl/score multiplier/score decimals — базовые настройки выдачи.
- personalized interest/popularity/recency weight — веса персонализированного скоринга.
- cold start popularity/recency weight/fallback/limit — параметры холодного старта.
- popularity/recency smoothing/numerator — сглаживание и параметры формулы.

Параметры экспорта:
- export max items/text preview limit/community limits/graph sample limit — ограничения экспорта и выборок.

Параметры клиентов БД и интеграций:
- pg pool min/max size/timeout — пул PostgreSQL.
- neo4j readiness retries/sleep — ожидание готовности Neo4j.
- redis score tolerance — допуск сравнения score в cleanup.
- mastodon public visibility/account lookup limit — параметры интеграции с Mastodon.

Логирование и отладка:
- log level/log file — уровень и файл логирования.
- `-v`, `--verbose` — подробный вывод с полными stack trace при ошибках.

## Подготовка баз данных

### PostgreSQL (read-only пользователь для Mastodon DB)

HintGrid требует только чтение из PostgreSQL. Создайте отдельного пользователя с минимальными правами:

```bash
# Подключение к PostgreSQL под суперпользователем
sudo -u postgres psql

# Создание пользователя с паролем
CREATE USER hintgrid WITH PASSWORD 'your_secure_password';

# Выдача прав на подключение к базе Mastodon
GRANT CONNECT ON DATABASE mastodon_production TO hintgrid;

# Переключение на базу Mastodon
\c mastodon_production

# Выдача прав на чтение схемы
GRANT USAGE ON SCHEMA public TO hintgrid;

# Выдача прав на чтение всех существующих таблиц
GRANT SELECT ON ALL TABLES IN SCHEMA public TO hintgrid;

# Автоматическое предоставление прав на новые таблицы
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO hintgrid;

# Выход
\q
```

Проверка подключения:

```bash
psql -h localhost -U hintgrid -d mastodon_production -c "SELECT COUNT(*) FROM accounts;"
```

### Redis (опциональная настройка пароля)

Если Redis требует аутентификацию, настройте пароль:

```bash
# Редактирование конфигурации Redis
sudo nano /etc/redis/redis.conf

# Добавьте или раскомментируйте строку:
# requirepass your_redis_password

# Перезапуск Redis
sudo systemctl restart redis
```

Проверка подключения:

```bash
# Без пароля
redis-cli ping

# С паролем
redis-cli -a your_redis_password ping
```

Для изоляции HintGrid можно использовать отдельную базу Redis (--redis-db):

```bash
# HintGrid использует базу 1, Mastodon остаётся на базе 0
hintgrid run --redis-db 1
```

## Примеры запуска CLI

Полный запуск (все параметры, запись в Redis):

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

Dry-run (без записи в Redis):

```bash
hintgrid run --dry-run
```

Обработка одного пользователя:

```bash
hintgrid run --user-id 101
```

Загрузка данных за последние 30 дней (ускоряет первый запуск):

```bash
hintgrid run --load-since 30d
```

Комбинация с dry-run для быстрой проверки:

```bash
hintgrid run --dry-run --load-since 7d
```

Подробный вывод для отладки (показывает полные stack trace при ошибках):

```bash
hintgrid run --verbose
hintgrid run -v --dry-run
```

Экспорт состояния пользователя (--user-id обязателен):

```bash
hintgrid export user_101_state.md --user-id 101
```

Очистка графа и ключей HintGrid в Redis:

```bash
hintgrid clean
```

Получение user id по адресу:

```bash
hintgrid get-user-info @username@mastodon.social
```

## Тестовый режим (полные сценарии)

Ниже приведены полные сценарии для тестирования и экспериментов без влияния на production данные.

### Тестирование для одного пользователя

Полный цикл: вычисления без записи в Redis + экспорт результата:

```bash
# Шаг 1: Получаем информацию о пользователе (включая user id)
hintgrid get-user-info @username@mastodon.social

# Шаг 2: Dry-run — все вычисления без записи в Redis
hintgrid run --dry-run --user-id 101

# Шаг 3: Экспорт — сравнение текущей (Redis) и новой (Neo4j) ленты
hintgrid export user_101_state.md --user-id 101
```

Результат: файл `user_101_state.md` содержит:
- **Redis Timeline** — текущая лента в Redis (до изменений)
- **Neo4j Timeline** — новая лента, которая была бы записана

### Тестирование для всех пользователей

Полный цикл для всех пользователей без записи в Redis:

```bash
# Шаг 1: Dry-run — обработка всех пользователей без записи
hintgrid run --dry-run

# Шаг 2: Экспорт конкретного пользователя для проверки
hintgrid export user_101_state.md --user-id 101
hintgrid export user_202_state.md --user-id 202
```

### Инкрементальный запуск

После первого полного прогона, последующие запуски обрабатывают только новые данные:

```bash
# Первый запуск — обработка всех данных с нуля
hintgrid run --dry-run

# Второй запуск — только новые посты, лайки, подписки
# (состояние курсоров хранится в Neo4j AppState)
hintgrid run --dry-run

# Проверка результата для пользователя
hintgrid export user_101_state.md --user-id 101
```

### Очистка для повторных экспериментов

Полный сброс для чистого эксперимента:

```bash
# Очистка Neo4j графа и ключей HintGrid в Redis
hintgrid clean

# Теперь можно запустить с нуля
hintgrid run --dry-run

# Экспорт результата
hintgrid export user_101_state.md --user-id 101
```

### Загрузка только свежих данных

Для ускорения первого запуска или при ограничении на период анализа:

```bash
# Загрузить только данные за последние 30 дней
hintgrid run --load-since 30d

# Загрузить данные за последнюю неделю (быстрый первый прогон)
hintgrid run --load-since 7d

# Комбинация с dry-run для проверки
hintgrid run --dry-run --load-since 14d

# Экспорт после анализа свежих данных
hintgrid export fresh_data_result.md --user-id 101
```

**Важно:** При каждом запуске с `--load-since` окно пересчитывается относительно текущего времени. Инкрементальное состояние для statuses, favourites, reblogs, replies игнорируется. Follows, blocks, mutes загружаются всегда полностью для консистентности графа подписок.

### Полный цикл эксперимента

Комплексный сценарий для тестирования изменений параметров:

```bash
# 1. Очистка предыдущих данных
hintgrid clean

# 2. Запуск с экспериментальными параметрами (без записи в Redis)
hintgrid run --dry-run \
  --leiden-resolution 1.5 \
  --similarity-threshold 0.9 \
  --feed-size 100

# 3. Экспорт результата для анализа
hintgrid export experiment_v1.md --user-id 101

# 4. Очистка и повтор с другими параметрами
hintgrid clean

hintgrid run --dry-run \
  --leiden-resolution 0.8 \
  --similarity-threshold 0.75 \
  --feed-size 100

hintgrid export experiment_v2.md --user-id 101

# 5. Сравнение experiment_v1.md и experiment_v2.md
diff experiment_v1.md experiment_v2.md
```

## Архитектура обработки

1. Инкрементальная загрузка данных из PostgreSQL.
2. Векторизация контента и запись в Neo4j.
3. Кластеризация сообществ через Neo4j GDS.
4. Генерация персональных лент и запись в Redis.

## Модель данных (кратко)

- User, Post, UserCommunity, PostCommunity.
- Связи FOLLOWS, FAVORITED, WROTE, BELONGS_TO, INTERESTED_IN, WAS_RECOMMENDED.

## Требования инфраструктуры

- PostgreSQL 14+ (Mastodon DB, read-only доступ).
- Neo4j 5+ с GDS.
- Redis 7+ для лент.
- LLM/embeddings провайдер (по умолчанию встроенный TF-IDF, опционально Ollama/OpenAI).

## Логирование и обработка ошибок

Логи пишутся в файл (полный формат с timestamp) и в консоль (компактный формат с цветами).

**Формат вывода:**
- Консоль: `LEVEL: message` (цветной вывод в терминале)
- Файл: `2026-02-04 20:00:00 INFO hintgrid.app - message`

**Обработка ошибок:**
- При ошибках подключения к БД выводятся понятные сообщения с подсказками
- Stack trace скрыт по умолчанию для чистого вывода
- Флаг `--verbose` включает полные stack trace для отладки

**Примеры сообщений об ошибках:**

```
ERROR: Cannot connect to Neo4j at localhost:7687
Hint: Check that Neo4j is running and accessible. Verify HINTGRID_NEO4J_HOST and HINTGRID_NEO4J_PORT settings.
```

```
ERROR: Cannot connect to PostgreSQL at localhost:5432/mastodon_production
Hint: Authentication failed. Verify HINTGRID_POSTGRES_USER and HINTGRID_POSTGRES_PASSWORD settings.
```

## Статус

Production-ready для инстансов до 100k пользователей.
