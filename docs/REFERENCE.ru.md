# HintGrid: Community-Based Recommendation System

> 🌐 **[English version](REFERENCE.en.md)** | Русская версия

**Краткая документация** — полное описание Python-утилиты для персонализированных рекомендаций в Mastodon.

---

## 📋 Оглавление

1. [Обзор системы](#обзор-системы)
2. [Архитектура](#архитектура)
3. [Модель данных](#модель-данных)
4. [Два подхода к кластеризации](#два-подхода-к-кластеризации)
5. [Основные SQL запросы](#основные-sql-запросы)
6. [Neo4j операции](#neo4j-операции)
7. [Redis операции](#redis-операции)
   - [Сводка: где лежат рекомендации](#сводка-где-лежат-рекомендации)
   - [Хранение персональных лент](#хранение-персональных-лент)
   - [Интеграция с Mastodon FeedManager](#интеграция-с-mastodon-feedmanager)
   - [Публичные таймлайны Mastodon](#публичные-таймлайны-mastodon)
8. [Библиотеки и зависимости](#библиотеки-и-зависимости)
9. [Установка и настройка](#установка-и-настройка)
10. [Распространение моделей (Model Bundle)](#распространение-моделей-model-bundle)
    - [Экспорт модели](#экспорт-модели)
    - [Импорт модели](#импорт-модели)
    - [Формат архива](#формат-архива)
    - [Режимы бандла](#режимы-бандла)
    - [Валидация и совместимость](#валидация-и-совместимость)
11. [Graceful Shutdown (Ctrl+C)](#graceful-shutdown-ctrlc)
    - [Поведение при прерывании](#поведение-при-прерывании)
    - [Стратегии возобновления шагов](#стратегии-возобновления-шагов)
    - [Курсор активности пользователей](#курсор-активности-пользователей)
    - [Курсор генерации лент](#курсор-генерации-лент)
    - [Rich UI панель прерывания](#rich-ui-панель-прерывания)
12. [Neo4j GDS API](#neo4j-gds-api)
13. [Визуализация и отладка](#визуализация-и-отладка)
    - [Экспорт полного дампа](#экспорт-полного-дампа)
    - [Диагностика](#диагностика)
    - [Обработка ошибок](#обработка-ошибок)
14. [Заключение](#заключение)

---

## Обзор системы

### Назначение

Персонализированная рекомендательная система для Mastodon, основанная на анализе социальных сообществ. Реализована как **синхронная CLI-утилита на Python** для batch-обработки данных.

### Ключевые принципы

1. **Community-First**: Рекомендации через связи между сообществами, а не отдельными пользователями
2. **Batch Processing**: Утилита запускается, обрабатывает данные и завершается
3. **Graph-Native Clustering**: Leiden algorithm для естественного выделения сообществ
4. **Incremental Loading**: SQL-запросы с `WHERE id > last_processed_id`
5. **Idempotent**: Повторный запуск безопасен (MERGE-операции)
6. **Schedule-Based**: Запуск по cron/systemd timer
7. **Exponential Decay & TTL**: Плавное экспоненциальное затухание важности взаимодействий (`exp(-λ·age)`) и автоматическое устаревание связей INTERESTED_IN
8. **Incremental Refresh**: Лёгкое обновление интересов (глобальный decay + пересчёт только «грязных» сообществ) без полной перестройки
9. **Mastodon Integration**: Rank-based interest scoring с base = max_post_id * multiplier для обхода Mastodon FeedManager
10. **Embedding Migration**: смена конфигурации эмбеддингов приводит к переиндексации и переэмбеддингу
11. **Graceful Shutdown**: корректная остановка по Ctrl+C с сохранением прогресса и Rich UI панелью статуса
12. **Selective Feed Updates**: Обновление лент только для «грязных» пользователей (новые посты, изменённые интересы, высокое потребление)
13. **Sub-Daily Recency**: Точность свежести постов в часах для лучшего ранжирования внутри одного дня
14. **Local-Only Feed Generation**: Генерация персональных лент только для локальных пользователей сервера (аналитика на полном графе)
15. **Language Boost**: Языковой бонус для постов на предпочитаемых языках пользователя (мягкий сигнал, не фильтр)
16. **Public Timelines**: Заполнение **публичных** лент инстанса (`timeline:public`, `timeline:public:local` по умолчанию; переопределяются `HINTGRID_PUBLIC_TIMELINE_KEY` / `HINTGRID_LOCAL_TIMELINE_KEY`) — **отдельно** от персональных домашних лент (`feed:home:{account_id}`). Сводка ключей: [Redis → Сводка](#сводка-где-лежат-рекомендации)
17. **Rank-Based Interest Scoring**: Порядок постов в Redis по интересности (не хронологический), с гарантией вытеснения записей Mastodon
18. **Bookmarks as Signal**: Закладки как сильный неявный сигнал интереса (вес выше лайков)

### Стек технологий

| Технология | Версия | Назначение |
|------------|--------|------------|
| **Python** | 3.11+ | Основной язык утилиты |
| **PostgreSQL** | 16+ | Источник данных (Mastodon DB) |
| **Neo4j Community** | 2025.12+ | Графовая БД с векторным поиском |
| **Neo4j GDS Community** | latest | Graph Data Science (Leiden) |
| **FastText (Gensim)** | 4.3+ | Встроенные эмбеддинги по умолчанию |
| **NLTK** | 3.8+ | Токенизация социального текста |
| **LiteLLM** | 1.30+ | Внешние AI-эмбеддинги (опционально) |
| **Ollama** | latest | Локальные AI embeddings (опционально) |
| **Redis** | 7+ | Хранение готовых лент |
| **psycopg3[pool]** | 3.1+ | PostgreSQL driver |
| **neo4j** | 6.1+ | Neo4j Python driver |
| **redis** | 7.1+ | Redis client |
| **msgspec** | 0.19+ | Type-safe data validation and serialization |

**Память Neo4j.** Экземпляр Neo4j рассчитан на большой объём оперативной памяти: для типичного инстанса с порядка **2,5 млн постов** в графе обычно нужно около **8 ГБ RAM** на один процесс Neo4j (плюс запас на пиковые операции GDS). Фактическое потребление зависит от объёма графа, настроек heap/page cache и профиля запросов.

---

## Архитектура

### Execution Flow

Пайплайн выполняет следующие шаги:
1. Инициализация подключений к PostgreSQL, Neo4j, Redis и валидация настроек.
2. Проверка embedding-signature в Neo4j AppState и создание индексов (включая Vector Index).
3. При смене конфигурации эмбеддингов — сброс векторного индекса и переэмбеддинг существующих постов без перезагрузки из PostgreSQL.
4. Инкрементальная выборка данных из PostgreSQL по id (или по времени при `load_since`).
5. Векторизация контента и запись узлов/связей в граф. Посты фильтруются по `min_embedding_tokens` и `embedding_skip_percentile` — посты без эмбеддингов не создаются в Neo4j.
6. Инкрементальная загрузка агрегированных взаимодействий пользователей (INTERACTS_WITH) из PostgreSQL с атомарным обновлением курсоров.
7. Загрузка статистики постов (status_stats: лайки, репосты, ответы) из PostgreSQL.
8. Аналитика через Neo4j GDS: кластеризация пользователей (Leiden на **INTERACTS_WITH**; сигнал подписок из PostgreSQL входит в этот тип, отдельные рёбра `FOLLOWS` в граф не пишутся) и постов (Leiden на SIMILAR_TO).
9. Полная перестройка INTERESTED_IN (с экспоненциальным затуханием) и добавление serendipity-связей.
10. Генерация **персональных** лент (домашняя лента Mastodon) и запись в Redis-ключи `feed:home:{account_id}` (если не `--dry-run`). Обновляются только «грязные» **локальные** пользователи (`isLocal = true`, dirty-user detection на основе `feedGeneratedAt`), если не задан `feed_force_refresh`. После генерации ленты на узле User устанавливается `feedGeneratedAt = datetime()`. Скоринг в Redis — по интересности (rank-based), не хронологический.
11. Генерация **публичных** лент (отдельно от шага 10): ключи по умолчанию `timeline:public` и `timeline:public:local`, запись в Redis с тем же rank-based scoring (см. [сводку по Redis](#сводка-где-лежат-рекомендации)).
12. Сохранение состояния в AppState (включая `last_interests_rebuild_at`).
13. **Graceful Shutdown**: На любом шаге пайплайн проверяет флаг прерывания (Ctrl+C). При получении сигнала текущий batch завершается, состояние сохраняется, и выводится Rich UI панель с прогрессом и стратегиями возобновления.

**Альтернативный режим — инкрементальное обновление (`refresh`):**
1. Применение глобального экспоненциального decay ко всем существующим INTERESTED_IN.
2. Определение «грязных» UserCommunity (с новыми взаимодействиями).
3. Удаление и пересчёт INTERESTED_IN только для грязных сообществ.
4. Удаление связей с score ниже порога (< 0.01).

### Логическая схема

Логическая схема: PostgreSQL → загрузчики → обработчики/векторизация → Neo4j GDS аналитика → генерация лент → Redis → Mastodon API.

---

## Модель данных

### Граф в Neo4j

#### Узлы

```cypher
// User - пользователь Mastodon
(:User {
  id: INTEGER,                    // Mastodon user ID
  cluster_id: INTEGER,            // UserCommunity ID после Leiden
  lastActive: DATETIME,           // Последняя активность (из PostgreSQL)
  feedGeneratedAt: DATETIME,      // Время последней генерации ленты (для selective refresh)
  isLocal: BOOLEAN,               // true = локальный пользователь (accounts.domain IS NULL)
  uiLanguage: STRING,              // Язык UI: нормализованный users.locale (или null)
  languages: LIST<STRING>         // chosen_languages (нормализованные коды; порядок не используется в скоринге)
})

// Post - пост/статус
(:Post {
  id: INTEGER,                    // Mastodon status ID
  authorId: INTEGER,
  text: STRING,
  language: STRING,
  embedding: LIST<FLOAT>,         // FastText (128) или внешние LLM (llm_dimensions)
  cluster_id: INTEGER,            // PostCommunity ID после Leiden
  createdAt: DATETIME,
  totalFavourites: INTEGER,       // Суммарные лайки (локальные + федеративные)
  totalReblogs: INTEGER,          // Суммарные репосты (локальные + федеративные)
  totalReplies: INTEGER           // Количество ответов (из status_stats)
})

// UserCommunity - социальное племя (dynamic, Leiden)
(:UserCommunity {
  id: INTEGER,                    // Community ID из Leiden
  size: INTEGER                   // Количество пользователей
})

// PostCommunity - тематический топик (dynamic, Leiden)
(:PostCommunity {
  id: INTEGER,                    // Community ID из Leiden
  size: INTEGER                   // Количество постов
})

// AppState - состояние приложения (Singleton Node)
(:AppState {
  id: STRING,                     // 'main' (константа)
  last_processed_status_id: INTEGER,      // Курсор для statuses
  last_processed_favourite_id: INTEGER,   // Курсор для favourites
  last_processed_follow_id: INTEGER,      // Курсор для follows
  last_processed_block_id: INTEGER,       // Курсор для blocks
  last_processed_mute_id: INTEGER,        // Курсор для mutes
  last_processed_reblog_id: INTEGER,      // Курсор для reblogs
  last_processed_reply_id: INTEGER,       // Курсор для replies
  last_processed_activity_account_id: INTEGER, // Курсор для user activity (сбрасывается при каждом полном запуске)
  last_processed_feed_user_id: INTEGER,   // Курсор для генерации лент (resume после Ctrl+C)
  last_processed_status_stats_id: INTEGER, // Курсор для status_stats
  last_processed_bookmark_id: INTEGER,    // Курсор для bookmarks
  last_interests_rebuild_at: STRING,      // ISO timestamp последнего пересчёта интересов
  embedding_signature: STRING,            // provider:model:dim
  updated_at: INTEGER                     // Unix timestamp последнего обновления
})

// FastTextState - состояние FastText модели (Singleton Node)
(:FastTextState {
  id: STRING,                     // 'main' (константа)
  version: INTEGER,               // Версия модели
  last_trained_post_id: INTEGER,  // Последний post_id при обучении
  vocab_size: INTEGER,            // Размер словаря
  corpus_size: INTEGER,           // Размер корпуса
  updated_at: INTEGER             // Unix timestamp последнего обновления
})
```

#### Связи

```cypher
// Социальные связи
// Подписки Mastodon в Neo4j не хранятся как отдельный тип FOLLOWS — они агрегируются в INTERACTS_WITH (см. загрузку из PostgreSQL).
(:User)-[:HATES_USER]->(:User)           // Блоки/муты
(:User)-[:INTERACTS_WITH {weight: FLOAT}]->(:User)  // Агрегированные взаимодействия (лайки, ответы, репосты, упоминания, подписки)
(:User)-[:FAVORITED {at: DATETIME}]->(:Post)  // Лайки (только для существующих Post с embedding)
(:User)-[:REBLOGGED {at: DATETIME}]->(:Post)   // Репосты (только для существующих Post с embedding)
(:User)-[:REPLIED {at: DATETIME}]->(:Post)     // Ответы (только для существующих Post с embedding)
(:User)-[:BOOKMARKED {at: DATETIME}]->(:Post)  // Закладки (сильнее лайков, только для существующих Post)
(:User)-[:WROTE]->(:Post)                // Авторство

// Граф сходства постов (для кластеризации)
(:Post)-[:SIMILAR_TO {weight: FLOAT}]->(:Post)  // Семантическое сходство (cosine)

// Принадлежность к сообществам
(:User)-[:BELONGS_TO]->(:UserCommunity)  // Пользователь в племени
(:Post)-[:BELONGS_TO]->(:PostCommunity)  // Пост в топике

// Интересы сообществ (перестраивается каждый цикл или обновляется инкрементально)
(:UserCommunity)-[:INTERESTED_IN {
  score: FLOAT,       // 0.0-1.0, вес интереса (нормализован по max_weight в UC)
  based_on: FLOAT,    // взвешенная сумма взаимодействий (с экспоненциальным затуханием)
  last_updated: DATETIME,  // время последнего обновления
  expires_at: DATETIME     // TTL: удалить после этой даты
}]->(:PostCommunity)

// История рекомендаций
(:User)-[:WAS_RECOMMENDED {
  at: DATETIME,
  score: FLOAT
}]->(:Post)
```

### Индексы

Создаются приложением (`ensure_graph_indexes`). Имена индексов в параллельных тестах
получают суффикс воркера; для сообществ **не** создаются UNIQUE-ограничения по `id`
(идентификаторы Leiden не считаются глобальным ключом — см. комментарии в коде).

```cypher
// Уникальность (только без изоляции воркера в тестах)
CREATE CONSTRAINT user_id_unique IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE;
CREATE CONSTRAINT post_id_unique IF NOT EXISTS FOR (p:Post) REQUIRE p.id IS UNIQUE;
CREATE CONSTRAINT app_state_id_unique IF NOT EXISTS FOR (s:AppState) REQUIRE s.id IS UNIQUE;
CREATE CONSTRAINT progress_tracker_id_unique IF NOT EXISTS FOR (pt:ProgressTracker) REQUIRE pt.id IS UNIQUE;

// Производительность (узлы)
CREATE INDEX post_created_at IF NOT EXISTS FOR (p:Post) ON (p.createdAt);
CREATE INDEX post_author_id IF NOT EXISTS FOR (p:Post) ON (p.authorId);
CREATE INDEX user_username IF NOT EXISTS FOR (u:User) ON (u.username);
CREATE INDEX user_last_active IF NOT EXISTS FOR (u:User) ON (u.lastActive);
CREATE INDEX user_feed_generated_at IF NOT EXISTS FOR (u:User) ON (u.feedGeneratedAt);
CREATE INDEX user_is_local IF NOT EXISTS FOR (u:User) ON (u.isLocal);
CREATE INDEX post_cluster_id IF NOT EXISTS FOR (p:Post) ON (p.cluster_id);
CREATE INDEX user_cluster_id IF NOT EXISTS FOR (u:User) ON (u.cluster_id);
CREATE INDEX user_community_id IF NOT EXISTS FOR (uc:UserCommunity) ON (uc.id);
CREATE INDEX post_community_id IF NOT EXISTS FOR (pc:PostCommunity) ON (pc.id);

// Производительность (рёбра — Neo4j 5+)
CREATE INDEX rel_interested_in_last_updated IF NOT EXISTS FOR ()-[r:INTERESTED_IN]-() ON (r.last_updated);
CREATE INDEX rel_was_recommended_at IF NOT EXISTS FOR ()-[r:WAS_RECOMMENDED]-() ON (r.at);
CREATE INDEX rel_similar_to_weight IF NOT EXISTS FOR ()-[r:SIMILAR_TO]-() ON (r.weight);

// Vector Search (HNSW для быстрого поиска похожих постов)
// Важно: `vector.dimensions` должен совпадать с размерностью embedding.
// Для FastText (по умолчанию): 128, для внешних LLM: llm_dimensions (768)
CREATE VECTOR INDEX post_embedding_index IF NOT EXISTS
FOR (n:Post)
ON n.embedding
OPTIONS {
    indexConfig: {
        `vector.dimensions`: 128,  // или llm_dimensions для внешних LLM
        `vector.similarity_function`: 'cosine'
    }
};
```

---

## Два подхода к кластеризации

### 1. User Clustering: Graph-Based (Leiden)

**Назначение**: Выделение социальных племен на основе паттернов подписок и взаимодействий.

**Характеристики:**
- **Источник**: Граф INTERACTS_WITH (агрегированные взаимодействия, включая FOLLOWS через SQL)
- **Алгоритм**: Leiden Community Detection
- **Провайдер**: Neo4j GDS (встроенный плагин)
- **Особенности**: 
  - Работает напрямую с графом (без векторов)
  - Детерминированный результат
  - Динамическое количество кластеров (не нужно задавать заранее)
  - FOLLOWS включён в INTERACTS_WITH через SQL с параметризуемым весом `follows_weight`
  - Все рёбра имеют веса, что позволяет точно контролировать важность различных типов взаимодействий
- **Свойство**: `User.cluster_id`
- **Узлы сообществ**: `UserCommunity` с динамическими ID

**Как работает:**
1. Из PostgreSQL агрегируются взаимодействия пользователей (лайки, ответы, репосты, упоминания, подписки) → INTERACTS_WITH с весами
   - Каждый компонент имеет параметризуемый вес: `likes_weight`, `replies_weight`, `reblogs_weight`, `mentions_weight`, `follows_weight`
   - FOLLOWS включается в INTERACTS_WITH через SQL (если `follows_weight > 0`)
2. Leiden анализирует плотность связей INTERACTS_WITH между пользователями с учётом весов
3. Выделяет естественные группы пользователей (племена)
4. Количество сообществ определяется автоматически
5. Пользователи в одном племени имеют похожие паттерны взаимодействий
6. Перед материализацией нового членства снимаются все существующие рёбра `BELONGS_TO` от пользователей к `UserCommunity` **батчами** через `apoc.periodic.iterate` (размер батча — `HINTGRID_APOC_BATCH_SIZE`, как для следующего шага и для SIMILAR_TO; в логе прогресса фаза удаления считает **рёбра**, а не узлы)
7. Создаются узлы `UserCommunity` и связи `BELONGS_TO` батчами через `apoc.periodic.iterate` (тот же размер батча; прогресс по числу пользователей с заданным `cluster_id`)
8. **GC сиротских сообществ**: после пересборки `BELONGS_TO` (и в случае, когда ни у одного пользователя/поста нет `cluster_id` — после массового снятия старых рёбер) удаляются узлы `UserCommunity`/`PostCommunity`, на которые больше не указывает ни одно входящее `BELONGS_TO`. Так исчезают «старые» узлы сообществ после смены `cluster_id` и схлопывания синглтонов в общий `noise_community_id`, чтобы метрики и граф не копили мусор.

**Пример логики GC (идея запроса; в пайплайне выполняется для пользовательских и постовых сообществ отдельно):**
```cypher
MATCH (uc:UserCommunity)
WHERE NOT (()-[:BELONGS_TO]->(uc))
DETACH DELETE uc;
```

```cypher
MATCH (pc:PostCommunity)
WHERE NOT (()-[:BELONGS_TO]->(pc))
DETACH DELETE pc;
```

**Параметры кластеризации:**
- `leiden_resolution` (gamma) — управляет размером кластеров.
- `leiden_max_levels` — максимум уровней алгоритма Leiden.
- `leiden_diagnostics` (`HINTGRID_LEIDEN_DIAGNOSTICS`) — при включении в журнал (INFO) пишется одна JSON-строка с: агрегатами по рёбрам (`INTERACTS_WITH` или `SIMILAR_TO`: число рёбер, сумма и перцентили веса, перцентили исходящей степени узлов, число узлов без исходящих рёбер; **учитываются только рёбра между узлами `User`—`User` и `Post`—`Post` с учётом `neo4j_worker_label`**, без глобального `MATCH ()-[r]->()` по всей базе); параметрами `leiden_resolution` и `leiden_max_levels`; подсказкой вида `leiden_resolution/weight_sum` (для сопоставления с документацией Neo4j GDS: взвешенный граф нормализует gamma на сумму весов рёбер); результатом одного вызова `gds.leiden.write` — в том числе `ranLevels`, `didConverge`, `modularities`, `communityDistribution`. Полная карта `communityDistribution` дополнительно дублируется на уровне DEBUG.
- `singleton_collapse_enabled` (`HINTGRID_SINGLETON_COLLAPSE_ENABLED`) — после записи Leiden в свойство `cluster_id` узлы, чей `cluster_id` встречается ровно у одного узла (кластер размера 1), получают единый зарезервированный `noise_community_id`. Переназначение — один проход Cypher: группировка по `cluster_id`, фильтр «размер кластера = 1», `UNWIND` и `SET` (без второго полного сканирования по каждому `cluster_id` и без `apoc.periodic.iterate` на этом шаге). Затем снятие старых `BELONGS_TO` батчами и материализация `UserCommunity`/`PostCommunity` и новых `BELONGS_TO` батчами через `apoc.periodic.iterate` (см. шаги 6–7 выше).
- `singleton_collapse_in_transactions_of` (`HINTGRID_SINGLETON_COLLAPSE_IN_TRANSACTIONS_OF`) — `0`: одна транзакция Cypher без отдельного предварительного `COUNT` (один проход агрегации + `SET`). Положительное значение (по умолчанию `100000`): отдельного `COUNT` тоже нет — после `UNWIND` подзапрос `CALL (*) { … SET … } IN TRANSACTIONS OF … ROWS` (Neo4j 5.23+; импорт переменных внешней строки через scope clause `(*)`, без параметризованных лейблов), чтобы на очень больших графах постов ограничить размер одной транзакции.
- `noise_community_id` (`HINTGRID_NOISE_COMMUNITY_ID`) — значение `cluster_id` для «корзины» одиночных кластеров; совпадает с будущим `UserCommunity.id` / `PostCommunity.id` для этой корзины. Не должен быть `0` (ноль зарезервирован для сценария «нет рёбер в графе взаимодействий/похожести»). Пересборка `INTERESTED_IN`, персональная и публичная лента, serendipity и проекция для `gds.nodeSimilarity` по сообществам исключают этот id, чтобы кластер-шум не смешивал сигналы.

**Пример: число кластеров размера 1 по `cluster_id` у постов (после Leiden):**
```cypher
MATCH (p:Post) WHERE p.cluster_id IS NOT NULL
WITH p.cluster_id AS cid, count(*) AS n
WHERE n = 1
RETURN count(*) AS singleton_cluster_count;
```

**Пример ручной проверки суммы весов (INTERACTS_WITH) в Neo4j Browser:**
```cypher
MATCH ()-[r:INTERACTS_WITH]->()
RETURN count(r) AS rel_count, sum(r.weight) AS weight_sum,
       min(r.weight) AS w_min, max(r.weight) AS w_max,
       avg(r.weight) AS w_avg;
```

**Пример запуска Leiden (только INTERACTS_WITH):**
```cypher
-- Проекция использует только INTERACTS_WITH (FOLLOWS включён через SQL)
CALL gds.graph.project(
    'user-graph',
    'User',
    {
        INTERACTS_WITH: {
            orientation: 'UNDIRECTED',
            properties: 'weight'
        }
    }
);

-- Всегда используются веса, так как все рёбра имеют свойство weight
CALL gds.leiden.write('user-graph', {
    writeProperty: 'cluster_id',
    relationshipWeightProperty: 'weight',
    gamma: $gamma,
    maxLevels: $max_levels
})
YIELD nodePropertiesWritten, communityCount, modularity
RETURN nodePropertiesWritten, communityCount, modularity;
```

**Важно:** 
- Проекция графа использует только INTERACTS_WITH (FOLLOWS включён в INTERACTS_WITH через SQL с весом `follows_weight`)
- Все рёбра INTERACTS_WITH имеют свойство `weight`, поэтому `relationshipWeightProperty` всегда используется
- Если `follows_weight = 0.0`, FOLLOWS не включается в агрегацию INTERACTS_WITH
- Веса компонентов настраиваются через параметры: `likes_weight`, `replies_weight`, `reblogs_weight`, `mentions_weight`, `follows_weight`

**Особенности подхода:**
- ✅ Детерминированность (Leiden на фиксированном графе)
- ✅ Автоматическое определение количества сообществ
- ✅ Не требует предварительного выбора K
- ✅ Если нет FOLLOWS и INTERACTS_WITH, всем пользователям присваивается `cluster_id = 0`
- ✅ Взаимодействие через посты (лайки, ответы, репосты, упоминания) — достаточное основание для объединения в сообщество

### 2. Post Clustering: Vector-to-Graph (Vector Index + Leiden)

**Назначение**: Кластеризация постов в топики по семантическому содержанию.

**Характеристики:**
- **Источник**: Текст постов → AI embeddings
- **Алгоритм**: Vector Index → SIMILAR_TO → Leiden
- **Провайдер**: FastText по умолчанию, LiteLLM/Ollama — опционально
- **Размерность**: FastText=128 (fasttext_vector_size), LLM=768 (llm_dimensions)
- **Свойство**: `Post.embedding`
- **Кластеры**: Через Leiden → `Post.cluster_id` (динамическое количество)

**Как работает:**
1. **Векторизация**: FastText (или внешний LLM) формирует embedding поста
2. **Vector Index**: для каждого поста ищем ближайшие посты по embedding
   - `topK = knn_neighbors + knn_self_neighbor_offset`
   - учитывается окно свежести `similarity_recency_days`
3. **SIMILAR_TO graph**: создаём связи сходства между постами при `score > similarity_threshold`
4. **Leiden Clustering**: анализируем граф SIMILAR_TO и выделяем тематические кластеры
5. **Community Assignment**: посты получают `cluster_id`; перед материализацией снимаются старые `BELONGS_TO` к `PostCommunity` батчами (`apoc.periodic.iterate`, `HINTGRID_APOC_BATCH_SIZE`), затем создаются узлы `PostCommunity` и новые `BELONGS_TO` тем же механизмом (как в пользовательской ветке)
6. **GC сиротских PostCommunity**: после пересборки `BELONGS_TO` удаляются узлы `PostCommunity` без входящего `BELONGS_TO` (та же логика, что в пользовательской ветке; пример Cypher — в разделе User Clustering выше)
7. **Pruning (опционально)**: удаляем SIMILAR_TO после кластеризации, если включён `prune_after_clustering`

**Пример построения графа сходства и кластеризации:**
```cypher
// 1) Создаем связи SIMILAR_TO через Vector Index
MATCH (p:Post)
WHERE p.embedding IS NOT NULL 
  AND p.createdAt > datetime() - duration({days: $recency_days})
CALL db.index.vector.queryNodes(
    'post_embedding_index',
    $top_k,            // Top K = knn_neighbors + knn_self_neighbor_offset
    p.embedding
)
YIELD node AS neighbor, score
WHERE neighbor.id <> p.id 
  AND score > $threshold
MERGE (p)-[r:SIMILAR_TO]->(neighbor)
SET r.weight = score;

// 2) Проецируем граф постов с SIMILAR_TO (Cypher projection с фильтрацией по дате)
CALL gds.graph.project.cypher(
    'post-similarity',
    'MATCH (p:Post) WHERE p.createdAt > datetime() - duration({days: $recency_days}) RETURN id(p) AS id',
    'MATCH (s:Post)-[r:SIMILAR_TO]->(t:Post) WHERE s.createdAt > datetime() - duration({days: $recency_days}) RETURN id(s) AS source, id(t) AS target, r.weight AS weight'
);

// 3) Leiden кластеризация
CALL gds.leiden.stream('post-similarity')
YIELD nodeId, communityId
WITH nodeId, communityId
SET gds.util.asNode(nodeId).cluster_id = communityId
RETURN count(DISTINCT communityId) AS num_topics;
```

**Проверка embeddings в Neo4j:**
```cypher
MATCH (p:Post)
WHERE p.embedding IS NOT NULL
RETURN p.id AS post_id, size(p.embedding) AS dim
ORDER BY p.id
LIMIT 20;
```

**Преимущества подхода Vector → Graph → Leiden:**
- ✅ Детерминированный результат (стабильные кластеры)
- ✅ Учет структуры сходства, а не только расстояния вектора
- ✅ Естественные тематические сообщества постов

**Оптимизация памяти: Cypher Graph Projection с фильтрацией по дате**

Для снижения использования памяти GDS при кластеризации постов используется Cypher projection с фильтрацией по `createdAt`:

```cypher
// Проецируем только свежие посты (последние similarity_recency_days дней)
CALL gds.graph.project.cypher(
  'post-graph',
  'MATCH (p:Post) WHERE p.createdAt > datetime() - duration({days: $recency_days}) RETURN id(p) AS id',
  'MATCH (s:Post)-[r:SIMILAR_TO]->(t:Post) WHERE s.createdAt > datetime() - duration({days: $recency_days}) RETURN id(s) AS source, id(t) AS target, r.weight AS weight'
)
```

Это значительно снижает использование heap памяти GDS, игнорируя исторические данные.

### 3. PageRank для влиятельных постов

**Назначение**: Выявление наиболее центральных/прототипичных постов внутри каждого сообщества.

**Характеристики:**
- **Алгоритм**: PageRank на графе SIMILAR_TO
- **Провайдер**: Neo4j GDS
- **Свойство**: `Post.pagerank` — при индексации новый узел создаётся с начальным значением `0`, после чего GDS перезаписывает его результатом `pageRank.write` (отдельного дозаполнения по уже существующим узлам нет)

**Как работает:**
1. При загрузке поста в граф задаётся начальный `pagerank` (до кластеризации)
2. После кластеризации постов проецируется граф SIMILAR_TO (с фильтрацией по дате)
3. Запускается PageRank с учетом весов связей; результаты записываются в `Post.pagerank`
4. При генерации ленты посты с высоким PageRank получают дополнительный boost

**Пример запуска PageRank:**
```cypher
CALL gds.pageRank.write('pagerank-graph', {
  writeProperty: 'pagerank',
  relationshipWeightProperty: 'weight',
  maxIterations: 20,
  dampingFactor: 0.85
})
YIELD nodePropertiesWritten, ranIterations, didConverge;
```

**Использование в скоринге ленты:**
```cypher
WITH p, 
     interest_score * $interest_weight + 
     log10(popularity + $popularity_smoothing) * $popularity_weight + 
     ($recency_numerator / (age_hours / 24.0 + $recency_smoothing)) * $recency_weight + 
     COALESCE(p.pagerank, 0.0) * $pagerank_weight AS score
```

**Преимущества:**
- ✅ Рекомендует "лучшие" посты темы, а не только самые новые
- ✅ Выделяет прототипичные посты внутри сообщества
- ✅ Улучшает качество рекомендаций

### 4. Сходство сообществ для умной серендипити

**Назначение**: Замена случайной серендипити на рекомендации на основе перекрытия сообществ.

**Характеристики:**
- **Алгоритм**: Node Similarity (Jaccard) по **общим участникам** между узлами `UserCommunity`
- **Доменный граф**: `(User)-[:BELONGS_TO]->(UserCommunity)` — как в операционной модели
- **Провайдер**: Neo4j GDS
- **Связи**: `SIMILAR_COMMUNITY` между узлами `UserCommunity` (вес — score сходства)
- **Посты и топики**: сходство **постов** строится отдельно (эмбеддинги, `SIMILAR_TO`, Leiden для `PostCommunity`); **Node Similarity по графу Post ↔ PostCommunity** в приложении не используется — не путать с этим шагом

**Как работает:**
1. В каталог GDS попадает **двудольная проекция** того же `BELONGS_TO`, но рёбра для GDS задаются как **UserCommunity → User** (`source = id(uc)`, `target = id(u)` в `rel_query` для `gds.graph.project.cypher`). Так **первая доля** графа в смысле GDS — это `UserCommunity`, и Jaccard считается между племенами по пересечению множеств пользователей. Если бы в проекции оставить направление User → UserCommunity, Node Similarity сравнивал бы **пользователей** по пересечению племён, а не племена между собой — связи `SIMILAR_COMMUNITY` между `UserCommunity` не появились бы.
2. Запускается `gds.nodeSimilarity.write` для записи пар похожих `UserCommunity` и веса
3. Создаются связи `SIMILAR_COMMUNITY` с score (0.0–1.0)
4. При серендипити: если UserCommunity 1 похожа на UserCommunity 2, и UC2 интересуется PostCommunity X, то UC1 получает рекомендации из PC X

**Пример вычисления сходства (согласован с `gds.graph.project.cypher` в приложении):**
```cypher
// Имя графа в каталоге GDS:
// - без worker: 'uc-similarity'
// - с neo4j_worker_label: '{worker_label}-uc-similarity'
// $noise — зарезервированный noise_community_id (исключается из узлов сообществ)

CALL gds.graph.project.cypher(
  'uc-similarity',
  'MATCH (u:User) RETURN id(u) AS id
   UNION
   MATCH (uc:UserCommunity) WHERE uc.id <> $noise RETURN id(uc) AS id',
  'MATCH (u:User)-[:BELONGS_TO]->(uc:UserCommunity)
   WHERE uc.id <> $noise
   RETURN id(uc) AS source, id(u) AS target'
);

CALL gds.nodeSimilarity.write('uc-similarity', {
  writeRelationshipType: 'SIMILAR_COMMUNITY',
  writeProperty: 'score',
  topK: 5,
  similarityCutoff: 0.0
});

CALL gds.graph.drop('uc-similarity');
```

Упрощённый вариант `CALL gds.graph.project(..., { BELONGS_TO: { orientation: 'UNDIRECTED' } })` в документации GDS встречается как иллюстрация; **в HintGrid он не используется** — ориентация рёбер в проекции должна явно ставить `UserCommunity` в роль исхода, иначе меняется набор узлов, между которыми пишется сходство.

**Важно:**
- Если одновременно отсутствуют узлы `User` и пригодные узлы `UserCommunity` (все сообщества — лишь с `id = noise_community_id` или узлов сообществ нет), **шаг не выполняется**: проекция в GDS не вызывается, пайплайн не падает (пустой `node_query` для `gds.graph.project.cypher` недопустим).
- При использовании `neo4j_worker_label` имя графа в каталоге GDS становится `{worker_label}-uc-similarity` (изоляция при параллельных тестах)
- Неверные `source`/`target` в `rel_query` (например User → UserCommunity) приводят к сравнению **User** вместо **UserCommunity** — регрессия: нет рёбер `SIMILAR_COMMUNITY` между племенами
- `similarityCutoff: 0.0` включает в топ-K и пары с нулевым сходством (полезно для серендипити)

**Использование в серендипити:**
```cypher
MATCH (uc1:UserCommunity)-[sim:SIMILAR_COMMUNITY]->(uc2:UserCommunity)
MATCH (uc2)-[i:INTERESTED_IN]->(pc:PostCommunity)
WHERE NOT (uc1)-[:INTERESTED_IN]->(pc)
WITH uc1, pc, sim.score * i.score AS combined_score
ORDER BY combined_score DESC
LIMIT $serendipity_limit
MERGE (uc1)-[s:INTERESTED_IN]->(pc)
SET s.score = $serendipity_score * combined_score, s.serendipity = true
```

**Преимущества:**
- ✅ Умная серендипити вместо случайной
- ✅ Рекомендации из смежных сообществ с перекрывающимися пользователями
- ✅ Более релевантные открытия

### 5. Интеграция: Community-to-Community

**Критически важно**: Связь между UserCommunity и PostCommunity через агрегированные интересы.

Цепочка интеграции: User → UserCommunity → INTERESTED_IN → PostCommunity → Post.

**Процесс:**
1. Leiden на графе FOLLOWS + INTERACTS_WITH → `User.cluster_id`, создаём UserCommunity узлы
2. Embedding Provider → `Post.embedding` (FastText: 128, LLM: llm_dimensions)
3. Vector Index → SIMILAR_TO граф
4. Leiden на SIMILAR_TO → `Post.cluster_id`, создаём PostCommunity узлы
5. **Learn Interests**: Анализ взаимодействий между сообществами с экспоненциальным затуханием
   - Какие UserCommunity взаимодействуют с постами из каких PostCommunity?
   - Вход в расчёт — **рёбра взаимодействий** (`FAVORITED`, `REBLOGGED`, …): для каждого существующего в графе типа строится ветка `MATCH (u)-[тип]->(p)` и `BELONGS_TO` к сообществам; ветки объединяются `UNION ALL` и суммируются (без декартова произведения всех пар пользователь–пост внутри пары сообществ)
   - Веса взаимодействий: LIKE=1.0, REBLOG=1.5, REPLY=3.0 (настраиваются)
   - Каждое взаимодействие вносит вклад `exp(-ln2 · age / decay_half_life_days)`
   - Создание `INTERESTED_IN` с весом (score) и TTL
   - **Полная перестройка** (команда `run`) или **инкрементальное обновление** (команда `refresh`)
6. **Cleanup**: Удаление устаревших INTERESTED_IN (expires_at < now() или score < 0.01)
7. **Рекомендации**: `User → UserCommunity -[INTERESTED_IN]-> PostCommunity → Posts`

**Почему так:**
- ✅ Интеграция через агрегированные интересы сообществ
- ✅ Interpretability: понятно, почему рекомендован пост
- ✅ Serendipity: случайные открытия через соседние сообщества
- ✅ Freshness: экспоненциальный decay обеспечивает плавное устаревание, инкрементальный refresh — быстрое обновление
- ✅ Stability: при изменении ID кластеров связи перестраиваются

---

## Основные SQL запросы

### Загрузка данных из PostgreSQL (Mastodon)

#### Поиск Mastodon user id по @username@domain

```sql
-- Пример входа: @username@mastodon.social
-- Локальный аккаунт: @username (domain = NULL)
-- Разбираем на username + domain (domain может быть NULL для локальных аккаунтов)
SELECT id
FROM accounts
WHERE lower(username) = lower(:username)
  AND (
    (domain IS NULL AND :domain IS NULL)
    OR lower(domain) = lower(:domain)
  )
LIMIT 1;
```

#### Поиск internal `statuses.id` по URL или публичному числу из URL

В Mastodon поле `statuses.id` — snowflake (внутренний первичный ключ). Число в конце веб-URL или в пути `.../statuses/<число>` **часто не совпадает** с `statuses.id`; однозначная строка поста хранится в `uri`. Команда `get-post-info <ссылка>` повторяет ту же логику в приложении: сначала попытка по PK (если ввод — только цифры и такая строка есть), иначе поиск по `uri`.

```sql
-- По фрагменту из URL (публичный id или подстрока uri)
SELECT id, account_id, uri
FROM statuses
WHERE uri LIKE '%' || :needle || '%'
  AND deleted_at IS NULL
LIMIT 2;
-- Ноль строк — не найдено; две и более — неоднозначно (нужна более длинная ссылка)

-- По известному internal id (snowflake из БД)
SELECT id, account_id, uri
FROM statuses
WHERE id = :internal_id
  AND deleted_at IS NULL;
```

#### Инкрементальная загрузка статусов

**Важно**: `visibility` - это integer enum (0=public, 1=unlisted, 2=private, 3=direct). Значение настраивается параметром `mastodon_public_visibility`.

```sql
SELECT 
    id,
    account_id,
    text,
    language,
    created_at
FROM statuses
WHERE id > :last_id
  AND deleted_at IS NULL
  AND visibility = :public_visibility
  AND reblog_of_id IS NULL
ORDER BY id ASC
LIMIT :limit;
```

#### Параметр `--load-since`: ограничение окна загрузки

Параметр `--load-since` позволяет ограничить глубину загрузки данных по времени. Формат значения: `<число>d`, где `d` означает дни (например, `30d` — последние 30 дней).

**Область применения:**
- ✅ Статусы (посты)
- ✅ Favourites (лайки)
- ✅ Reblogs (репосты)
- ✅ Replies (ответы)
- ❌ Follows (подписки) — загружаются всегда полностью
- ❌ Blocks (блоки) — загружаются всегда полностью
- ❌ Mutes (муты) — загружаются всегда полностью

**Логика работы:**

1. **Параметр не задан** (`--load-since` отсутствует):
   - Стандартное инкрементальное поведение: загрузка от `last_processed_*_id` до конца данных.

2. **Параметр задан, инкрементного состояния нет** (первый запуск или после `clean`):
   - Вычисляется дата `since_date = now() - load_since`.
   - Загружаются только записи с `created_at >= since_date` и `id > 0`.
   - После завершения сохраняется последний обработанный ID.

3. **Параметр задан, инкрементное состояние есть** (повторный запуск):
   - Вычисляется новая точка старта `since_date = now() - load_since`.
   - Используется `max(saved_last_id, min_id_from_date)` для предотвращения пропуска данных при прерывании процесса.
   - Загружаются записи с `id > max(saved_last_id, min_id_from_date)`.
   - После завершения инкрементное состояние **обновляется** последним обработанным ID.

**Пример SQL с учётом `--load-since`:**

```sql
-- Загрузка статусов с ограничением по времени
SELECT 
    id,
    account_id,
    text,
    language,
    created_at
FROM statuses
WHERE created_at >= :since_date
  AND id > :last_id
  AND deleted_at IS NULL
  AND visibility = :public_visibility
  AND reblog_of_id IS NULL
ORDER BY id ASC
LIMIT :limit;
```

**Пример использования:**

Параметр `load_since` задаётся в формате `Nd` и работает совместно с `--dry-run`. Подробности по запуску см. в разделе **Установка и настройка**.

**Важно:**
- Параметр полезен для быстрого первоначального прогона на свежих данных.
- При каждом запуске с `--load-since` окно пересчитывается относительно текущего времени.
- Пользователи (blocks, mutes) загружаются **всегда полностью**, независимо от параметра. FOLLOWS больше не загружаются отдельно, они включаются в INTERACTS_WITH через SQL.

Статусы используются и для взаимодействий:

**Важно:** Репосты и ответы загружаются единым потоком вместе со всеми статусами и создаются как полноценные узлы Post с эмбеддингами, что позволяет им участвовать в кластеризации и рекомендациях.

```sql
-- REBLOGGED: аккаунт репостит оригинальный статус
-- Включаем text и language для создания Post узла с эмбеддингом
SELECT
    id,
    account_id,
    text,
    language,
    reblog_of_id,
    created_at
FROM statuses
WHERE id > :last_id
  AND reblog_of_id IS NOT NULL
  AND deleted_at IS NULL
ORDER BY id ASC
LIMIT :limit;

-- REPLIED: аккаунт отвечает на оригинальный статус
-- Включаем text и language для создания Post узла с эмбеддингом
SELECT
    id,
    account_id,
    text,
    language,
    in_reply_to_id,
    created_at
FROM statuses
WHERE id > :last_id
  AND in_reply_to_id IS NOT NULL
  AND deleted_at IS NULL
ORDER BY id ASC
LIMIT :limit;
```

#### Загрузка favourites (лайков)

```sql
SELECT 
    id,
    account_id,
    status_id,
    created_at
FROM favourites
WHERE id > :last_id
ORDER BY id ASC
LIMIT 10000;
```

#### Загрузка подписок

```sql
SELECT 
    id,
    account_id,
    target_account_id,
    created_at
FROM follows
WHERE id > :last_id
ORDER BY id ASC
LIMIT 10000;
```

#### Загрузка блоков

```sql
SELECT id, account_id, target_account_id, 'block' AS type, created_at
FROM blocks
WHERE id > :last_id
ORDER BY id ASC
LIMIT 10000;
```

#### Загрузка мутов

```sql
SELECT id, account_id, target_account_id, 'mute' AS type, created_at
FROM mutes
WHERE id > :last_id
ORDER BY id ASC
LIMIT 10000;
```

**Важно:** Блоки и муты загружаются **отдельными запросами** с независимыми курсорами
`last_block_id` и `last_mute_id`, так как таблицы `blocks` и `mutes` имеют
независимые последовательности `id`.

#### Загрузка активности пользователей (lastActive)

Стриминговая загрузка данных об активности пользователей с поддержкой курсора для возобновления после прерывания (Ctrl+C).

```sql
-- Загрузка активности пользователей с курсором
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
) >= NOW() - :active_days * INTERVAL '1 day'
  AND a.id > :last_account_id
ORDER BY a.id ASC;
```

**Важно:**
- Курсор `last_activity_account_id` **сбрасывается в 0** при каждом полном запуске (`hintgrid run`), так как `lastActive` — мутабельное значение (меняется при каждом входе пользователя)
- Курсор используется **только для resume** при Ctrl+C в рамках текущего запуска, чтобы не пересканировать уже обработанных пользователей
- При следующем полном запуске все активные пользователи пересканируются заново для актуализации `lastActive`
- Загрузка через server-side cursor (стриминг), данные не загружаются в память целиком
- Запрос отсортирован по `a.id ASC`, что обеспечивает монотонность курсора
- `is_local` определяет локальность: `accounts.domain IS NULL` → локальный пользователь
- `locale` — язык интерфейса Mastodon (`users.locale`); в графе сохраняется как нормализованный `User.uiLanguage`
- `chosen_languages` — массив предпочитаемых языков из таблицы `users` (только для локальных); в графе — `User.languages` (нормализованные коды)

#### Загрузка агрегированных взаимодействий пользователей (INTERACTS_WITH)

Взаимодействия пользователей агрегируются в PostgreSQL из пяти источников (favourites, replies, reblogs, mentions, follows) и загружаются **инкрементально** как взвешенные рёбра INTERACTS_WITH между пользователями. Каждый компонент имеет параметризуемый вес, что позволяет гибко контролировать важность различных типов взаимодействий для кластеризации пользователей.

**Инкрементальная модель** использует 4 отдельных курсора (независимых от курсоров загрузки данных):
- `last_interaction_favourite_id` — последний обработанный `favourites.id`
- `last_interaction_status_id` — последний обработанный `statuses.id` (для replies И reblogs, т.к. это одна таблица)
- `last_interaction_mention_id` — последний обработанный `mentions.id`
- `last_interaction_follow_id` — последний обработанный `follows.id`

```sql
-- Инкрементальные user-user взаимодействия с курсорами и локальными максимумами
SELECT
    source_id,
    target_id,
    SUM(weight) AS total_weight,
    MAX(max_favourite_id) AS max_favourite_id,
    MAX(max_status_id) AS max_status_id,
    MAX(max_mention_id) AS max_mention_id,
    MAX(max_follow_id) AS max_follow_id
FROM (
    -- Лайки: count(*) * likes_weight
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

    -- Ответы: count(*) * replies_weight (курсор last_interaction_status_id)
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

    -- Репосты: count(*) * reblogs_weight (тот же курсор last_interaction_status_id)
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

    -- Упоминания: count(*) * mentions_weight
    SELECT s.account_id, m.account_id,
           count(*) * %(mentions_weight)s,
           NULL::bigint, NULL::bigint, MAX(m.id), NULL::bigint
    FROM mentions m
    JOIN statuses s ON m.status_id = s.id
    WHERE m.id > %(last_interaction_mention_id)s
      AND s.account_id != m.account_id AND m.silent = false
    GROUP BY s.account_id, m.account_id

    UNION ALL

    -- Подписки: follows_weight
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
```

**Оптимизация SQL:** Каждый подзапрос использует `MAX(id)` в `GROUP BY` для получения локального максимума ID. Это дешевая агрегация, не требующая оконных функций. `NULL::bigint` используется для столбцов, не относящихся к данному источнику. Внешний `MAX(...)` по каждому столбцу собирает глобальные максимумы на уровне строки.

**Важно:**
- Self-взаимодействия (source == target) исключаются
- Silent-упоминания (`m.silent = true`) игнорируются
- Replies и reblogs используют один курсор `last_interaction_status_id` (одна таблица `statuses`)
- FOLLOWS включён в INTERACTS_WITH через SQL с параметризуемым весом `follows_weight`
- Если `follows_weight = 0.0`, FOLLOWS не включается в агрегацию
- Все компоненты имеют настраиваемые веса: `likes_weight`, `replies_weight`, `reblogs_weight`, `mentions_weight`, `follows_weight`
- Курсоры обновляются атомарно в Neo4j (см. раздел «Создание INTERACTS_WITH связей»)

#### Загрузка статистики постов (status_stats)

Статистика постов загружается инкрементально по `status_id` и используется как сигнал популярности на существующих Post узлах.

```sql
-- Инкрементальная загрузка статистики постов
SELECT status_id AS id,
       COALESCE(favourites_count, 0)
           + COALESCE(untrusted_favourites_count, 0) AS total_favourites,
       COALESCE(reblogs_count, 0)
           + COALESCE(untrusted_reblogs_count, 0) AS total_reblogs,
       replies_count AS total_replies
FROM status_stats
WHERE status_id > :last_id
ORDER BY status_id;
```

**Важно:**
- Учитываются как локальные, так и федеративные (untrusted) счётчики
- Свойства `totalFavourites`, `totalReblogs`, `totalReplies` устанавливаются только на существующих Post узлах (посты без эмбеддингов игнорируются)

#### Загрузка закладок (bookmarks)

Закладки — сильнейший неявный сигнал интереса (пользователь сохраняет пост для повторного чтения). Загружаются инкрементально.

```sql
-- Инкрементальная загрузка закладок
SELECT id, account_id, status_id, created_at
FROM bookmarks
WHERE id > :last_id
ORDER BY id ASC;

-- Или с фильтром по дате (при load_since)
SELECT id, account_id, status_id, created_at
FROM bookmarks
WHERE id > :last_id
  AND created_at >= :since_date
ORDER BY id ASC;
```

**Важно:**
- Связь `BOOKMARKED` создаётся только для существующих Post узлов (с эмбеддингами)
- Вес закладок в расчёте `INTERESTED_IN` выше, чем у лайков (`bookmark_weight`, по умолчанию 2.0)

---

## Neo4j операции

### Управление состоянием (Singleton Node Pattern)

#### Инициализация состояния

```cypher
// Создание узла AppState при первом запуске
MERGE (s:AppState {id: 'main'})
ON CREATE SET
    s.last_processed_status_id = 0,
    s.last_processed_favourite_id = 0,
    s.last_processed_follow_id = 0,
    s.last_processed_block_id = 0,
    s.last_processed_mute_id = 0,
    s.last_processed_reblog_id = 0,
    s.last_processed_reply_id = 0,
    s.last_processed_activity_account_id = 0,
    s.last_processed_feed_user_id = 0,
    s.last_processed_status_stats_id = 0,
    s.last_interests_rebuild_at = '',
    s.embedding_signature = '',
    s.updated_at = timestamp()
RETURN s;
```

#### Чтение состояния (Load State)

```cypher
// Получение курсоров для инкрементальной загрузки
MATCH (s:AppState {id: 'main'})
RETURN
    s.last_processed_status_id AS last_status_id,
    s.last_processed_favourite_id AS last_favourite_id,
    s.last_processed_follow_id AS last_follow_id,
    s.last_processed_block_id AS last_block_id,
    s.last_processed_mute_id AS last_mute_id,
    s.last_processed_reblog_id AS last_reblog_id,
    s.last_processed_reply_id AS last_reply_id,
    s.last_processed_activity_account_id AS last_activity_account_id,
    s.last_processed_feed_user_id AS last_feed_user_id,
    s.last_processed_status_stats_id AS last_status_stats_id,
    s.last_interests_rebuild_at AS last_interests_rebuild_at,
    s.embedding_signature AS embedding_signature,
    s.updated_at AS last_updated;
```

#### Атомарное обновление состояния (Save State)

```cypher
// Обновление курсоров после успешной загрузки
MATCH (s:AppState {id: 'main'})
SET s.last_processed_status_id = $last_status_id,
    s.last_processed_favourite_id = $last_favourite_id,
    s.last_processed_follow_id = $last_follow_id,
    s.last_processed_block_id = $last_block_id,
    s.last_processed_mute_id = $last_mute_id,
    s.last_processed_reblog_id = $last_reblog_id,
    s.last_processed_reply_id = $last_reply_id,
    s.last_processed_activity_account_id = $last_activity_account_id,
    s.last_processed_feed_user_id = $last_feed_user_id,
    s.last_processed_status_stats_id = $last_status_stats_id,
    s.last_interests_rebuild_at = $last_interests_rebuild_at,
    s.embedding_signature = $embedding_signature,
    s.updated_at = timestamp()
RETURN s;
```

#### Унифицированное атомарное обновление состояния для всех инкрементальных загрузок

Все инкрементальные загрузки используют **атомарное обновление состояния** в одной транзакции с вставкой данных. Это предотвращает потерю данных при прерывании процесса (kill -9).

**Применяется к:**
- `merge_posts` → `last_processed_status_id`
- `merge_favourites` → `last_processed_favourite_id`
- `merge_bookmarks` → `last_processed_bookmark_id`
- `merge_status_stats` → `last_processed_status_stats_id`
- `update_user_activity` → `last_processed_activity_account_id`
- `merge_blocks` → `last_processed_block_id`
- `merge_mutes` → `last_processed_mute_id`

**Унифицированный паттерн:**

1. **Вставка данных и обновление состояния в одном запросе:**
```cypher
UNWIND $batch AS row
-- Вставка/обновление данных (посты, связи, свойства)
...
WITH $batch_max_id AS batch_max_id
MATCH (s:AppState {id: $state_id})
SET s.last_processed_<entity>_id = CASE 
  WHEN batch_max_id > s.last_processed_<entity>_id 
  THEN batch_max_id 
  ELSE s.last_processed_<entity>_id 
END
RETURN s.last_processed_<entity>_id AS new_cursor
```

2. **Пример для favourites:**
```cypher
UNWIND $batch AS row
CALL apoc.merge.node($user_labels, {id: row.account_id}, {}, {}) YIELD node AS u
WITH u, row
MATCH (p:Post {id: row.status_id})
MERGE (u)-[f:FAVORITED]->(p)
ON CREATE SET f.at = datetime(row.created_at)
WITH $batch_max_id AS batch_max_id
MATCH (s:AppState {id: $state_id})
SET s.last_processed_favourite_id = CASE 
  WHEN batch_max_id > s.last_processed_favourite_id 
  THEN batch_max_id 
  ELSE s.last_processed_favourite_id 
END
RETURN s.last_processed_favourite_id AS new_cursor
```

3. **Пример для blocks:**
```cypher
UNWIND $batch AS row
CALL apoc.merge.node($user_labels, {id: row.account_id}, {}, {}) YIELD node AS u1
CALL apoc.merge.node($user_labels, {id: row.target_account_id}, {}, {}) YIELD node AS u2
MERGE (u1)-[:HATES_USER]->(u2)
WITH $batch_max_id AS batch_max_id
MATCH (s:AppState {id: $state_id})
SET s.last_processed_block_id = CASE 
  WHEN batch_max_id > s.last_processed_block_id 
  THEN batch_max_id 
  ELSE s.last_processed_block_id 
END
RETURN s.last_processed_block_id AS new_cursor
```

4. **Пример для mutes:**
```cypher
UNWIND $batch AS row
CALL apoc.merge.node($user_labels, {id: row.account_id}, {}, {}) YIELD node AS u1
CALL apoc.merge.node($user_labels, {id: row.target_account_id}, {}, {}) YIELD node AS u2
MERGE (u1)-[:HATES_USER]->(u2)
WITH $batch_max_id AS batch_max_id
MATCH (s:AppState {id: $state_id})
SET s.last_processed_mute_id = CASE 
  WHEN batch_max_id > s.last_processed_mute_id 
  THEN batch_max_id 
  ELSE s.last_processed_mute_id 
END
RETURN s.last_processed_mute_id AS new_cursor
```

**Оптимизация производительности:**
- Вместо вычисления `max()` по всему батчу (O(n)), используется последний элемент батча (O(1))
- Данные гарантированно отсортированы благодаря `ORDER BY id ASC` в PostgreSQL запросе
- Последний элемент батча = максимальный ID
- Добавлена проверка сортировки для безопасности (логирование предупреждения при нарушении)

**Исправление логики `load_since`:**
- При использовании `load_since` используется `max(saved_last_id, min_id_from_date)` вместо игнорирования сохраненного состояния
- Это предотвращает пропуск данных при прерывании процесса и повторном запуске

**Преимущества:**
- **Атомарность**: Если вставка данных упадет, обновление `AppState` тоже откатится
- **Защита от потери данных**: При прерывании процесса курсор не обновляется, если данные не были вставлены
- **Производительность**: Один сетевой вызов вместо двух
- **Безопасность конкуренции**: Neo4j берет write-lock на узел `AppState`
- **Унификация**: Единый подход для всех инкрементальных загрузок через `_run_entity_loader`

**Инкрементальная загрузка INTERACTS_WITH:**

`merge_interactions` использует атомарное обновление 4 курсоров (`last_processed_interaction_favourite_id`, `last_processed_interaction_status_id`, `last_processed_interaction_mention_id`, `last_processed_interaction_follow_id`) в AppState в той же транзакции с merge данных. Глобальные максимумы ID вычисляются через `reduce()` в Cypher, без отдельного Python-вычисления. Replies и reblogs используют один курсор `last_processed_interaction_status_id` (одна таблица `statuses`).

**Преимущества подхода Singleton Node:**

| Характеристика | JSON-файл | Neo4j AppState |
|----------------|-----------|-------------------|
| **Атомарность** | ❌ Нет. Если скрипт упадет после записи в БД, но до сохранения JSON, данные дублируются | ✅ Да. Обновление курсора и данных в одной транзакции |
| **Удобство деплоя** | ❌ Требует пробрасывания volume/файла в контейнер | ✅ Состояние живет внутри данных. Бэкап базы автоматически включает состояние |
| **Персистентность** | ⚠️ Зависит от файловой системы | ✅ Neo4j WAL + снэпшоты обеспечивают надежность |
| **Редактирование** | ✅ Легко (текстовый редактор) | ⚠️ Требует Cypher-запроса |
| **Идиоматичность** | ⚠️ Стандарт для CLI-утилит | ✅ Стандарт для сервисов с БД |
| **Stateless контейнер** | ❌ Нет (нужен volume) | ✅ Да (все в базе) |

**Надежность**: Neo4j обеспечивает персистентность через Write-Ahead Logging (WAL) и периодические снэпшоты. Сохраненное состояние переживает перезагрузку контейнера так же надежно, как основные данные.

**Примечание:** Блоки и муты имеют **отдельные курсоры** (`last_block_id` и `last_mute_id`),
поскольку таблицы `blocks` и `mutes` имеют независимые последовательности `id`.

### Создание узлов и связей

#### Batch MERGE постов

```cypher
UNWIND $batch AS row
MERGE (p:Post {id: row.id})
ON CREATE SET
  p.authorId = row.authorId,
  p.text = row.text,
  p.language = row.language,
  p.embedding = row.embedding,
  p.createdAt = row.createdAt
ON MATCH SET
  p.embedding = row.embedding

WITH p, row
MERGE (u:User {id: row.authorId})
MERGE (u)-[:WROTE]->(p)
```

#### Создание FAVORITED связей

**Важно:** Используется MATCH вместо MERGE для Post, чтобы не создавать «пустые» Post-узлы для постов без эмбеддингов. Если Post не найден (нет эмбеддинга), связь не создаётся — сигнал взаимодействия сохраняется через INTERACTS_WITH.

```cypher
UNWIND $batch AS row
MERGE (u:User {id: row.account_id})
WITH u, row
MATCH (p:Post {id: row.status_id})
MERGE (u)-[f:FAVORITED]->(p)
ON CREATE SET f.at = row.created_at
```

REBLOGGED и REPLIED формируются из таблицы `statuses`:

**Важно:** Ответы и репосты создаются как полноценные узлы Post с эмбеддингами, а не только как отношения. Это позволяет им участвовать в кластеризации и рекомендациях. Связи REBLOGGED/REPLIED к оригинальным постам создаются только если целевой Post уже существует в Neo4j (имеет эмбеддинг). Сигнал взаимодействия между пользователями сохраняется через INTERACTS_WITH.

```cypher
// REBLOGGED: используем MATCH для оригинального поста (без создания пустых узлов)
UNWIND $batch AS row
MERGE (u:User {id: row.account_id})
WITH u, row
MATCH (p:Post {id: row.reblog_of_id})
MERGE (u)-[r:REBLOGGED]->(p)
ON CREATE SET r.at = datetime(row.created_at);

// REPLIED: используем MATCH для оригинального поста (без создания пустых узлов)
UNWIND $batch AS row
MERGE (u:User {id: row.account_id})
WITH u, row
MATCH (p:Post {id: row.in_reply_to_id})
MERGE (u)-[r:REPLIED]->(p)
ON CREATE SET r.at = datetime(row.created_at);
```

#### Создание HATES_USER связей

```cypher
UNWIND $batch AS row
MERGE (u1:User {id: row.account_id})
MERGE (u2:User {id: row.target_account_id})
MERGE (u1)-[:HATES_USER]->(u2)
```

#### Создание INTERACTS_WITH связей

Агрегированные взаимодействия между пользователями (из favourites, replies, reblogs, mentions, follows), вычисленные в PostgreSQL. Загрузка инкрементальная — веса накапливаются через `ON MATCH SET r.weight = r.weight + row.total_weight`.

Курсоры обновляются атомарно в той же транзакции Neo4j через `reduce()` для вычисления глобальных максимумов из батча:

```cypher
-- Вычисление глобальных максимумов ID из батча через reduce()
WITH $batch AS data,
  reduce(m = 0, x IN $batch |
    CASE WHEN coalesce(x.max_favourite_id, 0) > m
    THEN coalesce(x.max_favourite_id, 0) ELSE m END) AS batch_max_fav,
  reduce(m = 0, x IN $batch |
    CASE WHEN coalesce(x.max_status_id, 0) > m
    THEN coalesce(x.max_status_id, 0) ELSE m END) AS batch_max_stat,
  reduce(m = 0, x IN $batch |
    CASE WHEN coalesce(x.max_mention_id, 0) > m
    THEN coalesce(x.max_mention_id, 0) ELSE m END) AS batch_max_ment,
  reduce(m = 0, x IN $batch |
    CASE WHEN coalesce(x.max_follow_id, 0) > m
    THEN coalesce(x.max_follow_id, 0) ELSE m END) AS batch_max_foll
-- Атомарное обновление 4 курсоров в AppState (CASE WHEN для монотонности)
MATCH (state:AppState {id: $state_id})
SET state.last_processed_interaction_favourite_id = CASE
      WHEN batch_max_fav > state.last_processed_interaction_favourite_id
      THEN batch_max_fav ELSE state.last_processed_interaction_favourite_id END,
    state.last_processed_interaction_status_id = CASE
      WHEN batch_max_stat > state.last_processed_interaction_status_id
      THEN batch_max_stat ELSE state.last_processed_interaction_status_id END,
    state.last_processed_interaction_mention_id = CASE
      WHEN batch_max_ment > state.last_processed_interaction_mention_id
      THEN batch_max_ment ELSE state.last_processed_interaction_mention_id END,
    state.last_processed_interaction_follow_id = CASE
      WHEN batch_max_foll > state.last_processed_interaction_follow_id
      THEN batch_max_foll ELSE state.last_processed_interaction_follow_id END
-- Merge данных в граф
WITH data
UNWIND data AS row
CALL apoc.merge.node(['User'], {id: row.source_id}, {}, {}) YIELD node AS u1
CALL apoc.merge.node(['User'], {id: row.target_id}, {}, {}) YIELD node AS u2
MERGE (u1)-[r:INTERACTS_WITH]->(u2)
ON CREATE SET r.weight = row.total_weight
ON MATCH SET r.weight = r.weight + row.total_weight
```

**Важно:**
- `reduce()` вычисляет глобальные максимумы ID прямо в Cypher, без Python-вычислений
- `coalesce(x.max_favourite_id, 0)` обрабатывает NULL-значения (строка содержит только один не-NULL max_*_id)
- `CASE WHEN ... > state... THEN ... ELSE state... END` гарантирует монотонное возрастание курсоров
- Обновление курсоров и merge данных происходят в одной транзакции (атомарно)
- При перезапуске скрипт продолжит с места остановки благодаря курсорам в `AppState`

#### Обновление статистики постов (status_stats)

Свойства популярности устанавливаются только на существующих Post узлах. Посты без эмбеддингов (не существующие в Neo4j) игнорируются.

```cypher
UNWIND $batch AS row
MATCH (p:Post {id: row.id})
SET p.totalFavourites = row.total_favourites,
    p.totalReblogs = row.total_reblogs,
    p.totalReplies = row.total_replies
```

### Аналитика и кластеризация

Перед и после этапа аналитики пайплайн проверяет, **есть ли уже материализованные сообщества**: наличие хотя бы одного узла `UserCommunity` и хотя бы одного узла `PostCommunity`. Это не обращается к свойству `cluster_id` на `User`/`Post`, пока оно ни разу не записывалось в граф (иначе Memgraph может выдать предупреждение GQL `01N52` о несуществующем ключе свойства). После команды `clean --clusters` узлы сообществ удаляются, и проверка корректно показывает отсутствие кластеров.

```cypher
// Признак: пользовательские сообщества уже созданы после кластеризации
MATCH (uc:UserCommunity)
RETURN count(uc) AS user_communities;

// Признак: постовые сообщества уже созданы после кластеризации
MATCH (pc:PostCommunity)
RETURN count(pc) AS post_communities;
```

Для **сводной статистики исходящих INTERACTS_WITH по пользователям** (среднее, медиана, максимум, число изолированных) используется выражение `COUNT { … }` на пользователя, а не связка `OPTIONAL MATCH` + `count(r)`. Так Neo4j не формирует предупреждение GQL `01G11` («null value eliminated in set function») при последующих `avg` / `percentileCont` по степеням исхода.

```cypher
MATCH (u:User)
WITH u, COUNT { (u)-[:INTERACTS_WITH]->() } AS out_degree
RETURN
  avg(out_degree) AS avg_interacts,
  percentileCont(out_degree, 0.5) AS median_interacts,
  max(out_degree) AS max_interacts,
  sum(CASE WHEN out_degree = 0 THEN 1 ELSE 0 END) AS isolated_users;
```

#### User Clustering (Leiden Community Detection)

**Имя графа зависит от `neo4j_worker_label`:**
- Если `worker_label` задан: `{worker_label}-user-graph`
- Если `worker_label` не задан: `user-graph`

**Fallback поведение:**
- Если нет INTERACTS_WITH связей, всем пользователям присваивается `cluster_id = 0`
- Это обеспечивает корректную работу системы даже при отсутствии взаимодействий

```cypher
// Проекция графа пользователей (только INTERACTS_WITH)
// FOLLOWS включён в INTERACTS_WITH через SQL с параметризуемым весом follows_weight
// Имя графа: 'user-graph' или '{worker_label}-user-graph'
CALL gds.graph.project(
    'user-graph',  // или '{worker_label}-user-graph'
    'User',
    {
        INTERACTS_WITH: {
            orientation: 'UNDIRECTED',
            properties: 'weight'
        }
    }
);

// Leiden кластеризация с учётом весов INTERACTS_WITH
// Все рёбра имеют свойство weight, поэтому relationshipWeightProperty всегда используется
CALL gds.leiden.write('user-graph', {
    writeProperty: 'cluster_id',
    relationshipWeightProperty: 'weight',
    gamma: $gamma,
    maxLevels: $max_levels
})
YIELD nodePropertiesWritten, communityCount, modularity
RETURN nodePropertiesWritten, communityCount, modularity;

// Создание UserCommunity узлов и связей BELONGS_TO
// ⚠️ ВАЖНО: Удаляем старые BELONGS_TO для идемпотентности (при пересчёте кластеров)
MATCH (u:User)-[old:BELONGS_TO]->(:UserCommunity)
DELETE old;

// Создание новых BELONGS_TO на основе актуальных cluster_id
MATCH (u:User)
WHERE u.cluster_id IS NOT NULL
WITH u, u.cluster_id AS cluster_id
MERGE (uc:UserCommunity {id: cluster_id})
MERGE (u)-[:BELONGS_TO]->(uc)
RETURN count(*) AS relationships_created;

// Обновление размеров сообществ
MATCH (u:User)-[:BELONGS_TO]->(uc:UserCommunity)
WITH uc, count(u) AS size
SET uc.size = size
RETURN count(uc) AS communities_updated;
```

#### Post Clustering (Vector Index + SIMILAR_TO + Leiden)

**Имя векторного индекса зависит от `neo4j_worker_label`:**
- Если `worker_label` задан: `{worker_label}_posts`
- Если `worker_label` не задан: `post_embedding_index`

**Имя графа для Leiden:**
- Если `worker_label` задан: `{worker_label}-post-graph`
- Если `worker_label` не задан: `post-graph`

**Fallback поведение:**
- Если нет SIMILAR_TO связей, всем постам присваивается `cluster_id = 0`

**Построение SIMILAR_TO использует `apoc.periodic.iterate`:**
- Для предотвращения OOM на больших графах построение SIMILAR_TO выполняется батчами
- Размер батча для этого шага — `similarity_iterate_batch_size` (по умолчанию 2000), отдельно от общего `apoc_batch_size`: на каждый пост в батче выполняется запрос к векторному индексу, и слишком крупный батч упирается в лимит транзакционной памяти Neo4j (`dbms.memory.transaction.total.max`)
- В **iterator**-запросе в поток попадают только идентификаторы узлов (`id(p)`), без поля `embedding`: вектор читается в **action** после `MATCH (p)` как `p.embedding`, чтобы не раздувать поток драйвера миллионами векторов
- При полной пересборке (смена сигнатуры похожести) существующие рёбра `SIMILAR_TO` удаляются батчами через `apoc.periodic.iterate` с размером `apoc_batch_size` (исходящие от узлов `Post` воркера), а не одним `DELETE`
- `parallel: false` для избежания блокировок при MERGE операциях

```cypher
// Шаг 1: Создание векторного индекса (выполняется один раз при инициализации)
// Размерность зависит от провайдера: FastText=128, внешние LLM=llm_dimensions
// Имя индекса: 'post_embedding_index' или '{worker_label}_posts'
CREATE VECTOR INDEX post_embedding_index IF NOT EXISTS
FOR (p:Post)
ON p.embedding
OPTIONS {
    indexConfig: {
        `vector.dimensions`: 128,  // fasttext_vector_size или llm_dimensions
        `vector.similarity_function`: 'cosine'
    }
};

// Шаг 2: Построение графа сходства (SIMILAR_TO) через apoc.periodic.iterate
// Iterator query: только id постов (поток без векторов)
MATCH (p:Post)
WHERE p.embedding IS NOT NULL 
  AND p.createdAt > datetime() - duration({days: $recency_days})
RETURN id(p) AS post_id;

// Action query (логика; APOC подставляет батч в $_batch):
// UNWIND $_batch AS row
// MATCH (p:Post) WHERE id(p) = row.post_id
// CALL db.index.vector.queryNodes('post_embedding_index', $top_k, p.embedding)
// YIELD node AS neighbor, score
// WHERE neighbor.id <> p.id AND score > $threshold
// MERGE (p)-[r:SIMILAR_TO]->(neighbor) SET r.weight = score;

// Шаг 3: Leiden кластеризация на графе SIMILAR_TO
// Имя графа: 'post-graph' или '{worker_label}-post-graph'
CALL gds.graph.project(
    'post-graph',  // или '{worker_label}-post-graph'
    'Post',
    {
        SIMILAR_TO: {
            properties: 'weight',
            orientation: 'UNDIRECTED'
        }
    }
);

CALL gds.leiden.write('post-graph', {
    writeProperty: 'cluster_id',
    relationshipWeightProperty: 'weight',
    gamma: $gamma,
    maxLevels: $max_levels
})
YIELD nodePropertiesWritten, communityCount, modularity
RETURN nodePropertiesWritten, communityCount, modularity;

// Шаг 4: Создание PostCommunity узлов и связей BELONGS_TO
// ⚠️ ВАЖНО: Сначала удаляем старые BELONGS_TO для идемпотентности!
// При пересчёте Leiden посты могут менять cluster_id, 
// и без очистки они будут принадлежать сразу двум кластерам

// 4.1: Удаление старых BELONGS_TO связей
MATCH (p:Post)-[old:BELONGS_TO]->(:PostCommunity)
DELETE old
RETURN count(old) AS old_links_deleted;

// 4.2: Создание новых BELONGS_TO на основе актуальных cluster_id
MATCH (p:Post)
WHERE p.cluster_id IS NOT NULL
WITH p, p.cluster_id AS cluster_id
MERGE (pc:PostCommunity {id: cluster_id})
MERGE (p)-[:BELONGS_TO]->(pc)
RETURN count(*) AS relationships_created;

// Шаг 5: Обновление размеров сообществ
MATCH (p:Post)-[:BELONGS_TO]->(pc:PostCommunity)
WITH pc, count(p) AS size
SET pc.size = size
RETURN count(pc) AS communities_updated;

// Шаг 6 (ОПЦИОНАЛЬНО): Pruning - удаление SIMILAR_TO после кластеризации
// SIMILAR_TO связи выполнили свою роль (помогли Leiden найти кластеры)
// Теперь используются только BELONGS_TO, поэтому можно освободить память
MATCH ()-[r:SIMILAR_TO]->()
DELETE r
RETURN count(r) AS pruned_links;

// ⚠️ ВАЖНО: После pruning невозможно пересчитать кластеры без пересоздания графа!
// Используйте только если уверены в качестве кластеризации.
```

**Pruning стратегии:**

1. **aggressive** (рекомендуется для production):
   - Удаляет все SIMILAR_TO после успешной кластеризации.
   - Максимальная экономия памяти.
   - На больших графах выполняется батчами через `apoc.periodic.iterate` (размер батча задаётся `apoc_batch_size`), чтобы не исчерпывать лимит памяти одной транзакции (`dbms.memory.transaction.total.max`).

2. **partial**:
   - Удаляет SIMILAR_TO связи с весом ниже порога `prune_similarity_threshold`.
   - Баланс между памятью и точностью.
   - Как и **aggressive**, реализовано через `apoc.periodic.iterate`: в фазе iterate выбираются внутренние id рёбер, подлежащих удалению, в фазе action — удаление по батчам (не один монолитный `MATCH … DELETE`).

3. **temporal**:
   - Удаляет SIMILAR_TO связи для постов старше `prune_days` дней.
   - Автоматическая очистка устаревших связей.
   - Также через `apoc.periodic.iterate` с тем же смыслом, что у **partial**: отбор кандидатов по `createdAt` поста и батчевое удаление по id связи.

4. **none**:
   - Связи SIMILAR_TO не удаляются.

**Прогресс в UI:** при полном прогоне analytics шаг Similarity pruning для стратегий **aggressive**, **partial** и **temporal** дополнительно отображает вложенную полосу по количеству рёбер, подлежащих удалению (предварительный `COUNT` с теми же фильтрами, что и фаза iterate; обновление через узел `ProgressTracker` и фоновый опрос — как у других массовых batched-операций в пайплайне). Отдельный вызов только pruning (без общего analytics) может выполняться без этого вложенного прогресса.

#### Learn Interests (Community-to-Community с экспоненциальным затуханием)

Вместо жёсткого отсечения по TTL (`WHERE f.at > datetime() - duration(...)`) используется
**экспоненциальное затухание**: каждое взаимодействие вносит вклад `exp(-λ · age_days)`,
где `λ = ln(2) / decay_half_life_days ≈ 0.693147 / decay_half_life_days`.

Это означает, что взаимодействие в возрасте `decay_half_life_days` дней вносит ровно 50 %
от вклада свежего взаимодействия, а старые взаимодействия плавно теряют значимость, но
никогда не отсекаются резко.

**Процесс выполняется в два этапа для поддержки больших графов:**

1. **Вычисление max_weight для каждого UserCommunity**: Сначала определяется максимальный вес среди всех PostCommunity для каждого UserCommunity (нужен для нормализации score).
2. **Создание связей через apoc.periodic.iterate**: Затем связи создаются батчами с использованием `apoc.periodic.iterate` для предотвращения OOM на больших графах.

**Механизм нормализации:**
- `max_weight` вычисляется отдельно для каждого UserCommunity
- Временно сохраняется на узле UserCommunity как свойство `max_weight_temp` (**одним** запросом `UNWIND $rows` по списку пар `(uc_id, max_weight)`, без отдельного round-trip на каждое сообщество)
- Используется при создании связей для нормализации: `score = weight / max_weight`
- После завершения свойства `max_weight_temp` удаляются со всех узлов

**Схема агрегации весов (упрощённо):** для каждого типа взаимодействия, реально присутствующего в графе, строится подзапрос вида «найти ребро `(u)-[:ТИП]->(p)`, присоединить `UserCommunity` и `PostCommunity`», затем ветки объединяются `UNION ALL`, после чего по `(uc, pc)` суммируются столбцы вкладов (лайки, реблоги, …) и применяются веса из настроек, порог `min_interactions` и при включённом CTR — учёт `WAS_RECOMMENDED`. Обертка `CALL { … }` группирует union; итог совпадает по смыслу с прежней суммой decay по каждому типу, но без перебора всех пар пользователь–пост внутри пары сообществ.

```cypher
// 1. Удаление ВСЕХ старых INTERESTED_IN (полная перестройка каждый цикл)
MATCH (:UserCommunity)-[i:INTERESTED_IN]->(:PostCommunity)
DELETE i;

// 2. Иллюстрация: одна ветка на тип + UNION ALL (остальные типы — аналогично)
CALL {
  MATCH (u:User)-[f:FAVORITED]->(p:Post)
  MATCH (u)-[:BELONGS_TO]->(uc:UserCommunity), (p)-[:BELONGS_TO]->(pc:PostCommunity)
  WITH uc, pc, sum(exp(-0.693147 * duration.between(f.at, datetime()).days
      / toFloat($half_life_days))) AS likes
  RETURN uc, pc, likes, 0 AS reblogs, 0 AS replies, 0 AS bookmarks
  UNION ALL
  MATCH (u:User)-[r:REBLOGGED]->(p:Post)
  MATCH (u)-[:BELONGS_TO]->(uc:UserCommunity), (p)-[:BELONGS_TO]->(pc:PostCommunity)
  WITH uc, pc, sum(exp(-0.693147 * duration.between(r.at, datetime()).days
      / toFloat($half_life_days))) AS reblogs
  RETURN uc, pc, 0 AS likes, reblogs, 0 AS replies, 0 AS bookmarks
}
WITH uc, pc,
     sum(likes) AS likes, sum(reblogs) AS reblogs, sum(replies) AS replies, sum(bookmarks) AS bookmarks
// далее — взвешенная сумма, фильтры, при CTR — отдельная ветка для WAS_RECOMMENDED;
// max_weight: WITH uc, max(weight) …; iterate возвращает (uc_id, pc_id, weight, interactions)

// 3. Установка max_weight_temp батчем (концептуально)
UNWIND $rows AS row
MATCH (uc:UserCommunity) WHERE uc.id = row.uc_id
SET uc.max_weight_temp = row.max_weight;

// 4. Action в apoc.periodic.iterate: MERGE INTERESTED_IN с score = weight / max_weight_temp

// 5. Очистка временных свойств
MATCH (uc:UserCommunity) REMOVE uc.max_weight_temp;
```

**Важно:**
- Процесс использует `apoc.periodic.iterate` с батчами размера `apoc_batch_size` (настраивается через параметр `apoc_batch_size`, по умолчанию 10000)
- `parallel: false` для избежания блокировок при MERGE операциях
- Временное свойство `max_weight_temp` удаляется после завершения всех операций

**Формула decay:**

| Возраст взаимодействия | Вклад (при `decay_half_life_days = 14`) |
|------------------------|----------------------------------------|
| 0 дней (сегодня)      | 1.000 (100 %)                          |
| 7 дней                 | 0.707 (~71 %)                          |
| 14 дней (1 half-life)  | 0.500 (50 %)                           |
| 28 дней (2 half-lives) | 0.250 (25 %)                           |
| 42 дня (3 half-lives)  | 0.125 (~13 %)                          |

**Важные принципы:**
- ✅ **Полная перестройка**: Связи не накапливаются, а пересоздаются каждый цикл (команда `run`)
- ✅ **Экспоненциальное затухание**: Свежие взаимодействия вносят больший вклад, старые — плавно уменьшаются
- ✅ **TTL**: Каждая связь имеет `expires_at` (настраивается через `interests_ttl_days`)
- ✅ **Взвешенные взаимодействия**: REPLY/REBLOG учитываются сильнее, чем LIKE (веса настраиваются)
- ✅ **Защита от протухания**: Старые связи удаляются отдельной процедурой cleanup
- ✅ **Стабильность при изменении ID**: При смене cluster_id связи перестроятся корректно
- ✅ **CTR (Click-Through Rate)**: Учитываются неявные взаимодействия (рекомендации без реакции)
- ✅ **Инкрементальное обновление**: Команда `refresh` обновляет интересы без полной перестройки

#### Инкрементальное обновление интересов (refresh)

Для частого обновления без полной перестройки графа используется команда `refresh`.
Она применяет глобальный decay ко всем существующим score, затем пересчитывает
только «грязные» сообщества (с новыми взаимодействиями с момента последнего пересчёта).

```cypher
// Шаг 1: Глобальный decay — применяем ко всем существующим INTERESTED_IN
// Множитель: exp(-0.693147 * hours_since_last / (half_life_days * 24))
// Это учитывает время, прошедшее с последнего пересчёта, и плавно снижает все score
MATCH (uc:UserCommunity)-[i:INTERESTED_IN]->(pc:PostCommunity)
SET i.score = i.score * exp(-0.693147 * $hours_since_last
    / (toFloat($half_life_days) * 24.0));

// Шаг 2: Определение «грязных» UserCommunity
// (у которых есть новые взаимодействия с момента последнего пересчёта)
// Проверяются все типы взаимодействий: FAVORITED, REBLOGGED, REPLIED, BOOKMARKED
// с условием at > last_rebuild_at
MATCH (u:User)-[:BELONGS_TO]->(uc:UserCommunity)
WHERE EXISTS { MATCH (u)-[f:FAVORITED]->() WHERE f.at > datetime($last_rebuild_at) }
   OR EXISTS { MATCH (u)-[r:REBLOGGED]->() WHERE r.at > datetime($last_rebuild_at) }
   OR EXISTS { MATCH (u)-[rp:REPLIED]->() WHERE rp.at > datetime($last_rebuild_at) }
   OR EXISTS { MATCH (u)-[bk:BOOKMARKED]->() WHERE bk.at > datetime($last_rebuild_at) }
RETURN DISTINCT id(uc) AS uc_id;

// Шаг 3: Удаление INTERESTED_IN только для грязных UC
MATCH (uc:UserCommunity)-[i:INTERESTED_IN]->(pc:PostCommunity)
WHERE id(uc) IN $dirty_uc_ids
DELETE i;

// Шаг 4: Пересчёт INTERESTED_IN для грязных UC (аналогично полной перестройке)
// Используется тот же двухэтапный процесс с max_weight_temp и apoc.periodic.iterate,
// но только для UserCommunity из списка $dirty_uc_ids
// ... (тот же запрос, что и в rebuild, но с WHERE id(uc) IN $dirty_uc_ids)

// Шаг 5: Удаление связей с near-zero score
// После глобального decay некоторые связи могут иметь score < 0.01 (менее 1% от пика)
// Такие связи удаляются, так как они практически не влияют на рекомендации
MATCH (uc:UserCommunity)-[i:INTERESTED_IN]->(pc:PostCommunity)
WHERE i.score < 0.01
DELETE i;
```

**Преимущества инкрементального обновления:**
- ✅ Значительно быстрее полной перестройки
- ✅ Глобальный decay поддерживает актуальность score без полного пересчёта
- ✅ Пересчитываются только сообщества с новой активностью
- ✅ Автоматический fallback на полную перестройку при отсутствии `last_interests_rebuild_at`
- ✅ Автоматическая очистка связей с score < 0.01 после decay

#### Учет CTR (Click-Through Rate)

При включенном `ctr_enabled` система учитывает не только явные взаимодействия (лайки, репосты, ответы), но и факт того, что пользователь не отреагировал на рекомендованные посты. Это позволяет более точно определять интересы сообществ.

**Формула расчета CTR (с экспоненциальным затуханием):**

```cypher
// Расширенный запрос с учетом CTR и экспоненциальным decay
MATCH (u:User)-[:BELONGS_TO]->(uc:UserCommunity),
      (p:Post)-[:BELONGS_TO]->(pc:PostCommunity)
OPTIONAL MATCH (u)-[f:FAVORITED]->(p)
OPTIONAL MATCH (u)-[r:REBLOGGED]->(p)
OPTIONAL MATCH (u)-[rp:REPLIED]->(p)
OPTIONAL MATCH (u)-[bk:BOOKMARKED]->(p)
OPTIONAL MATCH (u)-[rec:WAS_RECOMMENDED]->(p)
WITH uc, pc,
     // Decay-взвешенные суммы вместо count()
     sum(exp(-0.693147 * duration.between(f.at, datetime()).days
         / toFloat($half_life_days))) AS likes,
     sum(exp(-0.693147 * duration.between(r.at, datetime()).days
         / toFloat($half_life_days))) AS reblogs,
     sum(exp(-0.693147 * duration.between(rp.at, datetime()).days
         / toFloat($half_life_days))) AS replies,
     sum(exp(-0.693147 * duration.between(bk.at, datetime()).days
         / toFloat($half_life_days))) AS bookmarks,
     sum(exp(-0.693147 * duration.between(rec.at, datetime()).days
         / toFloat($half_life_days))) AS recommendations
WITH uc, pc,
     likes * $likes_weight + reblogs * $reblogs_weight
     + replies * $replies_weight + bookmarks * $bookmark_weight AS base_weight,
     (likes + reblogs + replies + bookmarks) AS interactions,
     recommendations
WHERE interactions >= $min_interactions
WITH uc, pc, base_weight, interactions, recommendations,
     // CTR = (явные взаимодействия + smoothing) / (рекомендации + smoothing)
     CASE WHEN recommendations > 0 OR $ctr_smoothing > 0
          THEN toFloat(interactions + $ctr_smoothing)
               / toFloat(recommendations + $ctr_smoothing)
          ELSE 0.0 END AS ctr
WHERE ctr >= $min_ctr  // Фильтрация по минимальному CTR
WITH uc, pc, base_weight, interactions, ctr,
     // Применение CTR к весу: weight = base_weight * (CTR * ctr_weight + (1 - ctr_weight))
     base_weight * ($ctr_weight * ctr + (1.0 - $ctr_weight)) AS weight
// ... нормализация и создание связей
```

**Параметры CTR:**

- `ctr_enabled`: Включить/выключить учет CTR (по умолчанию `true`)
- `ctr_weight`: Вес CTR в расчете интереса (0.0-1.0). При `0.0` CTR не влияет, при `1.0` только CTR учитывается
- `min_ctr`: Минимальный CTR для учета интереса (по умолчанию `0.0`)
- `ctr_smoothing`: Сглаживание для избежания деления на ноль (по умолчанию `1.0`)

**Полная формула расчета веса с CTR:**

1. **Вычисление base_weight**: Сначала вычисляется базовый вес на основе взвешенных взаимодействий:
   ```
   base_weight = likes * likes_weight + reblogs * reblogs_weight 
                + replies * replies_weight + bookmarks * bookmark_weight
   ```

2. **Вычисление CTR**: Затем вычисляется Click-Through Rate:
   ```
   CTR = (interactions + ctr_smoothing) / (recommendations + ctr_smoothing)
   ```
   где `interactions = likes + reblogs + replies + bookmarks`

3. **Применение CTR к весу**: Финальный вес вычисляется как:
   ```
   weight = base_weight * (ctr_weight * CTR + (1.0 - ctr_weight))
   ```

**Логика работы:**

1. **Высокий CTR** (много взаимодействий на рекомендации): вес интереса увеличивается
2. **Низкий CTR** (мало взаимодействий на рекомендации): вес интереса снижается
3. **Нет рекомендаций**: используется только базовый вес (без учета CTR), так как CTR не вычисляется
4. **Нет взаимодействий, но есть рекомендации**: CTR = 0, вес снижается пропорционально `ctr_weight`

**Пример расчета:**

- Сообщество получило 100 рекомендаций постов из PostCommunity A (с учетом decay)
- Пользователи отреагировали на 30 постов (лайки/репосты/ответы, с учетом decay)
- `base_weight = 10.0` (вычислен из взвешенных взаимодействий)
- `CTR = (30 + 1.0) / (100 + 1.0) = 0.307`
- При `ctr_weight = 0.5`: 
  ```
  weight = 10.0 * (0.5 * 0.307 + 0.5) 
         = 10.0 * (0.154 + 0.5) 
         = 10.0 * 0.654 
         = 6.54
  ```

Это означает, что интерес к PostCommunity A будет снижен на ~35% из-за низкого CTR.


#### Serendipity (случайные связи)

```cypher
// Добавление случайных связей для обнаружения нового контента
// Выполняется ПОСЛЕ основного Learn Interests
MATCH (uc:UserCommunity), (pc:PostCommunity)
WHERE NOT (uc)-[:INTERESTED_IN]->(pc)
  AND rand() < $probability  // вероятность serendipity

// Проверяем что у сообщества есть хотя бы один интерес (не изолированное)
MATCH (uc)-[:INTERESTED_IN]->(:PostCommunity)

WITH uc, pc
LIMIT $serendipity_limit  // ограничение на количество случайных связей

MERGE (uc)-[i:INTERESTED_IN]->(pc)
SET i.score = $serendipity_score,          // низкий score для случайных связей
    i.based_on = $serendipity_based_on,
    i.serendipity = true,   // метка случайной связи
    i.last_updated = datetime(),
    i.expires_at = datetime() + duration({days: $ttl_days})  // TTL как у обычных
RETURN count(i) AS serendipity_links;
```

**Важно:**
- Serendipity связи тоже имеют TTL (через `interests_ttl_days`)
- Добавляются ПОСЛЕ основных INTERESTED_IN (не удаляются при перестройке)
- Если нужно полностью пересоздать все связи, сначала выполняйте DELETE всех INTERESTED_IN

### Генерация персональной ленты

**Одноступенчатый скоринг в Cypher:**
- Один запрос формирует персональную ленту по интересам сообществ, популярности и свежести.
- Веса контролируются параметрами `personalized_*`.
- Если персональных результатов нет, применяется cold start (глобально популярные посты).

#### Запрос ленты для пользователя (с холодным стартом)

**Персонализированная лента:**
```cypher
MATCH (u:User {id: $user_id})-[:BELONGS_TO]->(uc:UserCommunity)
      -[i:INTERESTED_IN]->(pc:PostCommunity)<-[:BELONGS_TO]-(p:Post)
WHERE p.createdAt > datetime() - duration({days: $feed_days})
  AND NOT EXISTS { (u)-[:WAS_RECOMMENDED]->(p) }
  AND NOT EXISTS { (u)-[:WROTE]->(p) }
  AND NOT EXISTS { (u)-[:FAVORITED]->(p) }
  AND NOT EXISTS { (p)<-[:WROTE]-(:User)<-[:HATES_USER]-(u) }

WITH u, p, i.score AS interest_score,
     COUNT { (p)<-[:FAVORITED]-() } AS popularity,
     duration.between(datetime(p.createdAt), datetime()).hours AS age_hours,
     COALESCE(p.pagerank, 0.0) AS pagerank

WITH p,
     interest_score * $interest_weight +
     log10(popularity + $popularity_smoothing) * $popularity_weight +
     ($recency_numerator / (age_hours / 24.0 + $recency_smoothing)) * $recency_weight +
     pagerank * $pagerank_weight +
     CASE
       WHEN p.language IS NULL THEN $language_match_weight
       WHEN u.uiLanguage IS NULL AND (u.languages IS NULL OR size(coalesce(u.languages, [])) = 0)
       THEN $language_match_weight
       WHEN u.uiLanguage IS NOT NULL AND p.language = u.uiLanguage THEN $ui_language_match_weight
       WHEN u.languages IS NOT NULL AND p.language IN u.languages THEN $language_match_weight
       ELSE 0.0
     END AS score

RETURN p.id AS post_id, score
ORDER BY score DESC
LIMIT $feed_size;
```

> **Языковой буст**: Сначала проверяется совпадение с языком UI (`uiLanguage` из `users.locale`) — бонус `ui_language_match_weight` (по умолчанию 0.5, не меньше `language_match_weight`). Иначе, если язык поста входит в `chosen_languages` (`u.languages`), — бонус `language_match_weight` (по умолчанию 0.3). Если у пользователя не заданы ни UI-язык, ни выбранные языки, либо у поста не указан язык — применяется мягкий бонус `language_match_weight` (как для «неизвестного» контента). Посты на «чужих» языках **не отсекаются**, они просто не получают бонус. Это мягкий сигнал, не фильтр.

> **Примечание по recency:** Возраст поста вычисляется в **часах** (`age_hours`), а не днях, для более точного ранжирования свежих постов. Формула `age_hours / 24.0` нормализует значение обратно к дням, но сохраняет суб-дневную гранулярность: пост возрастом 6 часов получит значительно более высокий recency-score, чем пост возрастом 18 часов (оба были бы `age = 0` при расчёте в днях).

**Cold Start (если нет результатов персонализации):**
```cypher
MATCH (u:User {id: $user_id})
MATCH (p:Post)
WHERE p.createdAt > datetime() - duration({days: $feed_days})
  AND p.embedding IS NOT NULL
  AND NOT EXISTS { (u)-[:WAS_RECOMMENDED]->(p) }
  AND NOT EXISTS { (u)-[:WROTE]->(p) }
  AND NOT EXISTS { (u)-[:FAVORITED]->(p) }
  AND NOT EXISTS { (p)<-[:WROTE]-(:User)<-[:HATES_USER]-(u) }

WITH u, p,
     COUNT { (p)<-[:FAVORITED]-() } AS popularity,
     duration.between(datetime(p.createdAt), datetime()).hours AS age_hours

WITH p,
     log10(popularity + $popularity_smoothing) * $popularity_weight +
     ($recency_numerator / (age_hours / 24.0 + $recency_smoothing)) * $recency_weight +
     CASE
       WHEN p.language IS NULL THEN $language_match_weight
       WHEN u.uiLanguage IS NULL AND (u.languages IS NULL OR size(coalesce(u.languages, [])) = 0)
       THEN $language_match_weight
       WHEN u.uiLanguage IS NOT NULL AND p.language = u.uiLanguage THEN $ui_language_match_weight
       WHEN u.languages IS NOT NULL AND p.language IN u.languages THEN $language_match_weight
       ELSE 0.0
     END AS score

RETURN p.id AS post_id, score
ORDER BY score DESC
LIMIT $cold_start_limit;
```

**Примечание по cold start:**
- В текущей реализации используется только `global_top`.
- Остальные стратегии оставлены в конфигурации для будущих расширений.

#### Пометка постов как рекомендованных

```cypher
// Сохранение истории рекомендаций для исключения повторов
MATCH (u:User {id: $user_id})
UNWIND $batch AS row
MATCH (p:Post {id: row.post_id})
MERGE (u)-[r:WAS_RECOMMENDED]->(p)
ON CREATE SET r.at = datetime(), r.score = row.score
```

> **Параметры**: `$batch` — список объектов `{post_id: int, score: float}`

#### Отметка времени генерации ленты

После генерации ленты и записи в Redis на узле User устанавливается время последней генерации. Это используется для dirty-user detection при следующем запуске:

```cypher
MATCH (u:User {id: $user_id})
SET u.feedGeneratedAt = datetime();
```

#### Параллельная генерация лент

При `feed_workers > 1` используется `ThreadPoolExecutor` для параллельной генерации лент нескольких пользователей одновременно. Это значительно ускоряет обработку больших списков пользователей.

**Особенности параллельной обработки:**
- Каждый пользователь обрабатывается независимо в отдельном потоке
- Neo4j сессии и Redis pipeline'ы потокобезопасны и изолированы для каждого вызова
- При `feed_workers == 1` используется последовательная обработка (обратная совместимость)
- Проверка флага `shutdown_requested` выполняется между пользователями для корректной обработки Ctrl+C

**Checkpoint логика:**
- Курсор `last_processed_feed_user_id` обновляется каждые `checkpoint_interval` пользователей
- При параллельной обработке сохраняется максимальный обработанный `user_id` среди завершённых задач
- При прерывании (Ctrl+C) курсор сохраняется для возобновления с последнего обработанного пользователя
- После полного завершения генерации всех лент курсор сбрасывается в 0

**Параметр `feed_score_decimals`:**
- Используется при экспорте лент в Markdown (команда `export`)
- Определяет количество знаков после запятой для округления score в таблицах экспорта
- Не влияет на внутренние вычисления или запись в Redis

---

## Redis операции

### Сводка: где лежат рекомендации

HintGrid пишет рекомендации в **два независимых семейства ключей** Mastodon (оба — sorted set, rank-based scoring одинаковый по идее, см. ниже):

| Назначение | Ключи Redis (по умолчанию) | Что это в UI Mastodon | Персонализация |
|------------|----------------------------|----------------------|----------------|
| Домашняя лента | `feed:home:{account_id}` | Лента «Главная» у конкретного пользователя | Да, запрос к графу по `account_id` |
| Публичные ленты | `timeline:public`, `timeline:public:local` | Объединённая публичная лента и локальная публичная лента инстанса | Нет, один общий ранжированный список на ключ (агрегат по интересам сообществ) |

**Важно:**

- Публичные ключи можно переименовать через `HINTGRID_PUBLIC_TIMELINE_KEY` и `HINTGRID_LOCAL_TIMELINE_KEY`; домашняя лента в коде HintGrid всегда задаётся шаблоном `feed:home:{account_id}` (как в `FeedManager#key(:home, …)` в Mastodon).
- Префикс `HINTGRID_REDIS_NAMESPACE` в HintGrid **применяется при записи публичных** таймлайнов (`timeline:*`), чтобы совпасть с `REDIS_NAMESPACE` Mastodon для этих ключей. Запись в `feed:home:*` выполняется **без** этого префикса в коде — физический ключ в Redis должен совпадать с тем, куда пишет ваш процесс Mastodon/Sidekiq (при необходимости сверьте с `redis-cli KEYS` / `SCAN` на инстансе).
- Отключение только публичных лент: `HINTGRID_PUBLIC_FEED_ENABLED=false` (домашние ленты не затрагивает).

### Хранение персональных лент

Этот подраздел — только про **домашнюю** ленту (`feed:home:*`). Публичные таймлайны описаны в [Публичные таймлайны Mastodon](#публичные-таймлайны-mastodon).

**КРИТИЧЕСКИ ВАЖНО**: HintGrid работает совместно с Mastodon FeedManager в одном и том же Redis `feed:home:{account_id}`.

**Запись рекомендаций (rank-based scoring):**
- HintGrid записывает рекомендации напрямую в `feed:home:{account_id}` через `ZADD`.
- Score рассчитывается по формуле: `base = max(post_id) * multiplier`, `redis_score = base + (N - rank)`, где `rank 0` = самый интересный пост.
- **Предварительная сортировка**: Рекомендации из Cypher-запроса приходят отсортированными по `score DESC` (интересность), порядок сохраняется. Функция `write_feed_to_redis` ожидает предварительно отсортированный список (гарантируется Cypher `ORDER BY score DESC`).
- **Обработка пустого списка**: Если список рекомендаций пуст, функция завершается без записи в Redis (early return).
- Все score выше Mastodon-записей (`score = post_id`), что гарантирует вытеснение.
- Параметр `feed_ttl` для `feed:home:*` игнорируется, чтобы не конфликтовать с Mastodon FeedManager.
- Генерация лент выполняется **только для локальных пользователей** (`isLocal = true`), но аналитика (кластеризация, интересы, PageRank) работает на полном графе.

### Интеграция с Mastodon FeedManager

#### Проблема

Mastodon FeedManager:
- Держит в `feed:home:{account_id}` до **800 постов** (константа `MAX_ITEMS`)
- Использует `status.id` как **member** и как **score**: `redis.zadd(timeline_key, status.id, status.id)`
- Хранит служебные ключи для агрегации репостов:
  - `feed:home:{account_id}:reblogs` (sorted set)
  - `feed:home:{account_id}:reblogs:{status_id}` (set)
- Использует `REBLOG_FALLOFF = 40` для ограничения повторных репостов в топе ленты
- Автоматически обрезает ленту (trim) до 800 постов
- Мы **НЕ контролируем** Mastodon сервер

**Если использовать обычный score (0.0-1.0):**
Mastodon перезапишет score по status.id, и рекомендации будут вытеснены при trim.

#### Решение: rank-based interest scoring

**Используем rank-based scoring для сортировки по интересности:**

```
Cypher возвращает N рекомендаций, отсортированных по score DESC
→ rank 0 = самый интересный, rank N-1 = наименее интересный

base = max(post_id в батче) * multiplier
redis_score[rank] = base + (N - rank)

Самый интересный (rank 0):    base + N     ← наивысший score
Наименее интересный (rank N-1): base + 1   ← наинизший, но всё равно > Mastodon
Mastodon-записи:                post_id * 1 ← всегда ниже → обрезаются trim
```

**Почему это работает:**

1. **Mastodon**: `ZADD feed:home:123 101 101` (score = post_id = 101)
2. **HintGrid (rank 0, самый интересный)**: `ZADD feed:home:123 {base+N} 101` — наивысший score
3. **HintGrid (rank N-1)**: `ZADD feed:home:123 {base+1} 105` — всё равно > Mastodon
4. **Результат**: HintGrid score **ВСЕГДА выше** → посты в топе ленты, отсортированные по интересности
5. **Trim**: Mastodon удаляет старые посты (свои), наши остаются

**Преимущества:**
- ✅ Не нужно модифицировать Mastodon
- ✅ Наши рекомендации всегда в топе
- ✅ Mastodon сам удаляет старые посты
- ✅ Плавная деградация (если HintGrid не работает, остаются Mastodon посты)

**Важно про reblogs:**
- Не пишите данные в ключи `feed:home:{account_id}:reblogs` и `feed:home:{account_id}:reblogs:{status_id}`.
- Эти ключи используются FeedManager для агрегации репостов и соблюдения `REBLOG_FALLOFF`.

**Ограничения:**

| Ограничение | Причина |
|-------------|---------|
| ❌ **НЕ** используйте `EXPIRE` на `feed:home:*` | Mastodon требует персистентный feed |
| ❌ **НЕ** используйте `ZREMRANGEBYRANK` вручную | Mastodon сам обрезает до 800 |
| ❌ **НЕ** используйте score < status_id | Mastodon удалит при trim |
| ✅ **ВСЕГДА** используйте rank-based scoring: `base + (N - rank)` | Гарантия что посты в топе и отсортированы по интересности |
| ✅ Работает для `feed:home:*` **и** публичных лент | Единая модель скоринга |

#### Команды Redis
Используются операции ZADD, ZREVRANGE, ZSCORE, ZREM, ZCARD и DEL для управления лентой.

#### Проверка корректности интеграции
Проверяйте, что score рекомендаций больше, и что в топе находятся HintGrid посты.

#### Мониторинг интеграции

```cypher
// Cypher запрос: проверка что посты попали в Redis
MATCH (u:User {id: 123})-[r:WAS_RECOMMENDED]->(p:Post)
WHERE r.at > datetime() - duration({hours: 1})
RETURN p.id AS post_id, r.at AS recommended_at
ORDER BY r.at DESC
LIMIT 10;
```
Сверяйте результаты WAS_RECOMMENDED с содержимым Redis.

### Публичные таймлайны Mastodon

Это **не** домашние ленты: рекомендации для экранов «публичная лента» / «локальная публичная лента» хранятся в ключах `timeline:*`, а не в `feed:home:*` ([сводка](#сводка-где-лежат-рекомендации)).

HintGrid заполняет эти публичные ленты Mastodon рекомендациями. Mastodon хранит их как Redis Sorted Sets:

| Ключ | Содержимое | Лимит |
|------|-----------|-------|
| `timeline:public` | Все публичные посты (локальные + федеративные) | ~400 |
| `timeline:public:local` | Только локальные публичные посты | ~400 |

**Стратегии заполнения** (настройка `public_feed_strategy`):

| Стратегия | Глобальная лента | Локальная лента | Ресурсы |
|-----------|-----------------|-----------------|---------|
| `local_communities` (по умолчанию) | Интересы только локальных сообществ | Интересы локальных + только локальные авторы | Экономичнее |
| `all_communities` | Интересы всех сообществ | Интересы локальных + только локальные авторы | Чуть больше |

**Cypher-запрос (стратегия `local_communities`, глобальная лента):**
```cypher
MATCH (u:User {isLocal: true})-[:BELONGS_TO]->(uc:UserCommunity)
      -[i:INTERESTED_IN]->(pc:PostCommunity)<-[:BELONGS_TO]-(p:Post)
WHERE p.createdAt > datetime() - duration({days: $feed_days})
WITH p,
     sum(i.score) AS community_interest,
     COUNT { (p)<-[:FAVORITED]-() } AS popularity,
     COALESCE(p.pagerank, 0.0) AS pagerank,
     duration.between(datetime(p.createdAt), datetime()).hours AS age_hours
WITH p,
     community_interest * $interest_weight +
     log10(popularity + $popularity_smoothing) * $popularity_weight +
     ($recency_numerator / (age_hours / 24.0 + $recency_smoothing))
         * $recency_weight +
     pagerank * $pagerank_weight AS score
RETURN p.id AS post_id, score
ORDER BY score DESC LIMIT $public_feed_size;
```

**Скоринг** — аналогичен home feed: `base = max(post_id) * multiplier`, `redis_score = base + (N - rank)`.

**Очистка**: Mastodon автоматически обрезает публичные ленты до ~400 записей через `zremrangebyrank`. Записи Mastodon (score = post_id) всегда ниже записей HintGrid и вытесняются автоматически.

**Redis namespace**: Если Mastodon использует `REDIS_NAMESPACE`, HintGrid добавляет тот же префикс к ключам (настройка `redis_namespace`). Например: `cache:timeline:public`.

## Библиотеки и зависимости

### Конфигурация через CLI и env

HintGrid не использует `config.yml`. Параметры можно задать через CLI, переменные окружения или файлы `.env` / `.env.local`. Приоритет: CLI → env → `.env` → `.env.local` → значения по умолчанию.

Все параметры из таблицы ниже доступны как CLI флаги для всех команд. Для булевых параметров используется пара флагов вида `--prune-after-clustering` / `--no-prune-after-clustering`.

**Согласование учётных данных с внешними сервисами.** Значения в `.env` / CLI не «создают» пользователей в БД — они должны совпадать с уже настроенными учётными записями:

- **PostgreSQL:** `HINTGRID_POSTGRES_USER` и `HINTGRID_POSTGRES_PASSWORD` — та же роль и пароль, что заданы в `CREATE USER` / `ALTER USER`; `HINTGRID_POSTGRES_DATABASE` — существующая база Mastodon. Опечатка в имени роли даёт `FATAL: password authentication failed for user "…"`.
- **Neo4j:** `HINTGRID_NEO4J_USERNAME` и `HINTGRID_NEO4J_PASSWORD` — совпадают с пользователем Bolt и паролем (в Docker — с `NEO4J_AUTH`, формат `neo4j/пароль`, либо с паролем после смены в Neo4j). Неверный пароль — ошибки `Neo.ClientError.Security.*`; многократные попытки — `AuthenticationRateLimit`.
- **Redis:** если в Redis включён `requirepass`, `HINTGRID_REDIS_PASSWORD` должен совпадать; если пароля нет — переменная пустая.

Unix-пользователь ОС (например `hintgrid` для каталога приложения), роль PostgreSQL и пользователь Neo4j `neo4j` — разные сущности; совпадение имён в примерах не означает общий пароль.

| Параметр | CLI флаг | Env переменная | По умолчанию | Пример значения | Описание |
|---------|----------|----------------|--------------|-----------------|----------|
| postgres.host | `--postgres-host` | `HINTGRID_POSTGRES_HOST` | `localhost` | `db.internal` | PostgreSQL host |
| postgres.port | `--postgres-port` | `HINTGRID_POSTGRES_PORT` | `5432` | `5432` | PostgreSQL port |
| postgres.database | `--postgres-database` | `HINTGRID_POSTGRES_DATABASE` | `mastodon_production` | `mastodon_production` | PostgreSQL database |
| postgres.user | `--postgres-user` | `HINTGRID_POSTGRES_USER` | `mastodon` | `mastodon` | PostgreSQL user |
| postgres.password | `--postgres-password` | `HINTGRID_POSTGRES_PASSWORD` | `null` | `s3cr3t` | PostgreSQL password (optional) |
| neo4j.host | `--neo4j-host` | `HINTGRID_NEO4J_HOST` | `localhost` | `neo4j.internal` | Neo4j host |
| neo4j.port | `--neo4j-port` | `HINTGRID_NEO4J_PORT` | `7687` | `7687` | Neo4j Bolt port |
| neo4j.username | `--neo4j-username` | `HINTGRID_NEO4J_USERNAME` | `neo4j` | `neo4j` | Neo4j username |
| neo4j.password | `--neo4j-password` | `HINTGRID_NEO4J_PASSWORD` | `password` | `password` | Neo4j password |
| redis.host | `--redis-host` | `HINTGRID_REDIS_HOST` | `localhost` | `redis.internal` | Redis host |
| redis.port | `--redis-port` | `HINTGRID_REDIS_PORT` | `6379` | `6379` | Redis port |
| redis.db | `--redis-db` | `HINTGRID_REDIS_DB` | `0` | `0` | Redis DB |
| redis.password | `--redis-password` | `HINTGRID_REDIS_PASSWORD` | `null` | `redispass` | Redis password (optional) |
| redis_namespace | `--redis-namespace` | `HINTGRID_REDIS_NAMESPACE` | `null` | `cache` | Redis namespace (префикс для ключей публичных лент, как в Mastodon `REDIS_NAMESPACE`) |
| llm.provider | `--llm-provider` | `HINTGRID_LLM_PROVIDER` | `ollama` | `ollama` | LLM provider (`ollama`, `openai`, `fasttext`) |
| llm.base_url | `--llm-base-url` | `HINTGRID_LLM_BASE_URL` | `null` | `http://llm:11434` | LLM base URL (если пусто — используется FastText) |
| llm.model | `--llm-model` | `HINTGRID_LLM_MODEL` | `nomic-embed-text` | `nomic-embed-text` | LLM model |
| llm.dimensions | `--llm-dimensions` | `HINTGRID_LLM_DIMENSIONS` | `768` | `768` | Embedding dimensions (для внешних LLM) |
| llm.timeout | `--llm-timeout` | `HINTGRID_LLM_TIMEOUT` | `30` | `30` | LLM timeout (seconds) |
| llm.max_retries | `--llm-max-retries` | `HINTGRID_LLM_MAX_RETRIES` | `3` | `3` | LLM retries |
| llm.api_key | `--llm-api-key` | `HINTGRID_LLM_API_KEY` | `null` | `sk-example` | LLM API key (optional, if required) |
| llm.batch_size | `--llm-batch-size` | `HINTGRID_LLM_BATCH_SIZE` | `256` | `256` | Размер суб-батча для LLM embedding запросов (1–10000). Тексты разбиваются на чанки этого размера для экономии API-вызовов и гранулярных retry. Для FastText игнорируется |
| fasttext.vector_size | `--fasttext-vector-size` | `HINTGRID_FASTTEXT_VECTOR_SIZE` | `128` | `128` | Размерность FastText (используется при отсутствии llm.base_url) |
| fasttext.window | `--fasttext-window` | `HINTGRID_FASTTEXT_WINDOW` | `3` | `3` | Окно контекста FastText |
| fasttext.min_count | `--fasttext-min-count` | `HINTGRID_FASTTEXT_MIN_COUNT` | `10` | `10` | Минимальная частота токенов (агрессивная обрезка для соцсетей) |
| fasttext.max_vocab_size | `--fasttext-max-vocab-size` | `HINTGRID_FASTTEXT_MAX_VOCAB_SIZE` | `500000` | `500000` | Максимальный размер словаря (ограничение роста для экономии памяти) |
| fasttext.epochs | `--fasttext-epochs` | `HINTGRID_FASTTEXT_EPOCHS` | `5` | `5` | Эпохи обучения |
| fasttext.bucket | `--fasttext-bucket` | `HINTGRID_FASTTEXT_BUCKET` | `10000` | `10000` | Размер bucket для n-grams |
| fasttext.min_documents | `--fasttext-min-documents` | `HINTGRID_FASTTEXT_MIN_DOCUMENTS` | `100` | `100` | Минимум документов для обучения |
| fasttext.model_path | `--fasttext-model-path` | `HINTGRID_FASTTEXT_MODEL_PATH` | `~/.hintgrid/models` | `~/.hintgrid/models` | Путь хранения модели |
| fasttext.quantize | `--fasttext-quantize` / `--no-fasttext-quantize` | `HINTGRID_FASTTEXT_QUANTIZE` | `true` | `true` | Включить квантование модели (сжатие в 10-50 раз) |
| fasttext.quantize_qdim | `--fasttext-quantize-qdim` | `HINTGRID_FASTTEXT_QUANTIZE_QDIM` | `64` | `64` | Число подквантов PQ (compress-fasttext): при включённом квантовании `fasttext_vector_size` должно делиться на `quantize_qdim` нацело (и `quantize_qdim` ≤ `vector_size`) |

После полного сохранения чекпоинта (`fasttext_v{N}.bin`) при включённом квантовании дополнительно записывается сжатый файл `fasttext_v{N}.q.bin`: векторы словаря и n-грамм проходят product quantization (библиотека **compress-fasttext**; параметр `quantize_qdim` — число подквантов M, требование PQ: размерность вектора кратна M). В режиме инференса при наличии `.q.bin` загружается он (формат KeyedVectors), иначе — полная модель.

| batch_size | `--batch-size` | `HINTGRID_BATCH_SIZE` | `10000` | `10000` | Batch size |
| load_since | `--load-since` | `HINTGRID_LOAD_SINCE` | `null` | `30d` | Окно загрузки данных (посты и поведение) |
| max_retries | `--max-retries` | `HINTGRID_MAX_RETRIES` | `3` | `3` | Global retries |
| user_communities | `--user-communities` | `HINTGRID_USER_COMMUNITIES` | `dynamic` | `dynamic` | User communities strategy |
| post_communities | `--post-communities` | `HINTGRID_POST_COMMUNITIES` | `dynamic` | `dynamic` | Post communities strategy |
| leiden_resolution | `--leiden-resolution` | `HINTGRID_LEIDEN_RESOLUTION` | `0.1` | `0.1` | Leiden resolution |
| knn_neighbors | `--knn-neighbors` | `HINTGRID_KNN_NEIGHBORS` | `10` | `10` | KNN neighbors |
| similarity_threshold | `--similarity-threshold` | `HINTGRID_SIMILARITY_THRESHOLD` | `0.7` | `0.7` | Similarity threshold |
| serendipity_probability | `--serendipity-probability` | `HINTGRID_SERENDIPITY_PROBABILITY` | `0.1` | `0.1` | Serendipity probability |
| interests_ttl_days | `--interests-ttl-days` | `HINTGRID_INTERESTS_TTL_DAYS` | `30` | `30` | TTL for INTERESTED_IN |
| interests_min_favourites | `--interests-min-favourites` | `HINTGRID_INTERESTS_MIN_FAVOURITES` | `5` | `5` | Min favourites |
| feed_size | `--feed-size` | `HINTGRID_FEED_SIZE` | `500` | `500` | Feed size |
| feed_days | `--feed-days` | `HINTGRID_FEED_DAYS` | `7` | `7` | Feed age window (days) |
| feed_ttl | `--feed-ttl` | `HINTGRID_FEED_TTL` | `none` | `none` | Feed TTL (для `feed:home:*` игнорируется) |
| feed_score_multiplier | `--feed-score-multiplier` | `HINTGRID_FEED_SCORE_MULTIPLIER` | `2` | `2` | Score multiplier (base = max_post_id * multiplier для rank-based scoring) |
| feed_force_refresh | `--feed-force-refresh` / `--no-feed-force-refresh` | `HINTGRID_FEED_FORCE_REFRESH` | `false` | `false` | Принудительно обновлять ленты всех активных пользователей (отключает dirty-user detection) |
| feed_workers | `--feed-workers` | `HINTGRID_FEED_WORKERS` | `1` | `4` | Число потоков для параллельной генерации лент (ThreadPoolExecutor) |
| active_user_days | `--active-user-days` | `HINTGRID_ACTIVE_USER_DAYS` | `90` | `90` | Окно активности пользователей (дни). Неактивные пользователи пропускаются |
| cold_start_fallback | `--cold-start-fallback` | `HINTGRID_COLD_START_FALLBACK` | `global_top` | `global_top` | Cold start strategy (в текущей реализации используется `global_top`) |
| cold_start_limit | `--cold-start-limit` | `HINTGRID_COLD_START_LIMIT` | `500` | `500` | Cold start limit |
| public_feed_enabled | `--public-feed-enabled` / `--no-public-feed-enabled` | `HINTGRID_PUBLIC_FEED_ENABLED` | `true` | `true` | Включить заполнение публичных лент рекомендациями |
| public_feed_size | `--public-feed-size` | `HINTGRID_PUBLIC_FEED_SIZE` | `400` | `400` | Лимит публичной ленты (Mastodon default ~400) |
| public_feed_strategy | `--public-feed-strategy` | `HINTGRID_PUBLIC_FEED_STRATEGY` | `local_communities` | `local_communities` | Стратегия: `local_communities` или `all_communities` |
| public_timeline_key | `--public-timeline-key` | `HINTGRID_PUBLIC_TIMELINE_KEY` | `timeline:public` | `timeline:public` | Ключ Redis для глобальной ленты |
| local_timeline_key | `--local-timeline-key` | `HINTGRID_LOCAL_TIMELINE_KEY` | `timeline:public:local` | `timeline:public:local` | Ключ Redis для локальной ленты |
| min_embedding_tokens | `--min-embedding-tokens` | `HINTGRID_MIN_EMBEDDING_TOKENS` | `1` | `1` | Минимум токенов для создания эмбеддинга (посты с меньшим числом токенов не загружаются в Neo4j) |
| embedding_skip_percentile | `--embedding-skip-percentile` | `HINTGRID_EMBEDDING_SKIP_PERCENTILE` | `0.0` | `0.05` | Процент самых коротких постов (по длине текста), пропускаемых при эмбеддинге (0.05 = пропустить 5%) |
| similarity_pruning | `--similarity-pruning` | `HINTGRID_SIMILARITY_PRUNING` | `aggressive` | `aggressive` | Pruning strategy (`aggressive`, `partial`, `temporal`, `none`) |
| prune_after_clustering | `--prune-after-clustering` | `HINTGRID_PRUNE_AFTER_CLUSTERING` | `true` | `true` | Prune after clustering |
| prune_similarity_threshold | `--prune-similarity-threshold` | `HINTGRID_PRUNE_SIMILARITY_THRESHOLD` | `0.9` | `0.9` | Partial pruning threshold |
| prune_days | `--prune-days` | `HINTGRID_PRUNE_DAYS` | `30` | `30` | Temporal pruning days |
| checkpoint_interval | `--checkpoint-interval` | `HINTGRID_CHECKPOINT_INTERVAL` | `1000` | `1000` | Checkpoint interval |
| log_level | `--log-level` | `HINTGRID_LOG_LEVEL` | `INFO` | `INFO` | Log level |
| log_file | `--log-file` | `HINTGRID_LOG_FILE` | `hintgrid.log` | `hintgrid.log` | Log file |
| progress_output | — | `HINTGRID_PROGRESS_OUTPUT` | `auto` | `auto` | Режим индикаторов: `auto` (TTY — Rich, иначе построчный вывод через `hintgrid.progress`), `rich`, `plain`. Построчный прогресс в stderr/journald виден при том же `log_level`, что и файл (по умолчанию `INFO`); при `WARNING` строки прогресса не выводятся |
| progress_poll_interval_seconds | `--progress-poll-interval-seconds` | `HINTGRID_PROGRESS_POLL_INTERVAL_SECONDS` | `0.5` | `2.0` | Пауза в секундах между опросами узла `ProgressTracker` в Neo4j при отображении прогресса `apoc.periodic.iterate`. Меньше — чаще обновление и больше нагрузка на БД; больше — реже обновление. Допустимый диапазон: от 0 (не включая) до 300 с |
| verbose | `-v`, `--verbose` | `HINTGRID_VERBOSE` | `false` | — | Подробный вывод с полными stack trace |

#### Дополнительные параметры (доступны через CLI и env)

Ниже перечислены параметры, которые можно настраивать через CLI и переменные окружения.
Это помогает управлять качеством рекомендаций, ресурсами и объёмом экспорта.

**Параметры аналитики (интересы и серендипити):**
| Параметр | CLI флаг | Env переменная | По умолчанию | Пример значения | Назначение |
|---------|----------|----------------|--------------|-----------------|-----------|
| decay_half_life_days | `--decay-half-life-days` | `HINTGRID_DECAY_HALF_LIFE_DAYS` | `14` | `14` | Период полураспада (дни) для экспоненциального затухания взаимодействий |
| likes_weight | `--likes-weight` | `HINTGRID_LIKES_WEIGHT` | `1.0` | `1.0` | Вес `FAVORITED` при расчёте `INTERESTED_IN` и в INTERACTS_WITH агрегации (SQL: count(*) * likes_weight) |
| reblogs_weight | `--reblogs-weight` | `HINTGRID_REBLOGS_WEIGHT` | `3.0` | `3.0` | Вес `REBLOGGED` при расчёте `INTERESTED_IN` и в INTERACTS_WITH агрегации (SQL: count(*) * reblogs_weight). Основано на индустриальных практиках (Twitter Heavy Ranker, Meta): репосты в 3-10 раз ценнее лайков |
| replies_weight | `--replies-weight` | `HINTGRID_REPLIES_WEIGHT` | `5.0` | `5.0` | Вес `REPLIED` при расчёте `INTERESTED_IN` и в INTERACTS_WITH агрегации (SQL: count(*) * replies_weight). Основано на индустриальных практиках: ответы в 10-15 раз ценнее лайков (Twitter оценивает беседы в 75x) |
| follows_weight | `--follows-weight` | `HINTGRID_FOLLOWS_WEIGHT` | `10.0` | `10.0` | Вес FOLLOWS в INTERACTS_WITH агрегации (SQL: каждый follow добавляет follows_weight). Установите 0.0 для исключения FOLLOWS из кластеризации. Основано на индустриальных практиках: подписки в 20-50 раз ценнее лайков, так как сигнализируют о долгосрочном интересе |
| mentions_weight | `--mentions-weight` | `HINTGRID_MENTIONS_WEIGHT` | `5.0` | `5.0` | Вес упоминаний в INTERACTS_WITH агрегации (SQL: count(*) * mentions_weight). Основано на индустриальных практиках: упоминания в 10-15 раз ценнее лайков, аналогично ответам |
| bookmark_weight | `--bookmark-weight` | `HINTGRID_BOOKMARK_WEIGHT` | `2.0` | `2.0` | Вес `BOOKMARKED` (сильнее лайков) |
| serendipity_limit | `--serendipity-limit` | `HINTGRID_SERENDIPITY_LIMIT` | `100` | `100` | Максимум serendipity связей за запуск |
| serendipity_score | `--serendipity-score` | `HINTGRID_SERENDIPITY_SCORE` | `0.1` | `0.1` | Базовый score для serendipity |
| serendipity_based_on | `--serendipity-based-on` | `HINTGRID_SERENDIPITY_BASED_ON` | `0` | `0` | Поле `based_on` для serendipity связей |
| ctr_enabled | `--ctr-enabled` / `--no-ctr-enabled` | `HINTGRID_CTR_ENABLED` | `true` | `true` | Включить учет CTR (Click-Through Rate) при расчете интересов |
| ctr_weight | `--ctr-weight` | `HINTGRID_CTR_WEIGHT` | `0.5` | `0.5` | Вес CTR в расчете интереса (0.0-1.0) |
| min_ctr | `--min-ctr` | `HINTGRID_MIN_CTR` | `0.0` | `0.0` | Минимальный CTR для учета интереса |
| ctr_smoothing | `--ctr-smoothing` | `HINTGRID_CTR_SMOOTHING` | `1.0` | `1.0` | Сглаживание CTR для избежания деления на ноль |

**Параметры PageRank:**
| Параметр | CLI флаг | Env переменная | По умолчанию | Пример значения | Назначение |
|---------|----------|----------------|--------------|-----------------|-----------|
| pagerank_enabled | `--pagerank-enabled` / `--no-pagerank-enabled` | `HINTGRID_PAGERANK_ENABLED` | `true` | `true` | Включить расчёт PageRank для постов |
| pagerank_weight | `--pagerank-weight` | `HINTGRID_PAGERANK_WEIGHT` | `0.1` | `0.1` | Вес PageRank в итоговом score поста (множитель) |
| pagerank_damping_factor | `--pagerank-damping-factor` | `HINTGRID_PAGERANK_DAMPING_FACTOR` | `0.85` | `0.85` | Damping factor для PageRank (0–1) |
| pagerank_max_iterations | `--pagerank-max-iterations` | `HINTGRID_PAGERANK_MAX_ITERATIONS` | `20` | `20` | Максимум итераций PageRank |

**Параметры Community Similarity (serendipity на основе сообществ):**
| Параметр | CLI флаг | Env переменная | По умолчанию | Пример значения | Назначение |
|---------|----------|----------------|--------------|-----------------|-----------|
| community_similarity_enabled | `--community-similarity-enabled` / `--no-community-similarity-enabled` | `HINTGRID_COMMUNITY_SIMILARITY_ENABLED` | `true` | `true` | Включить расчёт Jaccard-подобия между UserCommunity |
| community_similarity_top_k | `--community-similarity-top-k` | `HINTGRID_COMMUNITY_SIMILARITY_TOP_K` | `5` | `5` | Число ближайших похожих сообществ |

**Параметры ранжирования ленты:**
| Параметр | CLI флаг | Env переменная | По умолчанию | Пример значения | Назначение |
|---------|----------|----------------|--------------|-----------------|-----------|
| personalized_interest_weight | `--personalized-interest-weight` | `HINTGRID_PERSONALIZED_INTEREST_WEIGHT` | `0.5` | `0.5` | Вклад интересов в итоговый score |
| personalized_popularity_weight | `--personalized-popularity-weight` | `HINTGRID_PERSONALIZED_POPULARITY_WEIGHT` | `0.3` | `0.3` | Вклад популярности |
| personalized_recency_weight | `--personalized-recency-weight` | `HINTGRID_PERSONALIZED_RECENCY_WEIGHT` | `0.2` | `0.2` | Вклад свежести |
| cold_start_popularity_weight | `--cold-start-popularity-weight` | `HINTGRID_COLD_START_POPULARITY_WEIGHT` | `0.7` | `0.7` | Вклад популярности при cold start |
| cold_start_recency_weight | `--cold-start-recency-weight` | `HINTGRID_COLD_START_RECENCY_WEIGHT` | `0.3` | `0.3` | Вклад свежести при cold start |
| popularity_smoothing | `--popularity-smoothing` | `HINTGRID_POPULARITY_SMOOTHING` | `1.0` | `1.0` | Сглаживание популярности (`log10`), должно быть > 0 |
| recency_smoothing | `--recency-smoothing` | `HINTGRID_RECENCY_SMOOTHING` | `1.0` | `0.8` | Слагаемое к возрасту в днях в знаменателе свежести; вещественное, > 0 |
| recency_numerator | `--recency-numerator` | `HINTGRID_RECENCY_NUMERATOR` | `1.0` | `1.0` | Числитель для компоненты свежести. Используется в формуле `recency_numerator / (age_hours / 24.0 + recency_smoothing)` |
| language_match_weight | `--language-match-weight` | `HINTGRID_LANGUAGE_MATCH_WEIGHT` | `0.3` | `0.3` | Бонус за совпадение языка поста с `chosen_languages` (и в «нейтральных» ветках CASE; не ниже приоритета UI) |
| ui_language_match_weight | `--ui-language-match-weight` | `HINTGRID_UI_LANGUAGE_MATCH_WEIGHT` | `0.5` | `0.5` | Бонус за совпадение с нормализованным `users.locale`; должен быть ≥ `language_match_weight` |

**Параметры кластеризации и similarity графа:**
| Параметр | CLI флаг | Env переменная | По умолчанию | Пример значения | Назначение |
|---------|----------|----------------|--------------|-----------------|-----------|
| leiden_max_levels | `--leiden-max-levels` | `HINTGRID_LEIDEN_MAX_LEVELS` | `10` | `10` | Максимум уровней Leiden |
| leiden_diagnostics | — | `HINTGRID_LEIDEN_DIAGNOSTICS` | `false` | `true` | Расширенные метрики графа и результата Leiden в логе (без второго прогона алгоритма) |
| singleton_collapse_enabled | `--singleton-collapse-enabled` | `HINTGRID_SINGLETON_COLLAPSE_ENABLED` | `true` | `false` | Слияние одиночных Leiden-кластеров в `noise_community_id` перед материализацией сообществ |
| singleton_collapse_in_transactions_of | `--singleton-collapse-in-transactions-of` | `HINTGRID_SINGLETON_COLLAPSE_IN_TRANSACTIONS_OF` | `100000` | `0` | `0` — одна транзакция; иначе размер батча `IN TRANSACTIONS` при `SET` после `UNWIND` |
| noise_community_id | `--noise-community-id` | `HINTGRID_NOISE_COMMUNITY_ID` | `-1` | `-2` | Резервируемый id «корзины» для одиночных кластеров; не `0` |
| similarity_recency_days | `--similarity-recency-days` | `HINTGRID_SIMILARITY_RECENCY_DAYS` | `7` | `7` | Окно свежести постов для `SIMILAR_TO` |
| knn_self_neighbor_offset | `--knn-self-neighbor-offset` | `HINTGRID_KNN_SELF_NEIGHBOR_OFFSET` | `1` | `1` | Смещение для `top_k` (исключение self‑neighbor) |
| similarity_iterate_batch_size | — | `HINTGRID_SIMILARITY_ITERATE_BATCH_SIZE` | `2000` | `1000` | Размер батча для построения графа `SIMILAR_TO` (отдельно от `apoc_batch_size`; снижать при ошибках транзакционной памяти Neo4j) |

**Параметры экспорта Markdown:**
| Параметр | CLI флаг | Env переменная | По умолчанию | Пример значения | Назначение |
|---------|----------|----------------|--------------|-----------------|-----------|
| export_max_items | `--export-max-items` | `HINTGRID_EXPORT_MAX_ITEMS` | `50` | `50` | Максимум элементов в списке ленты |
| text_preview_limit | `--text-preview-limit` | `HINTGRID_TEXT_PREVIEW_LIMIT` | `60` | `60` | Длина текстового превью поста |
| community_interest_limit | `--community-interest-limit` | `HINTGRID_COMMUNITY_INTEREST_LIMIT` | `30` | `30` | Лимит интересов на сообщество |
| community_member_sample | `--community-member-sample` | `HINTGRID_COMMUNITY_MEMBER_SAMPLE` | `5` | `5` | Размер сэмпла пользователей |
| community_sample_limit | `--community-sample-limit` | `HINTGRID_COMMUNITY_SAMPLE_LIMIT` | `5` | `5` | Число сообществ для примера |
| graph_sample_limit | `--graph-sample-limit` | `HINTGRID_GRAPH_SAMPLE_LIMIT` | `10` | `10` | Лимит связей/узлов в Mermaid |
| feed_score_decimals | `--feed-score-decimals` | `HINTGRID_FEED_SCORE_DECIMALS` | `4` | `4` | Точность округления score при экспорте лент в Markdown (команда `export`) |

**Параметры загрузки данных:**
| Параметр | CLI флаг | Env переменная | По умолчанию | Пример значения | Назначение |
|---------|----------|----------------|--------------|-----------------|-----------|
| load_since | `--load-since` | `HINTGRID_LOAD_SINCE` | `null` | `30d` | Окно загрузки постов и поведения (не влияет на пользователей) |

**Параметры клиентов БД и интеграций:**
| Параметр | CLI флаг | Env переменная | По умолчанию | Пример значения | Назначение |
|---------|----------|----------------|--------------|-----------------|-----------|
| pg_pool_min_size | `--pg-pool-min-size` | `HINTGRID_PG_POOL_MIN_SIZE` | `1` | `1` | Минимум соединений PostgreSQL |
| pg_pool_max_size | `--pg-pool-max-size` | `HINTGRID_PG_POOL_MAX_SIZE` | `5` | `5` | Максимум соединений PostgreSQL |
| pg_pool_timeout_seconds | `--pg-pool-timeout-seconds` | `HINTGRID_PG_POOL_TIMEOUT_SECONDS` | `30` | `30` | Таймаут пула PostgreSQL |
| neo4j_ready_retries | `--neo4j-ready-retries` | `HINTGRID_NEO4J_READY_RETRIES` | `30` | `30` | Число попыток ожидания Neo4j |
| neo4j_ready_sleep_seconds | `--neo4j-ready-sleep-seconds` | `HINTGRID_NEO4J_READY_SLEEP_SECONDS` | `1` | `1` | Пауза между попытками Neo4j |
| redis_score_tolerance | `--redis-score-tolerance` | `HINTGRID_REDIS_SCORE_TOLERANCE` | `1e-6` | `1e-6` | Допуск сравнения score в Redis cleanup |
| mastodon_public_visibility | `--mastodon-public-visibility` | `HINTGRID_MASTODON_PUBLIC_VISIBILITY` | `0` | `0` | `visibility` для публичных статусов |
| mastodon_account_lookup_limit | `--mastodon-account-lookup-limit` | `HINTGRID_MASTODON_ACCOUNT_LOOKUP_LIMIT` | `1` | `1` | Лимит поиска аккаунта |

**Параметры параллелизма и инфраструктуры:**
| Параметр | CLI флаг | Env переменная | По умолчанию | Пример значения | Назначение |
|---------|----------|----------------|--------------|-----------------|-----------|
| postgres_schema | `--postgres-schema` | `HINTGRID_POSTGRES_SCHEMA` | `public` | `public` | Схема PostgreSQL для запросов |
| neo4j_worker_label | `--neo4j-worker-label` | `HINTGRID_NEO4J_WORKER_LABEL` | `null` | `worker1` | Префикс-лейбл для изоляции графов GDS (multi-worker mode) |
| loader_workers | `--loader-workers` | `HINTGRID_LOADER_WORKERS` | `1` | `4` | Число ThreadPool workers для параллельной загрузки сущностей |
| fasttext_training_workers | `--fasttext-training-workers` | `HINTGRID_FASTTEXT_TRAINING_WORKERS` | `0` | `4` | Число потоков для обучения FastText (0 = auto-detect через `os.cpu_count()`) |
| apoc_batch_size | `--apoc-batch-size` | `HINTGRID_APOC_BATCH_SIZE` | `10000` | `10000` | Размер батча для `apoc.periodic.iterate`: INTERESTED_IN, узлы UserCommunity/PostCommunity и BELONGS_TO, cleanup, **батчевое удаление** исходящих `SIMILAR_TO` при полной пересборке похожести, aggressive pruning, **полное удаление узлов Neo4j при `hintgrid clean` (ветка `--graph` или полный `clean`)** — чтобы не исчерпывать лимит транзакционной памяти Neo4j на больших графах (1–100000; при редких OOM можно уменьшить). Построение `SIMILAR_TO` использует `similarity_iterate_batch_size` |
| memory_interval | `--memory-interval` | `HINTGRID_MEMORY_INTERVAL` | `10` | `10` | Интервал в секундах для мониторинга памяти во время выполнения пайплайна |

### FastText embeddings по умолчанию

**По умолчанию HintGrid использует встроенные FastText эмбеддинги** — внешний LLM сервер не требуется. Если `llm.base_url` не задан, FastText включается автоматически.

Для использования внешнего LLM (Ollama, OpenAI и др.) необходимо явно задать `HINTGRID_LLM_BASE_URL`.

**Поведение встроенного FastText сервиса:**
- Обучается на данных из PostgreSQL через server-side cursor (без загрузки всего корпуса в память).
- Использует TweetTokenizer и детекцию биграмм для социального текста.
- Хранит состояние модели в Neo4j (узел `FastTextState`) и файлы модели на диске.
- Поддерживает полное и инкрементальное обучение.
- При первом запуске без модели выполняет автоматическое full-обучение (если доступен PostgreSQL).

**Важно:** Все состояние приложения хранится в Neo4j. Модели FastText хранятся в файловой системе (см. `fasttext.model_path`).

При необходимости можно подключить внешний LLM сервер, задав `llm.base_url` и соответствующие параметры модели.

### Переменные окружения

Переменные окружения можно задавать в shell, systemd, Docker или через любой менеджер секретов. Если параметр передан и через CLI, и через env, будет использовано значение CLI.

---

## Установка и настройка

### 1. Установка Ollama (опционально)

**Примечание:** По умолчанию HintGrid использует встроенные FastText embeddings. Ollama требуется только если вы хотите использовать AI-эмбеддинги.

Для использования Ollama: установите его, загрузите модель эмбеддингов, убедитесь в доступности API и задайте `HINTGRID_LLM_BASE_URL=http://localhost:11434`.

### 2. Установка Neo4j Community + GDS

Разверните Neo4j с Graph Data Science и проверьте доступность базы и процедур.

**Важно для Community Edition:**
- GDS в Community ограничен 4 потоками CPU (для всех алгоритмов)
- Требуется выделить достаточно heap памяти (минимум 2GB, рекомендуется 4GB+)
- Плагин GDS устанавливается автоматически через переменную `NEO4J_PLUGINS`

**Docker Compose вариант:**

В docker-compose.dev.yml есть готовый пример конфигурации Neo4j для разработки.

### 3. Конфигурация

Настройте окружение одним из способов:
- Передайте параметры через CLI при запуске
- Задайте переменные окружения (удобно для systemd, Docker, CI)

Оба способа равноправны, приоритет у CLI.

### 4. Запуск утилиты

Запускайте утилиту с нужным набором параметров и режимом работы. Доступные команды:

**Основные команды:**
- `run`: полный цикл обработки; флаги `--dry-run`, `--user-id`, `--train` (инкрементальное обучение перед пайплайном).
- `export <filename>`: экспорт состояния в Markdown; обязательный `--user-id`.
- `train`: обучение FastText; обязателен один из режимов `--full` или `--incremental` (взаимоисключающие); `--since` принимает `YYYY-MM-DD` или `Nd`.
- `validate`: проверка конфигурации и вывод текущей embedding-signature.
- `reindex`: переиндексация эмбеддингов; флаг `--dry-run`.
- `refresh`: лёгкое инкрементальное обновление интересов (глобальный decay + пересчёт только «грязных» сообществ). Не требует полного пересчёта пайплайна. Если `last_interests_rebuild_at` отсутствует, выполняется полная перестройка.
- `clean`: очистка Neo4j и удаление рекомендаций HintGrid из Redis. Поддерживает селективную очистку: `--graph`, `--redis`, `--models`, `--embeddings`, `--clusters`, `--similarity`, `--interests`, `--interactions`, `--recommendations`, `--fasttext-state`. Удаление всех узлов графа выполняется **батчами** через APOC (`apoc.periodic.iterate`), размер батча задаётся `HINTGRID_APOC_BATCH_SIZE` (не требует увеличивать `dbms.memory.transaction.total.max` только ради полного `clean`). Для полного `clean` и для ветки `--graph` прогресс удаления узлов Neo4j отображается так же, как у батчевых этапов пайплайна: опрос узла `ProgressTracker` в Neo4j; в интерактивном TTY — полоса Rich, без TTY — построчный вывод `hintgrid.progress` (при уровне логирования, достаточном для INFO). Режим вывода и интервал опроса задаются `HINTGRID_PROGRESS_OUTPUT` и `HINTGRID_PROGRESS_POLL_INTERVAL_SECONDS` (см. таблицу настроек выше).
- `get-user-info <handle>`: получение детальной информации о пользователе Mastodon по handle (@user или @user@domain). Выводит таблицу с username, domain, языками и другой информацией через Rich UI; дополнительно — **топ-3 поста** из домашней ленты Redis (`feed:home:<account_id>`) с тем же составом полей, что и `get-post-info` (включая score в Redis для сверки с `ZREVRANGE`). Перед блоком с постами выводится **снимок релевантных настроек ленты** (текущие значения из конфигурации) и строка режима объяснения. Для каждого из топ-постов — **объяснение скоринга**: путь `personalized` (цепочка `UserCommunity`→`INTERESTED_IN`→`PostCommunity`) или `cold_start`, проверки графовых фильтров, разложение итогового Cypher-скоринга на вклады (интерес, популярность, свежесть, при необходимости PageRank, языковой бонус), свойства рёбра `INTERESTED_IN` при персонализации, а также **место в Redis** (`ZSCORE` / `ZREVRANK` по ключу `feed:home:<account_id>`). **По умолчанию** при объяснении **не** применяется исключение по связи `WAS_RECOMMENDED` (чтобы для постов, уже попавших в выдачу и помеченных в графе, по-прежнему показывались путь и разложение score). Флаг **`--feed-explain-respect-was-recommended`** (переменная окружения `HINTGRID_FEED_EXPLAIN_RESPECT_WAS_RECOMMENDED`) включает **строгий** режим: те же фильтры, что и у генерации ленты, включая отсев постов с уже существующей связью `WAS_RECOMMENDED` для данного пользователя. Это **реконструкция по текущему графу и текущим настройкам**, а не снимок на момент последнего запуска генерации лент.
- `get-post-info <ссылка>`: информация о посте по URL Mastodon, по публичному id из пути `/statuses/...` или по internal `statuses.id` (если такая строка есть в БД). См. SQL выше про различие id в URL и `statuses.id`. Опция **`--viewer` / `-w`** (handle аккаунта-читателя) добавляет тот же отчёт об объяснении попадания поста в домашнюю ленту этого зрителя, что и для элементов топа в `get-user-info` (снимок настроек, строка режима, разложение скоринга + Redis). Поведение **`--feed-explain-respect-was-recommended`** такое же, как у `get-user-info`. Без `--viewer` команда только печатает свойства поста и автора из Neo4j/PostgreSQL.
- `model-export <output.tar.gz>`: экспорт предобученной модели FastText + Phraser в `.tar.gz` архив; опция `--mode` (`inference` или `full`).
- `model-import <archive.tar.gz>`: импорт модели из `.tar.gz` архива; флаг `--force` для перезаписи.

Подробное описание команды export см. в разделе [Визуализация и отладка](#визуализация-и-отладка).
Подробное описание model-export / model-import см. в разделе [Распространение моделей (Model Bundle)](#распространение-моделей-model-bundle).

### 5. Настройка cron

Настройте периодический запуск утилиты и отдельные задания для очистки устаревших связей.
Для более частого обновления интересов между полными прогонами используйте команду `refresh`.

### 6. Systemd Timer (альтернатива cron)

Альтернатива cron — unit `Type=oneshot` для команды `hintgrid run` и **timer** с расписанием. Готовые примеры лежат в репозитории:

- `deploy/systemd/hintgrid-run.service` — пониженный приоритет CPU (`Nice=10`), класс планирования ввода-вывода **best-effort** (`IOSchedulingClass=best-effort`), увеличенный таймаут старта (`TimeoutStartSec=infinity`), переменные из `/opt/hintgrid/.env`; **`After=`** / **`Wants=`** для одного хоста с Mastodon: `postgresql.service`, `redis-server.service`, `docker.service`, `mastodon-web.service`, `mastodon-sidekiq.service` (PostgreSQL и Redis локально, Neo4j в Docker). Параллельный второй экземпляр того же unit не стартует, пока идёт `ExecStart` (`Type=oneshot`).
- `deploy/systemd/hintgrid-run.timer` — по умолчанию `OnBootSec` и **`OnUnitInactiveSec=10min`**: следующий запуск через 10 минут после того, как сервис **завершился** (не фиксированные :00/:10 на часах). В комментариях в файле — опциональный вариант **`OnCalendar=*-*-* *:0/10:00`** для расписания по стеночным часам.

Пошаговая установка на сервер Mastodon (пользователь ОС `hintgrid`, **создание в PostgreSQL роли только на чтение** для базы Mastodon, **Neo4j в Docker** через `deploy/docker-compose.neo4j.yml`, пути, команды `systemctl`, интеграция Redis с Mastodon) описана в [INSTALL.ru.md](../INSTALL.ru.md) и [INSTALL.md](../INSTALL.md) (English). **Обновление** установленного HintGrid (`git pull` и `pip`, wheel, при необходимости `docker compose` для Neo4j и `daemon-reload` для systemd) — раздел **«Обновление HintGrid»** в INSTALL. Там же — почему **первый** запуск `hintgrid run` обычно дольше и тяжелее для сервера, чем последующие (курсоры загрузки, первое обучение FastText, объём графа и лент). Очистка рекомендаций HintGrid в Redis (преимущественно `hintgrid clean --redis`; в INSTALL также предупреждение о бэкапе и опасный необязательный вариант с `redis-cli DEL` — **не проверен**, может сломать инстанс) — раздел **«Очистка рекомендаций HintGrid в Redis»** в INSTALL.

---

## Распространение моделей (Model Bundle)

HintGrid позволяет упаковать предобученную модель FastText и Phraser в единый `.tar.gz` архив для удобного распространения между инсталляциями. Архив содержит файлы модели, метаданные обучения, контрольные суммы (SHA-256) и информацию о совместимости.

### Экспорт модели

Команда `model-export` читает состояние модели из Neo4j (`FastTextState`), собирает файлы модели с диска и упаковывает их в `.tar.gz` архив вместе с манифестом.

**Требования:**
- Модель должна быть обучена (`hintgrid train --full`)
- Доступ к Neo4j для чтения `FastTextState`
- Файлы модели на диске (по пути `fasttext.model_path`)

**Примеры использования:**

```bash
# Экспорт для инференса (минимальный размер, квантованная модель)
hintgrid model-export models_v1.tar.gz

# Экспорт полной модели (для дообучения на другом сервере)
hintgrid model-export models_v1.tar.gz --mode full

# С явным указанием подключения к Neo4j
hintgrid model-export models_v1.tar.gz --mode inference \
  --neo4j-host neo4j.internal --neo4j-password s3cr3t
```

### Импорт модели

Команда `model-import` извлекает архив, проверяет контрольные суммы, валидирует совместимость параметров и копирует файлы модели в `fasttext.model_path`. После успешного импорта обновляется `FastTextState` в Neo4j.

**Примеры использования:**

```bash
# Импорт модели
hintgrid model-import models_v1.tar.gz

# Импорт с перезаписью существующих файлов
hintgrid model-import models_v1.tar.gz --force

# С явным указанием подключения к Neo4j
hintgrid model-import models_v1.tar.gz \
  --neo4j-host neo4j.internal --neo4j-password s3cr3t
```

**Важно:** После импорта модели рекомендуется выполнить `hintgrid reindex` для переиндексации эмбеддингов в Neo4j с использованием новой модели.

### Формат архива

Архив имеет стандартный формат `.tar.gz` и содержит:

| Файл | Описание |
|------|----------|
| `manifest.json` | Метаданные: версия модели, параметры обучения, контрольные суммы, совместимость |
| `phraser_v{N}.pkl` | Phraser модель для детекции биграмм (всегда включён) |
| `fasttext_v{N}.q.bin` | Сжатые векторы FastText (KeyedVectors / compress-fasttext; inference/full) |
| `fasttext_v{N}.bin` | Полная FastText модель (только full) |
| `phrases_v{N}.pkl` | Phrases модель (только full) |
| `fasttext_v{N}.bin.wv.vectors_ngrams.npy` | N-gram векторы (inference/full) |

Пример содержимого `manifest.json`:

```json
{
  "schema_version": 1,
  "version": 3,
  "mode": "inference",
  "training_params": {
    "vector_size": 128,
    "window": 3,
    "min_count": 10,
    "bucket": 10000,
    "epochs": 5,
    "max_vocab_size": 500000
  },
  "statistics": {
    "vocab_size": 15000,
    "corpus_size": 42000,
    "last_trained_post_id": 98765
  },
  "compatibility": {
    "hintgrid_version": "0.1.0",
    "gensim_version": "4.3.3",
    "python_version": "3.11.8"
  },
  "files": {
    "phraser_v3.pkl": "a1b2c3d4e5f6...",
    "fasttext_v3.q.bin": "f6e5d4c3b2a1..."
  },
  "created_at": "2026-02-07T12:00:00+00:00"
}
```

### Режимы бандла

| Режим | Описание | Включённые файлы | Размер |
|-------|----------|-------------------|--------|
| `inference` | Минимальный набор для генерации эмбеддингов | Phraser + квантованная FastText + n-gram векторы | Маленький (10-50× сжатие) |
| `full` | Полный набор для продолжения обучения | Phrases + Phraser + полная FastText + квантованная + n-gram | Большой |

**Рекомендации:**
- Используйте `inference` для распространения модели конечным пользователям
- Используйте `full` для переноса модели на другой сервер с возможностью дообучения

### Валидация и совместимость

При импорте выполняются следующие проверки:

1. **Контрольные суммы (SHA-256)**: Каждый файл в архиве проверяется на целостность. При несовпадении хеша импорт прерывается.

2. **Совместимость `vector_size`**: Размерность эмбеддингов в бандле должна совпадать с текущей настройкой `HINTGRID_FASTTEXT_VECTOR_SIZE`. При несовпадении — ошибка с подсказкой.

3. **Версия Gensim**: При несовпадении версии Gensim выводится предупреждение (модель может не загрузиться).

4. **Версия Python**: При несовпадении major.minor версии Python выводится предупреждение (pickle-файлы могут быть несовместимы).

5. **Конфликт версий**: Если файлы модели с такой версией уже существуют, импорт отклоняется. Используйте `--force` для перезаписи.

---

## Graceful Shutdown (Ctrl+C)

### Поведение при прерывании

HintGrid поддерживает корректное завершение работы при нажатии Ctrl+C (SIGINT) или получении SIGTERM:

1. **Первое нажатие Ctrl+C**: устанавливает флаг `shutdown_requested`. Текущий batch дообрабатывается до конца, состояние (курсоры) сохраняется в Neo4j, и пайплайн корректно завершается с отображением Rich UI панели прерывания.
2. **Второе нажатие Ctrl+C**: немедленное принудительное завершение через `KeyboardInterrupt`.

Менеджер корректного завершения (`ShutdownManager`) работает как context manager, устанавливая собственные обработчики SIGINT/SIGTERM при входе и восстанавливая оригинальные при выходе. Все операции потокобезопасны — проверка флага и обновление статусов шагов защищены блокировками.

### Стратегии возобновления шагов

Каждый шаг пайплайна имеет одну из двух стратегий возобновления:

| Шаг | Стратегия | Описание |
|-----|-----------|----------|
| **Загрузка: statuses** | Resumes | Продолжает с `last_processed_status_id` |
| **Загрузка: favourites** | Resumes | Продолжает с `last_processed_favourite_id` |
| **Загрузка: blocks** | Resumes | Продолжает с `last_processed_block_id` |
| **Загрузка: mutes** | Resumes | Продолжает с `last_processed_mute_id` |
| **Загрузка: user activity** | Resumes | Продолжает с `last_processed_activity_account_id` (сбрасывается при полном запуске) |
| **User clustering (Leiden)** | Restarts | GDS проекция транзиентна, шаг начинается заново |
| **Post clustering (Leiden)** | Restarts | GDS проекция транзиентна, шаг начинается заново |
| **PageRank** | Restarts | GDS проекция транзиентна, шаг начинается заново |
| **Interest rebuild** | Restarts | Полная перестройка INTERESTED_IN |
| **Community similarity** | Restarts | Пересчёт Jaccard similarity |
| **Serendipity** | Restarts | Случайные связи создаются заново |
| **Генерация лент** | Resumes | Продолжает с `last_processed_feed_user_id` |

**Resumes** — шаг продолжает с сохранённого курсора; данные, обработанные до прерывания, не теряются.

**Restarts** — шаг начинается заново, так как его результаты зависят от транзиентных in-memory проекций GDS или требуют полного пересчёта. Операции идемпотентны (MERGE), поэтому повторный запуск безопасен.

### Курсор активности пользователей

Курсор `last_processed_activity_account_id` имеет особую семантику:

- **Сбрасывается в 0** при каждом полном запуске `hintgrid run`
- Используется **только** для resume после Ctrl+C в рамках текущего запуска
- При следующем полном запуске все активные пользователи пересканируются заново

Причина: `lastActive` — мутабельное значение, которое меняется при каждом входе пользователя в систему. Простое продолжение с курсора пропустило бы обновления для уже обработанных аккаунтов.

```sql
-- SQL запрос отсортирован по a.id ASC, что обеспечивает монотонность курсора
SELECT a.id AS account_id,
       GREATEST(
           COALESCE(s.last_status_at, a.created_at),
           COALESCE(u.current_sign_in_at, a.created_at)
       ) AS last_active
FROM accounts a
LEFT JOIN account_stats s ON s.account_id = a.id
LEFT JOIN users u ON u.account_id = a.id
WHERE a.id > :last_account_id
ORDER BY a.id ASC;
```

### Курсор генерации лент

Курсор `last_processed_feed_user_id` позволяет возобновлять генерацию лент после прерывания:

- Пропускает пользователей, для которых ленты уже сгенерированы (`user_id <= last_feed_user_id`)
- **Сбрасывается в 0** после полного завершения генерации лент
- Используется для resume после Ctrl+C

### Селективное обновление лент (Dirty-User Detection)

Для минимизации числа обновлений лент система отслеживает свойство `feedGeneratedAt` на узлах `User`. После генерации ленты для пользователя устанавливается текущее время:

```cypher
MATCH (u:User {id: $user_id})
SET u.feedGeneratedAt = datetime();
```

При следующем запуске обновляются **только** локальные пользователи (`isLocal = true`), чьё состояние графа изменилось:

```cypher
MATCH (u:User)
WHERE u.isLocal = true
  AND (u.lastActive IS NULL
   OR u.lastActive >= datetime() - duration({days: $active_days}))
AND (
  -- 1. Лента никогда не генерировалась
  u.feedGeneratedAt IS NULL
  -- 2. Появились новые посты в связанных PostCommunity
  OR EXISTS {
    MATCH (u)-[:BELONGS_TO]->(:UserCommunity)-[:INTERESTED_IN]->(pc:PostCommunity)
          <-[:BELONGS_TO]-(p:Post)
    WHERE p.createdAt > u.feedGeneratedAt
  }
  -- 3. Обновились интересы сообщества (INTERESTED_IN)
  OR EXISTS {
    MATCH (u)-[:BELONGS_TO]->(uc:UserCommunity)-[i:INTERESTED_IN]->(:PostCommunity)
    WHERE i.last_updated > u.feedGeneratedAt
  }
  -- 4. Пользователь «потребил» большую часть ленты (≥80% WAS_RECOMMENDED)
  OR size([(u)-[wr:WAS_RECOMMENDED]->(:Post)
    WHERE wr.at > u.feedGeneratedAt | wr]) >= $consumption_threshold
)
RETURN u.id AS id
ORDER BY u.id;
```

**Важно:**
- Dirty-user detection работает **только для локальных пользователей** (`isLocal = true`)
- Функция `stream_dirty_user_ids` возвращает только локальных пользователей
- Федеративные пользователи не получают персональные ленты, но участвуют в аналитике (кластеризация, интересы, PageRank)

**Критерии «грязного» пользователя:**

| Критерий | Описание |
|----------|----------|
| `feedGeneratedAt IS NULL` | Лента ещё не генерировалась |
| Новые посты в PostCommunity | В связанных топиках появились свежие посты |
| Обновлённые INTERESTED_IN | Интересы сообщества пересчитаны (after rebuild/refresh) |
| Высокое потребление (≥80%) | Пользователь просмотрел/получил рекомендации на большинство постов |

**Принудительное обновление:**

Параметр `feed_force_refresh` отключает dirty-user detection и обновляет ленты **всех** активных пользователей (активных в пределах `active_user_days`).

**Индекс для производительности:**

```cypher
CREATE INDEX user_feed_generated_at IF NOT EXISTS
FOR (u:User) ON (u.feedGeneratedAt);
```

### Rich UI панель прерывания

При прерывании (Ctrl+C) выводится Rich UI панель с детальной информацией:

```
⚠ Pipeline Interrupted (Ctrl+C)

┌───────────── Pipeline Steps ─────────────┐
│ Step                 │ Status      │ Items │ On Resume │
│ Data loading: statuses  │ ✓ Completed │ 1,500 │ Resumes   │
│ Data loading: favourites│ ⚠ Interrupted │  250 │ Resumes   │
│ User clustering (Leiden)│ ○ Pending   │   —   │ Restarts  │
│ Feed generation         │ ○ Pending   │   —   │ Resumes   │
└─────────────────────────────────────────────┘

┌──── Saved Cursors ────┐
│ Cursor          │ Value │
│ last_status_id  │ 1,500 │
│ last_favourite_id│  250  │
└───────────────────────┘

Pipeline will resume from saved cursors on next run.
```

Панель содержит:
- **Pipeline Steps** — таблица всех шагов с их статусом (✓ Completed / ⚠ Interrupted / ○ Pending), количеством обработанных элементов и стратегией возобновления
- **Saved Cursors** — таблица курсоров с ненулевыми значениями, сохранённых в Neo4j AppState
- Сообщение о том, что пайплайн возобновится с сохранённых курсоров

---

## Neo4j GDS API

### Leiden Community Detection (GDS)

Neo4j использует Graph Data Science (GDS) library для алгоритмов кластеризации. В Community Edition доступен полный функционал Leiden, но с ограничением в 4 потока CPU.

**Создание проекции графа (Project Graph):**

```cypher
// Проекция графа пользователей для кластеризации (FOLLOWS + INTERACTS_WITH)
CALL gds.graph.project(
    'user-graph',              // Имя проекции
    'User',                    // Узлы
    {
        FOLLOWS: { orientation: 'UNDIRECTED' },
        INTERACTS_WITH: { orientation: 'UNDIRECTED', properties: 'weight' }
    }
)
YIELD graphName, nodeCount, relationshipCount;
```

**Запуск Leiden:**

```cypher
// Кластеризация пользователей по подпискам и взаимодействиям
CALL gds.leiden.write(
    'user-graph',              // Имя проекции
    {
        writeProperty: 'cluster_id',  // Куда записать результат
        relationshipWeightProperty: 'weight',  // Учитываем веса INTERACTS_WITH
        maxLevels: 10,                // Максимум уровней (итераций)
        gamma: 1.0                    // Разрешение (выше = больше мелких кластеров)
    }
)
YIELD nodePropertiesWritten, communityCount, modularity
RETURN nodePropertiesWritten, communityCount, modularity;
```

**Удаление проекции после использования:**

```cypher
CALL gds.graph.drop('user-graph') YIELD graphName
YIELD graphName;
```

**Пример для постов с весами:**

```cypher
// 1. Проекция графа постов с весами
CALL gds.graph.project(
    'post-graph',
    'Post',
    {
        SIMILAR_TO: {
            properties: 'weight'       // Используем cosine similarity
        }
    }
)
YIELD graphName, nodeCount, relationshipCount;

// 2. Кластеризация с учетом весов
CALL gds.leiden.write(
    'post-graph',
    {
        writeProperty: 'cluster_id',
        relationshipWeightProperty: 'weight',  // Учитываем веса
        maxLevels: 10,
        gamma: 1.0
    }
)
YIELD nodePropertiesWritten, communityCount, modularity;

// 3. Очистка
CALL gds.graph.drop('post-graph') YIELD graphName;
```

**Важно:**
- GDS требует создания проекции (in-memory graph) перед запуском алгоритмов
- В Community Edition алгоритмы используют максимум 4 потока CPU
- Leiden детерминированный (одинаковый граф → одинаковые кластеры)
- После завершения работы проекцию нужно удалить для освобождения памяти
- Для постов используется Cypher projection с фильтрацией по дате для экономии памяти

### KNN (K-Nearest Neighbors) для построения графа сходства

**Как используется в HintGrid:**
- Поиск ближайших соседей реализован через Neo4j Vector Index (`db.index.vector.queryNodes`).
- `topK = knn_neighbors + knn_self_neighbor_offset`.
- Дополнительно применяется порог `similarity_threshold` и окно свежести `similarity_recency_days`.

```cypher
MATCH (p:Post)
WHERE p.embedding IS NOT NULL
  AND p.createdAt > datetime() - duration({days: $recency_days})
CALL db.index.vector.queryNodes(
    'post_embedding_index',
    $top_k,
    p.embedding
)
YIELD node AS neighbor, score
WHERE neighbor.id <> p.id
  AND score > $threshold
MERGE (p)-[r:SIMILAR_TO]->(neighbor)
SET r.weight = score;
```

### Vector Index (Native Neo4j)

Neo4j 2025.12+ поддерживает нативный векторный поиск без GDS.

**Создание векторного индекса:**

```cypher
// Размерность автоматически определяется из конфигурации провайдера
CREATE VECTOR INDEX post_embedding_index IF NOT EXISTS
FOR (p:Post)
ON p.embedding
OPTIONS {
    indexConfig: {
        `vector.dimensions`: 128,  // fasttext_vector_size или llm_dimensions
        `vector.similarity_function`: 'cosine'
    }
};
```

**Поиск похожих постов:**

```cypher
MATCH (query:Post {id: 12345})
CALL db.index.vector.queryNodes(
    'post_embedding_index',
    10,                        // Top K результатов
    query.embedding
)
YIELD node, score
WHERE node.id <> query.id
RETURN node.id, node.text, score
ORDER BY score DESC;
```

**Важно:**
- Vector Index создается один раз при инициализации
- Автоматически обновляется при добавлении новых узлов
- Очень быстрый поиск (миллисекунды для 100K+ постов)
- Поддерживаемые метрики: `cosine`, `euclidean`

### APOC Optimizations

#### apoc.periodic.iterate для больших батчей

Для предотвращения OOM и переполнения transaction log при массовых операциях используется `apoc.periodic.iterate`:

```cypher
CALL apoc.periodic.iterate(
  "MATCH (u:User)-[:BELONGS_TO]->(uc:UserCommunity), (p:Post)-[:BELONGS_TO]->(pc:PostCommunity) ... RETURN uc, pc, weight",
  "MERGE (uc)-[r:INTERESTED_IN]->(pc) SET r.score = weight",
  {batchSize: 10000, parallel: false}
)
```

**Применяется для:**
- Перестройка INTERESTED_IN связей (rebuild_interests)
- Создание SIMILAR_TO связей через Vector Index

**Преимущества:**
- Предотвращает OOM на больших графах
- Быстрее для массовых операций
- Контролируемый размер транзакций

#### Отслеживание прогресса (ProgressTracker)

Для длительных `apoc.periodic.iterate` операций клиент создаёт узел `ProgressTracker` в Neo4j, который обновляется после каждого батча.

```cypher
-- Создание трекера
MERGE (pt:ProgressTracker {id: $operation_id})
SET pt.processed = 0, pt.batches = 0, pt.total = $total,
    pt.started_at = datetime(), pt.last_updated = datetime();

-- Обновление прогресса (добавляется к action-запросу автоматически)
-- Для BATCH-режима (UNWIND $_batch):
UNWIND $_batch AS row MATCH (n) WHERE id(n) = row.id SET n.prop = value
WITH size($_batch) AS batch_size, $progress_tracker_id AS pt_id, count(*) AS _agg
MATCH (pt:ProgressTracker {id: pt_id})
SET pt.batches = pt.batches + 1,
    pt.processed = pt.processed + batch_size,
    pt.last_updated = datetime();

-- Запрос прогресса
MATCH (pt:ProgressTracker {id: $operation_id})
RETURN pt.processed AS processed, pt.batches AS batches, pt.total AS total;
```

**Ключевые детали:**
- `count(*) AS _agg` служит барьером агрегации, схлопывая все строки UNWIND в одну перед обновлением ProgressTracker
- `size($_batch)` подсчитывает элементы батча через параметр, а не через строки pipeline
- Для не-BATCH операций используется `count(*)` как метрика обработанных строк

### Получение документации GDS

```cypher
// Список всех доступных GDS процедур
CALL gds.list()
YIELD name, description
RETURN name, description
ORDER BY name;

// Информация о конкретном алгоритме
CALL gds.leiden.stream.estimate(
    {
        nodeCount: 10000,
        relationshipCount: 50000
    }
)
YIELD requiredMemory, bytesMin, bytesMax;
```

---

#### Статистика графа

```cypher
// Количество узлов по типам
MATCH (n)
RETURN labels(n)[0] AS label, count(*) AS count
ORDER BY count DESC;

// Количество связей по типам
MATCH ()-[r]->()
RETURN type(r) AS relationship, count(*) AS count
ORDER BY count DESC;

// Посты с embeddings
MATCH (p:Post)
WHERE p.embedding IS NOT NULL
RETURN count(p) AS posts_with_embeddings;

// Посты в кластерах
MATCH (p:Post)
WHERE p.cluster_id IS NOT NULL
RETURN count(p) AS posts_clustered;

// Граф сходства (SIMILAR_TO связи)
MATCH ()-[r:SIMILAR_TO]->()
RETURN count(r) AS similarity_links,
       avg(r.weight) AS avg_similarity,
       min(r.weight) AS min_similarity,
       max(r.weight) AS max_similarity;
```

#### Проверка сообществ

```cypher
// Размеры UserCommunity (сортировка по размеру)
MATCH (uc:UserCommunity)
RETURN uc.id AS community_id, uc.size AS size
ORDER BY uc.size DESC;

// Размеры PostCommunity
MATCH (pc:PostCommunity)
RETURN pc.id AS community_id, pc.size AS size
ORDER BY pc.size DESC;

// INTERESTED_IN связи (топ интересов + проверка TTL)
MATCH (uc:UserCommunity)-[i:INTERESTED_IN]->(pc:PostCommunity)
RETURN uc.id AS user_community, 
       pc.id AS post_community, 
       i.score AS score, 
       i.based_on AS based_on_interactions,
       i.last_updated AS updated,
       i.expires_at AS expires,
       CASE WHEN i.expires_at < datetime() THEN 'EXPIRED' ELSE 'ACTIVE' END AS status
ORDER BY i.score DESC
LIMIT 50;

// Устаревшие INTERESTED_IN (требуют cleanup)
MATCH (uc:UserCommunity)-[i:INTERESTED_IN]->(pc:PostCommunity)
WHERE i.expires_at < datetime()
RETURN count(i) AS expired_interests;

// Распределение по возрасту INTERESTED_IN
MATCH ()-[i:INTERESTED_IN]->()
WITH i, (datetime() - i.last_updated).day AS age_days
RETURN 
    CASE 
        WHEN age_days < 7 THEN '0-7 days'
        WHEN age_days < 14 THEN '7-14 days'
        WHEN age_days < 21 THEN '14-21 days'
        WHEN age_days < 30 THEN '21-30 days'
        ELSE '30+ days (expired)'
    END AS age_range,
    count(*) AS count
ORDER BY age_range;

// Serendipity vs Regular INTERESTED_IN
MATCH ()-[i:INTERESTED_IN]->()
RETURN 
    CASE WHEN i.serendipity = true THEN 'Serendipity' ELSE 'Regular' END AS type,
    count(*) AS count,
    avg(i.score) AS avg_score;

// Распределение пользователей по сообществам
MATCH (u:User)
WHERE u.cluster_id IS NOT NULL
RETURN u.cluster_id AS community_id, count(u) AS users
ORDER BY users DESC;

// Распределение постов по сообществам
MATCH (p:Post)
WHERE p.cluster_id IS NOT NULL
RETURN p.cluster_id AS community_id, count(p) AS posts
ORDER BY posts DESC;
```

---

## Визуализация и отладка

### Экспорт полного дампа

HintGrid предоставляет экспорт таймлайна пользователя в Markdown файл с диаграммами Mermaid. В режиме export выполняется только выгрузка данных, без пересчёта и без записи в Redis.

**Важно:** Команда `export` требует обязательный параметр `--user-id`. Команда `run`, напротив, работает как с `--user-id` (обработка одного пользователя), так и без него (обработка всех пользователей).

Параметры экспорта:
- имя файла для экспорта (обязательный параметр);
- `--user-id` — ID пользователя для экспорта (обязательный параметр).

Экспорт включает:
- Redis timeline пользователя;
- Neo4j timeline пользователя (сгенерированный, но не записанный в Redis);
- граф интересов UserCommunity → PostCommunity;
- графы UserCommunity, PostCommunity, FOLLOWS и SIMILAR_TO;
- статистику по кластерам и связям.

**Экспорт таймлайна пользователя:**

Выводится полный таймлайн для заданного пользователя:

1. **Redis Timeline** — текущее состояние ленты в Redis (`feed:home:{user_id}`)
2. **Neo4j Timeline** — лента, сгенерированная на основе данных из Neo4j (интересы сообщества, персонализация)

Это особенно полезно после `--dry-run`:

Шаг 1: выполнить пересчёт без записи в Redis (`run` с флагом `--dry-run`).  
Шаг 2: выполнить экспорт таймлайна пользователя командой `export` с параметрами `filename` и `--user-id`.

В результате `dump.md` будет содержать:
- **Redis Timeline**: что сейчас хранится в Redis (старая лента или пустая, если пользователь новый)
- **Neo4j Timeline**: что было бы записано в Redis, если бы не было `--dry-run`

Это позволяет сравнить текущую и новую ленту перед применением изменений.

**Преимущества:**
- ✅ Один файл содержит все данные
- ✅ Версионируется в git
- ✅ Mermaid автоматически рендерится в GitHub/GitLab/Notion
- ✅ Текстовый формат - легко читать и искать
- ✅ Полная картина системы в одном месте
- ✅ Удобно для отладки и анализа

### Диагностика

#### Обзор графа после загрузки

Панель **Graph Overview** показывает счётчик **INTERACTS_WITH** как основной user-user сигнал. Отдельная строка «FOLLOWS» не используется: подписки из PostgreSQL агрегируются в **INTERACTS_WITH**, а не создают рёбра типа `FOLLOWS` в Neo4j. Предупреждение об изоляции пользователей относится к отсутствию исходящих **INTERACTS_WITH**, а не к типу `FOLLOWS`.

#### Pipeline Summary

После завершения пайплайна автоматически выводится итоговая панель со всеми ключевыми метриками:

```
📊 Pipeline Summary
  Duration:         2m 15s
    ├── Load:            12.3s
    ├── Analytics:       45.7s
    └── Feeds:           117.0s

  Data loaded
    ├── Users:           5,230
    ├── Posts:           12,450
    └── Interactions:    358,643

  Clustering
    ├── User clusters:   1,082 (modularity: 0.665)
    └── Post clusters:   324 (modularity: 0.412)

  Feeds:           1,200 generated

  Status:          HEALTHY
```

**Статусы здоровья:**
- **HEALTHY** — все компоненты работают нормально
- **DEGRADED** — есть предупреждения (отсутствуют эмбеддинги, нет постов, нет кластеров)
- **FAILED** — критические проблемы (не созданы пользовательские кластеры)

#### Динамические Cypher-запросы

Система автоматически определяет существующие типы связей в графе через каталог схемы Neo4j:

```cypher
CALL db.relationshipTypes() YIELD relationshipType
RETURN collect(relationshipType) AS types
```

Этот запрос читает из каталога схемы и выполняется за O(1) — в отличие от сканирования всех рёбер графа.

Предупреждение Neo4j `01N51` ("relationship type does not exist") генерируется на уровне **схемы БД целиком**: если хотя бы одна связь данного типа существует, предупреждения не будет. Предупреждение возникает **только** когда тип полностью отсутствует в базе данных.

Запросы во всех модулях строятся динамически на основе `rel_types` — множества типов связей, присутствующих в графе. Фильтрация применяется к следующим типам:

| Тип связи | Где используется | Что пропускается при отсутствии |
|---|---|---|
| `BOOKMARKED` | `interests`, `stats` | `OPTIONAL MATCH`, `size()`, `EXISTS` |
| `WAS_RECOMMENDED` | `feed`, `interests`, `neo4j` | `NOT EXISTS`, `size()`, `EXISTS` |
| `FAVORITED` | `feed`, `interests`, `stats` | `NOT EXISTS`, `COUNT`, `OPTIONAL MATCH`, `size()` |
| `REBLOGGED` | `interests`, `stats` | `OPTIONAL MATCH`, `size()`, multi-type паттерн |
| `REPLIED` | `interests`, `stats` | `OPTIONAL MATCH`, `size()`, multi-type паттерн |
| `HATES_USER` | `feed`, `stats` | `NOT EXISTS`, `count()` |
| `INTERACTS_WITH` | `stats`, `app` | `count()` |
| `FOLLOWS` | `stats` | `count()` |

Результат кеширования `get_existing_rel_types()` в `Neo4jClient` гарантирует один запрос к БД на весь pipeline run. Кеш инвалидируется после загрузки данных (`invalidate_rel_types_cache()`).

Это:
- Устраняет предупреждения Neo4j о несуществующих типах (`01N51`)
- Повышает производительность (меньше `OPTIONAL MATCH` по пустым типам)
- Сохраняет корректность алгоритмов (отсутствующие типы дают 0)

#### Метрика INTERACTS_WITH

Статистика активности сообществ включает `INTERACTS_WITH` как основную метрику. Даже если в графе нет постов (и, соответственно, FAVORITED/REBLOGGED/REPLIED), показатели активности пользователей остаются информативными.

Для диагностики используйте очистку данных перед повторным прогоном и сравнение экспортов до и после изменений.

### Обработка ошибок

HintGrid предоставляет понятные сообщения об ошибках без stack trace для удобства пользователей.

**Типы ошибок и коды выхода:**

| Код | Тип ошибки | Описание |
|-----|-----------|----------|
| 0 | Успех | Команда выполнена успешно |
| 1 | Общая ошибка | Неизвестная ошибка |
| 2 | Ошибка подключения | Недоступна БД (Neo4j, PostgreSQL, Redis) |
| 3 | Ошибка конфигурации | Неверные параметры |
| 4 | Ошибка пайплайна | Ошибка при выполнении пайплайна |
| 5 | GDS недоступен | Neo4j GDS плагин не установлен |
| 130 | Прерывание | Пользователь нажал Ctrl+C |

**Примеры сообщений:**

```
ERROR: Cannot connect to Neo4j at localhost:7687
Hint: Check that Neo4j is running and accessible. Verify HINTGRID_NEO4J_HOST and HINTGRID_NEO4J_PORT settings.
```

```
ERROR: Cannot connect to PostgreSQL at localhost:5432/mastodon_production
Hint: Authentication failed. Verify HINTGRID_POSTGRES_USER and HINTGRID_POSTGRES_PASSWORD settings.
```

```
ERROR: Neo4j GDS (Graph Data Science) plugin is not available
Hint: HintGrid requires Neo4j with GDS plugin installed. For Docker, use NEO4J_PLUGINS='["graph-data-science"]' environment variable.
```

**Режим отладки:**

Для получения полного stack trace используйте флаг `--verbose` или `-v`:

Используйте флаг `--verbose` (или `-v`) для получения полного stack trace и детализированных логов.

В режиме verbose:
- Уровень логирования устанавливается в DEBUG
- При ошибках выводится полный stack trace
- Показывается подробная информация о каждом шаге пайплайна

## Заключение

Это краткая, но полная документация утилиты HintGrid. Основные компоненты:

1. **Graph-Native Clustering**: Leiden для естественного выделения сообществ (FOLLOWS + INTERACTS_WITH)
2. **Interaction-Based Communities**: Взаимодействия (лайки, ответы, репосты, упоминания) как основа для кластеризации при разреженном графе подписок
3. **Vector-to-Graph Pipeline**: Embeddings → Vector Index → SIMILAR_TO → Leiden для топиков
4. **Batch processing**: Инкрементальная загрузка из PostgreSQL
5. **Graph analytics**: Neo4j GDS (Leiden) + Vector Index
6. **Community-based**: Рекомендации через UserCommunity → PostCommunity
7. **Exponential Decay & TTL**: Плавное экспоненциальное затухание взаимодействий (`exp(-λ·age)`) и автоматическое устаревание связей INTERESTED_IN
8. **Incremental Refresh**: Лёгкое обновление интересов без полной перестройки (команда `refresh`)
9. **Flexible AI**: LiteLLM позволяет менять провайдера (Ollama → OpenAI → Anthropic)
10. **Cold Start Handling**: Fallback механизмы для новых сообществ (с фильтрацией постов без эмбеддингов)
11. **Memory Optimization**: Pruning стратегии для SIMILAR_TO связей
12. **Smart Post Loading**: Посты без эмбеддингов не создаются в Neo4j, сигнал взаимодействий сохраняется через INTERACTS_WITH
13. **Idempotency**: Безопасные пересчёты с автоматической очисткой
14. **Export to Markdown**: Экспорт всего состояния в один файл с Mermaid диаграммами
15. **Graceful Shutdown**: Корректная остановка по Ctrl+C с сохранением прогресса, Rich UI панелью статуса и автоматическим возобновлением с сохранённых курсоров
16. **Selective Feed Updates**: Dirty-user detection на основе `feedGeneratedAt` — обновляются только ленты пользователей с изменённым состоянием графа
17. **Sub-Daily Recency Precision**: Расчёт свежести постов в часах вместо дней для точного ранжирования внутри одного дня

**Ключевые преимущества новой архитектуры:**
- ✅ Детерминированность (Leiden vs K-Means)
- ✅ Динамическое количество кластеров (не нужно угадывать K)
- ✅ Естественность для соцсетей (граф-ориентированный подход)
- ✅ Кластеризация по взаимодействиям при разреженном графе FOLLOWS
- ✅ Плавное затухание взаимодействий (экспоненциальный decay + TTL для INTERESTED_IN)
- ✅ Инкрементальное обновление интересов (`refresh`) без полной перестройки
- ✅ Neo4j Community + GDS (бесплатно, все функции доступны)
- ✅ Масштабируемость (pruning для экономии памяти)
- ✅ Надёжность (холодный старт, идемпотентность)
- ✅ Бесшовная интеграция с Mastodon (score = status_id * 2)
- ✅ Атомарное управление состоянием (Singleton Node в Neo4j)
- ✅ Stateless контейнеры (состояние в базе, не в файлах)
- ✅ **Единый экспорт**: Один Markdown файл с Redis feeds и Mermaid графами
- ✅ **Graceful Shutdown**: Корректная остановка по Ctrl+C с Rich UI панелью, сохранением курсоров и автоматическим resume
- ✅ **Selective Feed Updates**: Обновление лент только для «грязных» пользователей (dirty-user detection через `feedGeneratedAt`)
- ✅ **Sub-Daily Recency**: Часовая гранулярность свежести постов для точного ранжирования

**Особенности Neo4j Community Edition:**
- ✅ Полный функционал GDS (Leiden) - бесплатно
- ✅ Векторный поиск (Vector Index) - встроен в ядро
- ⚠️ Ограничение: алгоритмы GDS используют максимум 4 потока CPU
- ⚠️ Требует больше памяти (JVM heap) - рекомендуется минимум 4GB RAM
- ✅ Активное сообщество и отличная документация
- ✅ Stateless контейнеры (состояние в базе, не в файлах)
- ✅ **Единый экспорт**: Один Markdown файл с Redis feeds и Mermaid графами

**Инструменты:**
- `export` - экспорт таймлайна пользователя в Markdown с Mermaid диаграммами
  - Требует обязательный параметр `--user-id`
  - Выводит Redis timeline и Neo4j timeline (полезно после `--dry-run`)
- `refresh` - инкрементальное обновление интересов (глобальный decay + пересчёт грязных сообществ)
  - Не требует PostgreSQL (работает только с Neo4j)
  - Автоматический fallback на полную перестройку при первом запуске
- `clean` - полная очистка Neo4j и удаление рекомендаций,
  записанных приложением в Redis (остальные ключи Redis не затрагиваются)

**Полезные ресурсы:**
- [Neo4j Documentation](https://neo4j.com/docs/)
- [Neo4j GDS Library (Graph Data Science)](https://neo4j.com/docs/graph-data-science/current/)
- [Neo4j Community vs Enterprise](https://neo4j.com/licensing/)
- [LiteLLM Documentation](https://docs.litellm.ai/)
- [Ollama Models](https://ollama.com/library)
- [Leiden Algorithm Paper](https://arxiv.org/abs/1810.08473)
- [Mermaid Live Editor](https://mermaid.live/)
- [Mermaid Documentation](https://mermaid.js.org/)

**Поддержка:**
- [GitHub Issues](https://github.com/yourusername/hintgrid/issues)
- Email: <support@hintgrid.com>
