---
name: Fix interests queries and update FOLLOWS documentation
overview: Исправление запросов интересов (COALESCE для NULL, проверка типов отношений) и обновление документации о переходе с FOLLOWS на INTERACTS_WITH
todos:
  - id: decay-constant
    content: Заменить магическое число 0.693147 на log(2.0) в Cypher-запросах и добавить комментарии к запросам
    status: completed
  - id: coalesce-sum
    content: Добавить COALESCE() для всех sum() в запросах интересов (rebuild_interests и refresh_interests)
    status: completed
    dependencies:
      - decay-constant
  - id: check-relationships
    content: Добавить функцию _check_relationship_exists() и проверку наличия BOOKMARKED/WAS_RECOMMENDED перед использованием
    status: completed
  - id: logging
    content: Добавить логирование причин пропуска расчетов (CTR без WAS_RECOMMENDED, отсутствие BOOKMARKED)
    status: completed
    dependencies:
      - check-relationships
  - id: update-stats-isolation
    content: Обновить _collect_user_connectivity() для использования INTERACTS_WITH вместо FOLLOWS
    status: in_progress
  - id: update-export-graph
    content: Обновить _append_follows_graph() для использования INTERACTS_WITH вместо FOLLOWS
    status: pending
  - id: update-docs-model
    content: Обновить раздел модели данных в документации (FOLLOWS deprecated, включен в INTERACTS_WITH)
    status: pending
  - id: update-docs-sql
    content: Обновить раздел SQL запросов (удалить/пометить deprecated загрузку FOLLOWS)
    status: pending
  - id: update-docs-cypher
    content: Обновить раздел Neo4j операций (уточнить что INTERACTS_WITH включает FOLLOWS)
    status: pending
---

# План: Исправление запросов интересов и обновление документации FOLLOWS

## Анализ текущего состояния

### FOLLOWS vs INTERACTS_WITH

**Текущая реализация:**

- FOLLOWS **не загружается отдельно** в Neo4j (нет `_load_follows`, `merge_follows`)
- FOLLOWS **включен в INTERACTS_WITH** через SQL запрос `stream_user_interactions` в [src/hintgrid/clients/postgres.py](src/hintgrid/clients/postgres.py:681-764)
- Кластеризация использует **только INTERACTS_WITH** (см. [src/hintgrid/pipeline/clustering.py](src/hintgrid/pipeline/clustering.py:138))
- FOLLOWS упоминается только в:
  - Статистике изолированности ([src/hintgrid/pipeline/stats.py](src/hintgrid/pipeline/stats.py:131-168))
  - Экспорте графа ([src/hintgrid/pipeline/exporter.py](src/hintgrid/pipeline/exporter.py:323-350))

**Проблемы:**

1. Статистика изолированности проверяет FOLLOWS, но FOLLOWS не создаются в Neo4j
2. Экспорт пытается визуализировать FOLLOWS, которых нет в графе
3. Документация содержит устаревшую информацию о FOLLOWS

### Запросы интересов

**Проблемы:**

1. `sum()` без COALESCE возвращает NULL при отсутствии совпадений
2. Нет проверки существования типов отношений (BOOKMARKED, WAS_RECOMMENDED)
3. Нет логирования причин пропуска расчетов

## Задачи

### 1. Использовать функцию log(2.0) в Cypher-запросах вместо магического числа

**Файлы:**

- [src/hintgrid/pipeline/interests.py](src/hintgrid/pipeline/interests.py)

**Изменения:**

- Заменить все использования `0.693147` на `log(2.0)` (натуральный логарифм в Cypher):
  - Строка 38-43: обновить комментарий и `decay_expr` для `rebuild_interests()`
  - Строка 419: в глобальном decay для `refresh_interests()`
  - Строка 470-473: обновить `decay_expr` для `refresh_interests()` (внутри функции)

**Пример:**

```python
# Было:
# Decay constant: ln(2) / half_life_days ≈ 0.693147 / half_life_days
# An interaction at exactly half_life_days ago contributes 0.5 weight
decay_expr = (
    "exp(-0.693147 * duration.between({rel}.at, datetime()).days "
    "/ toFloat($half_life_days))"
)

# Станет:
# Exponential decay with half-life: weight = exp(-ln(2) * age / half_life_days)
# When age = half_life_days: exp(-ln(2)) = 0.5 (50% weight)
# This ensures interactions at exactly half_life_days ago contribute 50% weight
decay_expr = (
    "exp(-log(2.0) * duration.between({rel}.at, datetime()).days "
    "/ toFloat($half_life_days))"
)
```

**Примечание:**

- Использовать `log(2.0)` в Cypher (натуральный логарифм, эквивалент `ln(2)`)
- Добавить комментарии к запросам, объясняющие математику экспоненциального затухания
- В глобальном decay (строка 419) также заменить на `log(2.0)`

### 2. Добавить COALESCE() для всех sum() в запросах интересов

**Файлы:**

- [src/hintgrid/pipeline/interests.py](src/hintgrid/pipeline/interests.py)

**Изменения:**

- Строки 57-60: обернуть `sum()` для likes, reblogs, replies, bookmarks в `COALESCE(..., 0.0)`
- Строка 63: обернуть `sum()` для recommendations в `COALESCE(..., 0.0)`
- Строки 145-148: аналогично в `iterate_query`
- Строка 151: аналогично для recommendations в `iterate_query`
- Строка 72: `(likes + reblogs + replies + bookmarks)` уже безопасно после COALESCE
- Строка 160: аналогично

**Пример:**

```cypher
-- Было:
sum(exp(-0.693147 * duration.between(f.at, datetime()).days / toFloat($half_life_days))) AS likes

-- Станет (с log(2.0) и COALESCE):
COALESCE(sum(exp(-log(2.0) * duration.between(f.at, datetime()).days / toFloat($half_life_days))), 0.0) AS likes
```

**Примечание:**

- Использовать `log(2.0)` в Cypher вместо магического числа `0.693147`
- Обернуть все `sum()` в `COALESCE(..., 0.0)` для обработки NULL

**Также в `refresh_interests()`:**

- Аналогичные изменения в строках с `sum()` для decay-выражений

### 3. Проверка наличия типов отношений перед использованием

**Файлы:**

- [src/hintgrid/pipeline/interests.py](src/hintgrid/pipeline/interests.py)

**Добавить функцию:**

```python
def _check_relationship_exists(neo4j: "Neo4jClient", rel_type: str) -> bool:
    """Check if relationship type exists in graph."""
    result = list(
        neo4j.execute_and_fetch(
            "CALL db.relationshipTypes() YIELD relationshipType "
            f"WHERE relationshipType = '{rel_type}' "
            "RETURN count(*) AS count"
        )
    )
    return result[0].get("count", 0) > 0 if result else False
```

**Использование в `rebuild_interests()`:**

- Перед построением `max_weights_query` проверить:
  - `has_bookmarked = _check_relationship_exists(neo4j, "BOOKMARKED")`
  - `has_was_recommended = False`
  - Если `settings.ctr_enabled`: `has_was_recommended = _check_relationship_exists(neo4j, "WAS_RECOMMENDED")`
- Условно добавлять `OPTIONAL MATCH` только если отношения существуют
- Аналогично в `iterate_query`

**Использование в `refresh_interests()`:**

- Аналогичная проверка перед построением запросов

### 4. Логирование причин пропуска расчетов

**Файлы:**

- [src/hintgrid/pipeline/interests.py](src/hintgrid/pipeline/interests.py)

**Добавить логирование:**

- Если `ctr_enabled` но `WAS_RECOMMENDED` не существует:
  ```python
  logger.warning(
      "CTR enabled but WAS_RECOMMENDED relationships not found. "
      "CTR calculation will be skipped. Run feed generation first."
  )
  ```

- Если `BOOKMARKED` не существует:
  ```python
  logger.debug("BOOKMARKED relationships not found, skipping in interest calculation")
  ```


### 5. Обновление документации о FOLLOWS и INTERACTS_WITH

**Файлы:**

- [docs/REFERENCE.ru.md](docs/REFERENCE.ru.md)

**Изменения:**

#### 4.1. Модель данных (строки 196-200)

- Удалить или пометить как deprecated: `(:User)-[:FOLLOWS]->(:User)`
- Добавить примечание: FOLLOWS включен в INTERACTS_WITH через SQL

#### 4.2. SQL запросы (строки 691-703)

- Удалить раздел "Загрузка подписок" или пометить как deprecated
- Добавить примечание: FOLLOWS загружается через `stream_user_interactions` как часть INTERACTS_WITH

#### 4.3. Neo4j операции (строки 1118-1126)

- Удалить раздел "Создание FOLLOWS связей" или пометить как deprecated
- Обновить раздел "Создание INTERACTS_WITH связей" (строки 1127-1139):
  - Уточнить, что INTERACTS_WITH включает FOLLOWS через SQL с весом `follows_weight`

#### 4.4. User Clustering (строки 1155-1209)

- Уже корректно указано, что используется только INTERACTS_WITH
- Добавить примечание: FOLLOWS не создается отдельно, включен в INTERACTS_WITH

#### 4.5. Статистика изолированности

- Обновить описание в [src/hintgrid/pipeline/stats.py](src/hintgrid/pipeline/stats.py:132):
  - Изменить комментарий: "Collect user connectivity statistics (INTERACTS_WITH, not FOLLOWS)"
  - Обновить docstring: указать, что проверяется INTERACTS_WITH, а не FOLLOWS
  - Изменить запрос: использовать INTERACTS_WITH вместо FOLLOWS
  - Обновить имена переменных: `avg_interacts`, `median_interacts`, `max_interacts`
  - Обновить предупреждение (строка 389): "All users are isolated (no INTERACTS_WITH relationships)"

#### 4.6. Экспорт графа

- Обновить [src/hintgrid/pipeline/exporter.py](src/hintgrid/pipeline/exporter.py:323-350):
  - Изменить `_append_follows_graph` на `_append_interacts_graph`
  - Использовать INTERACTS_WITH вместо FOLLOWS
  - Обновить заголовок: "User INTERACTS_WITH Graph (sample)"

#### 4.7. Статистика в консоли

- Обновить [src/hintgrid/pipeline/stats.py](src/hintgrid/pipeline/stats.py:324):
  - Изменить "FOLLOWS" на "INTERACTS_WITH" в выводе
  - Обновить строки 340-342: использовать `avg_interacts`, `median_interacts`, `max_interacts`

## Порядок выполнения

1. **Использование log(2.0) в запросах** - замена магического числа 0.693147 на вычисление ln(2) прямо в Cypher-запросах с комментариями
2. **COALESCE для sum()** - исправляет предупреждения о NULL (использует LN_2)
3. **Проверка типов отношений** - предотвращает предупреждения о несуществующих типах
4. **Логирование** - улучшает диагностику
5. **Обновление документации** - синхронизирует документацию с кодом
6. **Обновление статистики** - исправляет проверку изолированности
7. **Обновление экспорта** - исправляет визуализацию графа

## Тестирование

После изменений проверить:

- Запуск pipeline без предупреждений о NULL и несуществующих типах отношений
- Корректная работа статистики изолированности на основе INTERACTS_WITH
- Корректная визуализация графа в экспорте
- Логи содержат предупреждения при отсутствии WAS_RECOMMENDED/BOOKMARKED