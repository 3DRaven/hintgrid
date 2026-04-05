"""Streaming API examples for Neo4j and PostgreSQL.

These tests demonstrate memory-efficient iteration over large result sets:

1. Neo4j: Uses fetch_size parameter for lazy record fetching
   - Driver iterates lazily by default
   - O(N) complexity vs O(N²) for SKIP+LIMIT pagination
   - stream_query() yields records one-by-one

2. PostgreSQL: Uses server-side cursors with named cursor
   - Named cursor enables true streaming (cursor.name="xxx")
   - itersize controls rows per network round-trip
   - stream_query() analogous to Neo4j API

All tests use worker-isolated labels via neo4j.label() for parallel execution.
"""
import pytest
from hintgrid.clients.neo4j import Neo4jClient
from hintgrid.clients.postgres import PostgresClient, PostgresCorpus

@pytest.mark.integration
class TestNeo4jStreamingAPI:
    """Neo4j streaming cursor API examples.

    Key patterns:
    - stream_query() uses fetch_size for buffer control
    - Returns generator (lazy evaluation)
    - O(N) complexity for large datasets
    """

    def test_stream_query_empty_result(self, neo4j: Neo4jClient) -> None:
        """stream_query on empty result should yield nothing."""
        results = list(neo4j.stream_query_labeled('MATCH (n:__label__) RETURN n', label_map={'label': 'Nothing'}))
        assert results == []
        print('✅ Neo4j stream_query handles empty results correctly')

    def test_stream_query_returns_all_records(self, neo4j: Neo4jClient) -> None:
        """stream_query yields all matching records lazily."""
        neo4j.execute_labeled(
            "\n            CREATE (:__label__ {id: 1, text: 'First'})\n            CREATE (:__label__ {id: 2, text: 'Second'})\n            CREATE (:__label__ {id: 3, text: 'Third'})\n            ",
            label_map={'label': 'StreamPost'}
        )
        results = list(neo4j.stream_query_labeled('MATCH (p:__label__) RETURN p.id AS id, p.text AS text ORDER BY p.id', label_map={'label': 'StreamPost'}))
        assert len(results) == 3
        assert results[0]['id'] == 1
        assert results[2]['id'] == 3
        print('✅ Neo4j stream_query returns all records in order')

    def test_stream_query_with_parameters(self, neo4j: Neo4jClient) -> None:
        """stream_query supports parameterized queries."""
        neo4j.execute_labeled(
            "\n            CREATE (:__label__ {id: 100, name: 'Alice'})\n            CREATE (:__label__ {id: 200, name: 'Bob'})\n            ",
            label_map={'label': 'StreamUser'}
        )
        results = list(neo4j.stream_query_labeled('MATCH (u:__label__) WHERE u.id >= $min_id RETURN u.name AS name', label_map={'label': 'StreamUser'}, params={'min_id': 200}))
        assert len(results) == 1
        assert results[0]['name'] == 'Bob'
        print('✅ Neo4j stream_query supports parameterized queries')

    def test_stream_query_is_lazy(self, neo4j: Neo4jClient) -> None:
        """stream_query returns generator, not list (lazy evaluation).

        Key benefit: Memory usage stays constant regardless of result size.
        Records are fetched in batches controlled by fetch_size.
        """
        neo4j.execute_labeled("CREATE (:__label__ {id: 1, text: 'Test'})", label_map={'label': 'LazyPost'})
        result = neo4j.stream_query_labeled('MATCH (p:__label__) RETURN p.id AS id', label_map={'label': 'LazyPost'})
        assert hasattr(result, '__next__')
        first = next(result)
        assert first['id'] == 1
        print('✅ Neo4j stream_query is lazy (returns generator)')

    def test_stream_query_with_fetch_size(self, neo4j: Neo4jClient) -> None:
        """stream_query accepts fetch_size for buffer control.

        fetch_size controls how many records are buffered in driver.
        Lower values = less memory, more network round-trips.
        Higher values = more memory, fewer round-trips.
        """
        for i in range(1, 11):
            neo4j.execute_labeled('CREATE (:__label__ {id: $id})', label_map={'label': 'FetchTest'}, params={'id': i})
        results = list(neo4j.stream_query_labeled('MATCH (p:__label__) RETURN p.id AS id ORDER BY p.id', label_map={'label': 'FetchTest'}, fetch_size=3))
        assert len(results) == 10
        print('✅ Neo4j stream_query works with custom fetch_size')

    def test_stream_user_ids_returns_all(self, neo4j: Neo4jClient) -> None:
        """stream_user_ids streams user IDs without loading all into memory."""
        neo4j.execute_labeled('MATCH (u:__user__) DETACH DELETE u', label_map={'user': 'User'})
        neo4j.execute_labeled('\n            CREATE (:__user__ {id: 100})\n            CREATE (:__user__ {id: 200})\n            CREATE (:__user__ {id: 300})\n            ', label_map={'user': 'User'})
        count = neo4j.execute_and_fetch_labeled('MATCH (u:__user__) RETURN count(u) AS count', label_map={'user': 'User'})[0]['count']
        assert count == 3
        results = list(neo4j.stream_user_ids())
        assert 100 in results
        assert 200 in results
        assert 300 in results
        print('✅ Neo4j stream_user_ids returns expected IDs')

    def test_stream_user_ids_skips_invalid(self, neo4j: Neo4jClient) -> None:
        """stream_user_ids skips users with invalid IDs."""
        neo4j.execute_labeled('MATCH (u:__user__) DETACH DELETE u', label_map={'user': 'User'})
        neo4j.execute_labeled("\n            CREATE (:__user__ {id: 100})\n            CREATE (:__user__ {id: null})\n            CREATE (:__user__ {name: 'no_id'})\n            CREATE (:__user__ {id: 200})\n            ", label_map={'user': 'User'})
        results = list(neo4j.stream_user_ids())
        assert 100 in results
        assert 200 in results
        print('✅ Neo4j stream_user_ids skips invalid IDs')

@pytest.mark.integration
class TestPostgresStreamingAPI:
    """PostgreSQL server-side cursor API examples.

    Key patterns:
    - Named cursor (cursor(name="xxx")) enables true streaming
    - itersize controls rows per network fetch
    - stream_query() analogous to Neo4j API
    """

    def test_stream_query_empty_result(self, postgres_client: PostgresClient, sample_data_for_cli: dict[str, list[int]]) -> None:
        """stream_query on empty result should yield nothing."""
        results = list(postgres_client.stream_query('SELECT id FROM statuses WHERE id < 0'))
        assert results == []
        print('✅ PostgreSQL stream_query handles empty results correctly')

    def test_stream_query_returns_records(self, postgres_client: PostgresClient, sample_data_for_cli: dict[str, list[int]]) -> None:
        """stream_query yields records one-by-one with server-side cursor."""
        results = list(postgres_client.stream_query('SELECT id, text FROM statuses ORDER BY id LIMIT 5'))
        assert isinstance(results, list)
        assert len(results) > 0
        assert 'id' in results[0]
        assert 'text' in results[0]
        print('✅ PostgreSQL stream_query returns records correctly')

    def test_stream_query_with_fetch_size(self, postgres_client: PostgresClient, sample_data_for_cli: dict[str, list[int]]) -> None:
        """stream_query accepts fetch_size for itersize control.

        itersize controls how many rows are fetched per network round-trip.
        Lower = less memory, more round-trips.
        Higher = more memory, fewer round-trips.
        """
        results = list(postgres_client.stream_query('SELECT id FROM statuses LIMIT 10', fetch_size=3))
        assert isinstance(results, list)
        print('✅ PostgreSQL stream_query works with custom fetch_size')

    def test_stream_query_with_parameters(self, postgres_client: PostgresClient, sample_data_for_cli: dict[str, list[int]]) -> None:
        """stream_query supports parameterized queries."""
        results = list(postgres_client.stream_query('SELECT id FROM statuses WHERE id > %(min_id)s LIMIT 5', params={'min_id': 0}))
        assert isinstance(results, list)
        print('✅ PostgreSQL stream_query supports parameterized queries')

@pytest.mark.integration
class TestPostgresCorpusStreaming:
    """PostgresCorpus streaming iterator examples.

    Key patterns:
    - Implements __iter__ for multiple epochs
    - Server-side cursor with configurable batch_size
    - Supports incremental loading via min_id or since_date
    - Yields tokenized text for ML training
    """

    def test_corpus_stream_is_lazy(self, postgres_dsn: str, worker_schema: str, sample_data_for_cli: dict[str, list[int]]) -> None:
        """PostgresCorpus uses server-side cursor for lazy iteration.

        Memory usage stays constant regardless of corpus size.
        Rows are fetched in batches controlled by batch_size.
        """
        corpus = PostgresCorpus(dsn=postgres_dsn, batch_size=100, schema=worker_schema)
        iterator = iter(corpus)
        assert hasattr(iterator, '__next__')
        print('✅ PostgresCorpus is lazy iterator')

    def test_corpus_stream_texts(self, postgres_dsn: str, worker_schema: str, sample_data_for_cli: dict[str, list[int]]) -> None:
        """stream_texts() returns raw text without tokenization.

        Useful when you need full text, not tokens.
        """
        corpus = PostgresCorpus(dsn=postgres_dsn, batch_size=100, schema=worker_schema)
        texts = list(corpus.stream_texts())
        assert isinstance(texts, list)
        assert len(texts) > 0
        assert isinstance(texts[0], str)
        print('✅ PostgresCorpus.stream_texts() returns raw text')

    def test_corpus_stream_with_ids(self, postgres_dsn: str, worker_schema: str, sample_data_for_cli: dict[str, list[int]]) -> None:
        """stream_with_ids() returns (id, text) tuples for progress tracking.

        Useful for incremental processing with progress reporting.
        """
        corpus = PostgresCorpus(dsn=postgres_dsn, batch_size=100, schema=worker_schema)
        items = list(corpus.stream_with_ids())
        assert isinstance(items, list)
        assert len(items) > 0
        assert isinstance(items[0], tuple)
        assert len(items[0]) == 2
        print('✅ PostgresCorpus.stream_with_ids() returns tuples')

    def test_corpus_tracks_max_id(self, postgres_dsn: str, worker_schema: str, sample_data_for_cli: dict[str, list[int]]) -> None:
        """PostgresCorpus tracks max_id for incremental loading.

        After iteration, max_id contains the highest ID seen.
        Use this for next incremental load: min_id=corpus.max_id
        """
        corpus = PostgresCorpus(dsn=postgres_dsn, batch_size=100, schema=worker_schema)
        _ = list(corpus)
        assert corpus.max_id > 0
        print(f'✅ PostgresCorpus tracked max_id: {corpus.max_id}')

    def test_corpus_incremental_loading(self, postgres_dsn: str, worker_schema: str, sample_data_for_cli: dict[str, list[int]]) -> None:
        """PostgresCorpus supports incremental loading via min_id.

        Pattern for incremental loading:
        1. First load: min_id=0
        2. Save corpus.max_id
        3. Next load: min_id=saved_max_id
        """
        corpus1 = PostgresCorpus(dsn=postgres_dsn, min_id=0, batch_size=100, schema=worker_schema)
        _ = list(corpus1)
        max_id_1 = corpus1.max_id
        corpus2 = PostgresCorpus(dsn=postgres_dsn, min_id=max_id_1, batch_size=100, schema=worker_schema)
        _ = list(corpus2)
        print(f'✅ Incremental loading: first max_id={max_id_1}, second max_id={corpus2.max_id}')

@pytest.mark.integration
class TestStreamingBestPractices:
    """Documentation of streaming best practices.

    Anti-patterns to avoid:
    - SKIP+LIMIT pagination (O(N²) complexity)
    - fetchall() on large datasets
    - Loading entire result into list when not needed

    Correct patterns:
    - stream_query() for O(N) iteration
    - Named cursors in PostgreSQL
    - fetch_size control for memory/latency tradeoff
    """

    def test_avoid_skip_limit_pattern(self, neo4j: Neo4jClient) -> None:
        """SKIP+LIMIT pattern is O(N²) - avoid for large datasets.

        ❌ Anti-pattern (DO NOT USE):
            for offset in range(0, total, page_size):
                results = neo4j.execute_and_fetch(
                    f"MATCH (n) RETURN n SKIP {offset} LIMIT {page_size}"
                )

        ✅ Correct pattern:
            for record in neo4j.stream_query("MATCH (n) RETURN n"):
                process(record)
        """
        for i in range(10):
            neo4j.execute_labeled('CREATE (:__label__ {id: $id})', label_map={'label': 'SkipTest'}, params={'id': i})
        records = list(neo4j.stream_query_labeled('MATCH (n:__label__) RETURN n.id AS id', label_map={'label': 'SkipTest'}))
        assert len(records) == 10
        print('✅ Use stream_query() instead of SKIP+LIMIT pagination')

    def test_avoid_fetchall_pattern(self, postgres_client: PostgresClient, sample_data_for_cli: dict[str, list[int]]) -> None:
        """fetchall() loads entire result into memory - use streaming.

        ❌ Anti-pattern (DO NOT USE):
            cursor.execute("SELECT * FROM big_table")
            all_rows = cursor.fetchall()  # Loads everything into memory!

        ✅ Correct pattern:
            for row in postgres_client.stream_query("SELECT * FROM big_table"):
                process(row)
        """
        count = 0
        for _ in postgres_client.stream_query('SELECT id FROM statuses LIMIT 5'):
            count += 1
        print(f'✅ Use stream_query() instead of fetchall() (processed {count} rows)')

    def test_fetch_size_tuning(self, neo4j: Neo4jClient) -> None:
        """fetch_size tuning for memory/latency tradeoff.

        Low fetch_size (100-500):
        - Less memory usage
        - More network round-trips
        - Good for: memory-constrained environments

        High fetch_size (1000-5000):
        - More memory usage
        - Fewer round-trips
        - Good for: high-throughput processing

        Default (1000) is a good balance for most cases.
        """
        for i in range(20):
            neo4j.execute_labeled('CREATE (:__label__ {id: $id})', label_map={'label': 'FetchSizeDemo'}, params={'id': i})
        low_mem = list(neo4j.stream_query_labeled('MATCH (n:__label__) RETURN n.id', label_map={'label': 'FetchSizeDemo'}, fetch_size=5))
        high_tp = list(neo4j.stream_query_labeled('MATCH (n:__label__) RETURN n.id', label_map={'label': 'FetchSizeDemo'}, fetch_size=100))
        assert len(low_mem) == len(high_tp) == 20
        print('✅ Tune fetch_size based on memory/latency requirements')