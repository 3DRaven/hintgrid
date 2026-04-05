"""FastText embedding model tests with PostgreSQL streaming."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt
import pytest
from gensim.models import FastText
from gensim.utils import simple_preprocess
from psycopg import Connection
from psycopg.rows import TupleRow

# FastText constants
FASTTEXT_VECTOR_SIZE = 64  # Small dimension for tests (memory efficient)
FASTTEXT_WINDOW = 3  # Context window size
FASTTEXT_MIN_COUNT = 1  # Minimum word frequency (low for small test data)
FASTTEXT_WORKERS = 1  # Single worker for deterministic tests
FASTTEXT_EPOCHS = 5  # Training epochs
FASTTEXT_BUCKET = 10000  # Small bucket size for n-grams (memory efficient)


class PostgresCorpus:
    """Streaming iterator that reads posts from Postgres row by row.

    Never loads all data into RAM - uses server-side cursor for memory efficiency.
    """

    def __init__(self, conn: Connection[TupleRow], min_text_length: int = 10) -> None:
        self.conn = conn
        self.min_text_length = min_text_length

    def __iter__(self) -> Iterator[list[str]]:
        """Yield tokenized posts from database."""
        with self.conn.cursor(name="fasttext_trainer_cursor") as cur:
            cur.execute(
                """
                SELECT COALESCE(text, '')
                FROM statuses
                WHERE text IS NOT NULL AND deleted_at IS NULL AND length(text) > %s
                """,
                (self.min_text_length,),
            )

            for record in cur:
                text = str(record[0])
                tokens = simple_preprocess(text, min_len=2, max_len=20)
                if tokens:
                    yield tokens


@pytest.fixture
def fasttext_test_data(
    postgres_conn: Connection[TupleRow],
) -> Connection[TupleRow]:
    """Create test data for FastText training in PostgreSQL."""
    with postgres_conn.cursor() as cur:
        # Create statuses table
        cur.execute("""
            DROP TABLE IF EXISTS statuses CASCADE;
            CREATE TABLE statuses (
                id BIGINT PRIMARY KEY,
                account_id BIGINT NOT NULL,
                text TEXT NOT NULL DEFAULT '',
                language VARCHAR(10),
                visibility INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                deleted_at TIMESTAMP
            );
        """)

        # Insert sample posts with diverse content for training
        sample_posts = [
            # Technology cluster
            (1, 101, "Python programming is amazing for data science and machine learning"),
            (2, 101, "Learning Rust for systems programming and memory safety"),
            (3, 102, "JavaScript frameworks like React and Vue are great for web development"),
            (4, 102, "TypeScript adds static typing to JavaScript making code more reliable"),
            (5, 103, "Docker containers and Kubernetes orchestration for microservices"),
            (6, 103, "GraphQL provides flexible API queries compared to REST endpoints"),
            # Art and creativity cluster
            (7, 104, "Digital painting techniques with Procreate and Photoshop brushes"),
            (8, 104, "Watercolor landscapes with vibrant colors and soft textures"),
            (9, 105, "Photography composition rules like rule of thirds and leading lines"),
            (10, 105, "Portrait photography with natural lighting and bokeh effects"),
            # Science cluster
            (11, 106, "Quantum computing uses qubits for parallel computation"),
            (12, 106, "Machine learning algorithms like neural networks and transformers"),
            (13, 107, "Climate science research on carbon emissions and global warming"),
            (14, 107, "Astronomy observations of exoplanets and galaxy formation"),
            # Repeat some patterns to have enough data for min_count
            (15, 108, "Python data analysis with pandas and numpy libraries"),
            (16, 108, "Machine learning models for natural language processing"),
            (17, 109, "Web development with Python frameworks like Django and Flask"),
            (18, 109, "Database optimization and SQL query performance tuning"),
            (19, 110, "Open source software development and community contributions"),
            (20, 110, "Code review best practices and collaborative development"),
        ]

        for post_id, account_id, text in sample_posts:
            cur.execute(
                "INSERT INTO statuses (id, account_id, text, language) VALUES (%s, %s, %s, %s)",
                (post_id, account_id, text, "en"),
            )

        # Add one deleted post (should be excluded from training)
        cur.execute("""
            INSERT INTO statuses (id, account_id, text, language, deleted_at)
            VALUES (100, 111, 'This deleted post should not be in training', 'en', NOW())
        """)

        postgres_conn.commit()

    return postgres_conn


@pytest.mark.integration
@pytest.mark.smoke
def test_fasttext_postgres_corpus_streaming(
    fasttext_test_data: Connection[TupleRow],
) -> None:
    """Test PostgresCorpus streaming iterator."""
    corpus = PostgresCorpus(fasttext_test_data, min_text_length=10)

    # Collect all tokens
    all_docs: list[list[str]] = []
    for tokens in corpus:
        all_docs.append(tokens)

    # Should have 20 non-deleted posts
    assert len(all_docs) == 20, f"Expected 20 documents, got {len(all_docs)}"

    # Each document should have tokens
    assert all(len(doc) > 0 for doc in all_docs), "All documents should have tokens"

    # Check that tokenization works correctly
    first_doc = all_docs[0]
    assert "python" in first_doc, f"First doc should contain 'python': {first_doc}"
    assert "programming" in first_doc, f"First doc should contain 'programming': {first_doc}"

    # Verify deleted post is not included (id=100)
    all_text = " ".join(" ".join(doc) for doc in all_docs)
    assert "deleted" not in all_text, "Deleted post should not be in corpus"

    print(f"✅ PostgresCorpus: streamed {len(all_docs)} documents")


@pytest.mark.integration
@pytest.mark.smoke
def test_fasttext_training_from_postgres(
    fasttext_test_data: Connection[TupleRow],
    tmp_path: Path,
) -> None:
    """Test FastText model training from PostgreSQL data."""
    corpus = PostgresCorpus(fasttext_test_data, min_text_length=10)

    # Initialize model with MEMORY-EFFICIENT settings
    model = FastText(
        vector_size=FASTTEXT_VECTOR_SIZE,
        window=FASTTEXT_WINDOW,
        min_count=FASTTEXT_MIN_COUNT,
        workers=FASTTEXT_WORKERS,
        sg=1,  # Skip-gram (better for semantics than CBOW)
        bucket=FASTTEXT_BUCKET,
        word_ngrams=1,
    )

    # Build vocabulary from corpus (first pass)
    model.build_vocab(corpus_iterable=corpus)

    vocab_size = len(model.wv)
    assert vocab_size > 0, "Vocabulary should not be empty"
    print(f"✅ Vocabulary built: {vocab_size} words")

    # Train model (second pass)
    corpus2 = PostgresCorpus(fasttext_test_data, min_text_length=10)
    model.train(
        corpus_iterable=corpus2,
        total_examples=model.corpus_count,
        epochs=FASTTEXT_EPOCHS,
    )

    assert model.corpus_count == 20, f"Expected 20 documents, got {model.corpus_count}"
    print(f"✅ Model trained: {model.corpus_count} documents, {FASTTEXT_EPOCHS} epochs")

    # Verify model produces embeddings
    test_word = "python"
    if test_word in model.wv:
        vector = model.wv[test_word]
        assert len(vector) == FASTTEXT_VECTOR_SIZE
        assert not np.allclose(vector, 0), "Vector should not be all zeros"
        print(f"✅ Word embedding for '{test_word}': dimension={len(vector)}")

    # FastText can handle OOV words via character n-grams
    oov_word = "pythonista"  # Not in training data
    oov_vector = model.wv[oov_word]
    assert len(oov_vector) == FASTTEXT_VECTOR_SIZE
    print(f"✅ OOV word '{oov_word}' handled via n-grams")

    print("🎉 FastText training from PostgreSQL completed!")


@pytest.mark.integration
@pytest.mark.smoke
def test_fasttext_save_and_load(
    fasttext_test_data: Connection[TupleRow],
    tmp_path: Path,
) -> None:
    """Test FastText model save and load with mmap for memory efficiency."""
    model_path = tmp_path / "test_fasttext.model"

    # Train model
    corpus = PostgresCorpus(fasttext_test_data, min_text_length=10)
    model = FastText(
        vector_size=FASTTEXT_VECTOR_SIZE,
        window=FASTTEXT_WINDOW,
        min_count=FASTTEXT_MIN_COUNT,
        workers=FASTTEXT_WORKERS,
        sg=1,
        bucket=FASTTEXT_BUCKET,
    )
    model.build_vocab(corpus_iterable=corpus)

    corpus2 = PostgresCorpus(fasttext_test_data, min_text_length=10)
    model.train(
        corpus_iterable=corpus2,
        total_examples=model.corpus_count,
        epochs=FASTTEXT_EPOCHS,
    )

    # Get vector before save for comparison
    original_vector = model.wv["python"].copy()
    original_vocab_size = len(model.wv)

    # Save model
    model.save(str(model_path))
    assert model_path.exists(), "Model file should exist after save"
    print(f"✅ Model saved to {model_path}")

    # Load model with mmap (memory-efficient for production)
    loaded_model = FastText.load(str(model_path), mmap="r")

    # Verify loaded model
    assert len(loaded_model.wv) == original_vocab_size

    loaded_vector = loaded_model.wv["python"]
    assert np.allclose(original_vector, loaded_vector)

    # Verify OOV handling still works
    oov_vector = loaded_model.wv["pythonista"]
    assert len(oov_vector) == FASTTEXT_VECTOR_SIZE

    print(f"✅ Model loaded with mmap: {len(loaded_model.wv)} words")
    print("🎉 FastText save/load with mmap completed!")


@pytest.mark.integration
@pytest.mark.smoke
def test_fasttext_vectorization(
    fasttext_test_data: Connection[TupleRow],
    tmp_path: Path,
) -> None:
    """Test text vectorization with trained FastText model."""
    # Train model
    corpus = PostgresCorpus(fasttext_test_data, min_text_length=10)
    model = FastText(
        vector_size=FASTTEXT_VECTOR_SIZE,
        window=FASTTEXT_WINDOW,
        min_count=FASTTEXT_MIN_COUNT,
        workers=FASTTEXT_WORKERS,
        sg=1,
        bucket=FASTTEXT_BUCKET,
    )
    model.build_vocab(corpus_iterable=corpus)

    corpus2 = PostgresCorpus(fasttext_test_data, min_text_length=10)
    model.train(
        corpus_iterable=corpus2,
        total_examples=model.corpus_count,
        epochs=FASTTEXT_EPOCHS,
    )

    # Test vectorization of new texts
    def vectorize(text: str) -> list[float]:
        """Convert text to embedding vector."""
        tokens = simple_preprocess(text, min_len=2, max_len=20)
        if not tokens:
            return [0.0] * model.vector_size
        vector = model.wv.get_sentence_vector(tokens)
        result: list[float] = list(vector.tolist())
        return result

    # Test 1: Vectorize similar technology texts
    tech_text1 = "Python programming with data science libraries"
    tech_text2 = "Python development for machine learning projects"
    art_text = "Painting landscapes with watercolor brushes"

    vec_tech1 = np.array(vectorize(tech_text1))
    vec_tech2 = np.array(vectorize(tech_text2))
    vec_art = np.array(vectorize(art_text))

    assert len(vec_tech1) == FASTTEXT_VECTOR_SIZE
    assert not np.allclose(vec_tech1, 0)

    # Compute cosine similarities
    def cosine_similarity(v1: npt.NDArray[np.floating[Any]], v2: npt.NDArray[np.floating[Any]]) -> float:  # type: ignore[explicit-any]
        norm1 = np.linalg.norm(v1)
        norm2 = np.linalg.norm(v2)
        if norm1 == 0 or norm2 == 0:
            return 0.0
        return float(np.dot(v1, v2) / (norm1 * norm2))

    sim_tech_tech = cosine_similarity(vec_tech1, vec_tech2)
    sim_tech_art = cosine_similarity(vec_tech1, vec_art)

    print("✅ Vectorization results:")
    print(f"   Tech1 vs Tech2 similarity: {sim_tech_tech:.4f}")
    print(f"   Tech1 vs Art similarity: {sim_tech_art:.4f}")

    assert sim_tech_tech > 0, "Similar texts should have positive similarity"

    # Test 2: Vectorize text with OOV words
    oov_text = "Pythonista programing with typooos"
    vec_oov = vectorize(oov_text)
    assert len(vec_oov) == FASTTEXT_VECTOR_SIZE
    assert not np.allclose(vec_oov, 0)
    print("✅ OOV text vectorized successfully")

    # Test 3: Empty text handling
    empty_vec = vectorize("")
    assert len(empty_vec) == FASTTEXT_VECTOR_SIZE
    assert np.allclose(empty_vec, 0)
    print("✅ Empty text handling correct")

    print("🎉 FastText vectorization completed!")


@pytest.mark.integration
@pytest.mark.quality
def test_fasttext_full_workflow(
    fasttext_test_data: Connection[TupleRow],
    tmp_path: Path,
) -> None:
    """Full FastText workflow: PostgreSQL → train → save → load → vectorize."""
    model_path = tmp_path / "hintgrid_fasttext.model"

    # Step 1: Create streaming corpus from PostgreSQL
    corpus = PostgresCorpus(fasttext_test_data, min_text_length=10)

    # Step 2: Initialize and train model
    model = FastText(
        vector_size=FASTTEXT_VECTOR_SIZE,
        window=FASTTEXT_WINDOW,
        min_count=FASTTEXT_MIN_COUNT,
        workers=FASTTEXT_WORKERS,
        sg=1,
        bucket=FASTTEXT_BUCKET,
        word_ngrams=1,
    )

    model.build_vocab(corpus_iterable=corpus)
    print(f"✅ Step 1-2: Vocabulary built from PostgreSQL: {len(model.wv)} words")

    corpus2 = PostgresCorpus(fasttext_test_data, min_text_length=10)
    model.train(
        corpus_iterable=corpus2,
        total_examples=model.corpus_count,
        epochs=FASTTEXT_EPOCHS,
    )
    print(f"✅ Step 2: Model trained: {model.corpus_count} docs, {FASTTEXT_EPOCHS} epochs")

    # Step 3: Save model
    model.save(str(model_path))
    file_size_kb = model_path.stat().st_size / 1024
    print(f"✅ Step 3: Model saved ({file_size_kb:.1f} KB)")

    # Step 4: Load with mmap (production pattern)
    loaded_model = FastText.load(str(model_path), mmap="r")
    print("✅ Step 4: Model loaded with mmap")

    # Step 5: Vectorize new posts
    new_posts = [
        "Exploring Python libraries for data visualization",
        "Beautiful sunset photography with golden hour lighting",
        "Machine learning transformers for NLP tasks",
    ]

    vectors: list[list[float]] = []
    for post in new_posts:
        tokens = simple_preprocess(post, min_len=2)
        if tokens:
            vec = loaded_model.wv.get_sentence_vector(tokens)
            vectors.append(vec.tolist())
        else:
            vectors.append([0.0] * loaded_model.vector_size)

    assert len(vectors) == len(new_posts)
    assert all(len(v) == FASTTEXT_VECTOR_SIZE for v in vectors)
    assert all(not np.allclose(v, 0) for v in vectors)

    print(f"✅ Step 5: Vectorized {len(new_posts)} new posts")

    # Verify semantic relationships
    def cosine_sim(v1: list[float], v2: list[float]) -> float:
        a, b = np.array(v1), np.array(v2)
        norm_a, norm_b = np.linalg.norm(a), np.linalg.norm(b)
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return float(np.dot(a, b) / (norm_a * norm_b))

    sim_python_ml = cosine_sim(vectors[0], vectors[2])
    sim_python_photo = cosine_sim(vectors[0], vectors[1])

    print(f"   Python↔ML similarity: {sim_python_ml:.4f}")
    print(f"   Python↔Photo similarity: {sim_python_photo:.4f}")

    assert sim_python_ml != sim_python_photo

    print("🎉 Full FastText workflow completed!")


@pytest.mark.integration
def test_phrases_incremental_learning(tmp_path: Path) -> None:
    """Test that Phrases model can be incrementally updated with new data."""
    from gensim.models.phrases import Phraser, Phrases

    # Initial training data
    initial_docs = [
        ["new", "york", "city", "skyline"],
        ["new", "york", "times", "article"],
        ["new", "york", "pizza", "best"],
        ["san", "francisco", "bay", "area"],
        ["san", "francisco", "giants", "game"],
        ["san", "francisco", "coffee", "culture"],
    ]

    # Step 1: Create initial Phrases model
    phrases = Phrases(initial_docs, min_count=2, threshold=1)
    phraser = Phraser(phrases)

    # Verify initial bigrams work
    test_sentence = ["new", "york", "is", "great"]
    result = phraser[test_sentence]
    assert "new_york" in result, f"Expected 'new_york' bigram, got {result}"
    print("✅ Step 1: Initial bigram 'new_york' detected")

    # Verify unknown bigram not detected yet
    test_ml = ["machine", "learning", "is", "cool"]
    result_ml = phraser[test_ml]
    assert "machine_learning" not in result_ml
    print("✅ Step 2: 'machine_learning' not detected (as expected)")

    # Step 3: Add new vocabulary with add_vocab
    new_docs = [
        ["machine", "learning", "models"],
        ["machine", "learning", "algorithms"],
        ["machine", "learning", "training"],
        ["deep", "learning", "neural", "networks"],
        ["deep", "learning", "transformers"],
        ["deep", "learning", "applications"],
    ]

    phrases.add_vocab(new_docs)
    phraser = Phraser(phrases)

    # Verify new bigram is detected
    result_ml_updated = phraser[test_ml]
    assert "machine_learning" in result_ml_updated
    print("✅ Step 3: 'machine_learning' detected after add_vocab()")

    # Original bigrams should still work
    result_ny = phraser[test_sentence]
    assert "new_york" in result_ny
    print("✅ Step 4: Original 'new_york' still works")

    # Step 5: Save and load Phrases
    phrases_path = tmp_path / "phrases_source.pkl"
    phrases.save(str(phrases_path))

    loaded_phrases = Phrases.load(str(phrases_path))
    loaded_phraser = Phraser(loaded_phrases)

    # Verify loaded model works
    result_loaded = loaded_phraser[["machine", "learning", "rocks"]]
    assert "machine_learning" in result_loaded
    print("✅ Step 5: Phrases saved and loaded successfully")

    print("🎉 Phrases incremental learning test passed!")


@pytest.mark.integration
def test_fasttext_incremental_learning(tmp_path: Path) -> None:
    """Test that FastText model can be incrementally trained with new data."""
    # Domain A: Programming
    programming_docs = [
        ["python", "programming", "language"],
        ["python", "code", "functions"],
        ["python", "data", "structures"],
        ["javascript", "web", "development"],
        ["javascript", "frontend", "react"],
        ["javascript", "nodejs", "backend"],
        ["java", "enterprise", "applications"],
        ["java", "spring", "framework"],
    ]

    # Domain B: Cooking (completely different domain)
    cooking_docs = [
        ["recipe", "ingredients", "cooking"],
        ["recipe", "baking", "cake"],
        ["recipe", "soup", "vegetables"],
        ["chef", "kitchen", "restaurant"],
        ["chef", "cuisine", "gourmet"],
        ["chef", "dishes", "presentation"],
    ]

    # Step 1: Initial training on programming
    model = FastText(
        vector_size=64,
        window=3,
        min_count=1,
        workers=1,
        sg=1,
        epochs=10,
    )

    model.build_vocab(corpus_iterable=programming_docs)
    model.train(
        corpus_iterable=programming_docs,
        total_examples=model.corpus_count,
        epochs=model.epochs,
    )

    initial_vocab_size = len(model.wv)
    print(f"✅ Step 1: Initial training complete, vocabulary size: {initial_vocab_size}")

    # Verify programming words are in vocabulary
    assert "python" in model.wv.key_to_index
    assert "javascript" in model.wv.key_to_index

    # Cooking words should NOT be in vocabulary yet
    assert "recipe" not in model.wv.key_to_index
    assert "chef" not in model.wv.key_to_index
    print("✅ Step 2: Cooking words not in vocabulary (as expected)")

    # Step 3: Incremental training on cooking domain
    model.build_vocab(corpus_iterable=cooking_docs, update=True)
    model.train(
        corpus_iterable=cooking_docs,
        total_examples=model.corpus_count,
        epochs=model.epochs,
    )

    updated_vocab_size = len(model.wv)
    print(f"✅ Step 3: Incremental training complete, vocabulary size: {updated_vocab_size}")

    # Vocabulary should have grown
    assert updated_vocab_size > initial_vocab_size

    # New cooking words should be in vocabulary
    assert "recipe" in model.wv.key_to_index
    assert "chef" in model.wv.key_to_index

    # Original programming words should still be there
    assert "python" in model.wv.key_to_index
    assert "javascript" in model.wv.key_to_index
    print("✅ Step 4: Both domains represented in vocabulary")

    # Step 5: Test OOV handling via character n-grams
    python_vec = model.wv.get_vector("python")
    pythonic_vec = model.wv.get_vector("pythonic")

    similarity = np.dot(python_vec, pythonic_vec) / (
        np.linalg.norm(python_vec) * np.linalg.norm(pythonic_vec)
    )
    print(f"✅ Step 5: OOV handling - python↔pythonic similarity: {similarity:.4f}")

    # Step 6: Save and load
    model_path = tmp_path / "fasttext_incremental.bin"
    model.save(str(model_path))

    loaded_model = FastText.load(str(model_path))
    assert len(loaded_model.wv) == updated_vocab_size
    assert "recipe" in loaded_model.wv.key_to_index
    assert "python" in loaded_model.wv.key_to_index
    print("✅ Step 6: Model saved and loaded successfully")

    print("🎉 FastText incremental learning test passed!")


@pytest.mark.integration
def test_full_pipeline_incremental_learning(
    postgres_conn: Connection[TupleRow],
    setup_mastodon_schema_for_cli: None,
    tmp_path: Path,
) -> None:
    """Test full pipeline with Phrases + FastText incremental learning."""
    from gensim.models.phrases import Phraser, Phrases
    from nltk.tokenize import TweetTokenizer

    # Setup tokenizer
    tokenizer = TweetTokenizer(preserve_case=False, strip_handles=True, reduce_len=True)

    # Insert batch 1: Tech posts
    with postgres_conn.cursor() as cur:
        cur.execute("DELETE FROM statuses")
        batch1_posts = [
            "Love using machine learning for data analysis",
            "Machine learning models are amazing",
            "Deep learning with neural networks",
            "Deep learning transformers rock",
            "Python for machine learning projects",
            "Machine learning in production systems",
        ]
        for i, text in enumerate(batch1_posts, start=1):
            cur.execute(
                "INSERT INTO statuses (id, text, account_id) VALUES (%s, %s, %s)",
                (i, text, 1),
            )
        postgres_conn.commit()

    # Step 1: Initial training
    with postgres_conn.cursor() as cur:
        cur.execute("SELECT text FROM statuses")
        texts = [row[0] for row in cur.fetchall()]

    tokenized_batch1 = [tokenizer.tokenize(text) for text in texts]

    # Train Phrases
    phrases = Phrases(tokenized_batch1, min_count=2, threshold=1)
    phraser = Phraser(phrases)

    # Apply phrases and train FastText
    phrased_batch1 = [phraser[tokens] for tokens in tokenized_batch1]

    ft_model = FastText(
        vector_size=64,
        window=3,
        min_count=1,
        workers=1,
        epochs=10,
    )
    ft_model.build_vocab(corpus_iterable=phrased_batch1)
    ft_model.train(
        corpus_iterable=phrased_batch1,
        total_examples=ft_model.corpus_count,
        epochs=ft_model.epochs,
    )

    # Verify machine_learning bigram works
    test_tokens = tokenizer.tokenize("machine learning is great")
    phrased = phraser[test_tokens]
    assert "machine_learning" in phrased
    print("✅ Step 1: Initial training - 'machine_learning' detected")

    # Verify new_york NOT detected yet
    ny_tokens = tokenizer.tokenize("new york city")
    ny_phrased = phraser[ny_tokens]
    assert "new_york" not in ny_phrased
    print("✅ Step 2: 'new_york' not detected (as expected)")

    # Step 3: Insert batch 2 - Travel posts
    with postgres_conn.cursor() as cur:
        batch2_posts = [
            "Visiting New York this summer",
            "New York has amazing food",
            "New York subway is efficient",
            "San Francisco golden gate bridge",
            "San Francisco tech scene is hot",
            "San Francisco coffee culture rocks",
        ]
        for i, text in enumerate(batch2_posts, start=100):
            cur.execute(
                "INSERT INTO statuses (id, text, account_id) VALUES (%s, %s, %s)",
                (i, text, 2),
            )
        postgres_conn.commit()

    # Fetch only new posts
    with postgres_conn.cursor() as cur:
        cur.execute("SELECT text FROM statuses WHERE id >= 100")
        new_texts = [row[0] for row in cur.fetchall()]

    tokenized_batch2 = [tokenizer.tokenize(text) for text in new_texts]

    # Step 4: Incremental update
    phrases.add_vocab(tokenized_batch2)
    phraser = Phraser(phrases)

    phrased_batch2 = [phraser[tokens] for tokens in tokenized_batch2]

    ft_model.build_vocab(corpus_iterable=phrased_batch2, update=True)
    ft_model.train(
        corpus_iterable=phrased_batch2,
        total_examples=ft_model.corpus_count,
        epochs=ft_model.epochs,
    )

    print("✅ Step 4: Incremental training completed")

    # Step 5: Verify new bigram detected
    ny_phrased_updated = phraser[ny_tokens]
    assert "new_york" in ny_phrased_updated
    print("✅ Step 5: 'new_york' now detected")

    # Original bigram should still work
    ml_phrased = phraser[test_tokens]
    assert "machine_learning" in ml_phrased
    print("✅ Step 6: Original 'machine_learning' still works")

    # Step 7: Save both models
    phrases.save(str(tmp_path / "pipeline.phrases"))
    ft_model.save(str(tmp_path / "pipeline.fasttext"))

    # Load and verify
    loaded_phrases = Phrases.load(str(tmp_path / "pipeline.phrases"))
    loaded_phraser = Phraser(loaded_phrases)
    loaded_ft = FastText.load(str(tmp_path / "pipeline.fasttext"))

    # Verify loaded models work
    final_ny = loaded_phraser[ny_tokens]
    assert "new_york" in final_ny

    # Vectorize with loaded model
    vec = loaded_ft.wv.get_sentence_vector(["new_york", "city"])
    assert not np.allclose(vec, 0)

    print("✅ Step 7: Models saved and loaded successfully")
    print("🎉 Full pipeline incremental learning test passed!")
