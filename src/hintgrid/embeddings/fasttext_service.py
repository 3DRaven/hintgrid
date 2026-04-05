"""FastText embedding service with TweetTokenizer and Phraser.

Features:
- NLTK TweetTokenizer for social media text (handles emojis, mentions, URLs)
- Gensim Phrases/Phraser for bigram detection (new + york -> new_york)
- Gensim FastText for character n-gram embeddings (handles OOV words)
- Safe model versioning (new models saved before old deleted)
- Incremental training support
"""

from __future__ import annotations

import gc
import logging
import os
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
from numpy.typing import NDArray
from gensim.models import FastText
from gensim.models.phrases import Phraser, Phrases
from nltk.tokenize import TweetTokenizer

if TYPE_CHECKING:
    from rich.progress import Progress, TaskID

    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.clients.postgres import PostgresClient

from hintgrid.utils.coercion import coerce_int

from hintgrid.cli.console import (
    create_batch_progress,
    print_info,
    print_step,
    print_success,
)
from hintgrid.clients.postgres import PostgresCorpus, build_postgres_dsn
from hintgrid.config import HintGridSettings

logger = logging.getLogger(__name__)

# Training steps
TRAINING_STEPS_TOTAL = 3

# State node constants
STATE_NODE_ID = "main"
INITIAL_VERSION = 0
VERSION_INCREMENT = 1




@dataclass
class TrainResult:
    """Result of training operation."""

    success: bool
    corpus_size: int
    vocab_size: int
    version: int
    message: str


@dataclass
class FastTextState:
    """State stored in Neo4j for model versioning."""

    version: int
    last_trained_post_id: int
    vocab_size: int
    corpus_size: int


class TextPipeline:
    """Text preprocessing pipeline with phrase detection.

    Combines:
    - TweetTokenizer: handles social media text (emojis, mentions, hashtags)
    - Phraser: detects and joins bigrams (new + york -> new_york)
    
    Implements TokenizerProtocol for use with PostgresCorpus.
    """

    def __init__(self) -> None:
        # TweetTokenizer for social media:
        # - preserve_case=False: lowercase everything
        # - strip_handles=True: remove @mentions
        # - reduce_len=True: "waaaaaay" -> "waaay"
        self._tokenizer = TweetTokenizer(
            preserve_case=False,
            strip_handles=True,
            reduce_len=True,
        )
        self.phrases: Phrases | None = None
        self.phraser: Phraser | None = None

    def tokenize(self, text: str) -> list[str]:
        """Tokenize text using TweetTokenizer.
        
        This method implements TokenizerProtocol for use with PostgresCorpus.
        """
        if not text or not text.strip():
            return []
        return self._tokenizer.tokenize(text)

    def transform(self, text: str) -> list[str]:
        """Full preprocessing: tokenize + apply phrases."""
        tokens = self.tokenize(text)
        if not tokens:
            return []

        if self.phraser is not None:
            phrased: list[str] = list(self.phraser[tokens])
            return phrased

        return tokens

    # How often to refresh vocab size in progress description
    _VOCAB_DISPLAY_INTERVAL = 500

    def learn_phrases_from_stream(
        self, corpus: PostgresCorpus, min_count: int = 5
    ) -> int:
        """Learn phrase patterns from streaming corpus.
        
        Args:
            corpus: Streaming PostgresCorpus iterator
            min_count: Minimum count for phrase detection
            
        Returns:
            Number of documents processed
        """
        doc_count = 0
        
        # Suppress gensim INFO logs during phrase learning
        gensim_logger = logging.getLogger("gensim")
        original_level = gensim_logger.level
        gensim_logger.setLevel(logging.WARNING)
        
        try:
            self.phrases = Phrases(min_count=min_count, threshold=10)
            
            # Pre-compute total for percentage progress
            corpus_total = corpus.total_count() or None
            
            with create_batch_progress(corpus_total) as progress:
                task = progress.add_task(
                    "[cyan]Learning phrases[/cyan]", total=corpus_total
                )
                for tokens in corpus:
                    if tokens:
                        self.phrases.add_vocab([tokens])
                        doc_count += 1
                        progress.advance(task)
                        # Periodically show vocab size in description
                        if doc_count % self._VOCAB_DISPLAY_INTERVAL == 0:
                            vocab_size = len(self.phrases.vocab)
                            progress.update(
                                task,
                                description=(
                                    f"[cyan]Learning phrases[/cyan] "
                                    f"[dim](vocab: {vocab_size:,})[/dim]"
                                ),
                            )
                # Show final vocab in description
                vocab_size = len(self.phrases.vocab) if self.phrases else 0
                progress.update(
                    task,
                    description=(
                        f"[green]✓[/green] Learned phrases from {doc_count:,} "
                        f"docs (vocab: {vocab_size:,})"
                    ),
                    completed=corpus_total,
                )
                    
            if doc_count > 1:
                self.phraser = Phraser(self.phrases)
        finally:
            gensim_logger.setLevel(original_level)
            
        return doc_count

    def update_phrases_from_stream(self, corpus: PostgresCorpus) -> int:
        """Incrementally update phrase patterns from streaming corpus.
        
        Args:
            corpus: Streaming PostgresCorpus iterator
            
        Returns:
            Number of documents processed
        """
        if self.phrases is None:
            return self.learn_phrases_from_stream(corpus)

        # Suppress gensim INFO logs during phrase update
        gensim_logger = logging.getLogger("gensim")
        original_level = gensim_logger.level
        gensim_logger.setLevel(logging.WARNING)
        
        try:
            doc_count = 0
            # Pre-compute total for percentage progress
            corpus_total = corpus.total_count() or None
            
            with create_batch_progress(corpus_total) as progress:
                task = progress.add_task(
                    "[cyan]Updating phrases[/cyan]", total=corpus_total
                )
                for tokens in corpus:
                    if tokens:
                        self.phrases.add_vocab([tokens])
                        doc_count += 1
                        progress.advance(task)
                        # Periodically show vocab size in description
                        if doc_count % self._VOCAB_DISPLAY_INTERVAL == 0:
                            vocab_size = len(self.phrases.vocab)
                            progress.update(
                                task,
                                description=(
                                    f"[cyan]Updating phrases[/cyan] "
                                    f"[dim](vocab: {vocab_size:,})[/dim]"
                                ),
                            )
                # Show final vocab in description
                vocab_size = len(self.phrases.vocab) if self.phrases else 0
                progress.update(
                    task,
                    description=(
                        f"[green]✓[/green] Updated phrases from {doc_count:,} "
                        f"docs (vocab: {vocab_size:,})"
                    ),
                    completed=corpus_total,
                )

            self.phraser = Phraser(self.phrases)
        finally:
            gensim_logger.setLevel(original_level)
            
        return doc_count

    def learn_phrases(self, documents: list[list[str]], min_count: int = 5) -> None:
        """Learn phrase patterns from tokenized documents (in-memory)."""
        if len(documents) < 2:
            return

        # Suppress gensim INFO logs during phrase learning
        gensim_logger = logging.getLogger("gensim")
        original_level = gensim_logger.level
        gensim_logger.setLevel(logging.WARNING)
        try:
            self.phrases = Phrases(documents, min_count=min_count, threshold=10)
            self.phraser = Phraser(self.phrases)
        finally:
            gensim_logger.setLevel(original_level)

    def update_phrases(self, documents: list[list[str]]) -> None:
        """Incrementally update phrase patterns (in-memory)."""
        if self.phrases is None:
            self.learn_phrases(documents)
            return

        # Suppress gensim INFO logs during phrase update
        gensim_logger = logging.getLogger("gensim")
        original_level = gensim_logger.level
        gensim_logger.setLevel(logging.WARNING)
        try:
            self.phrases.add_vocab(documents)
            self.phraser = Phraser(self.phrases)
        finally:
            gensim_logger.setLevel(original_level)


class FastTextEmbeddingService:
    """FastText-based embedding service with Neo4j state management.

    Features:
    - TweetTokenizer for social media text
    - Phrase detection (new_york, machine_learning)
    - Character n-grams for OOV handling
    - Safe model versioning
    - Incremental training support
    - Memory-efficient streaming from PostgreSQL
    """

    def __init__(
        self,
        neo4j: Neo4jClient,
        settings: HintGridSettings,
        postgres: PostgresClient | None = None,
        dsn: str | None = None,
        since_date: datetime | None = None,
    ) -> None:
        """Initialize FastText embedding service.

        Args:
            neo4j: Neo4j client for state storage
            settings: Application settings
            postgres: Optional PostgreSQL client for training (batch mode)
            dsn: Optional PostgreSQL DSN for streaming training
            since_date: Optional date filter for auto-training
        """
        self._neo4j = neo4j
        self._settings = settings
        self._postgres = postgres
        self._dsn = dsn or self._build_dsn(settings)
        self._since_date = since_date

        self._pipeline = TextPipeline()
        self._model: FastText | None = None
        self._state: FastTextState | None = None

        # Model storage path
        self._model_path = Path(os.path.expanduser(settings.fasttext_model_path))
        self._model_path.mkdir(parents=True, exist_ok=True)

        # Ensure state node exists
        self._ensure_state_node()

    @staticmethod
    def _resolve_training_workers(settings: HintGridSettings) -> int:
        """Resolve effective number of training workers.

        Gensim FastText uses C-level POSIX threads that bypass the GIL,
        so multiple workers genuinely parallelize training.

        Args:
            settings: Application settings

        Returns:
            Number of workers (>= 1)
        """
        configured = settings.fasttext_training_workers
        if configured <= 0:
            # Auto-detect: use all CPUs but cap at a reasonable maximum
            cpu_count = os.cpu_count() or 1
            return min(cpu_count, 16)
        return configured

    @staticmethod
    def _build_dsn(settings: HintGridSettings) -> str:
        """Build PostgreSQL DSN from settings.

        Delegates to build_postgres_dsn to ensure client_encoding=UTF8
        and proper URL-encoding of the password.
        """
        return build_postgres_dsn(settings)

    def embed_texts(self, texts: Iterable[tuple[int, str]]) -> list[list[float]]:
        """Embed texts using FastText with batch normalization.

        On first run (no models), performs automatic full training.
        On subsequent runs, uses existing models without training.

        Optimizations over per-item processing:
        - Collects raw vectors into a 2D numpy array
        - Batch-normalizes all vectors in one vectorized operation
        - Uses ndarray.tolist() (C implementation) instead of Python loop

        Args:
            texts: Iterable of (post_id, text) tuples

        Returns:
            List of embedding vectors
        """
        items = list(texts)

        # Ensure models are loaded (auto-train on first run)
        if not self._ensure_models_loaded():
            # Auto full training on first run
            if self._postgres is None:
                raise RuntimeError(
                    "No trained models found and no PostgreSQL client provided. "
                    "Run 'hintgrid train --full' first."
                )
            print_info("No models found, starting automatic full training...")
            train_result = self.train_full(self._since_date)
            if not train_result.success:
                raise RuntimeError(f"Auto training failed: {train_result.message}")

        if self._model is None:
            raise RuntimeError("Model not loaded")

        # Collect raw vectors for batch normalization
        raw_vectors: list[NDArray[np.float32]] = []
        for _, text in items:
            tokens = self._pipeline.transform(text)
            if tokens:
                vec = self._model.wv.get_sentence_vector(tokens)
                raw_vectors.append(np.asarray(vec, dtype=np.float32))
            else:
                raw_vectors.append(self._generate_fallback_vector(text))

        # Batch normalize: stack into 2D array, compute norms, divide
        matrix = np.array(raw_vectors, dtype=np.float32)
        norms = np.linalg.norm(matrix, axis=1, keepdims=True)
        # Avoid division by zero
        norms[norms == 0] = 1.0
        normalized = matrix / norms

        # Convert to lists using C-level tolist() instead of Python loop
        embeddings: list[list[float]] = normalized.tolist()
        return embeddings

    def train_full(self, since_date: datetime | None = None) -> TrainResult:
        """Full training from PostgreSQL using streaming cursor.

        Uses server-side cursor to stream data without loading into memory.

        Args:
            since_date: Optional date filter for training data

        Returns:
            TrainResult with training statistics
        """
        logger.info("Starting full training with streaming cursor...")

        # Create streaming corpus with tokenizer
        corpus = PostgresCorpus(
            dsn=self._dsn,
            tokenizer=self._pipeline,
            min_id=0,
            since_date=since_date,
            batch_size=self._settings.batch_size,
            public_visibility=self._settings.mastodon_public_visibility,
            schema=self._settings.postgres_schema,
        )

        # Train using streaming
        return self._train_from_corpus(corpus, is_incremental=False)

    def train_incremental(self) -> TrainResult:
        """Incremental training with new posts since last training.

        Uses server-side cursor to stream new data without loading into memory.

        Returns:
            TrainResult with training statistics
        """
        # Load current state
        state = self._load_state()
        if state.version == INITIAL_VERSION:
            # No existing model, do full training
            logger.info("No existing model, performing full training instead")
            return self.train_full()

        # Ensure models are loaded in training mode (needs Phrases for add_vocab)
        if not self._ensure_models_loaded(for_training=True):
            logger.warning("Could not load existing models, performing full training")
            return self.train_full()

        logger.info(
            "Starting incremental training from post_id > %d",
            state.last_trained_post_id,
        )

        # Create streaming corpus starting from last trained post
        corpus = PostgresCorpus(
            dsn=self._dsn,
            tokenizer=self._pipeline,
            min_id=state.last_trained_post_id,
            batch_size=self._settings.batch_size,
            public_visibility=self._settings.mastodon_public_visibility,
            schema=self._settings.postgres_schema,
        )

        # Train using streaming
        return self._train_from_corpus(corpus, is_incremental=True)

    def _train_from_corpus(
        self, corpus: PostgresCorpus, is_incremental: bool
    ) -> TrainResult:
        """Train or update models using streaming corpus.

        This is the memory-efficient training method that uses server-side
        cursors to stream data from PostgreSQL.

        Args:
            corpus: Streaming PostgresCorpus iterator
            is_incremental: Whether this is incremental training

        Returns:
            TrainResult with training statistics
        """
        # Pass 1: Learn/update phrases from streaming corpus
        print_step(1, TRAINING_STEPS_TOTAL, "Learning phrases from corpus...")
        logger.info("Pass 1: Learning phrases from streaming corpus...")

        if is_incremental and self._pipeline.phrases is not None:
            doc_count = self._pipeline.update_phrases_from_stream(corpus)
        else:
            doc_count = self._pipeline.learn_phrases_from_stream(corpus)

        if doc_count < self._settings.fasttext_min_documents:
            return TrainResult(
                success=False,
                corpus_size=doc_count,
                vocab_size=0,
                version=0,
                message=f"Not enough documents: {doc_count} < {self._settings.fasttext_min_documents}",
            )

        logger.info("Learned phrases from %d documents", doc_count)

        # Pass 2: Build vocabulary for FastText
        # Create a wrapper that applies phraser to streaming corpus
        min_count = self._settings.fasttext_min_count
        max_vocab_size = self._settings.fasttext_max_vocab_size
        vec_size = self._settings.fasttext_vector_size
        bucket = self._settings.fasttext_bucket
        print_step(
            2,
            TRAINING_STEPS_TOTAL,
            f"Building FastText vocabulary "
            f"(min_count={min_count}, max_vocab={max_vocab_size:,}, "
            f"vector_size={vec_size}, bucket={bucket:,})...",
        )
        logger.info(
            "Pass 2: Building FastText vocabulary "
            "(min_count=%d, max_vocab_size=%d, vector_size=%d, bucket=%d)...",
            min_count,
            max_vocab_size,
            vec_size,
            bucket,
        )

        vocab_total = doc_count or None

        with create_batch_progress(vocab_total) as progress:
            task = progress.add_task(
                "[cyan]Building vocabulary[/cyan]", total=vocab_total
            )
            if is_incremental and self._model is not None:
                # Incremental: update vocab then train
                phrased_corpus = _PhrasedCorpusWrapper(
                    corpus, self._pipeline, progress, task,
                    min_count=min_count, is_vocab_build=True,
                )
                self._model.build_vocab(corpus_iterable=phrased_corpus, update=True)
                corpus_count = self._model.corpus_count
            else:
                # Full training: create new model
                training_workers = self._resolve_training_workers(self._settings)
                self._model = FastText(
                    vector_size=vec_size,
                    window=self._settings.fasttext_window,
                    min_count=min_count,
                    max_vocab_size=max_vocab_size,
                    workers=training_workers,
                    sg=1,  # Skip-gram
                    bucket=bucket,
                    word_ngrams=1,
                    epochs=self._settings.fasttext_epochs,
                )
                phrased_corpus = _PhrasedCorpusWrapper(
                    corpus, self._pipeline, progress, task,
                    min_count=min_count, is_vocab_build=True,
                )
                self._model.build_vocab(corpus_iterable=phrased_corpus)
                corpus_count = self._model.corpus_count

            built_vocab_size = len(self._model.wv)
            progress.update(
                task,
                description=(
                    f"[green]✓[/green] Vocabulary: {built_vocab_size:,} words "
                    f"from {corpus_count:,} docs "
                    f"(min_count={min_count})"
                ),
                completed=vocab_total,
            )

        print_success(
            f"Vocabulary: {built_vocab_size:,} words from "
            f"{corpus_count:,} documents (min_count={min_count})"
        )

        # Check for empty vocabulary before training
        if built_vocab_size == 0:
            logger.warning(
                "Empty vocabulary after build_vocab (min_count=%d, docs=%d). "
                "Try lowering fasttext_min_count or adding more data.",
                min_count,
                corpus_count,
            )
            return TrainResult(
                success=False,
                corpus_size=corpus_count,
                vocab_size=0,
                version=0,
                message=(
                    f"Empty vocabulary: no words appear >= {min_count} times "
                    f"across {corpus_count} documents. "
                    "Lower fasttext_min_count or add more data."
                ),
            )

        # Pass 3: Train the model
        epochs = self._settings.fasttext_epochs
        print_step(3, TRAINING_STEPS_TOTAL, f"Training FastText ({epochs} epochs)...")
        logger.info("Pass 3: Training FastText on %d documents...", corpus_count)

        train_total = corpus_count * epochs if corpus_count else None

        with create_batch_progress(train_total) as progress:
            task = progress.add_task(
                f"[cyan]Training epoch 1/{epochs}[/cyan]",
                total=train_total,
            )
            phrased_corpus = _PhrasedCorpusWrapper(
                corpus, self._pipeline, progress, task, total_epochs=epochs
            )
            self._model.train(
                corpus_iterable=phrased_corpus,
                total_examples=corpus_count,
                epochs=epochs,
            )
            progress.update(
                task,
                description=(
                    f"[green]✓[/green] Trained {epochs} epochs on "
                    f"{corpus_count:,} docs"
                ),
                completed=train_total,
            )

        vocab_size = len(self._model.wv)
        max_post_id = corpus.max_id

        # Get current version and increment
        current_state = self._load_state()
        new_version = current_state.version + VERSION_INCREMENT

        # Save models with new version (safe versioning)
        self._save_models(new_version)

        # Free Phrases from memory after saving (Phraser remains for inference)
        # This reduces memory usage significantly as Phrases stores frequency counters
        if self._pipeline.phrases is not None:
            self._pipeline.phrases = None
            gc.collect()
            logger.debug("Freed Phrases from memory after training (Phraser kept)")

        # Update state in Neo4j
        new_state = FastTextState(
            version=new_version,
            last_trained_post_id=max_post_id,
            vocab_size=vocab_size,
            corpus_size=doc_count,
        )
        self._save_state(new_state)

        # Delete old version files
        if current_state.version > INITIAL_VERSION:
            self._delete_model_files(current_state.version)

        print_success(f"Training complete: vocab={vocab_size:,}, corpus={doc_count:,}")
        logger.info(
            "Training complete: version=%d, vocab=%d, corpus=%d",
            new_version,
            vocab_size,
            doc_count,
        )

        return TrainResult(
            success=True,
            corpus_size=doc_count,
            vocab_size=vocab_size,
            version=new_version,
            message="Training completed successfully",
        )

    def _embed_single(self, text: str) -> list[float]:
        """Generate embedding for a single text."""
        tokens = self._pipeline.transform(text)

        # Fallback for empty/invalid tokens
        if not tokens:
            return self._generate_fallback_embedding(text)

        if self._model is None:
            raise RuntimeError("Model not loaded")

        # Get sentence vector
        embedding = self._model.wv.get_sentence_vector(tokens)

        # Normalize to unit vector
        norm = float(np.linalg.norm(embedding))
        if norm > 0:
            embedding = embedding / norm

        return [float(x) for x in embedding]

    def _generate_fallback_vector(self, text: str) -> NDArray[np.float32]:
        """Generate deterministic fallback vector for texts without valid tokens.

        Returns raw numpy array (not normalized) for batch processing.
        """
        text_hash = hash(text)
        seed = abs(text_hash) % (2**31)

        rng = np.random.RandomState(seed)
        vec: NDArray[np.float32] = (
            rng.randn(self._settings.fasttext_vector_size).astype(np.float32) * 0.1
        )
        return vec

    def _generate_fallback_embedding(self, text: str) -> list[float]:
        """Generate deterministic fallback for texts without valid tokens."""
        embedding = self._generate_fallback_vector(text)

        norm = float(np.linalg.norm(embedding))
        if norm > 0:
            embedding = embedding / norm

        return [float(x) for x in embedding]

    def _ensure_models_loaded(self, for_training: bool = False) -> bool:
        """Ensure models are loaded from disk.

        Args:
            for_training: If True, load Phrases for incremental training

        Returns:
            True if models were loaded successfully
        """
        if self._model is not None:
            # If already loaded but need training mode and Phrases is None, reload
            if for_training and self._pipeline.phrases is None:
                state = self._load_state()
                if state.version != INITIAL_VERSION:
                    return self._load_models(state.version, for_training=True)
            return True

        state = self._load_state()
        if state.version == INITIAL_VERSION:
            return False

        return self._load_models(state.version, for_training=for_training)

    def _load_models(self, version: int, for_training: bool = False) -> bool:
        """Load models from disk for given version.

        Two loading modes:
        - Inference mode (default): Load only Phraser (compact, fast)
        - Training mode: Load Phrases and derive Phraser (needed for add_vocab)

        Args:
            version: Model version to load
            for_training: If True, load Phrases for incremental training

        Returns:
            True if loaded successfully
        """
        phrases_path = self._model_path / f"phrases_v{version}.pkl"
        phraser_path = self._model_path / f"phraser_v{version}.pkl"
        fasttext_path = self._model_path / f"fasttext_v{version}.bin"

        if not fasttext_path.exists():
            logger.warning("FastText model file not found for version %d", version)
            return False

        try:
            if for_training:
                # Training mode: load Phrases (needed for add_vocab)
                if not phrases_path.exists():
                    logger.warning("Phrases file not found for training mode, version %d", version)
                    return False
                self._pipeline.phrases = Phrases.load(str(phrases_path))
                self._pipeline.phraser = Phraser(self._pipeline.phrases)
                logger.debug("Loaded Phrases+Phraser for training, version %d", version)
            else:
                # Inference mode: load only Phraser (compact)
                if phraser_path.exists():
                    self._pipeline.phraser = Phraser.load(str(phraser_path))
                    self._pipeline.phrases = None  # Don't keep Phrases in memory
                    logger.debug("Loaded Phraser only for inference, version %d", version)
                elif phrases_path.exists():
                    # Fallback: if Phraser missing, load Phrases and derive Phraser
                    logger.warning(
                        "Phraser file not found, loading Phrases as fallback for version %d",
                        version,
                    )
                    self._pipeline.phrases = Phrases.load(str(phrases_path))
                    self._pipeline.phraser = Phraser(self._pipeline.phrases)
                    # Free Phrases after deriving Phraser (inference mode)
                    self._pipeline.phrases = None
                else:
                    logger.warning("Neither Phraser nor Phrases file found for version %d", version)
                    return False

            # Load FastText model
            if for_training:
                # Training mode: load full model (needed for build_vocab(update=True))
                self._model = FastText.load(str(fasttext_path), mmap="r")
                logger.debug("Loaded full FastText model for training, version %d", version)
            else:
                # Inference mode: prefer quantized model (10-50x smaller)
                quantized_path = self._model_path / f"fasttext_v{version}.q.bin"
                if quantized_path.exists():
                    try:
                        self._model = FastText.load(str(quantized_path), mmap="r")
                        logger.debug(
                            "Loaded quantized FastText model for inference, version %d", version
                        )
                    except Exception as e:
                        logger.warning(
                            "Failed to load quantized model, falling back to full model: %s", e
                        )
                        self._model = FastText.load(str(fasttext_path), mmap="r")
                else:
                    # Fallback to full model if quantized not available
                    self._model = FastText.load(str(fasttext_path), mmap="r")
                    logger.debug(
                        "Quantized model not found, loaded full model for inference, version %d",
                        version,
                    )

            logger.info("Loaded models version %d (training=%s)", version, for_training)
            return True
        except Exception as e:
            logger.error("Failed to load models: %s", e)
            return False

    def _save_models(self, version: int) -> None:
        """Save models to disk with version suffix.

        Saves both Phrases (for future incremental training) and Phraser
        (for compact inference). Phrases is larger but needed for add_vocab().

        Args:
            version: Version number for file names
        """
        phrases_path = self._model_path / f"phrases_v{version}.pkl"
        phraser_path = self._model_path / f"phraser_v{version}.pkl"
        fasttext_path = self._model_path / f"fasttext_v{version}.bin"

        if self._pipeline.phrases is not None:
            # Save Phrases for incremental training
            self._pipeline.phrases.save(str(phrases_path))

        if self._pipeline.phraser is not None:
            # Save Phraser separately for compact inference loading
            self._pipeline.phraser.save(str(phraser_path))

        if self._model is not None:
            # Save full model (needed for incremental training)
            self._model.save(str(fasttext_path))

            # Quantize model if enabled (10-50x size reduction for inference)
            if self._settings.fasttext_quantize:
                try:
                    import compress_fasttext

                    quantized_path = self._model_path / f"fasttext_v{version}.q.bin"
                    logger.info(
                        "Quantizing model (qdim=%d) for version %d...",
                        self._settings.fasttext_quantize_qdim,
                        version,
                    )
                    quantized_model = compress_fasttext.quantize(
                        self._model, qdim=self._settings.fasttext_quantize_qdim
                    )
                    quantized_model.save(str(quantized_path))
                    logger.info(
                        "Saved quantized model version %d to %s", version, quantized_path
                    )
                except ImportError:
                    logger.warning(
                        "compress-fasttext not available, skipping quantization. "
                        "Install with: pip install compress-fasttext"
                    )
                except Exception as e:
                    logger.warning("Failed to quantize model: %s", e)

        logger.info("Saved models version %d to %s", version, self._model_path)

    def _delete_model_files(self, version: int) -> None:
        """Delete model files for given version.

        Args:
            version: Version to delete
        """
        phrases_path = self._model_path / f"phrases_v{version}.pkl"
        phraser_path = self._model_path / f"phraser_v{version}.pkl"
        fasttext_path = self._model_path / f"fasttext_v{version}.bin"
        quantized_path = self._model_path / f"fasttext_v{version}.q.bin"
        ngrams_path = self._model_path / f"fasttext_v{version}.bin.wv.vectors_ngrams.npy"

        for path in [phrases_path, phraser_path, fasttext_path, quantized_path, ngrams_path]:
            if path.exists():
                try:
                    path.unlink()
                    logger.debug("Deleted %s", path)
                except Exception as e:
                    logger.warning("Failed to delete %s: %s", path, e)

    def _ensure_state_node(self) -> None:
        """Ensure FastTextState node exists in Neo4j using APOC."""
        self._neo4j.execute(
            "CALL apoc.merge.node($labels, {id: $id}, "
            "{version: $version, last_trained_post_id: 0, "
            " vocab_size: 0, corpus_size: 0, "
            " updated_at: timestamp()}, {}) "
            "YIELD node",
            {
                "labels": self._neo4j.labels_list("FastTextState"),
                "id": STATE_NODE_ID,
                "version": INITIAL_VERSION,
            },
        )

    def _load_state(self) -> FastTextState:
        """Load state from Neo4j."""
        rows = list(self._neo4j.execute_and_fetch_labeled(
            "MATCH (s:__label__ {id: $id}) "
            "RETURN s.version AS version, "
            "       s.last_trained_post_id AS last_trained_post_id, "
            "       s.vocab_size AS vocab_size, "
            "       s.corpus_size AS corpus_size",
            {"label": "FastTextState"},
            {"id": STATE_NODE_ID},
        ))

        if not rows:
            return FastTextState(
                version=INITIAL_VERSION,
                last_trained_post_id=0,
                vocab_size=0,
                corpus_size=0,
            )

        row = rows[0]
        return FastTextState(
            version=coerce_int(row.get("version"), INITIAL_VERSION),
            last_trained_post_id=coerce_int(row.get("last_trained_post_id"), 0),
            vocab_size=coerce_int(row.get("vocab_size"), 0),
            corpus_size=coerce_int(row.get("corpus_size"), 0),
        )

    def _save_state(self, state: FastTextState) -> None:
        """Save state to Neo4j."""
        self._neo4j.execute_labeled(
            "MATCH (s:__label__ {id: $id}) "
            "SET s.version = $version, "
            "    s.last_trained_post_id = $last_trained_post_id, "
            "    s.vocab_size = $vocab_size, "
            "    s.corpus_size = $corpus_size, "
            "    s.updated_at = timestamp()",
            {"label": "FastTextState"},
            {
                "id": STATE_NODE_ID,
                "version": state.version,
                "last_trained_post_id": state.last_trained_post_id,
                "vocab_size": state.vocab_size,
                "corpus_size": state.corpus_size,
            },
        )

        self._state = state


class _PhrasedCorpusWrapper:
    """Wrapper that applies phraser to streaming corpus with progress tracking.

    This allows Gensim to iterate multiple times over the corpus
    (for multiple epochs) while applying phrase detection.
    Optionally tracks iteration progress via Rich Progress bar.
    """

    def __init__(
        self,
        corpus: PostgresCorpus,
        pipeline: TextPipeline,
        progress: Progress | None = None,
        task: TaskID | None = None,
        total_epochs: int = 1,
        min_count: int = 1,
        is_vocab_build: bool = False,
    ) -> None:
        self._corpus = corpus
        self._pipeline = pipeline
        self._progress = progress
        self._task = task
        self._total_epochs = total_epochs
        self._current_epoch = 0
        self._min_count = min_count
        self._is_vocab_build = is_vocab_build

    def __iter__(self) -> Iterator[list[str]]:
        """Yield phrased documents from streaming corpus."""
        self._current_epoch += 1

        # Update description at epoch start (for multi-epoch training)
        if (
            self._progress is not None
            and self._task is not None
            and self._total_epochs > 1
        ):
            self._progress.update(
                self._task,
                description=(
                    f"[cyan]Training epoch "
                    f"{self._current_epoch}/{self._total_epochs}[/cyan]"
                ),
            )

        for tokens in self._corpus:
            if tokens:
                # Apply phraser if available
                if self._pipeline.phraser is not None:
                    yield list(self._pipeline.phraser[tokens])
                else:
                    yield tokens

                # Advance progress if tracking
                if self._progress is not None and self._task is not None:
                    self._progress.advance(self._task)

        # After corpus exhausted: signal post-iteration processing phase
        # (build_vocab continues with prepare_vocab + prepare_weights internally)
        if (
            self._is_vocab_build
            and self._progress is not None
            and self._task is not None
        ):
            self._progress.update(
                self._task,
                description=(
                    f"[yellow]⏳ Preparing vocab & initializing weights "
                    f"(min_count={self._min_count})...[/yellow]"
                ),
            )
