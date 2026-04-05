"""FastText embedding service with advanced tokenization and phrase detection.

Uses:
- NLTK TweetTokenizer for social media text (handles emojis, mentions, URLs)
- Gensim Phrases/Phraser for bigram detection (new + york -> new_york)
- Gensim FastText for character n-gram embeddings (handles OOV words)
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import numpy as np
from flask import Flask, Response, jsonify, request
from gensim.models import FastText
from gensim.models.phrases import Phraser, Phrases
from nltk.tokenize import TweetTokenizer

# Default settings (can be overridden via FastTextConfig)
DEFAULT_VECTOR_SIZE = 128  # 64-300 recommended, 768 is overkill for FastText
DEFAULT_WINDOW = 3
DEFAULT_MIN_COUNT = 1
DEFAULT_EPOCHS = 5
DEFAULT_BUCKET = 10000  # Reduced from 2M for memory efficiency
DEFAULT_MIN_DOCUMENTS = 1
DEFAULT_PHRASE_MIN_COUNT = 2  # Minimum count for phrase detection
DEFAULT_PHRASE_THRESHOLD = 5  # Threshold for phrase detection


@dataclass
class _FastTextConfig:
    """Configuration for FastText embedding service.

    Recommended vector_size: 64-300 (768 is overkill for FastText).
    """

    vector_size: int = DEFAULT_VECTOR_SIZE
    window: int = DEFAULT_WINDOW
    min_count: int = DEFAULT_MIN_COUNT
    epochs: int = DEFAULT_EPOCHS
    bucket: int = DEFAULT_BUCKET
    min_documents: int = DEFAULT_MIN_DOCUMENTS
    phrase_min_count: int = DEFAULT_PHRASE_MIN_COUNT
    phrase_threshold: int = DEFAULT_PHRASE_THRESHOLD


class _TextPipeline:
    """Text preprocessing pipeline with phrase detection.

    Combines:
    - TweetTokenizer: handles social media text (emojis, mentions, hashtags)
    - Phraser: detects and joins bigrams (new + york -> new_york)
    """

    def __init__(self, config: _FastTextConfig) -> None:
        self.config = config
        # TweetTokenizer is good for social media:
        # - preserve_case=False: lowercase everything
        # - strip_handles=True: remove @mentions
        # - reduce_len=True: "waaaaaay" -> "waaay"
        self.tokenizer = TweetTokenizer(
            preserve_case=False,
            strip_handles=True,
            reduce_len=True,
        )
        self.phraser: Phraser | None = None

    def tokenize(self, text: str) -> list[str]:
        """Tokenize text using TweetTokenizer.

        Args:
            text: Input text

        Returns:
            List of tokens (handles emojis, hashtags, URLs)
        """
        if not text or not text.strip():
            return []
        return self.tokenizer.tokenize(text)

    def transform(self, text: str) -> list[str]:
        """Full preprocessing: tokenize + apply phrases.

        Args:
            text: Input text

        Returns:
            List of tokens with phrases joined (e.g., ['new_york'])
        """
        tokens = self.tokenize(text)
        if not tokens:
            return []

        # Apply phraser if trained
        if self.phraser is not None:
            return list(self.phraser[tokens])

        return tokens

    def learn_phrases(self, documents: list[list[str]]) -> None:
        """Learn phrase patterns from tokenized documents.

        Args:
            documents: List of tokenized documents
        """
        if len(documents) < self.config.min_documents:
            return

        phrases = Phrases(
            documents,
            min_count=self.config.phrase_min_count,
            threshold=self.config.phrase_threshold,
        )
        self.phraser = Phraser(phrases)

    def save(self, base_path: str) -> None:
        """Save phraser model to disk."""
        if self.phraser is not None:
            path = f"{base_path}.phrases"
            self.phraser.save(path)

    def load(self, base_path: str) -> None:
        """Load phraser model from disk."""
        path = f"{base_path}.phrases"
        if Path(path).exists():
            self.phraser = Phraser.load(path)


class _FastTextEmbeddingService:
    """FastText-based embedding service with advanced tokenization.

    Features:
    - TweetTokenizer for social media text
    - Phrase detection (new_york, machine_learning)
    - Character n-grams for OOV handling
    - Deterministic fallback for edge cases
    
    This class is private and should only be used internally by the web service.
    """

    def __init__(self, config: _FastTextConfig | None = None) -> None:
        """
        Initialize FastText embedding service.

        Args:
            config: FastText configuration (uses defaults if None)
        """
        self.config = config or _FastTextConfig()
        self.pipeline = _TextPipeline(self.config)

        # Cache for storing documents and model
        self.document_cache: list[list[str]] = []
        self.model: FastText | None = None
        self._fit_lock = threading.Lock()

    @property
    def embedding_dim(self) -> int:
        """Get embedding dimension (for compatibility)."""
        return self.config.vector_size

    def reset(self) -> None:
        """
        Reset the service state (clear cache and model).
        Useful for tests to ensure isolation between test cases.
        """
        with self._fit_lock:
            self.document_cache.clear()
            self.model = None
            self.pipeline = _TextPipeline(self.config)

    def _ensure_fitted(self, new_texts: str | list[str]) -> None:
        """
        Ensure model is trained, update if needed.

        Args:
            new_texts: New document(s) to potentially add to corpus
        """
        # Convert to list if single string
        texts_to_add = [new_texts] if isinstance(new_texts, str) else list(new_texts)

        with self._fit_lock:
            # Tokenize and add valid texts to cache
            for text in texts_to_add:
                tokens = self.pipeline.tokenize(text)
                if tokens and tokens not in self.document_cache:
                    self.document_cache.append(tokens)

            # Train only when we have enough documents and model not yet created
            should_train = (
                self.model is None and len(self.document_cache) >= self.config.min_documents
            )

            if should_train:
                self._train_model()

    def preload_training_data(self, texts: list[str]) -> None:
        """Preload training data and train model at service startup.
        
        This method is called once at service startup to train the model
        on test data, ensuring the model is ready before any embedding requests.
        
        Args:
            texts: List of text documents for training
        """
        with self._fit_lock:
            # Tokenize and add all texts to cache
            for text in texts:
                if not text or not text.strip():
                    continue
                tokens = self.pipeline.tokenize(text)
                if tokens and tokens not in self.document_cache:
                    self.document_cache.append(tokens)
            
            # Train if we have enough documents
            if len(self.document_cache) >= self.config.min_documents and self.model is None:
                self._train_model()

    def _train_model(self) -> None:
        """Train FastText model on cached documents with phrase detection."""
        if len(self.document_cache) < self.config.min_documents:
            return

        # Step 1: Learn phrases from raw tokens
        self.pipeline.learn_phrases(self.document_cache)

        # Step 2: Apply phrases to documents
        phrased_docs = [self.pipeline.transform(" ".join(doc)) for doc in self.document_cache]

        # Step 3: Train FastText on phrased documents
        self.model = FastText(
            vector_size=self.config.vector_size,
            window=self.config.window,
            min_count=self.config.min_count,
            workers=1,  # Single worker for thread safety
            sg=1,  # Skip-gram
            bucket=self.config.bucket,
            word_ngrams=1,
            seed=42,  # Deterministic training for reproducible test embeddings
        )

        self.model.build_vocab(corpus_iterable=phrased_docs)

        if not self.model.wv.key_to_index:
            # No vocabulary built (all tokens filtered out by min_count)
            self.model = None
            return

        self.model.train(
            corpus_iterable=phrased_docs,
            total_examples=self.model.corpus_count,
            epochs=self.config.epochs,
        )

        vocab_size = len(self.model.wv)
        corpus_size = len(self.document_cache)
        print(f"✅ FastText model trained: vocabulary={vocab_size}, corpus={corpus_size}")

    def embed_text(self, text: str) -> list[float]:
        """
        Generate embedding for text using FastText.

        Args:
            text: Input text

        Returns:
            Embedding vector of size config.vector_size

        Raises:
            RuntimeError: If model is not trained yet
        """
        # Transform text through pipeline (tokenize + phrases)
        tokens = self.pipeline.transform(text)

        # If no valid tokens, return deterministic fallback
        if not tokens:
            return self._generate_fallback_embedding(text)

        # If model not trained yet, this is a logic error
        if self.model is None:
            raise RuntimeError(
                f"FastText model not trained (have {len(self.document_cache)} docs, "
                f"need {self.config.min_documents}). Call _ensure_fitted() with valid text first."
            )

        # Get sentence vector (average of word vectors)
        # FastText handles OOV words via character n-grams
        embedding = self.model.wv.get_sentence_vector(tokens)

        # Normalize to unit vector
        norm = float(np.linalg.norm(embedding))
        if norm > 0:
            embedding = embedding / norm

        return cast("list[float]", embedding.tolist())

    def _generate_fallback_embedding(self, text: str) -> list[float]:
        """
        Generate deterministic fallback embedding for edge cases.

        Used when text has no valid tokens (empty, punctuation-only, emoji-only).
        Hash-based to ensure same text always gets same embedding.

        Args:
            text: Input text to hash

        Returns:
            Normalized embedding vector (deterministic per text)
        """
        # Hash text to get deterministic seed
        text_hash = hash(text)
        seed = abs(text_hash) % (2**31)

        # Generate deterministic embedding based on text hash
        rng = np.random.RandomState(seed)
        embedding = rng.randn(self.config.vector_size) * 0.1

        # Normalize to unit vector
        norm = float(np.linalg.norm(embedding))
        if norm > 0:
            embedding = embedding / norm

        return cast("list[float]", embedding.tolist())

    def openai_embeddings_handler(self, data: dict[str, object]) -> dict[str, object]:
        """
        Handle OpenAI /v1/embeddings request.

        Request format:
        {
            "model": "text-embedding-3-small",
            "input": "text to embed" or ["text1", "text2"]
        }

        Response format:
        {
            "object": "list",
            "data": [
                {
                    "object": "embedding",
                    "embedding": [0.1, 0.2, ...],
                    "index": 0
                }
            ],
            "model": "...",
            "usage": {...}
        }
        """
        input_text = data.get("input", "")
        model = data.get("model", "text-embedding-3-small")

        # Handle both single string and list of strings
        texts: list[str] = []
        if isinstance(input_text, str):
            texts = [input_text]
        elif isinstance(input_text, list):
            input_list = cast("list[object]", input_text)
            for item in input_list:
                if isinstance(item, str):
                    texts.append(item)

        # Pre-fit on all texts in batch (important for FastText vocabulary)
        if texts:
            self._ensure_fitted(texts)

        # Generate embeddings
        embeddings_data: list[dict[str, object]] = []
        total_tokens = 0

        for idx, text in enumerate(texts):
            embedding = self.embed_text(text)
            tokens = len(text.split()) if text else 0
            total_tokens += tokens

            embeddings_data.append(
                {
                    "object": "embedding",
                    "embedding": embedding,
                    "index": idx,
                }
            )

        return {
            "object": "list",
            "data": embeddings_data,
            "model": model,
            "usage": {
                "prompt_tokens": total_tokens,
                "total_tokens": total_tokens,
            },
        }


def _create_app(config: _FastTextConfig | None = None, preload_texts: list[str] | None = None) -> Flask:
    """Create Flask app with FastText embedding service.

    Args:
        config: FastText configuration (uses defaults if None)
        preload_texts: Optional list of texts to preload and train on at startup
    """
    app = Flask(__name__)
    service = _FastTextEmbeddingService(config=config)
    
    # Preload training data at startup if provided
    if preload_texts:
        service.preload_training_data(preload_texts)

    @app.route("/v1/embeddings", methods=["POST"])
    def embeddings() -> Response:
        """OpenAI-compatible embeddings endpoint."""
        data = request.get_json()
        response = service.openai_embeddings_handler(data)
        return jsonify(response)

    @app.route("/health", methods=["GET"])
    def health() -> Response:
        """Health check endpoint."""
        return jsonify(
            {
                "status": "ok",
                "engine": "fasttext",
                "vector_size": service.config.vector_size,
            }
        )

    @app.route("/reset", methods=["POST"])
    def reset() -> Response:
        """Reset service state (for testing)."""
        service.reset()
        return jsonify({"status": "reset", "message": "FastText service state cleared"})

    @app.route("/v1/reset", methods=["POST"])
    def reset_v1() -> Response:
        """Reset service state (OpenAI-compatible base path)."""
        service.reset()
        return jsonify({"status": "reset", "message": "FastText service state cleared"})

    # Register routes to avoid pyright unused warnings
    _ = embeddings, health, reset, reset_v1

    return app


class _EmbeddingServiceThread(threading.Thread):
    """Thread to run Flask app in background."""

    def __init__(
        self,
        port: int = 11434,
        embedding_dim: int = DEFAULT_VECTOR_SIZE,
        config: _FastTextConfig | None = None,
        preload_texts: list[str] | None = None,
    ) -> None:
        super().__init__(daemon=True)
        self.port = port
        # Use config if provided, otherwise create from embedding_dim
        self.config = config or _FastTextConfig(vector_size=embedding_dim)
        self.preload_texts = preload_texts
        self.app: Flask | None = None
        self.ready = threading.Event()

    def run(self) -> None:
        """Run Flask app."""
        self.app = _create_app(config=self.config, preload_texts=self.preload_texts)

        # Signal that server is ready
        self.ready.set()

        # Run with minimal logging
        self.app.run(
            host="127.0.0.1",
            port=self.port,
            debug=False,
            use_reloader=False,
            threaded=True,
        )


# Public type alias for return value (class itself is private)
EmbeddingServiceThread = _EmbeddingServiceThread


# Public API: only function to start embedding service
def start_embedding_service(
    port: int,
    vector_size: int = DEFAULT_VECTOR_SIZE,
    window: int = DEFAULT_WINDOW,
    min_count: int = DEFAULT_MIN_COUNT,
    epochs: int = DEFAULT_EPOCHS,
    bucket: int = DEFAULT_BUCKET,
    min_documents: int = DEFAULT_MIN_DOCUMENTS,
    preload_texts: list[str] | None = None,
) -> EmbeddingServiceThread:
    """Start FastText embedding service on specified port.
    
    This is the only public API for starting the embedding service.
    All internal classes are private and should not be used directly.
    
    Args:
        port: Port to run service on
        vector_size: Embedding vector size (default: 128)
        window: FastText window size (default: 3)
        min_count: Minimum word count (default: 1)
        epochs: Training epochs (default: 5)
        bucket: FastText bucket size (default: 10000)
        min_documents: Minimum documents for training (default: 1)
        preload_texts: Optional texts to preload and train on at startup
        
    Returns:
        Thread running the service (daemon thread, stops when main process ends)
    """
    config = _FastTextConfig(
        vector_size=vector_size,
        window=window,
        min_count=min_count,
        epochs=epochs,
        bucket=bucket,
        min_documents=min_documents,
    )
    thread = _EmbeddingServiceThread(port=port, config=config, preload_texts=preload_texts)
    thread.start()
    return thread


if __name__ == "__main__":
    # Run standalone for testing
    import sys

    port = int(sys.argv[1]) if len(sys.argv) > 1 else 11434
    vector_size = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_VECTOR_SIZE

    print(f"Starting FastText embedding service on port {port}")
    print(f"Vector size: {vector_size} (recommended: 64-300)")

    config = _FastTextConfig(vector_size=vector_size)
    app = _create_app(config=config)
    app.run(host="127.0.0.1", port=port, debug=True)
