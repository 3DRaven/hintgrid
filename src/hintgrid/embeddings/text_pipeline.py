"""Text preprocessing pipeline with phrase detection.

Combines:
- NLTK TweetTokenizer for social media text (emojis, mentions, hashtags)
- Gensim Phrases/Phraser for bigram detection (new + york -> new_york)

Optimizations:
- Batched add_vocab() calls (10K docs per batch) to reduce Python call overhead
- Optional disk caching of tokenized corpus for multi-pass training
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import IO, TYPE_CHECKING

from gensim.models.phrases import Phraser, Phrases
from nltk.tokenize import TweetTokenizer

if TYPE_CHECKING:
    from hintgrid.clients.postgres import PostgresCorpus

from hintgrid.cli.console import create_batch_progress

logger = logging.getLogger(__name__)

# Batch size for Phrases.add_vocab() calls to reduce Python function call overhead
_PHRASE_BATCH_SIZE = 10_000


class TextPipeline:
    """Text preprocessing pipeline with phrase detection.

    Combines:
    - TweetTokenizer: handles social media text (emojis, mentions, hashtags)
    - Phraser: detects and joins bigrams (new + york -> new_york)

    Implements TokenizerProtocol for use with PostgresCorpus.
    """

    # How often to refresh vocab size in progress description
    _VOCAB_DISPLAY_INTERVAL = 5_000

    def __init__(self) -> None:
        # TweetTokenizer for social media:
        # - preserve_case=False: lowercase everything
        # - strip_handles=True: remove @mentions
        # - reduce_len=True: "waaaaaay" -> "waaay"
        # Note: reduce_len uses regex internally and is slow.
        # Consider disabling if tokenization is a bottleneck,
        # or switching to a Rust-based tokenizer (e.g. HuggingFace tokenizers).
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

    def learn_phrases_from_stream(
        self,
        corpus: PostgresCorpus,
        min_count: int = 5,
        cache_path: Path | None = None,
    ) -> int:
        """Learn phrase patterns from streaming corpus.

        Uses batched add_vocab() for efficiency (10K docs per call).
        Optionally writes tokenized documents to disk cache for reuse
        in subsequent training passes (vocab build + epochs).

        Args:
            corpus: Streaming PostgresCorpus iterator
            min_count: Minimum count for phrase detection
            cache_path: If provided, write tokenized docs for later reuse

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
            corpus_total = corpus.total_count() or None

            cache_file = open(cache_path, "w", encoding="utf-8") if cache_path else None
            try:
                doc_count = self._stream_phrases(corpus, corpus_total, cache_file, "Learning")
            finally:
                if cache_file:
                    cache_file.close()

            if doc_count > 1:
                self.phraser = Phraser(self.phrases)
        finally:
            gensim_logger.setLevel(original_level)

        return doc_count

    def update_phrases_from_stream(
        self,
        corpus: PostgresCorpus,
        cache_path: Path | None = None,
    ) -> int:
        """Incrementally update phrase patterns from streaming corpus.

        Uses batched add_vocab() for efficiency (10K docs per call).
        Optionally writes tokenized documents to disk cache for reuse.

        Args:
            corpus: Streaming PostgresCorpus iterator
            cache_path: If provided, write tokenized docs for later reuse

        Returns:
            Number of documents processed
        """
        if self.phrases is None:
            return self.learn_phrases_from_stream(corpus, cache_path=cache_path)

        # Suppress gensim INFO logs during phrase update
        gensim_logger = logging.getLogger("gensim")
        original_level = gensim_logger.level
        gensim_logger.setLevel(logging.WARNING)

        try:
            corpus_total = corpus.total_count() or None

            cache_file = open(cache_path, "w", encoding="utf-8") if cache_path else None
            try:
                doc_count = self._stream_phrases(corpus, corpus_total, cache_file, "Updating")
            finally:
                if cache_file:
                    cache_file.close()

            self.phraser = Phraser(self.phrases)
        finally:
            gensim_logger.setLevel(original_level)

        return doc_count

    def _stream_phrases(
        self,
        corpus: PostgresCorpus,
        corpus_total: int | None,
        cache_file: IO[str] | None,
        action: str,
    ) -> int:
        """Stream corpus, learn phrases in batches, optionally cache to disk.

        Args:
            corpus: Streaming corpus iterator
            corpus_total: Total document count for progress bar
            cache_file: Optional file handle to write tokenized docs
            action: Description for progress bar ("Learning" or "Updating")

        Returns:
            Number of documents processed
        """
        if self.phrases is None:
            return 0

        doc_count = 0
        batch: list[list[str]] = []
        last_vocab_display = 0

        with create_batch_progress(corpus_total) as progress:
            task = progress.add_task(f"[cyan]{action} phrases[/cyan]", total=corpus_total)

            for tokens in corpus:
                if tokens:
                    batch.append(tokens)
                    doc_count += 1

                    # Write to disk cache for reuse in vocab build + training
                    if cache_file is not None:
                        cache_file.write(" ".join(tokens) + "\n")

                    if len(batch) >= _PHRASE_BATCH_SIZE:
                        self.phrases.add_vocab(batch)
                        progress.advance(task, len(batch))
                        batch = []

                        # Periodically show vocab size
                        if doc_count - last_vocab_display >= self._VOCAB_DISPLAY_INTERVAL:
                            last_vocab_display = doc_count
                            vocab_size = len(self.phrases.vocab)
                            progress.update(
                                task,
                                description=(
                                    f"[cyan]{action} phrases[/cyan] "
                                    f"[dim](vocab: {vocab_size:,})[/dim]"
                                ),
                            )

            # Flush remaining batch
            if batch:
                self.phrases.add_vocab(batch)
                progress.advance(task, len(batch))

            # Show final vocab in description
            vocab_size = len(self.phrases.vocab) if self.phrases else 0
            progress.update(
                task,
                description=(
                    f"[green]✓[/green] {action} phrases from {doc_count:,} "
                    f"docs (vocab: {vocab_size:,})"
                ),
                completed=corpus_total,
            )

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
