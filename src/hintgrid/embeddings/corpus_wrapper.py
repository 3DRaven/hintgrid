"""Corpus wrappers for Gensim training with progress tracking.

Provides two corpus iterators for Gensim's build_vocab() and train():
- CachedPhrasedCorpus: reads from disk cache (fast, avoids PG + tokenization)
- _PhrasedCorpusWrapper: reads from PostgresCorpus (legacy, for non-cached usage)

Both apply Phraser transformation and support Rich progress tracking
with throttled updates to avoid console bottlenecks.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rich.progress import Progress, TaskID

    from hintgrid.clients.postgres import PostgresCorpus
    from hintgrid.embeddings.text_pipeline import TextPipeline

__all__ = ["CachedPhrasedCorpus", "PhrasedCorpusWrapper"]

# How often to flush progress updates to Rich console
# Prevents console rendering from becoming a bottleneck
_PROGRESS_UPDATE_INTERVAL = 100


class CachedPhrasedCorpus:
    """Reads tokenized corpus from disk cache and applies phraser.

    Avoids re-reading from PostgreSQL and re-tokenizing for each
    training pass (vocab build + N epochs). Reads from a simple
    space-separated text file where each line is one document.

    This is the primary corpus wrapper used during training.
    """

    def __init__(
        self,
        cache_path: Path,
        pipeline: TextPipeline,
        progress: Progress | None = None,
        task: TaskID | None = None,
        total_epochs: int = 1,
        min_count: int = 1,
        is_vocab_build: bool = False,
    ) -> None:
        self._cache_path = cache_path
        self._pipeline = pipeline
        self._progress = progress
        self._task = task
        self._total_epochs = total_epochs
        self._current_epoch = 0
        self._min_count = min_count
        self._is_vocab_build = is_vocab_build

    def __iter__(self) -> Iterator[list[str]]:
        """Yield phrased documents from disk cache."""
        self._current_epoch += 1

        # Update description at epoch start (for multi-epoch training)
        if self._progress is not None and self._task is not None and self._total_epochs > 1:
            self._progress.update(
                self._task,
                description=(
                    f"[cyan]Training epoch {self._current_epoch}/{self._total_epochs}[/cyan]"
                ),
            )

        counter = 0
        with open(self._cache_path, encoding="utf-8") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue
                tokens = line.split(" ")

                # Apply phraser if available
                if self._pipeline.phraser is not None:
                    yield list(self._pipeline.phraser[tokens])
                else:
                    yield tokens

                counter += 1
                if (
                    self._progress is not None
                    and self._task is not None
                    and counter % _PROGRESS_UPDATE_INTERVAL == 0
                ):
                    self._progress.advance(self._task, _PROGRESS_UPDATE_INTERVAL)

        # Flush remaining progress
        remaining = counter % _PROGRESS_UPDATE_INTERVAL
        if remaining > 0 and self._progress is not None and self._task is not None:
            self._progress.advance(self._task, remaining)

        # After corpus exhausted: signal post-iteration processing phase
        if self._is_vocab_build and self._progress is not None and self._task is not None:
            self._progress.update(
                self._task,
                description=(
                    f"[yellow]⏳ Preparing vocab & initializing weights "
                    f"(min_count={self._min_count})...[/yellow]"
                ),
            )


class PhrasedCorpusWrapper:
    """Wrapper that applies phraser to streaming corpus with progress tracking.

    This allows Gensim to iterate multiple times over the corpus
    (for multiple epochs) while applying phrase detection.
    Optionally tracks iteration progress via Rich Progress bar.

    Progress updates are throttled to avoid Rich console rendering bottleneck.
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
        if self._progress is not None and self._task is not None and self._total_epochs > 1:
            self._progress.update(
                self._task,
                description=(
                    f"[cyan]Training epoch {self._current_epoch}/{self._total_epochs}[/cyan]"
                ),
            )

        counter = 0
        for tokens in self._corpus:
            if tokens:
                # Apply phraser if available
                if self._pipeline.phraser is not None:
                    yield list(self._pipeline.phraser[tokens])
                else:
                    yield tokens

                # Throttled progress update
                counter += 1
                if (
                    self._progress is not None
                    and self._task is not None
                    and counter % _PROGRESS_UPDATE_INTERVAL == 0
                ):
                    self._progress.advance(self._task, _PROGRESS_UPDATE_INTERVAL)

        # Flush remaining progress
        remaining = counter % _PROGRESS_UPDATE_INTERVAL
        if remaining > 0 and self._progress is not None and self._task is not None:
            self._progress.advance(self._task, remaining)

        # After corpus exhausted: signal post-iteration processing phase
        # (build_vocab continues with prepare_vocab + prepare_weights internally)
        if self._is_vocab_build and self._progress is not None and self._task is not None:
            self._progress.update(
                self._task,
                description=(
                    f"[yellow]⏳ Preparing vocab & initializing weights "
                    f"(min_count={self._min_count})...[/yellow]"
                ),
            )
