"""Model persistence: load, save, delete, and state management.

Handles FastText model versioning on disk and state tracking in Neo4j.
Supports two loading modes:
- Inference: loads compact Phraser + quantized FastText (if available)
- Training: loads full Phrases + full FastText (needed for incremental training)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path  # noqa: TC003
from typing import TYPE_CHECKING

from gensim.models import FastText
from gensim.models.phrases import Phraser, Phrases

from hintgrid.embeddings.fasttext_compression import (
    load_fasttext_for_inference,
    quantize_fasttext_to_file,
)

if TYPE_CHECKING:
    from gensim.models.fasttext import FastTextKeyedVectors

    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.config import HintGridSettings
    from hintgrid.embeddings.text_pipeline import TextPipeline

from hintgrid.utils.coercion import coerce_int

logger = logging.getLogger(__name__)

# State node constants
STATE_NODE_ID = "main"
INITIAL_VERSION = 0
VERSION_INCREMENT = 1



@dataclass
class FastTextState:
    """State stored in Neo4j for model versioning."""

    version: int
    last_trained_post_id: int
    vocab_size: int
    corpus_size: int


def load_models(
    model_path: Path,
    pipeline: TextPipeline,
    version: int,
    for_training: bool = False,
) -> FastText | FastTextKeyedVectors | None:
    """Load models from disk for given version.

    Two loading modes:
    - Inference mode (default): Load only Phraser (compact, fast)
    - Training mode: Load Phrases and derive Phraser (needed for add_vocab)

    Args:
        model_path: Directory containing model files
        pipeline: TextPipeline to populate with loaded phraser/phrases
        version: Model version to load
        for_training: If True, load Phrases for incremental training

    Returns:
        Loaded FastText or quantized KeyedVectors for inference, or None on failure
    """
    phrases_path = model_path / f"phrases_v{version}.pkl"
    phraser_path = model_path / f"phraser_v{version}.pkl"
    fasttext_path = model_path / f"fasttext_v{version}.bin"

    if not fasttext_path.exists():
        logger.warning("FastText model file not found for version %d", version)
        return None

    try:
        if for_training:
            if not phrases_path.exists():
                logger.warning(
                    "Phrases file not found for training mode, version %d",
                    version,
                )
                return None
            pipeline.phrases = Phrases.load(str(phrases_path))
            pipeline.phraser = Phraser(pipeline.phrases)
        else:
            if not _load_phraser_for_inference(pipeline, phraser_path, phrases_path, version):
                return None

        model = _load_fasttext_model(model_path, fasttext_path, version, for_training)
        logger.info("Loaded models version %d (training=%s)", version, for_training)
        return model
    except Exception as e:
        logger.error("Failed to load models: %s", e)
        return None


def _load_phraser_for_inference(
    pipeline: TextPipeline,
    phraser_path: Path,
    phrases_path: Path,
    version: int,
) -> bool:
    """Load Phraser for inference mode (compact, no frequency counters)."""
    if phraser_path.exists():
        pipeline.phraser = Phraser.load(str(phraser_path))
        pipeline.phrases = None
        logger.debug("Loaded Phraser for inference, version %d", version)
        return True
    if phrases_path.exists():
        logger.warning(
            "Phraser missing, loading Phrases as fallback, version %d",
            version,
        )
        pipeline.phrases = Phrases.load(str(phrases_path))
        pipeline.phraser = Phraser(pipeline.phrases)
        pipeline.phrases = None
        return True
    logger.warning("Neither Phraser nor Phrases file found for version %d", version)
    return False


def _load_fasttext_model(
    model_path: Path,
    fasttext_path: Path,
    version: int,
    for_training: bool,
) -> FastText | FastTextKeyedVectors:
    """Load FastText model (full for training, quantized KeyedVectors for inference)."""
    if for_training:
        model: FastText = FastText.load(str(fasttext_path), mmap="r")
        return model

    quantized_path = model_path / f"fasttext_v{version}.q.bin"
    return load_fasttext_for_inference(quantized_path, fasttext_path)


def save_models(
    model_path: Path,
    pipeline: TextPipeline,
    model: FastText | None,
    version: int,
    settings: HintGridSettings,
) -> None:
    """Save models to disk with version suffix.

    Saves both Phrases (for future incremental training) and Phraser
    (for compact inference). Phrases is larger but needed for add_vocab().
    """
    phrases_path = model_path / f"phrases_v{version}.pkl"
    phraser_path = model_path / f"phraser_v{version}.pkl"
    fasttext_path = model_path / f"fasttext_v{version}.bin"

    if pipeline.phrases is not None:
        pipeline.phrases.save(str(phrases_path))

    if pipeline.phraser is not None:
        pipeline.phraser.save(str(phraser_path))

    if model is not None:
        model.save(str(fasttext_path))

        if settings.fasttext_quantize:
            _quantize_model(model, model_path, version, settings)

    logger.info("Saved models version %d to %s", version, model_path)


def _quantize_model(
    model: FastText,
    model_path: Path,
    version: int,
    settings: HintGridSettings,
) -> None:
    """Quantize model for inference (10-50x size reduction)."""
    try:
        quantized_path = model_path / f"fasttext_v{version}.q.bin"
        logger.info(
            "Quantizing model (qdim=%d) for version %d...",
            settings.fasttext_quantize_qdim,
            version,
        )
        quantize_fasttext_to_file(
            model,
            quantized_path,
            settings.fasttext_quantize_qdim,
        )
        logger.info("Saved quantized model version %d to %s", version, quantized_path)
    except ImportError:
        logger.warning(
            "compress-fasttext not available, skipping quantization. "
            "Install with: pip install compress-fasttext"
        )
    except Exception as e:
        logger.warning("Failed to quantize model: %s", e)


def delete_model_files(model_path: Path, version: int) -> None:
    """Delete model files for given version."""
    patterns = [
        f"phrases_v{version}.pkl",
        f"phraser_v{version}.pkl",
        f"fasttext_v{version}.bin",
        f"fasttext_v{version}.q.bin",
        f"fasttext_v{version}.bin.wv.vectors_ngrams.npy",
    ]

    for name in patterns:
        path = model_path / name
        if path.exists():
            try:
                path.unlink()
                logger.debug("Deleted %s", path)
            except Exception as e:
                logger.warning("Failed to delete %s: %s", path, e)


def ensure_state_node(neo4j: Neo4jClient) -> None:
    """Ensure FastTextState node exists in Neo4j using APOC."""
    neo4j.execute(
        "CALL apoc.merge.node($labels, {id: $id}, "
        "{version: $version, last_trained_post_id: 0, "
        " vocab_size: 0, corpus_size: 0, "
        " updated_at: timestamp()}, {}) "
        "YIELD node",
        {
            "labels": neo4j.labels_list("FastTextState"),
            "id": STATE_NODE_ID,
            "version": INITIAL_VERSION,
        },
    )


def load_state(neo4j: Neo4jClient) -> FastTextState:
    """Load state from Neo4j."""
    rows = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (s:__label__ {id: $id}) "
            "RETURN s.version AS version, "
            "       s.last_trained_post_id AS last_trained_post_id, "
            "       s.vocab_size AS vocab_size, "
            "       s.corpus_size AS corpus_size",
            {"label": "FastTextState"},
            {"id": STATE_NODE_ID},
        )
    )

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


def save_state(neo4j: Neo4jClient, state: FastTextState) -> None:
    """Save state to Neo4j."""
    neo4j.execute_labeled(
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
