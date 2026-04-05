"""compress-fasttext integration: quantize FastText KeyedVectors for inference."""

from __future__ import annotations

import logging
from pathlib import Path  # noqa: TC003
from typing import TYPE_CHECKING, cast

from gensim.models import FastText

if TYPE_CHECKING:
    from gensim.models.fasttext import FastTextKeyedVectors

logger = logging.getLogger(__name__)


def quantize_fasttext_to_file(model: FastText, quantized_path: Path, qdim: int) -> None:
    """Product-quantize KeyedVectors and save for inference-only loading."""
    from compress_fasttext import quantize_ft

    quantized = quantize_ft(model.wv, qdim=qdim)
    quantized.save(str(quantized_path))


def load_fasttext_for_inference(
    quantized_path: Path,
    full_path: Path,
) -> FastText | FastTextKeyedVectors:
    """Load quantized KeyedVectors if present, else full FastText from disk."""
    if quantized_path.exists():
        try:
            from compress_fasttext.models import CompressedFastTextKeyedVectors

            loaded = CompressedFastTextKeyedVectors.load(str(quantized_path))
            logger.debug("Loaded CompressedFastTextKeyedVectors from %s", quantized_path)
            return cast("FastTextKeyedVectors", loaded)
        except Exception as e:
            logger.warning(
                "Failed to load quantized model as CompressedFastTextKeyedVectors: %s",
                e,
            )
            try:
                return FastText.load(str(quantized_path), mmap="r")
            except Exception as e2:
                logger.warning(
                    "Failed to load quantized file as FastText, using full model: %s",
                    e2,
                )
    return FastText.load(str(full_path), mmap="r")
