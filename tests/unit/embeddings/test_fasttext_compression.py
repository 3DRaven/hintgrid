"""Unit tests for compress-fasttext quantization helpers."""

from __future__ import annotations

from pathlib import Path  # noqa: TC003

import numpy as np
import pytest
from gensim.models import FastText

from hintgrid.embeddings.fasttext_compression import (
    load_fasttext_for_inference,
    quantize_fasttext_to_file,
)


@pytest.mark.unit
def test_quantize_fasttext_roundtrip_qdim_fits_vector_size(tmp_path: Path) -> None:
    """quantize_ft + save + CompressedFastTextKeyedVectors.load yields usable vectors."""
    vector_size = 32
    qdim = 16
    # compress-fasttext quantization requires more training rows than centroids (default 255)
    sentences = [[f"w{i}", f"w{i + 1}", f"w{i + 2}"] for i in range(300)]
    model = FastText(
        vector_size=vector_size,
        window=2,
        min_count=1,
        workers=1,
        sg=1,
        bucket=10000,
        word_ngrams=1,
        epochs=1,
        seed=42,
    )
    model.build_vocab(corpus_iterable=sentences)
    model.train(
        corpus_iterable=sentences,
        total_examples=len(sentences),
        epochs=5,
    )

    quantized_path = tmp_path / "fasttext_v1.q.bin"
    full_path = tmp_path / "fasttext_v1.bin"
    model.save(str(full_path))

    quantize_fasttext_to_file(model, quantized_path, qdim=qdim)
    assert quantized_path.is_file()

    loaded = load_fasttext_for_inference(quantized_path, full_path)
    vec = loaded.get_sentence_vector(["hello", "world"])
    assert isinstance(vec, np.ndarray)
    assert vec.shape == (vector_size,)
    assert np.isfinite(vec).all()
