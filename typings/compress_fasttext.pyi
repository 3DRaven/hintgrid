"""Type stub for compress_fasttext library."""

from gensim.models import FastText

def quantize(model: FastText, qdim: int = 100) -> FastText: ...
