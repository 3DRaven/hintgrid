from gensim.models.fasttext import FastTextKeyedVectors

def quantize_ft(
    ft: FastTextKeyedVectors,
    qdim: int = 100,
    centroids: int = 255,
    sample: object | None = None,
) -> FastTextKeyedVectors: ...
