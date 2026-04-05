from collections.abc import Callable, Coroutine, Sequence
from typing import Literal, TypedDict, overload


class EmbeddingItem(TypedDict):
    embedding: list[float]


class EmbeddingResponse(TypedDict):
    data: list[EmbeddingItem]


@overload
def embedding(
    *,
    model: str,
    input: Sequence[str],
    dimensions: int | None = ...,
    encoding_format: str | None = ...,
    timeout: int = ...,
    api_base: str | None = ...,
    api_version: str | None = ...,
    api_key: str | None = ...,
    api_type: str | None = ...,
    caching: bool = ...,
    user: str | None = ...,
    custom_llm_provider: str | None = ...,
    litellm_call_id: str | None = ...,
    logger_fn: Callable[[str], None] | None = ...,
    aembedding: Literal[False] = ...,
) -> EmbeddingResponse: ...


@overload
def embedding(
    *,
    model: str,
    input: Sequence[str],
    dimensions: int | None = ...,
    encoding_format: str | None = ...,
    timeout: int = ...,
    api_base: str | None = ...,
    api_version: str | None = ...,
    api_key: str | None = ...,
    api_type: str | None = ...,
    caching: bool = ...,
    user: str | None = ...,
    custom_llm_provider: str | None = ...,
    litellm_call_id: str | None = ...,
    logger_fn: Callable[[str], None] | None = ...,
    aembedding: Literal[True],
) -> Coroutine[None, None, EmbeddingResponse]: ...
