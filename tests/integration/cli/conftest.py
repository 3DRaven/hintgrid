"""Shared fixtures and helpers for CLI integration tests."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Protocol


from hintgrid import app as app_module
from hintgrid.config import HintGridSettings
from tests.parallel import parse_worker_number

if TYPE_CHECKING:

    import pytest
    from hintgrid.clients.neo4j import Neo4jClient
    from pathlib import Path
    from tests.conftest import DockerComposeInfo, EmbeddingServiceConfig


class RedisTestClient(Protocol):
    """Protocol for Redis client used in tests."""

    def exists(self, name: str) -> int: ...

    def zcard(self, name: str) -> int: ...

    def zrevrange(
        self, name: str, start: int, end: int, *, withscores: bool = False
    ) -> list[tuple[bytes | str, float]]: ...

    def zrange(
        self, name: str, start: int, end: int, *, withscores: bool = False
    ) -> list[tuple[bytes | str, float]]: ...

    def zadd(self, name: str, mapping: dict[str, float]) -> int: ...


def drop_index_if_exists(neo4j: Neo4jClient, idx_name: str) -> None:
    """Drop Neo4j index if it exists."""
    if not idx_name or not all(c.isalnum() or c == "_" for c in idx_name):
        return
    neo4j.execute_labeled(
        "DROP INDEX __name__ IF EXISTS",
        ident_map={"name": idx_name},
    )


def worker_schema(worker_id: str) -> str:
    """Get PostgreSQL schema name for worker."""
    if worker_id == "master":
        return "public"
    return f"test_{worker_id}"


def set_cli_env(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    fasttext_embedding_service: EmbeddingServiceConfig | None,
    log_file: Path,
    worker_id: str = "master",
    settings: HintGridSettings | None = None,
) -> HintGridSettings:
    """Set CLI environment variables with worker-specific isolation.

    Uses fixtures from conftest.py for cleanup via neo4j fixture.
    Does NOT create its own clients - relies on fixture-level cleanup.
    """
    resolved_worker_id = worker_id
    if resolved_worker_id == "master":
        resolved_worker_id = os.getenv("PYTEST_XDIST_WORKER", "master")
    worker_num = parse_worker_number(resolved_worker_id)

    if settings is None:
        settings = HintGridSettings()

    monkeypatch.setenv("HINTGRID_POSTGRES_HOST", docker_compose.postgres_host)
    monkeypatch.setenv("HINTGRID_POSTGRES_PORT", str(docker_compose.postgres_port))
    monkeypatch.setenv("HINTGRID_POSTGRES_DATABASE", docker_compose.postgres_db)
    monkeypatch.setenv("HINTGRID_POSTGRES_USER", docker_compose.postgres_user)
    monkeypatch.setenv("HINTGRID_POSTGRES_PASSWORD", docker_compose.postgres_password)
    monkeypatch.setenv("HINTGRID_POSTGRES_SCHEMA", worker_schema(resolved_worker_id))

    monkeypatch.setenv("HINTGRID_NEO4J_HOST", docker_compose.neo4j_host)
    monkeypatch.setenv("HINTGRID_NEO4J_PORT", str(docker_compose.neo4j_port))
    monkeypatch.setenv("HINTGRID_NEO4J_USERNAME", docker_compose.neo4j_user)
    monkeypatch.setenv("HINTGRID_NEO4J_PASSWORD", docker_compose.neo4j_password)
    monkeypatch.setenv(
        "HINTGRID_NEO4J_WORKER_LABEL", f"worker_{resolved_worker_id}"
    )

    monkeypatch.setenv("HINTGRID_REDIS_HOST", docker_compose.redis_host)
    monkeypatch.setenv("HINTGRID_REDIS_PORT", str(docker_compose.redis_port))
    # Use worker-specific Redis DB for isolation
    monkeypatch.setenv("HINTGRID_REDIS_DB", str(worker_num))

    monkeypatch.setenv("HINTGRID_LOG_FILE", str(log_file))
    monkeypatch.setenv("HINTGRID_LOG_LEVEL", "INFO")
    monkeypatch.setenv("HINTGRID_INTERESTS_MIN_FAVOURITES", "1")
    monkeypatch.setenv("HINTGRID_FEED_DAYS", "365")
    # Override min_count for small test datasets (default 10 is too high)
    monkeypatch.setenv("HINTGRID_FASTTEXT_MIN_COUNT", "1")
    # Set llm_dimensions to match FastText vector size
    monkeypatch.setenv("HINTGRID_LLM_DIMENSIONS", str(settings.fasttext_vector_size))

    if fasttext_embedding_service:
        monkeypatch.setenv("HINTGRID_LLM_PROVIDER", "openai")
        monkeypatch.setenv("HINTGRID_LLM_BASE_URL", fasttext_embedding_service["api_base"])
        monkeypatch.setenv("HINTGRID_LLM_MODEL", fasttext_embedding_service["model"])
        # LiteLLM requires OPENAI_API_KEY even for custom endpoints
        monkeypatch.setenv("OPENAI_API_KEY", "sk-fake-key-for-testing")

    result_settings = settings.model_copy(
        update={
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": worker_schema(resolved_worker_id),
            "neo4j_host": docker_compose.neo4j_host,
            "neo4j_port": docker_compose.neo4j_port,
            "neo4j_username": docker_compose.neo4j_user,
            "neo4j_password": docker_compose.neo4j_password,
            "neo4j_worker_label": f"worker_{resolved_worker_id}",
            "redis_host": docker_compose.redis_host,
            "redis_port": docker_compose.redis_port,
            "redis_db": worker_num,
            "log_file": str(log_file),
            "log_level": "INFO",
            "interests_min_favourites": 1,
            "feed_days": 365,
            "fasttext_min_count": 1,
            # Ensure vector dimensions match FastText embeddings
            "llm_dimensions": settings.fasttext_vector_size,
        }
    )
    if fasttext_embedding_service:
        result_settings = result_settings.model_copy(
            update={
                "llm_provider": "openai",
                "llm_base_url": fasttext_embedding_service["api_base"],
                "llm_model": fasttext_embedding_service["model"],
            }
        )

    return result_settings


def run_cli(monkeypatch: pytest.MonkeyPatch, args: list[str]) -> int:
    """Run CLI and return exit code.

    Typer raises SystemExit with the exit code, so we catch it.
    """
    monkeypatch.setattr("sys.argv", ["python", *args])
    try:
        app_module.main()
        return 0  # No SystemExit means success
    except SystemExit as exc:
        return int(exc.code) if exc.code is not None else 0


def create_embedding_service_with_dim(vector_size: int = 64) -> EmbeddingServiceConfig:
    """Create a FastText embedding service config with specific vector dimensions."""
    import threading
    from flask import Flask, request
    import socket

    app = Flask(__name__)
    app.config["TESTING"] = True
    embeddings_cache: dict[str, list[float]] = {}

    @app.route("/health", methods=["GET"])
    def health() -> tuple[dict[str, str], int]:
        return {"status": "ok"}, 200

    @app.route("/v1/embeddings", methods=["POST"])
    def embeddings() -> tuple[dict[str, object], int]:
        data = request.get_json()
        texts = data.get("input", [])
        if isinstance(texts, str):
            texts = [texts]

        result_data = []
        for i, text in enumerate(texts):
            if text in embeddings_cache:
                embedding = embeddings_cache[text]
            else:
                import hashlib
                hash_val = int(hashlib.md5(text.encode()).hexdigest(), 16)
                embedding = [(hash_val >> j) % 100 / 100.0 for j in range(vector_size)]
                embeddings_cache[text] = embedding

            result_data.append({"object": "embedding", "index": i, "embedding": embedding})

        return {
            "object": "list",
            "data": result_data,
            "model": f"fasttext-{vector_size}d",
            "usage": {"prompt_tokens": len(texts), "total_tokens": len(texts)},
        }, 200

    @app.route("/v1/reset", methods=["POST"])
    def reset() -> tuple[dict[str, str], int]:
        embeddings_cache.clear()
        return {"status": "reset"}, 200

    # Find free port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    # Start server in background thread
    server_thread = threading.Thread(
        target=lambda: app.run(host="127.0.0.1", port=port, threaded=True),
        daemon=True,
    )
    server_thread.start()

    # Wait for server to be ready
    import time
    for _ in range(50):
        try:
            import urllib.request
            urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=0.5)
            break
        except Exception:
            time.sleep(0.1)

    return {
        "api_base": f"http://127.0.0.1:{port}",
        "port": port,
        "model": f"fasttext-{vector_size}d",
    }
