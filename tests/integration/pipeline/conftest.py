"""Shared fixtures for pipeline integration tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import redis as redis_lib

from hintgrid.clients.redis import RedisClient

if TYPE_CHECKING:
    from collections.abc import Generator

    from tests.conftest import DockerComposeInfo


@pytest.fixture
def hintgrid_redis(
    docker_compose: DockerComposeInfo,
    worker_num: int,
) -> Generator[RedisClient, None, None]:
    """RedisClient wrapper with worker DB isolation.

    Unlike the raw ``redis_client`` fixture, this returns
    ``hintgrid.clients.redis.RedisClient`` which is what
    production code (e.g. exporter) expects.
    """
    raw = redis_lib.Redis(
        host=docker_compose.redis_host,
        port=docker_compose.redis_port,
        db=worker_num,
        decode_responses=False,
    )
    raw.flushdb()
    client = RedisClient(
        raw,
        host=docker_compose.redis_host,
        port=docker_compose.redis_port,
    )
    yield client
    raw.flushdb()
    client.close()
