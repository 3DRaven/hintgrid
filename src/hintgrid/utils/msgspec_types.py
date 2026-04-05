"""Base msgspec Struct types for type-safe data validation.

This module provides base Struct types for all data coming from external sources
(PostgreSQL, Neo4j, JSON, Redis) to ensure zero-overhead type validation at boundaries.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING

import msgspec

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jParameter, Neo4jValue


# Base types for PostgreSQL rows
class PostgresRow(msgspec.Struct):
    """Base type for PostgreSQL row data."""

    pass


# Base types for Neo4j records
class Neo4jRecord(msgspec.Struct):
    """Base type for Neo4j record data."""

    pass


# Specialized PostgreSQL types
class CorpusRow(msgspec.Struct):
    """Row from corpus query (id, text)."""

    id: int
    text: str


class StatusStatsRow(msgspec.Struct):
    """Row from status statistics query."""

    max_id: int | None
    max_date: datetime | None
    total_count: int


# Specialized Neo4j types
class Neo4jCountResult(msgspec.Struct):
    """Result from COUNT query."""

    count: int


class Neo4jTableStats(msgspec.Struct):
    """Table statistics from Neo4j."""

    max_id: int | None
    max_date: datetime | None
    total_count: int


# Types for JSON structures
class JsonDict(msgspec.Struct):
    """Base type for JSON dictionary structures."""

    pass


# Helper types for type conversion
class IntValue(msgspec.Struct):
    """Wrapper for integer validation."""

    value: int


class FloatValue(msgspec.Struct):
    """Wrapper for float validation."""

    value: float


class StrValue(msgspec.Struct):
    """Wrapper for string validation."""

    value: str


# Protocol for Decimal-like types (for PostgreSQL Decimal conversion)
class DecimalLike(msgspec.Struct):
    """Protocol-like Struct for Decimal conversion."""

    pass
