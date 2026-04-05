"""Type stubs for neo4j library matching actual project usage.

This stub reflects the exact types used in the codebase:
- GraphDatabase.driver() receives only uri and auth (no **kwargs)
- Session.run() accepts str | LiteralString for validated dynamic queries
- All types are concrete (no Unknown, no Any, no object)
"""

from datetime import datetime
from typing import Iterator, LiteralString
from collections.abc import Mapping, Sequence

# Type alias for query strings - allows validated dynamic queries
# Safe because our code validates all dynamic parts via _SAFE_IDENT_RE
QueryString = LiteralString | str

# Neo4j parameter types - what can be passed to queries
# Based on actual usage in the codebase:
# - int: top_k, batch_size, max_iterations, recency_days, half_life_days, etc.
# - float: gamma, threshold, damping_factor, likes_weight, etc.
# - str: progress_tracker_id, batch_mode, cypherIterate, cypherAction
# - bool: parallel, didConverge
# - datetime: created_at, last_active (Neo4j accepts datetime objects and converts them in queries)
# - list[int]: dirty_uc_ids
# - list[float]: embeddings (Neo4j accepts list[float] for vector embeddings)
# - list[str]: node_labels
# - Sequence[Mapping[str, Neo4jParameter]]: batch parameters (e.g., batch: list[dict[str, float]])
#   Using Sequence and Mapping for covariance - allows list[dict[str, float]] to be passed
# - Mapping[str, Neo4jParameter]: nested params (iterateParams)
# - None: optional parameters
Neo4jParameter = (
    int
    | float
    | str
    | bool
    | datetime
    | list[int]
    | list[float]
    | list[str]
    | Sequence[Mapping[str, "Neo4jParameter"]]
    | Mapping[str, "Neo4jParameter"]
    | None
)

# Neo4j return value types - what can be returned from queries
# Based on actual usage in the codebase:
# - int: post_id, author_id, popularity, count, communityCount, etc.
# - float: interest_score, age_hours, pagerank, language_match, final_score, etc.
# - str: post_text, post_language, version
# - bool: didConverge
# - datetime: post_created_at (Neo4j returns datetime objects)
# - list[str]: errorMessages from apoc.periodic.iterate
# - list[int]: collect() results (e.g., collect(p.id), collect(u.id))
# - list[float]: embeddings (vector embeddings returned from Neo4j)
# - Sequence[Mapping[str, Neo4jValue]]: collections of dicts (e.g., collect({lang: lang, count: cnt}))
#   Using Sequence and Mapping for covariance
# - Mapping[str, Neo4jValue]: nested dictionaries (for complex nested structures)
# - None: optional fields
Neo4jValue = (
    int
    | float
    | str
    | bool
    | datetime
    | list[str]
    | list[int]
    | list[float]
    | Sequence[Mapping[str, "Neo4jValue"]]
    | Mapping[str, "Neo4jValue"]
    | None
)


class Record:
    """Neo4j record - can be indexed like a dict and has .data() method."""
    
    def __getitem__(self, key: str) -> Neo4jValue:
        """Access record field by key."""
        ...
    
    def data(self) -> dict[str, Neo4jValue]:
        """Get record data as dictionary."""
        ...


class Result:
    """Result from a query execution."""
    
    def consume(self) -> None:
        """Consume all remaining records without fetching them."""
        ...
    
    def single(self) -> Record | None:
        """Return single record or None if no records."""
        ...
    
    def __iter__(self) -> Iterator[Record]:
        """Iterate over records."""
        ...


class Session:
    """Neo4j session with relaxed query type for validated queries."""
    
    def run(
        self,
        query: QueryString,  # ← str | LiteralString для валидированных запросов
        parameters: Mapping[str, Neo4jParameter] | None = None,
    ) -> Result:
        """Execute Cypher query.
        
        Args:
            query: Cypher query string (LiteralString or validated str)
            parameters: Query parameters as mapping
        """
        ...
    
    def __enter__(self) -> "Session":
        """Context manager entry."""
        ...
    
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Context manager exit."""
        ...


class Driver:
    """Neo4j driver."""
    
    def session(
        self,
        *,
        database: str | None = None,
        fetch_size: int = ...,  # ← Конкретный тип, не **kwargs
    ) -> Session:
        """Create a new session.
        
        Args:
            database: Database name (optional)
            fetch_size: Buffer size for fetching records
        """
        ...
    
    def close(self) -> None:
        """Close the driver and release all resources."""
        ...


class GraphDatabase:
    """GraphDatabase factory with exact types matching usage."""
    
    @staticmethod
    def driver(
        uri: str,
        *,
        auth: tuple[str, str] | None = None,  # ← Конкретный тип, не **config: Unknown
    ) -> Driver:
        """Create a new driver instance.
        
        Args:
            uri: Neo4j connection URI (e.g., "bolt://localhost:7687")
            auth: Authentication tuple (username, password)
        """
        ...
