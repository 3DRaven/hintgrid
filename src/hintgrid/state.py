"""State persistence for incremental loading using Neo4j Singleton Node."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient, Neo4jParameter, Neo4jValue

from hintgrid.utils.coercion import coerce_int, coerce_str

INITIAL_CURSOR = 0


@dataclass
class PipelineState:
    """Checkpoint state for incremental loading.

    Tracks:
    - Cursor positions for incremental data loading
    - Embedding configuration signature for detecting config changes
    """

    last_status_id: int = INITIAL_CURSOR
    last_favourite_id: int = INITIAL_CURSOR
    last_block_id: int = INITIAL_CURSOR
    last_mute_id: int = INITIAL_CURSOR
    last_reblog_id: int = INITIAL_CURSOR
    last_reply_id: int = INITIAL_CURSOR
    last_activity_account_id: int = INITIAL_CURSOR
    last_feed_user_id: int = INITIAL_CURSOR
    last_status_stats_id: int = INITIAL_CURSOR
    last_bookmark_id: int = INITIAL_CURSOR
    last_interaction_favourite_id: int = INITIAL_CURSOR
    last_interaction_status_id: int = INITIAL_CURSOR
    last_interaction_mention_id: int = INITIAL_CURSOR
    last_interaction_follow_id: int = INITIAL_CURSOR
    embedding_signature: str = ""  # "provider:model:dim" or empty for first run
    similarity_signature: str = ""  # "knn:K:threshold:T:recency:D" or empty for first run
    last_interests_rebuild_at: str = ""  # ISO datetime or empty for "never"

    @classmethod
    def from_dict(cls, data: Mapping[str, Neo4jValue]) -> PipelineState:
        return cls(
            last_status_id=coerce_int(data.get("last_status_id")),
            last_favourite_id=coerce_int(data.get("last_favourite_id")),
            last_block_id=coerce_int(data.get("last_block_id")),
            last_mute_id=coerce_int(data.get("last_mute_id")),
            last_reblog_id=coerce_int(data.get("last_reblog_id")),
            last_reply_id=coerce_int(data.get("last_reply_id")),
            last_activity_account_id=coerce_int(data.get("last_activity_account_id")),
            last_feed_user_id=coerce_int(data.get("last_feed_user_id")),
            last_status_stats_id=coerce_int(data.get("last_status_stats_id")),
            last_bookmark_id=coerce_int(data.get("last_bookmark_id")),
            last_interaction_favourite_id=coerce_int(data.get("last_interaction_favourite_id")),
            last_interaction_status_id=coerce_int(data.get("last_interaction_status_id")),
            last_interaction_mention_id=coerce_int(data.get("last_interaction_mention_id")),
            last_interaction_follow_id=coerce_int(data.get("last_interaction_follow_id")),
            embedding_signature=coerce_str(data.get("embedding_signature")),
            similarity_signature=coerce_str(data.get("similarity_signature")),
            last_interests_rebuild_at=coerce_str(data.get("last_interests_rebuild_at")),
        )

    def to_dict(self) -> dict[str, Neo4jValue]:
        return {
            "last_status_id": self.last_status_id,
            "last_favourite_id": self.last_favourite_id,
            "last_block_id": self.last_block_id,
            "last_mute_id": self.last_mute_id,
            "last_reblog_id": self.last_reblog_id,
            "last_reply_id": self.last_reply_id,
            "last_activity_account_id": self.last_activity_account_id,
            "last_feed_user_id": self.last_feed_user_id,
            "last_status_stats_id": self.last_status_stats_id,
            "last_bookmark_id": self.last_bookmark_id,
            "last_interaction_favourite_id": self.last_interaction_favourite_id,
            "last_interaction_status_id": self.last_interaction_status_id,
            "last_interaction_mention_id": self.last_interaction_mention_id,
            "last_interaction_follow_id": self.last_interaction_follow_id,
            "embedding_signature": self.embedding_signature,
            "similarity_signature": self.similarity_signature,
            "last_interests_rebuild_at": self.last_interests_rebuild_at,
        }


class StateStore:
    """Neo4j-based state storage using Singleton Node pattern."""

    def __init__(self, neo4j: Neo4jClient, state_id: str = "main") -> None:
        self._neo4j = neo4j
        self._state_id = state_id
        self._ensure_initialized()

    @property
    def state_id(self) -> str:
        """Get the state ID for this store."""
        return self._state_id

    def _ensure_initialized(self) -> None:
        """Create AppState node if it doesn't exist using APOC."""
        self._neo4j.execute(
            "CALL apoc.merge.node($labels, {id: $state_id}, "
            "{last_processed_status_id: $ic, "
            " last_processed_favourite_id: $ic, "
            " last_processed_block_id: $ic, "
            " last_processed_mute_id: $ic, "
            " last_processed_reblog_id: $ic, "
            " last_processed_reply_id: $ic, "
            " last_processed_activity_account_id: $ic, "
            " last_processed_feed_user_id: $ic, "
            " last_processed_status_stats_id: $ic, "
            " last_processed_bookmark_id: $ic, "
            " last_processed_interaction_favourite_id: $ic, "
            " last_processed_interaction_status_id: $ic, "
            " last_processed_interaction_mention_id: $ic, "
            " last_processed_interaction_follow_id: $ic, "
            " embedding_signature: '', "
            " similarity_signature: '', "
            " last_interests_rebuild_at: '', "
            " updated_at: timestamp()}, {}) "
            "YIELD node",
            {
                "labels": self._neo4j.labels_list("AppState"),
                "state_id": self._state_id,
                "ic": INITIAL_CURSOR,
            },
        )

    def load(self) -> PipelineState:
        """Load state from Neo4j AppState node."""
        rows = list(
            self._neo4j.execute_and_fetch_labeled(
                "MATCH (s:__label__ {id: $state_id}) "
                "RETURN "
                "  s.last_processed_status_id AS last_status_id, "
                "  s.last_processed_favourite_id AS last_favourite_id, "
                "  s.last_processed_block_id AS last_block_id, "
                "  s.last_processed_mute_id AS last_mute_id, "
                "  s.last_processed_reblog_id AS last_reblog_id, "
                "  s.last_processed_reply_id AS last_reply_id, "
                "  s.last_processed_activity_account_id AS last_activity_account_id, "
                "  s.last_processed_feed_user_id AS last_feed_user_id, "
                "  s.last_processed_status_stats_id AS last_status_stats_id, "
                "  s.last_processed_bookmark_id AS last_bookmark_id, "
                "  s.last_processed_interaction_favourite_id AS last_interaction_favourite_id, "
                "  s.last_processed_interaction_status_id AS last_interaction_status_id, "
                "  s.last_processed_interaction_mention_id AS last_interaction_mention_id, "
                "  s.last_processed_interaction_follow_id AS last_interaction_follow_id, "
                "  s.embedding_signature AS embedding_signature, "
                "  s.similarity_signature AS similarity_signature, "
                "  s.last_interests_rebuild_at AS last_interests_rebuild_at",
                {"label": "AppState"},
                {"state_id": self._state_id},
            )
        )
        if not rows:
            return PipelineState()

        row = rows[0]
        return PipelineState(
            last_status_id=coerce_int(row.get("last_status_id")),
            last_favourite_id=coerce_int(row.get("last_favourite_id")),
            last_block_id=coerce_int(row.get("last_block_id")),
            last_mute_id=coerce_int(row.get("last_mute_id")),
            last_reblog_id=coerce_int(row.get("last_reblog_id")),
            last_reply_id=coerce_int(row.get("last_reply_id")),
            last_activity_account_id=coerce_int(row.get("last_activity_account_id")),
            last_feed_user_id=coerce_int(row.get("last_feed_user_id")),
            last_status_stats_id=coerce_int(row.get("last_status_stats_id")),
            last_bookmark_id=coerce_int(row.get("last_bookmark_id")),
            last_interaction_favourite_id=coerce_int(row.get("last_interaction_favourite_id")),
            last_interaction_status_id=coerce_int(row.get("last_interaction_status_id")),
            last_interaction_mention_id=coerce_int(row.get("last_interaction_mention_id")),
            last_interaction_follow_id=coerce_int(row.get("last_interaction_follow_id")),
            embedding_signature=coerce_str(row.get("embedding_signature")),
            similarity_signature=coerce_str(row.get("similarity_signature")),
            last_interests_rebuild_at=coerce_str(row.get("last_interests_rebuild_at")),
        )

    def save(self, state: PipelineState) -> None:
        """Atomically update state in Neo4j AppState node."""
        params: dict[str, Neo4jParameter] = dict(state.to_dict())
        params["state_id"] = self._state_id
        self._neo4j.execute_labeled(
            "MATCH (s:__label__ {id: $state_id}) "
            "SET s.last_processed_status_id = $last_status_id, "
            "    s.last_processed_favourite_id = $last_favourite_id, "
            "    s.last_processed_block_id = $last_block_id, "
            "    s.last_processed_mute_id = $last_mute_id, "
            "    s.last_processed_reblog_id = $last_reblog_id, "
            "    s.last_processed_reply_id = $last_reply_id, "
            "    s.last_processed_activity_account_id = $last_activity_account_id, "
            "    s.last_processed_feed_user_id = $last_feed_user_id, "
            "    s.last_processed_status_stats_id = $last_status_stats_id, "
            "    s.last_processed_bookmark_id = $last_bookmark_id, "
            "    s.last_processed_interaction_favourite_id = $last_interaction_favourite_id, "
            "    s.last_processed_interaction_status_id = $last_interaction_status_id, "
            "    s.last_processed_interaction_mention_id = $last_interaction_mention_id, "
            "    s.last_processed_interaction_follow_id = $last_interaction_follow_id, "
            "    s.embedding_signature = $embedding_signature, "
            "    s.similarity_signature = $similarity_signature, "
            "    s.last_interests_rebuild_at = $last_interests_rebuild_at, "
            "    s.updated_at = timestamp() "
            "RETURN s",
            {"label": "AppState"},
            params,
        )
