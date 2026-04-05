"""Fixtures specific to integration tests.

Integration tests verify complete workflows with real database connections.
Common Protocol definitions are imported from tests.protocols.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow


@pytest.fixture
def sample_blocks_mutes(
    postgres_conn: Connection[TupleRow],
    sample_data_for_cli: dict[str, list[int]],
) -> dict[str, int]:
    """Insert sample blocks and mutes for incremental state tests.

    Requires sample_data_for_cli to be set up first (creates users and statuses).
    """
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO blocks (id, account_id, target_account_id)
            VALUES
                (10, 101, 102),
                (20, 102, 103)
            RETURNING id;
            """
        )
        block_ids = [row[0] for row in cur.fetchall()]
        cur.execute(
            """
            INSERT INTO mutes (id, account_id, target_account_id)
            VALUES
                (11, 101, 103),
                (21, 103, 101)
            RETURNING id;
            """
        )
        mute_ids = [row[0] for row in cur.fetchall()]
        postgres_conn.commit()

    return {"last_block_id": max(block_ids), "last_mute_id": max(mute_ids)}


@pytest.fixture
def sample_reblogs_replies(
    postgres_conn: Connection[TupleRow],
    sample_data_for_cli: dict[str, list[int]],
) -> dict[str, int]:
    """Insert sample reblogs and replies for REBLOGGED/REPLIED relationship tests.

    Requires sample_data_for_cli to be set up first.
    """
    with postgres_conn.cursor() as cur:
        # Create reblog statuses (status with reblog_of_id pointing to original post)
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, visibility, reblog_of_id)
            VALUES
                (100, 102, '', 0, 1),
                (101, 103, '', 0, 2)
            RETURNING id;
            """
        )
        reblog_ids = [row[0] for row in cur.fetchall()]

        # Create reply statuses (status with in_reply_to_id pointing to original post)
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, visibility, in_reply_to_id)
            VALUES
                (200, 101, 'Great post!', 0, 2),
                (201, 103, 'I agree!', 0, 3)
            RETURNING id;
            """
        )
        reply_ids = [row[0] for row in cur.fetchall()]
        postgres_conn.commit()

    return {
        "last_reblog_id": max(reblog_ids),
        "last_reply_id": max(reply_ids),
        "reblog_count": len(reblog_ids),
        "reply_count": len(reply_ids),
    }


def create_embedding_service_with_dim(port: int, vector_size: int) -> dict[str, str | int]:
    """Create embedding service with specific vector size.

    Helper function for vector size mismatch tests.
    """
    from tests.fasttext_embedding_service import start_embedding_service

    thread = start_embedding_service(port=port, vector_size=vector_size)
    thread.ready.wait(timeout=10)

    return {
        "api_base": f"http://127.0.0.1:{port}/v1",
        "port": port,
        "model": f"fasttext-{vector_size}",
    }
