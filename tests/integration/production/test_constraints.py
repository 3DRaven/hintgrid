"""Production-mode constraint tests that require exclusive Neo4j access.

These tests verify behaviour that only exists in production (no worker_label):
real uniqueness constraints, ``apoc.merge.node`` idempotency, and data
integrity under constraint enforcement.

Every test uses the ``exclusive_production_mode`` fixture which:
1. Pauses all other xdist workers.
2. Creates a Neo4j client with ``worker_label=None``.
3. Drops all constraints and data after the test.
"""
from __future__ import annotations
from typing import TYPE_CHECKING
import pytest
from neo4j.exceptions import ConstraintError
from hintgrid.pipeline.graph import ensure_graph_indexes, merge_posts
from hintgrid.utils.coercion import coerce_int, convert_batch_decimals
if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.config import HintGridSettings

@pytest.mark.integration
@pytest.mark.single_worker
def test_uniqueness_constraint_prevents_duplicates(exclusive_production_mode: Neo4jClient, settings: HintGridSettings) -> None:
    """Verify that real uniqueness constraints reject duplicate nodes.

    In production (no worker_label) ``ensure_graph_indexes`` creates
    ``UNIQUE`` constraints on ``User.id``, ``Post.id``, ``AppState.id``.
    A second ``CREATE`` with the same id must raise ``ConstraintError``.
    """
    neo4j = exclusive_production_mode
    prod_settings = settings.model_copy(update={'neo4j_worker_label': None})
    ensure_graph_indexes(neo4j, prod_settings)
    constraints = list(neo4j.execute_and_fetch('SHOW CONSTRAINTS YIELD name, type RETURN name, type'))
    unique_names = [str(c['name']) for c in constraints if str(c['type']) == 'UNIQUENESS']
    assert len(unique_names) >= 2, f'Expected at least User+Post uniqueness constraints, got: {unique_names}'
    neo4j.execute("CREATE (:User {id: 42, username: 'alice'})")
    with pytest.raises(ConstraintError):
        neo4j.execute("CREATE (:User {id: 42, username: 'alice_dup'})")
    rows = list(neo4j.execute_and_fetch('MATCH (u:User {id: 42}) RETURN count(u) AS cnt'))
    assert coerce_int(rows[0]['cnt']) == 1

@pytest.mark.integration
@pytest.mark.single_worker
def test_apoc_merge_node_cross_label_no_collision(exclusive_production_mode: Neo4jClient, settings: HintGridSettings) -> None:
    """Verify apoc.merge.node handles same id across different labels.

    In production, ``UserCommunity.id`` and ``Post.id`` can collide
    (e.g. both equal 10).  Without worker labels these are distinct
    label sets, so ``apoc.merge.node`` must create separate nodes
    without ``IndexEntryConflictException``.
    """
    neo4j = exclusive_production_mode
    prod_settings = settings.model_copy(update={'neo4j_worker_label': None})
    ensure_graph_indexes(neo4j, prod_settings)
    neo4j.execute("CALL apoc.merge.node(['Post'], {id: 10},   {text: 'hello', authorId: 1}, {}) YIELD node RETURN node")
    neo4j.execute("CALL apoc.merge.node(['UserCommunity'], {id: 10},   {size: 5}, {}) YIELD node RETURN node")
    posts = list(neo4j.execute_and_fetch('MATCH (p:Post {id: 10}) RETURN p.text AS text'))
    assert len(posts) == 1
    assert str(posts[0]['text']) == 'hello'
    communities = list(neo4j.execute_and_fetch('MATCH (uc:UserCommunity {id: 10}) RETURN uc.size AS size'))
    assert len(communities) == 1
    assert coerce_int(communities[0]['size']) == 5
    neo4j.execute("CALL apoc.merge.node(['Post'], {id: 10},   {}, {text: 'updated'}) YIELD node RETURN node")
    rows = list(neo4j.execute_and_fetch('MATCH (p:Post {id: 10}) RETURN count(p) AS cnt, p.text AS text'))
    assert coerce_int(rows[0]['cnt']) == 1, 'apoc.merge.node must upsert, not duplicate'
    assert str(rows[0]['text']) == 'updated'

@pytest.mark.integration
@pytest.mark.single_worker
def test_merge_posts_data_integrity_with_constraints(exclusive_production_mode: Neo4jClient, settings: HintGridSettings) -> None:
    """Verify merge_posts creates searchable data under real constraints.

    Uses the production ``merge_posts`` function (which internally calls
    ``apoc.merge.node``) with real uniqueness constraints active.
    Verifies that data is persisted, relationships are correct, and
    duplicate calls are idempotent.
    """
    neo4j = exclusive_production_mode
    prod_settings = settings.model_copy(update={'neo4j_worker_label': None})
    ensure_graph_indexes(neo4j, prod_settings)
    batch: list[dict[str, object]] = [{'id': 1, 'authorId': 100, 'text': 'First post about Python', 'language': 'en', 'embedding': [0.1, 0.2, 0.3], 'createdAt': '2025-01-01T00:00:00Z'}, {'id': 2, 'authorId': 100, 'text': 'Second post about Neo4j', 'language': 'en', 'embedding': [0.4, 0.5, 0.6], 'createdAt': '2025-01-02T00:00:00Z'}, {'id': 3, 'authorId': 200, 'text': 'Third post about Redis', 'language': 'en', 'embedding': [0.7, 0.8, 0.9], 'createdAt': '2025-01-03T00:00:00Z'}]
    merge_posts(neo4j, convert_batch_decimals(batch))
    post_rows = list(neo4j.execute_and_fetch('MATCH (p:Post) RETURN count(p) AS cnt'))
    assert coerce_int(post_rows[0]['cnt']) == 3
    user_rows = list(neo4j.execute_and_fetch('MATCH (u:User) RETURN count(u) AS cnt'))
    assert coerce_int(user_rows[0]['cnt']) == 2
    wrote_rows = list(neo4j.execute_and_fetch('MATCH (u:User)-[:WROTE]->(p:Post) RETURN u.id AS uid, p.id AS pid ORDER BY pid'))
    assert len(wrote_rows) == 3
    assert coerce_int(wrote_rows[0]['uid']) == 100
    assert coerce_int(wrote_rows[0]['pid']) == 1
    assert coerce_int(wrote_rows[2]['uid']) == 200
    assert coerce_int(wrote_rows[2]['pid']) == 3
    emb_rows = list(neo4j.execute_and_fetch('MATCH (p:Post {id: 1}) RETURN p.embedding AS emb'))
    emb_value = emb_rows[0]['emb']
    assert emb_value is not None
    assert isinstance(emb_value, list)
    assert len(emb_value) == 3
    updated_batch: list[dict[str, object]] = [{'id': 1, 'authorId': 100, 'text': 'First post UPDATED', 'language': 'en', 'embedding': [0.11, 0.22, 0.33], 'createdAt': '2025-01-01T00:00:00Z'}]
    merge_posts(neo4j, convert_batch_decimals(updated_batch))
    post_rows2 = list(neo4j.execute_and_fetch('MATCH (p:Post) RETURN count(p) AS cnt'))
    assert coerce_int(post_rows2[0]['cnt']) == 3
    emb_rows2 = list(neo4j.execute_and_fetch('MATCH (p:Post {id: 1}) RETURN p.embedding AS emb'))
    emb_value2 = emb_rows2[0]['emb']
    assert emb_value2 is not None
    assert isinstance(emb_value2, list)
    assert len(emb_value2) > 0
    assert isinstance(emb_value2[0], (int, float))
    assert abs(float(emb_value2[0]) - 0.11) < 0.01, 'Embedding should be updated after merge'