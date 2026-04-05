"""Leiden community detection stress tests for Neo4j GDS.

All tests use worker-isolated labels via neo4j.label() for parallel execution.
"""
import pytest
from hintgrid.clients.neo4j import Neo4jClient
from .conftest import COMMUNITY_SIZE, LARGE_GRAPH_NODES, MEDIUM_GRAPH_NODES, SMALL_GRAPH_NODES, gds_project_undirected

@pytest.mark.integration
def test_leiden_small_graph(neo4j: Neo4jClient) -> None:
    """
    Test Leiden community detection on small graph (10 nodes, 2 communities).
    """
    neo4j.label('User')
    graph_name = f'{neo4j.worker_label}-leiden-small' if neo4j.worker_label else 'leiden-small'
    for i in range(1, SMALL_GRAPH_NODES + 1):
        neo4j.execute_labeled('MERGE (u:__user__ {id: $id})', label_map={'user': 'User'}, params={'id': i})
    for i in range(1, COMMUNITY_SIZE + 1):
        for j in range(i + 1, COMMUNITY_SIZE + 1):
            neo4j.execute_labeled('\n                MATCH (u1:__user__ {id: $i}), (u2:__user__ {id: $j})\n                MERGE (u1)-[:FOLLOWS]->(u2)\n                MERGE (u2)-[:FOLLOWS]->(u1);\n                ', label_map={'user': 'User'}, params={'i': i, 'j': j})
    for i in range(COMMUNITY_SIZE + 1, SMALL_GRAPH_NODES + 1):
        for j in range(i + 1, SMALL_GRAPH_NODES + 1):
            neo4j.execute_labeled('\n                MATCH (u1:__user__ {id: $i}), (u2:__user__ {id: $j})\n                MERGE (u1)-[:FOLLOWS]->(u2)\n                MERGE (u2)-[:FOLLOWS]->(u1);\n                ', label_map={'user': 'User'}, params={'i': i, 'j': j})
    neo4j.execute_labeled('\n        MATCH (u1:__user__ {id: $size}), (u2:__user__ {id: $next})\n        MERGE (u1)-[:FOLLOWS]->(u2);\n        ', label_map={'user': 'User'}, params={'size': COMMUNITY_SIZE, 'next': COMMUNITY_SIZE + 1})
    gds_project_undirected(neo4j, graph_name, 'User')
    neo4j.execute_labeled("\n        CALL gds.leiden.stream('__graph_name__')\n        YIELD nodeId, communityId\n        WITH gds.util.asNode(nodeId) AS node, communityId\n        SET node.cluster_id = communityId;\n    ", ident_map={'graph_name': graph_name})
    count = neo4j.execute_and_fetch_labeled('MATCH (u:__user__) WHERE u.cluster_id IS NOT NULL RETURN count(u) AS count;', label_map={'user': 'User'})[0]['count']
    assert count == SMALL_GRAPH_NODES
    neo4j.execute_labeled("CALL gds.graph.drop('__graph_name__') YIELD graphName", ident_map={'graph_name': graph_name})
    print(f'✅ Leiden on small graph ({SMALL_GRAPH_NODES} nodes) completed successfully')

@pytest.mark.integration
def test_leiden_medium_graph(neo4j: Neo4jClient) -> None:
    """
    Test Leiden community detection on medium graph (50 nodes).

    This simulates a realistic social network size for batch processing.
    """
    neo4j.label('User')
    graph_name = f'{neo4j.worker_label}-leiden-medium' if neo4j.worker_label else 'leiden-medium'
    for i in range(1, MEDIUM_GRAPH_NODES + 1):
        neo4j.execute_labeled('MERGE (u:__user__ {id: $id})', label_map={'user': 'User'}, params={'id': i})
    for i in range(1, MEDIUM_GRAPH_NODES):
        for j in range(i + 1, min(i + 6, MEDIUM_GRAPH_NODES + 1)):
            neo4j.execute_labeled('\n                MATCH (u1:__user__ {id: $i}), (u2:__user__ {id: $j})\n                MERGE (u1)-[:FOLLOWS]->(u2);\n                ', label_map={'user': 'User'}, params={'i': i, 'j': j})
    gds_project_undirected(neo4j, graph_name, 'User')
    neo4j.execute_labeled("\n        CALL gds.leiden.stream('__graph_name__')\n        YIELD nodeId, communityId\n        WITH gds.util.asNode(nodeId) AS node, communityId\n        SET node.cluster_id = communityId;\n    ", ident_map={'graph_name': graph_name})
    count = neo4j.execute_and_fetch_labeled('MATCH (u:__user__) WHERE u.cluster_id IS NOT NULL RETURN count(u) AS count;', label_map={'user': 'User'})[0]['count']
    assert count == MEDIUM_GRAPH_NODES
    neo4j.execute_labeled("CALL gds.graph.drop('__graph_name__') YIELD graphName", ident_map={'graph_name': graph_name})
    print(f'✅ Leiden on medium graph ({MEDIUM_GRAPH_NODES} nodes) completed successfully')

@pytest.mark.integration
def test_leiden_large_graph(neo4j: Neo4jClient) -> None:
    """
    Test Leiden community detection on large graph (100 nodes).

    This is the upper bound for single-batch Leiden clustering.
    For larger graphs, consider partitioning or incremental approaches.
    """
    neo4j.label('User')
    graph_name = f'{neo4j.worker_label}-leiden-large' if neo4j.worker_label else 'leiden-large'
    for i in range(1, LARGE_GRAPH_NODES + 1):
        neo4j.execute_labeled('MERGE (u:__user__ {id: $id})', label_map={'user': 'User'}, params={'id': i})
    edges_created = 0
    for i in range(1, LARGE_GRAPH_NODES):
        for j in range(i + 1, min(i + 4, LARGE_GRAPH_NODES + 1)):
            neo4j.execute_labeled('\n                MATCH (u1:__user__ {id: $i}), (u2:__user__ {id: $j})\n                MERGE (u1)-[:FOLLOWS]->(u2);\n                ', label_map={'user': 'User'}, params={'i': i, 'j': j})
            edges_created += 1
    print(f'   Created {edges_created} edges')
    gds_project_undirected(neo4j, graph_name, 'User')
    neo4j.execute_labeled("\n        CALL gds.leiden.stream('__graph_name__')\n        YIELD nodeId, communityId\n        WITH gds.util.asNode(nodeId) AS node, communityId\n        SET node.cluster_id = communityId;\n    ", ident_map={'graph_name': graph_name})
    count = neo4j.execute_and_fetch_labeled('MATCH (u:__user__) WHERE u.cluster_id IS NOT NULL RETURN count(u) AS count;', label_map={'user': 'User'})[0]['count']
    assert count == LARGE_GRAPH_NODES
    num_communities = neo4j.execute_and_fetch_labeled('MATCH (u:__user__) RETURN count(DISTINCT u.cluster_id) AS count;', label_map={'user': 'User'})[0]['count']
    neo4j.execute_labeled("CALL gds.graph.drop('__graph_name__') YIELD graphName", ident_map={'graph_name': graph_name})
    print(f'✅ Leiden on large graph ({LARGE_GRAPH_NODES} nodes) completed successfully')
    print(f'   Found {num_communities} communities')