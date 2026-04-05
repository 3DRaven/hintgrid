"""FastRP embedding tests for Neo4j GDS."""
import pytest
from hintgrid.clients.neo4j import Neo4jClient
from .conftest import EMBEDDING_DIM_SMALL, MINIMAL_GRAPH_NODES, SMALL_GRAPH_EDGES, SMALL_GRAPH_NODES, gds_project, gds_project_undirected

@pytest.mark.integration
@pytest.mark.smoke
def test_fastRP_availability(neo4j: Neo4jClient) -> None:
    """Test FastRP availability in Neo4j GDS Community."""
    user_label = neo4j.label('User')
    graph_name = f'{neo4j.worker_label}-fastrp-avail' if neo4j.worker_label else 'fastrp-avail'
    neo4j.execute_labeled('CREATE (u1:__user__ {id: 1}), (u2:__user__ {id: 2})', label_map={'user': 'User'})
    neo4j.execute_labeled('MATCH (u1:__user__ {id: 1}), (u2:__user__ {id: 2}) CREATE (u1)-[:FOLLOWS]->(u2)', label_map={'user': 'User'})
    gds_project(neo4j, graph_name, user_label)
    result = list(neo4j.execute_and_fetch_labeled("\n        CALL gds.fastRP.stream('__graph_name__', {\n            embeddingDimension: $dim,\n            iterationWeights: [0.0, 1.0, 1.0]\n        })\n        YIELD nodeId, embedding\n        RETURN count(nodeId) AS count\n    ", ident_map={'graph_name': graph_name}, params={'dim': EMBEDDING_DIM_SMALL}))
    assert result[0]['count'] == MINIMAL_GRAPH_NODES
    print(f"✅ FastRP works: {result[0]['count']} nodes processed")
    neo4j.execute_labeled("CALL gds.graph.drop('__graph_name__') YIELD graphName", ident_map={'graph_name': graph_name})

@pytest.mark.integration
@pytest.mark.smoke
def test_neo4j_fastRP_availability(neo4j: Neo4jClient) -> None:
    """Test FastRP availability with parametrized query."""
    user_label = neo4j.label('User')
    graph_name = f'{neo4j.worker_label}-fastrp-test' if neo4j.worker_label else 'fastrp-test'
    neo4j.execute_labeled('CREATE (u1:__user__ {id: 1}), (u2:__user__ {id: 2})', label_map={'user': 'User'})
    neo4j.execute_labeled('MATCH (u1:__user__ {id: 1}), (u2:__user__ {id: 2}) CREATE (u1)-[:FOLLOWS]->(u2)', label_map={'user': 'User'})
    gds_project(neo4j, graph_name, user_label)
    result = neo4j.execute_and_fetch_labeled("\n        CALL gds.fastRP.stream('__graph_name__', {\n            embeddingDimension: $dim,\n            iterationWeights: [0.0, 1.0, 1.0]\n        })\n        YIELD nodeId, embedding\n        RETURN count(nodeId) AS count\n    ", ident_map={'graph_name': graph_name}, params={'dim': EMBEDDING_DIM_SMALL})
    count = result[0]['count']
    assert count == MINIMAL_GRAPH_NODES
    print(f'✅ Neo4j GDS FastRP works: {count} nodes processed')
    neo4j.execute_labeled("CALL gds.graph.drop('__graph_name__') YIELD graphName", ident_map={'graph_name': graph_name})

@pytest.mark.integration
@pytest.mark.smoke
def test_fastRP_simple_graph(neo4j: Neo4jClient) -> None:
    """Test FastRP on simple FOLLOWS graph."""
    user_label = neo4j.label('User')
    graph_name = f'{neo4j.worker_label}-user-graph' if neo4j.worker_label else 'user-graph'
    neo4j.execute_labeled("\n        CREATE (u1:__user__ {id: 1, username: 'alice'}),\n               (u2:__user__ {id: 2, username: 'bob'}),\n               (u3:__user__ {id: 3, username: 'charlie'}),\n               (u4:__user__ {id: 4, username: 'dave'}),\n               (u5:__user__ {id: 5, username: 'eve'})\n    ", label_map={'user': 'User'})
    neo4j.execute_labeled('MATCH (u1:__user__ {id: 1}), (u2:__user__ {id: 2}) CREATE (u1)-[:FOLLOWS]->(u2)', label_map={'user': 'User'})
    neo4j.execute_labeled('MATCH (u2:__user__ {id: 2}), (u3:__user__ {id: 3}) CREATE (u2)-[:FOLLOWS]->(u3)', label_map={'user': 'User'})
    neo4j.execute_labeled('MATCH (u1:__user__ {id: 1}), (u3:__user__ {id: 3}) CREATE (u1)-[:FOLLOWS]->(u3)', label_map={'user': 'User'})
    neo4j.execute_labeled('MATCH (u4:__user__ {id: 4}), (u5:__user__ {id: 5}) CREATE (u4)-[:FOLLOWS]->(u5)', label_map={'user': 'User'})
    neo4j.execute_labeled('MATCH (u5:__user__ {id: 5}), (u4:__user__ {id: 4}) CREATE (u5)-[:FOLLOWS]->(u4)', label_map={'user': 'User'})
    follows_count = list(neo4j.execute_and_fetch_labeled('MATCH (n:__user__)-[r:FOLLOWS]->(m:__user__) RETURN count(r) AS count', label_map={'user': 'User'}))
    assert follows_count[0]['count'] == SMALL_GRAPH_EDGES
    print(f"✅ Graph created: {follows_count[0]['count']} FOLLOWS edges")
    gds_project(neo4j, graph_name, user_label)
    neo4j.execute_labeled("\n        CALL gds.fastRP.write('__graph_name__', {\n            embeddingDimension: $dim,\n            iterationWeights: [0.0, 1.0, 1.0],\n            writeProperty: 'embedding'\n        })\n        YIELD nodePropertiesWritten\n        RETURN nodePropertiesWritten\n    ", ident_map={'graph_name': graph_name}, params={'dim': EMBEDDING_DIM_SMALL})
    embeddings_result = list(neo4j.execute_and_fetch_labeled('\n        MATCH (u:__user__)\n        WHERE u.embedding IS NOT NULL\n        RETURN u.id AS user_id, size(u.embedding) AS dimension\n        ORDER BY user_id\n    ', label_map={'user': 'User'}))
    assert len(embeddings_result) == SMALL_GRAPH_NODES
    assert all(r['dimension'] == EMBEDDING_DIM_SMALL for r in embeddings_result)
    print(f'✅ FastRP OK: {len(embeddings_result)} users with embeddings')
    neo4j.execute_labeled("CALL gds.graph.drop('__graph_name__') YIELD graphName", ident_map={'graph_name': graph_name})

@pytest.mark.integration
@pytest.mark.smoke
def test_neo4j_fastRP_simple_graph(neo4j: Neo4jClient) -> None:
    """Test FastRP on simple FOLLOWS graph with mutate mode."""
    user_label = neo4j.label('User')
    graph_name = f'{neo4j.worker_label}-fastrp-simple' if neo4j.worker_label else 'fastrp-simple'
    neo4j.execute_labeled("\n        CREATE (u1:__user__ {id: 1, username: 'alice'}),\n               (u2:__user__ {id: 2, username: 'bob'}),\n               (u3:__user__ {id: 3, username: 'charlie'}),\n               (u4:__user__ {id: 4, username: 'dave'}),\n               (u5:__user__ {id: 5, username: 'eve'})\n    ", label_map={'user': 'User'})
    neo4j.execute_labeled('MATCH (u1:__user__ {id: 1}), (u2:__user__ {id: 2}) CREATE (u1)-[:FOLLOWS]->(u2)', label_map={'user': 'User'})
    neo4j.execute_labeled('MATCH (u2:__user__ {id: 2}), (u3:__user__ {id: 3}) CREATE (u2)-[:FOLLOWS]->(u3)', label_map={'user': 'User'})
    neo4j.execute_labeled('MATCH (u1:__user__ {id: 1}), (u3:__user__ {id: 3}) CREATE (u1)-[:FOLLOWS]->(u3)', label_map={'user': 'User'})
    neo4j.execute_labeled('MATCH (u4:__user__ {id: 4}), (u5:__user__ {id: 5}) CREATE (u4)-[:FOLLOWS]->(u5)', label_map={'user': 'User'})
    neo4j.execute_labeled('MATCH (u5:__user__ {id: 5}), (u4:__user__ {id: 4}) CREATE (u5)-[:FOLLOWS]->(u4)', label_map={'user': 'User'})
    follows_count = neo4j.execute_and_fetch_labeled('MATCH (n:__user__)-[r:FOLLOWS]->(m:__user__) RETURN count(r) AS count', label_map={'user': 'User'})[0]['count']
    assert follows_count == SMALL_GRAPH_EDGES
    print(f'✅ Graph created: {follows_count} FOLLOWS edges')
    gds_project_undirected(neo4j, graph_name, user_label)
    neo4j.execute_labeled("\n        CALL gds.fastRP.mutate('__graph_name__', {\n            embeddingDimension: $dim,\n            iterationWeights: [0.0, 1.0, 1.0],\n            mutateProperty: 'embedding'\n        })\n        YIELD nodePropertiesWritten\n        RETURN nodePropertiesWritten\n    ", ident_map={'graph_name': graph_name}, params={'dim': EMBEDDING_DIM_SMALL})
    neo4j.execute_labeled("\n        CALL gds.graph.nodeProperties.stream('__graph_name__', ['embedding'])\n        YIELD nodeId, propertyValue\n        WITH gds.util.asNode(nodeId) AS node, propertyValue AS embedding\n        SET node.embedding = embedding\n    ", ident_map={'graph_name': graph_name})
    embeddings_result = list(neo4j.execute_and_fetch_labeled('\n        MATCH (u:__user__)\n        WHERE u.embedding IS NOT NULL\n        RETURN u.id AS user_id, size(u.embedding) AS dimension\n        ORDER BY user_id\n    ', label_map={'user': 'User'}))
    assert len(embeddings_result) == SMALL_GRAPH_NODES
    assert all(r['dimension'] == EMBEDDING_DIM_SMALL for r in embeddings_result)
    print(f'✅ Neo4j GDS FastRP OK: {len(embeddings_result)} users with embeddings')
    neo4j.execute_labeled("CALL gds.graph.drop('__graph_name__') YIELD graphName", ident_map={'graph_name': graph_name})