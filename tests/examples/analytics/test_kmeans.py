"""K-Means clustering tests for Neo4j GDS."""
import pytest
from hintgrid.clients.neo4j import Neo4jClient
from .conftest import MEDIUM_GRAPH_NODES, NODES_PER_GROUP, NUM_CLUSTERS, gds_project_with_embedding

@pytest.mark.integration
@pytest.mark.smoke
def test_kmeans_clustering(neo4j: Neo4jClient) -> None:
    """Test K-Means functionality."""
    user_label = neo4j.label('User')
    graph_name = f'{neo4j.worker_label}-kmeans-simple' if neo4j.worker_label else 'kmeans-simple'
    for i in range(1, NODES_PER_GROUP + 1):
        emb = [0.1 + i * 0.01, 0.2 + i * 0.01]
        neo4j.execute_labeled('CREATE (:__user__ {id: $id, embedding: $emb})', label_map={'user': 'User'}, params={'id': i, 'emb': emb})
    for i in range(NODES_PER_GROUP + 1, MEDIUM_GRAPH_NODES + 1):
        emb = [0.9 - (i - 6) * 0.01, 0.8 - (i - 6) * 0.01]
        neo4j.execute_labeled('CREATE (:__user__ {id: $id, embedding: $emb})', label_map={'user': 'User'}, params={'id': i, 'emb': emb})
    print(f'✅ Created {MEDIUM_GRAPH_NODES} nodes with embeddings (2 clusters)')
    gds_project_with_embedding(neo4j, graph_name, user_label)
    neo4j.execute_and_fetch_labeled("\n        CALL gds.kmeans.write('__graph_name__', {\n            nodeProperty: 'embedding',\n            k: $clusters,\n            writeProperty: 'cluster_id'\n        })\n        YIELD nodePropertiesWritten\n        RETURN nodePropertiesWritten\n    ", ident_map={'graph_name': graph_name}, params={'clusters': NUM_CLUSTERS})
    num_clusters = neo4j.execute_and_fetch_labeled('\n        MATCH (u:__user__)\n        WHERE u.cluster_id IS NOT NULL\n        RETURN count(DISTINCT u.cluster_id) AS num_clusters,\n               collect(DISTINCT u.cluster_id) AS cluster_ids\n    ', label_map={'user': 'User'})[0]
    assert num_clusters['num_clusters'] == NUM_CLUSTERS
    print(f"✅ K-Means: found {num_clusters['num_clusters']} clusters")
    cluster_sizes = list(neo4j.execute_and_fetch_labeled('\n        MATCH (u:__user__)\n        WHERE u.cluster_id IS NOT NULL\n        RETURN u.cluster_id AS cluster, count(u) AS size\n        ORDER BY cluster\n    ', label_map={'user': 'User'}))
    assert len(cluster_sizes) == NUM_CLUSTERS
    print(f'✅ Distribution: {cluster_sizes}')
    neo4j.execute_labeled("CALL gds.graph.drop('__graph_name__') YIELD graphName", ident_map={'graph_name': graph_name})

@pytest.mark.integration
@pytest.mark.smoke
def test_neo4j_kmeans_clustering(neo4j: Neo4jClient) -> None:
    """Test K-Means functionality with parametrized iterations."""
    user_label = neo4j.label('User')
    graph_name = f'{neo4j.worker_label}-kmeans-test' if neo4j.worker_label else 'kmeans-test'
    for i in range(1, NODES_PER_GROUP + 1):
        emb = [0.1 + i * 0.01, 0.2 + i * 0.01]
        neo4j.execute_labeled('CREATE (:__user__ {id: $id, embedding: $emb})', label_map={'user': 'User'}, params={'id': i, 'emb': emb})
    for i in range(NODES_PER_GROUP + 1, MEDIUM_GRAPH_NODES + 1):
        emb = [0.9 - (i - 6) * 0.01, 0.8 - (i - 6) * 0.01]
        neo4j.execute_labeled('CREATE (:__user__ {id: $id, embedding: $emb})', label_map={'user': 'User'}, params={'id': i, 'emb': emb})
    print(f'✅ Neo4j: Created {MEDIUM_GRAPH_NODES} nodes with embeddings')
    gds_project_with_embedding(neo4j, graph_name, user_label)
    neo4j.execute_labeled("\n        CALL gds.kmeans.write('__graph_name__', {\n            nodeProperty: 'embedding',\n            k: $clusters,\n            maxIterations: 10,\n            writeProperty: 'cluster_id'\n        })\n        YIELD nodePropertiesWritten\n        RETURN nodePropertiesWritten\n    ", ident_map={'graph_name': graph_name}, params={'clusters': NUM_CLUSTERS})
    result = neo4j.execute_and_fetch_labeled('\n        MATCH (u:__user__)\n        WHERE u.cluster_id IS NOT NULL\n        RETURN count(DISTINCT u.cluster_id) AS num_clusters\n    ', label_map={'user': 'User'})[0]
    num_clusters = result['num_clusters']
    assert num_clusters == NUM_CLUSTERS
    print(f'✅ Neo4j GDS K-Means: found {num_clusters} clusters')
    cluster_sizes = list(neo4j.execute_and_fetch_labeled('\n        MATCH (u:__user__)\n        WHERE u.cluster_id IS NOT NULL\n        RETURN u.cluster_id AS cluster, count(u) AS size\n        ORDER BY cluster\n    ', label_map={'user': 'User'}))
    assert len(cluster_sizes) == NUM_CLUSTERS
    print(f'✅ Neo4j distribution: {[dict(r) for r in cluster_sizes]}')
    neo4j.execute_labeled("CALL gds.graph.drop('__graph_name__') YIELD graphName", ident_map={'graph_name': graph_name})

@pytest.mark.integration
@pytest.mark.smoke
def test_post_clustering_with_kmeans(neo4j: Neo4jClient) -> None:
    """Real post clustering via K-Means."""
    post_label = neo4j.label('Post')
    graph_name = f'{neo4j.worker_label}-kmeans-posts' if neo4j.worker_label else 'kmeans-posts'
    for i in range(1, 6):
        embedding = [0.8 + i * 0.01, 0.7 + i * 0.01, 0.3]
        neo4j.execute_labeled('\n            CREATE (p:__post__ {\n                id: $id,\n                text: $text,\n                embedding: $emb\n            })\n        ', label_map={'post': 'Post'}, params={'id': i, 'text': f'Tech post {i}', 'emb': embedding})
    for i in range(6, 11):
        embedding = [0.2 + (i - 6) * 0.01, 0.3 + (i - 6) * 0.01, 0.9]
        neo4j.execute_labeled('\n            CREATE (p:__post__ {\n                id: $id,\n                text: $text,\n                embedding: $emb\n            })\n        ', label_map={'post': 'Post'}, params={'id': i, 'text': f'Art post {i}', 'emb': embedding})
    posts_count = neo4j.execute_and_fetch_labeled('MATCH (p:__post__) RETURN count(p) AS count', label_map={'post': 'Post'})[0]['count']
    assert posts_count == MEDIUM_GRAPH_NODES
    print(f'✅ Created {posts_count} posts with synthetic embeddings')
    gds_project_with_embedding(neo4j, graph_name, post_label)
    neo4j.execute_and_fetch_labeled("\n        CALL gds.kmeans.write('__graph_name__', {\n            nodeProperty: 'embedding',\n            k: $clusters,\n            writeProperty: 'cluster_id'\n        })\n        YIELD nodePropertiesWritten\n        RETURN nodePropertiesWritten\n    ", ident_map={'graph_name': graph_name}, params={'clusters': NUM_CLUSTERS})
    num_clusters = neo4j.execute_and_fetch_labeled('\n        MATCH (p:__post__)\n        WHERE p.cluster_id IS NOT NULL\n        RETURN count(DISTINCT p.cluster_id) AS num_clusters\n    ', label_map={'post': 'Post'})[0]['num_clusters']
    assert num_clusters == NUM_CLUSTERS
    print(f'✅ K-Means: {num_clusters} clusters')
    tech_cluster = list(neo4j.execute_and_fetch_labeled('\n        MATCH (p:__post__)\n        WHERE p.id < 6 AND p.cluster_id IS NOT NULL\n        RETURN DISTINCT p.cluster_id AS cluster\n    ', label_map={'post': 'Post'}))
    art_cluster = list(neo4j.execute_and_fetch_labeled('\n        MATCH (p:__post__)\n        WHERE p.id >= 6 AND p.cluster_id IS NOT NULL\n        RETURN DISTINCT p.cluster_id AS cluster\n    ', label_map={'post': 'Post'}))
    assert len(tech_cluster) == 1, 'Tech posts should map to exactly one cluster'
    assert len(art_cluster) == 1, 'Art posts should map to exactly one cluster'
    assert tech_cluster[0]['cluster'] != art_cluster[0]['cluster'], 'Posts from different semantic groups should be in different clusters'
    print(f"✅ Clusters correctly separated: Tech={tech_cluster[0]['cluster']}, Art={art_cluster[0]['cluster']}")
    neo4j.execute_labeled("CALL gds.graph.drop('__graph_name__') YIELD graphName", ident_map={'graph_name': graph_name})