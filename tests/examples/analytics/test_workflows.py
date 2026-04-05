"""Complex multi-algorithm workflow tests for Neo4j GDS."""
from collections import Counter
from typing import cast
import numpy as np
import pytest
from hintgrid.clients.neo4j import Neo4jClient
from hintgrid.utils.coercion import coerce_int
from .conftest import COMMUNITY_INTEREST_SCORE, EMBEDDING_DIM_MEDIUM, EMBEDDING_DIM_SMALL, FAVOURITES_COUNT, LARGE_GRAPH_EDGES, MEDIUM_GRAPH_NODES, MIN_CLUSTER_COHESION, NODES_PER_GROUP, NUM_CLUSTERS, QUALITY_MULTIPLIER, gds_project, gds_project_with_embedding, gds_project_undirected

@pytest.mark.integration
@pytest.mark.quality
def test_graph_analytics_workflow(neo4j: Neo4jClient) -> None:
    """Full workflow: graph -> FastRP -> clustering."""
    user_label = neo4j.label('User')
    graph_name = f'{neo4j.worker_label}-temp-graph' if neo4j.worker_label else 'temp-graph'
    graph_name_emb = f'{neo4j.worker_label}-temp-graph-emb' if neo4j.worker_label else 'temp-graph-emb'
    for i in range(1, MEDIUM_GRAPH_NODES + 1):
        neo4j.execute_labeled("CREATE (u:__user__ {id: $i, username: 'user$i'})", label_map={'user': 'User'}, params={'i': i})
    for i in range(1, NODES_PER_GROUP):
        for j in range(i + 1, NODES_PER_GROUP + 1):
            neo4j.execute_labeled('\n                MATCH (u1:__user__ {id: $i}), (u2:__user__ {id: $j})\n                CREATE (u1)-[:FOLLOWS]->(u2)\n            ', label_map={'user': 'User'}, params={'i': i, 'j': j})
    for i in range(NODES_PER_GROUP + 1, MEDIUM_GRAPH_NODES):
        for j in range(i + 1, MEDIUM_GRAPH_NODES + 1):
            neo4j.execute_labeled('\n                MATCH (u1:__user__ {id: $i}), (u2:__user__ {id: $j})\n                CREATE (u1)-[:FOLLOWS]->(u2)\n            ', label_map={'user': 'User'}, params={'i': i, 'j': j})
    users_count = neo4j.execute_and_fetch_labeled('MATCH (u:__user__) RETURN count(u) AS count', label_map={'user': 'User'})[0]['count']
    follows_count = neo4j.execute_and_fetch_labeled('MATCH (n:__user__)-[r:FOLLOWS]->(m:__user__) RETURN count(r) AS count', label_map={'user': 'User'})[0]['count']
    assert users_count == MEDIUM_GRAPH_NODES
    assert follows_count == LARGE_GRAPH_EDGES
    print(f'✅ Graph created: {users_count} users, {follows_count} follows')
    gds_project(neo4j, graph_name, user_label)
    neo4j.execute_labeled("\n        CALL gds.fastRP.write('__graph_name__', {\n            embeddingDimension: $dim,\n            iterationWeights: [0.0, 1.0, 1.0],\n            writeProperty: 'embedding'\n        })\n    ", ident_map={'graph_name': graph_name}, params={'dim': EMBEDDING_DIM_MEDIUM})
    result = list(neo4j.execute_and_fetch_labeled('\n        MATCH (u:__user__)\n        WHERE u.embedding IS NOT NULL\n        RETURN count(u) AS processed_nodes\n    ', label_map={'user': 'User'}))
    assert result[0]['processed_nodes'] == MEDIUM_GRAPH_NODES
    print(f"✅ FastRP processed: {result[0]['processed_nodes']} nodes")
    embeddings_count = neo4j.execute_and_fetch_labeled('\n        MATCH (u:__user__)\n        WHERE u.embedding IS NOT NULL\n        RETURN count(u) AS count\n    ', label_map={'user': 'User'})[0]['count']
    assert embeddings_count == users_count
    print(f'✅ FastRP: created {embeddings_count} embeddings')
    neo4j.execute_labeled("CALL gds.graph.drop('__graph_name__') YIELD graphName", ident_map={'graph_name': graph_name})
    gds_project_with_embedding(neo4j, graph_name_emb, user_label)
    neo4j.execute_and_fetch_labeled("\n        CALL gds.kmeans.write('__graph_name__', {\n            nodeProperty: 'embedding',\n            k: $clusters,\n            randomSeed: 42,\n            writeProperty: 'cluster_id'\n        })\n        YIELD nodePropertiesWritten\n        RETURN nodePropertiesWritten\n    ", ident_map={'graph_name': graph_name_emb}, params={'clusters': NUM_CLUSTERS})
    num_clusters = coerce_int(neo4j.execute_and_fetch_labeled('\n        MATCH (u:__user__)\n        WHERE u.cluster_id IS NOT NULL\n        RETURN count(DISTINCT u.cluster_id) AS num_clusters\n    ', label_map={'user': 'User'})[0]['num_clusters'])
    assert num_clusters == NUM_CLUSTERS
    print(f'✅ K-Means: found {num_clusters} clusters')
    all_clustered = neo4j.execute_and_fetch_labeled('\n        MATCH (u:__user__)\n        WHERE u.cluster_id IS NOT NULL\n        RETURN count(u) AS count\n    ', label_map={'user': 'User'})[0]['count']
    assert all_clustered == MEDIUM_GRAPH_NODES
    total_clusters = neo4j.execute_and_fetch_labeled('\n        MATCH (u:__user__)\n        WHERE u.cluster_id IS NOT NULL\n        RETURN count(DISTINCT u.cluster_id) AS count\n    ', label_map={'user': 'User'})[0]['count']
    assert total_clusters == NUM_CLUSTERS
    cluster1_ids = list(neo4j.execute_and_fetch_labeled('\n        MATCH (u:__user__)\n        WHERE u.id IN [1,2,3,4,5] AND u.cluster_id IS NOT NULL\n        RETURN u.cluster_id AS cid\n    ', label_map={'user': 'User'}))
    cluster2_ids = list(neo4j.execute_and_fetch_labeled('\n        MATCH (u:__user__)\n        WHERE u.id IN [6,7,8,9,10] AND u.cluster_id IS NOT NULL\n        RETURN u.cluster_id AS cid\n    ', label_map={'user': 'User'}))
    assert len(cluster1_ids) == NODES_PER_GROUP
    assert len(cluster2_ids) == NODES_PER_GROUP
    group1_clusters = [r['cid'] for r in cluster1_ids]
    group2_clusters = [r['cid'] for r in cluster2_ids]
    group1_majority_cluster = Counter(group1_clusters).most_common(1)[0][0]
    group2_majority_cluster = Counter(group2_clusters).most_common(1)[0][0]
    group1_cohesion = group1_clusters.count(group1_majority_cluster) / len(group1_clusters)
    group2_cohesion = group2_clusters.count(group2_majority_cluster) / len(group2_clusters)
    assert group1_cohesion >= MIN_CLUSTER_COHESION
    assert group2_cohesion >= MIN_CLUSTER_COHESION
    print('✅ Clustering quality:')
    print(f'   Group 1-5: {group1_cohesion:.1%} in cluster {group1_majority_cluster}')
    print(f'   Group 6-10: {group2_cohesion:.1%} in cluster {group2_majority_cluster}')
    print('🎉 Full workflow works correctly!')
    neo4j.execute_labeled("CALL gds.graph.drop('__graph_name__') YIELD graphName", ident_map={'graph_name': graph_name_emb})

@pytest.mark.integration
@pytest.mark.smoke
def test_community_interests_creation(neo4j: Neo4jClient) -> None:
    """Create INTERESTED_IN relationships based on FAVOURITED."""
    neo4j.execute_labeled("\n        CREATE (uc1:__uc__ {id: 1, label: 'Tech enthusiasts'}),\n               (uc2:__uc__ {id: 2, label: 'Artists'}),\n               (pc1:__pc__ {id: 10, label: 'Technology'}),\n               (pc2:__pc__ {id: 20, label: 'Art & Design'})\n    ", label_map={'uc': 'UserCommunity', 'pc': 'PostCommunity'})
    neo4j.execute_labeled("\n        CREATE (u1:__user__ {id: 101}),\n               (u2:__user__ {id: 102}),\n               (p1:__post__ {id: 201, text: 'Python tips'}),\n               (p2:__post__ {id: 202, text: 'Painting techniques'})\n    ", label_map={'user': 'User', 'post': 'Post'})
    neo4j.execute_labeled('\n        MATCH (u1:__user__ {id: 101}), (uc1:__uc__ {id: 1})\n        CREATE (u1)-[:BELONGS_TO]->(uc1)\n    ', label_map={'user': 'User', 'uc': 'UserCommunity'})
    neo4j.execute_labeled('\n        MATCH (u2:__user__ {id: 102}), (uc2:__uc__ {id: 2})\n        CREATE (u2)-[:BELONGS_TO]->(uc2)\n    ', label_map={'user': 'User', 'uc': 'UserCommunity'})
    neo4j.execute_labeled('\n        MATCH (p1:__post__ {id: 201}), (pc1:__pc__ {id: 10})\n        CREATE (p1)-[:BELONGS_TO]->(pc1)\n    ', label_map={'post': 'Post', 'pc': 'PostCommunity'})
    neo4j.execute_labeled('\n        MATCH (p2:__post__ {id: 202}), (pc2:__pc__ {id: 20})\n        CREATE (p2)-[:BELONGS_TO]->(pc2)\n    ', label_map={'post': 'Post', 'pc': 'PostCommunity'})
    neo4j.execute_labeled('\n        MATCH (u1:__user__ {id: 101}), (p1:__post__ {id: 201})\n        CREATE (u1)-[:FAVOURITED {at: datetime()}]->(p1)\n    ', label_map={'user': 'User', 'post': 'Post'})
    neo4j.execute_labeled('\n        MATCH (u2:__user__ {id: 102}), (p2:__post__ {id: 202})\n        CREATE (u2)-[:FAVOURITED {at: datetime()}]->(p2)\n    ', label_map={'user': 'User', 'post': 'Post'})
    neo4j.execute_labeled('\n        MATCH (u:__user__)-[:BELONGS_TO]->(uc:__uc__),\n              (u)-[:FAVOURITED]->(p:__post__)-[:BELONGS_TO]->(pc:__pc__)\n        WITH uc, pc, count(*) AS favourites\n        MERGE (uc)-[i:INTERESTED_IN]->(pc)\n        SET i.score = 0.8,\n            i.based_on = favourites\n    ', label_map={'user': 'User', 'uc': 'UserCommunity', 'post': 'Post', 'pc': 'PostCommunity'})
    interests = list(neo4j.execute_and_fetch_labeled('\n        MATCH (uc:__uc__)-[i:INTERESTED_IN]->(pc:__pc__)\n        RETURN uc.id AS uc_id,\n               uc.label AS uc_label,\n               pc.id AS pc_id,\n               pc.label AS pc_label,\n               i.score AS score,\n               i.based_on AS based_on\n        ORDER BY uc.id\n    ', label_map={'uc': 'UserCommunity', 'pc': 'PostCommunity'}))
    assert len(interests) == NUM_CLUSTERS
    tech_interest = next(i for i in interests if i['uc_id'] == 1)
    assert tech_interest['uc_label'] == 'Tech enthusiasts'
    assert tech_interest['pc_label'] == 'Technology'
    assert tech_interest['score'] == COMMUNITY_INTEREST_SCORE
    assert tech_interest['based_on'] == FAVOURITES_COUNT
    art_interest = next(i for i in interests if i['uc_id'] == NUM_CLUSTERS)
    assert art_interest['uc_label'] == 'Artists'
    assert art_interest['pc_label'] == 'Art & Design'
    assert art_interest['score'] == COMMUNITY_INTEREST_SCORE
    assert art_interest['based_on'] == FAVOURITES_COUNT
    print(f'✅ Created {len(interests)} edges INTERESTED_IN between communities')

@pytest.mark.integration
@pytest.mark.smoke
def test_fastRP_then_kmeans_workflow(neo4j: Neo4jClient) -> None:
    """Full workflow: graph → FastRP → K-Means clustering."""
    user_label = neo4j.label('User')
    graph_name = f'{neo4j.worker_label}-temp-graph' if neo4j.worker_label else 'temp-graph'
    graph_name_emb = f'{neo4j.worker_label}-temp-graph-emb' if neo4j.worker_label else 'temp-graph-emb'
    for i in range(1, NODES_PER_GROUP + 1):
        neo4j.execute_labeled('CREATE (:__user__ {id: $id})', label_map={'user': 'User'}, params={'id': i})
    for i in range(1, NODES_PER_GROUP):
        for j in range(i + 1, NODES_PER_GROUP + 1):
            neo4j.execute_labeled('\n                MATCH (u1:__user__ {id: $i}), (u2:__user__ {id: $j})\n                CREATE (u1)-[:FOLLOWS]->(u2)\n            ', label_map={'user': 'User'}, params={'i': i, 'j': j})
    for i in range(NODES_PER_GROUP + 1, MEDIUM_GRAPH_NODES + 1):
        neo4j.execute_labeled('CREATE (:__user__ {id: $id})', label_map={'user': 'User'}, params={'id': i})
    for i in range(NODES_PER_GROUP + 1, MEDIUM_GRAPH_NODES):
        for j in range(i + 1, MEDIUM_GRAPH_NODES + 1):
            neo4j.execute_labeled('\n                MATCH (u1:__user__ {id: $i}), (u2:__user__ {id: $j})\n                CREATE (u1)-[:FOLLOWS]->(u2)\n            ', label_map={'user': 'User'}, params={'i': i, 'j': j})
    users_count = neo4j.execute_and_fetch_labeled('MATCH (u:__user__) RETURN count(u) AS count', label_map={'user': 'User'})[0]['count']
    assert users_count == MEDIUM_GRAPH_NODES
    print(f'✅ Graph created: {users_count} users (2 components)')
    gds_project(neo4j, graph_name, user_label)
    result = list(neo4j.execute_and_fetch_labeled("\n        CALL gds.fastRP.write('__graph_name__', {\n            embeddingDimension: $dim,\n            iterationWeights: [0.0, 1.0, 1.0],\n            writeProperty: 'embedding'\n        })\n        YIELD nodePropertiesWritten\n        RETURN nodePropertiesWritten AS processed\n    ", ident_map={'graph_name': graph_name}, params={'dim': EMBEDDING_DIM_SMALL}))
    assert result[0]['processed'] == MEDIUM_GRAPH_NODES
    print(f"✅ FastRP: {result[0]['processed']} embeddings")
    neo4j.execute_labeled("CALL gds.graph.drop('__graph_name__') YIELD graphName", ident_map={'graph_name': graph_name})
    gds_project_with_embedding(neo4j, graph_name_emb, user_label)
    neo4j.execute_and_fetch_labeled("\n        CALL gds.kmeans.write('__graph_name__', {\n            nodeProperty: 'embedding',\n            k: $clusters,\n            randomSeed: 42,\n            writeProperty: 'cluster_id'\n        })\n        YIELD nodePropertiesWritten\n        RETURN nodePropertiesWritten\n    ", ident_map={'graph_name': graph_name_emb}, params={'clusters': NUM_CLUSTERS})
    num_clusters = neo4j.execute_and_fetch_labeled('\n        MATCH (u:__user__)\n        WHERE u.cluster_id IS NOT NULL\n        RETURN count(DISTINCT u.cluster_id) AS num_clusters\n    ', label_map={'user': 'User'})[0]['num_clusters']
    assert num_clusters == NUM_CLUSTERS
    print(f'✅ K-Means: {num_clusters} clusters')
    comp1_counts = list(neo4j.execute_and_fetch_labeled('\n            MATCH (u:__user__)\n            WHERE u.id < 6 AND u.cluster_id IS NOT NULL\n            RETURN u.cluster_id AS cluster, count(*) AS count\n            ORDER BY count DESC\n            ', label_map={'user': 'User'}))
    comp2_counts = list(neo4j.execute_and_fetch_labeled('\n            MATCH (u:__user__)\n            WHERE u.id >= 6 AND u.cluster_id IS NOT NULL\n            RETURN u.cluster_id AS cluster, count(*) AS count\n            ORDER BY count DESC\n            ', label_map={'user': 'User'}))
    assert coerce_int(comp1_counts[0]['count']) >= 3
    assert coerce_int(comp2_counts[0]['count']) >= 3
    print(f"✅ Components clustered: Comp1={comp1_counts[0]['cluster']}, Comp2={comp2_counts[0]['cluster']}")
    print('🎉 Full workflow FastRP → K-Means works correctly!')
    neo4j.execute_labeled("CALL gds.graph.drop('__graph_name__') YIELD graphName", ident_map={'graph_name': graph_name_emb})

@pytest.mark.integration
@pytest.mark.quality
def test_kmeans_clustering_quality(neo4j: Neo4jClient) -> None:
    """Test K-Means clustering QUALITY, not just that it runs."""
    user_label = neo4j.label('User')
    graph_name = f'{neo4j.worker_label}-kmeans-quality' if neo4j.worker_label else 'kmeans-quality'
    cluster1_embeddings = [[0.1 + i * 0.01, 0.2 + i * 0.01] for i in range(5)]
    cluster2_embeddings = [[0.9 - i * 0.01, 0.8 - i * 0.01] for i in range(5)]
    for i, emb in enumerate(cluster1_embeddings, 1):
        neo4j.execute_labeled('CREATE (:__user__ {id: $id, embedding: $emb, expected_cluster: 1})', label_map={'user': 'User'}, params={'id': i, 'emb': emb})
    for i, emb in enumerate(cluster2_embeddings, 6):
        neo4j.execute_labeled('CREATE (:__user__ {id: $id, embedding: $emb, expected_cluster: 2})', label_map={'user': 'User'}, params={'id': i, 'emb': emb})
    gds_project_with_embedding(neo4j, graph_name, user_label)
    neo4j.execute_and_fetch_labeled("\n        CALL gds.kmeans.write('__graph_name__', {\n            nodeProperty: 'embedding',\n            k: $clusters,\n            writeProperty: 'cluster_id'\n        })\n        YIELD nodePropertiesWritten\n        RETURN nodePropertiesWritten\n    ", ident_map={'graph_name': graph_name}, params={'clusters': NUM_CLUSTERS})
    results = list(neo4j.execute_and_fetch_labeled('\n        MATCH (u:__user__)\n        RETURN u.id AS id,\n               u.embedding AS emb,\n               u.cluster_id AS assigned,\n               u.expected_cluster AS expected\n        ORDER BY u.id\n    ', label_map={'user': 'User'}))
    cluster1_assigned = {coerce_int(r['assigned']) for r in results if coerce_int(r['id']) <= NODES_PER_GROUP}
    assert len(cluster1_assigned) == 1
    cluster2_assigned = {coerce_int(r['assigned']) for r in results if coerce_int(r['id']) > NODES_PER_GROUP}
    assert len(cluster2_assigned) == 1
    c1_id = next(iter(cluster1_assigned))
    c2_id = next(iter(cluster2_assigned))
    assert c1_id != c2_id
    c1_vectors = np.array([cast(list[float], r['emb']) for r in results if coerce_int(r['id']) <= NODES_PER_GROUP])
    c2_vectors = np.array([cast(list[float], r['emb']) for r in results if coerce_int(r['id']) > NODES_PER_GROUP])
    c1_mean = np.mean(c1_vectors, axis=0)
    c2_mean = np.mean(c2_vectors, axis=0)
    inter_cluster_dist = np.linalg.norm(c1_mean - c2_mean)
    intra_c1 = np.mean([np.linalg.norm(v - c1_mean) for v in c1_vectors])
    intra_c2 = np.mean([np.linalg.norm(v - c2_mean) for v in c2_vectors])
    assert inter_cluster_dist > intra_c1 * QUALITY_MULTIPLIER
    assert inter_cluster_dist > intra_c2 * QUALITY_MULTIPLIER
    print('✅ Clustering quality:')
    print(f'   Inter-cluster distance: {inter_cluster_dist:.4f}')
    print(f'   Intra-cluster 1: {intra_c1:.4f}')
    print(f'   Intra-cluster 2: {intra_c2:.4f}')
    neo4j.execute_labeled("CALL gds.graph.drop('__graph_name__') YIELD graphName", ident_map={'graph_name': graph_name})

@pytest.mark.integration
@pytest.mark.quality
def test_fastRP_embedding_quality(neo4j: Neo4jClient) -> None:
    """Test that FastRP embeddings reflect graph structure."""
    user_label = neo4j.label('User')
    graph_name = f'{neo4j.worker_label}-fastrp-quality' if neo4j.worker_label else 'fastrp-quality'
    for i in range(1, 4):
        neo4j.execute_labeled('CREATE (:__user__ {id: $id})', label_map={'user': 'User'}, params={'id': i})
    for i in range(1, 3):
        for j in range(i + 1, 4):
            neo4j.execute_labeled('\n                MATCH (u1:__user__ {id: $i}), (u2:__user__ {id: $j})\n                CREATE (u1)-[:FOLLOWS]->(u2), (u2)-[:FOLLOWS]->(u1)\n            ', label_map={'user': 'User'}, params={'i': i, 'j': j})
    for i in range(4, 7):
        neo4j.execute_labeled('CREATE (:__user__ {id: $id})', label_map={'user': 'User'}, params={'id': i})
    for i in range(4, 6):
        for j in range(i + 1, 7):
            neo4j.execute_labeled('\n                MATCH (u1:__user__ {id: $i}), (u2:__user__ {id: $j})\n                CREATE (u1)-[:FOLLOWS]->(u2), (u2)-[:FOLLOWS]->(u1)\n            ', label_map={'user': 'User'}, params={'i': i, 'j': j})
    gds_project_undirected(neo4j, graph_name, user_label)
    neo4j.execute_labeled("\n        CALL gds.fastRP.write('__graph_name__', {\n            embeddingDimension: $dim,\n            iterationWeights: [0.0, 1.0, 1.0],\n            writeProperty: 'embedding'\n        })\n    ", ident_map={'graph_name': graph_name}, params={'dim': EMBEDDING_DIM_SMALL})
    results = list(neo4j.execute_and_fetch_labeled('\n        MATCH (u:__user__)\n        WHERE u.embedding IS NOT NULL\n        RETURN u.id AS id, u.embedding AS embedding\n        ORDER BY u.id\n    ', label_map={'user': 'User'}))
    assert len(results) == 6

    def cosine_sim(v1: list[float], v2: list[float]) -> float:
        a, b = (np.array(v1), np.array(v2))
        return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))
    clique1_embeddings: list[list[float]] = [cast('list[float]', r['embedding']) for r in results if coerce_int(r['id']) <= 3]
    clique2_embeddings: list[list[float]] = [cast('list[float]', r['embedding']) for r in results if coerce_int(r['id']) > 3]
    intra_sims = []
    for i in range(len(clique1_embeddings)):
        for j in range(i + 1, len(clique1_embeddings)):
            intra_sims.append(cosine_sim(clique1_embeddings[i], clique1_embeddings[j]))
    inter_sims = []
    for e1 in clique1_embeddings:
        for e2 in clique2_embeddings:
            inter_sims.append(cosine_sim(e1, e2))
    avg_intra = np.mean(intra_sims)
    avg_inter = np.mean(inter_sims)
    print('✅ FastRP embedding quality:')
    print(f'   Avg intra-clique similarity: {avg_intra:.4f}')
    print(f'   Avg inter-clique similarity: {avg_inter:.4f}')
    assert avg_intra > avg_inter, f'Intra-clique similarity ({avg_intra:.4f}) should be > inter-clique similarity ({avg_inter:.4f})'
    neo4j.execute_labeled("CALL gds.graph.drop('__graph_name__') YIELD graphName", ident_map={'graph_name': graph_name})