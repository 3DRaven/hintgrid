"""K-Means clustering tests with embeddings.

All tests use worker-isolated labels via neo4j.label() for parallel execution.
"""


import pytest

from hintgrid.clients.neo4j import Neo4jClient
from hintgrid.utils.coercion import coerce_int, coerce_str
from tests.conftest import EmbeddingServiceConfig

from .conftest import (
    EMBEDDING_DIM,
    MIN_CLUSTER_SIZE,
    NUM_POSTS,
    NUM_POST_CLUSTERS,
    PostInput,
    gds_drop_graph,
    gds_project_with_embedding,
    generate_embeddings_via_service,
)


@pytest.mark.integration
def test_kmeans_clustering_posts(
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_posts: list[PostInput],
) -> None:
    """Test K-Means clustering of posts with REAL FastText embeddings."""
    post_label = neo4j.label("Post")

    # Generate embeddings via FastText service
    posts_with_embeddings = generate_embeddings_via_service(
        fasttext_embedding_service, sample_posts
    )

    # Create posts with embeddings
    for post in posts_with_embeddings:
        neo4j.execute_labeled(
            "CREATE (:__post__ {id: $id, text: $text, embedding: $emb})",
            {"post": "Post"},
            {"id": post["id"], "text": post["text"], "emb": post["embedding"]},
        )

    # Run K-Means clustering using Cypher projection
    graph_name = f"kmeans-posts-{neo4j.worker_label or 'master'}"
    gds_project_with_embedding(neo4j, graph_name, post_label)

    try:
        # Graph name is dynamic, use parameterized query
        neo4j.execute_and_fetch(
            "CALL gds.kmeans.write($graph_name, {"
            "nodeProperty: 'embedding', "
            "k: $k, "
            "randomSeed: 42, "
            "writeProperty: 'cluster_id'"
            "}) "
            "YIELD nodePropertiesWritten "
            "RETURN nodePropertiesWritten",
            {"graph_name": graph_name, "k": NUM_POST_CLUSTERS},
        )

        # Get clustering results
        results = list(
            neo4j.execute_and_fetch_labeled(
                "MATCH (p:__post__) "
                "RETURN p.id AS id, "
                "p.text AS text, "
                "p.cluster_id AS cluster "
                "ORDER BY p.cluster_id, p.id",
                {"post": "Post"},
            )
        )

        assert len(results) == NUM_POSTS

        # Verify clusters assigned
        clusters = {coerce_int(r["cluster"]) for r in results}
        assert len(clusters) == NUM_POST_CLUSTERS, (
            f"Should have exactly {NUM_POST_CLUSTERS} clusters, got {len(clusters)}"
        )

        # Group posts by cluster
        from hintgrid.clients.neo4j import Neo4jValue
        cluster_groups: dict[int, list[dict[str, Neo4jValue]]] = {}
        for r in results:
            cluster_id = coerce_int(r["cluster"])
            if cluster_id not in cluster_groups:
                cluster_groups[cluster_id] = []
            cluster_groups[cluster_id].append(r)

        # Verify each cluster has minimum size
        for _cluster_id, posts in cluster_groups.items():
            assert len(posts) >= MIN_CLUSTER_SIZE

        print(f"✅ K-Means created {len(clusters)} clusters from REAL FastText embeddings:")
        for cluster_id, posts in sorted(cluster_groups.items()):
            print(f"   Cluster {cluster_id}: {len(posts)} posts")
            for clustered_post in posts[:2]:
                post_id = coerce_int(clustered_post["id"])
                post_text = coerce_str(clustered_post["text"])
                print(f"     - Post {post_id}: {post_text[:50]}...")

    finally:
        gds_drop_graph(neo4j, graph_name)


@pytest.mark.integration
def test_post_clustering_quality(
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_posts: list[PostInput],
) -> None:
    """Test quality of post clustering with REAL FastText embeddings.

    NOTE: FastText does LEXICAL clustering (based on shared words), NOT semantic clustering.
    We check that clustering happens, but don't expect perfect topic separation.
    """
    post_label = neo4j.label("Post")

    # Generate embeddings via FastText service
    posts_with_embeddings = generate_embeddings_via_service(
        fasttext_embedding_service, sample_posts
    )

    # Create posts
    for post in posts_with_embeddings:
        neo4j.execute_labeled(
            "CREATE (:__post__ {id: $id, text: $text, embedding: $emb})",
            {"post": "Post"},
            {"id": post["id"], "text": post["text"], "emb": post["embedding"]},
        )

    # Cluster posts using Cypher projection
    graph_name = f"kmeans-posts-quality-{neo4j.worker_label or 'master'}"
    gds_project_with_embedding(neo4j, graph_name, post_label)

    try:
        # Graph name is dynamic, use parameterized query
        neo4j.execute_and_fetch(
            "CALL gds.kmeans.write($graph_name, {"
            "nodeProperty: 'embedding', "
            "k: $k, "
            "randomSeed: 42, "
            "writeProperty: 'cluster_id'"
            "}) "
            "YIELD nodePropertiesWritten "
            "RETURN nodePropertiesWritten",
            {"graph_name": graph_name, "k": NUM_POST_CLUSTERS},
        )

        # Get results
        results = list(
            neo4j.execute_and_fetch_labeled(
                "MATCH (p:__post__) "
                "RETURN p.id AS id, p.text AS text, p.cluster_id AS cluster, p.embedding AS emb "
                "ORDER BY p.id",
                {"post": "Post"},
            )
        )

        # Verify clustering happened
        clusters = {coerce_int(r["cluster"]) for r in results}
        assert len(clusters) == NUM_POST_CLUSTERS

        # Check cluster sizes are reasonable
        cluster_sizes: dict[int, int] = {}
        for r in results:
            cluster_id = coerce_int(r["cluster"])
            cluster_sizes[cluster_id] = cluster_sizes.get(cluster_id, 0) + 1

        assert all(size < len(sample_posts) for size in cluster_sizes.values())

        print("✅ FastText clustering quality check passed:")
        print(f"   Clusters created: {len(clusters)}")
        print(f"   Cluster sizes: {cluster_sizes}")

        # Show clusters
        for cluster_id in sorted(clusters):
            posts_in_cluster = [
                (coerce_int(r["id"]), coerce_str(r["text"])[:40])
                for r in results
                if coerce_int(r["cluster"]) == cluster_id
            ]
            print(f"   Cluster {cluster_id}:")
            for post_id, text in posts_in_cluster:
                print(f"     - Post {post_id}: {text}...")

    finally:
        gds_drop_graph(neo4j, graph_name)


@pytest.mark.integration
def test_create_post_communities(
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_posts: list[PostInput],
) -> None:
    """Test creating PostCommunity nodes from clusters with REAL FastText embeddings."""
    post_label = neo4j.label("Post")

    # Generate embeddings via FastText service
    posts_with_embeddings = generate_embeddings_via_service(
        fasttext_embedding_service, sample_posts
    )

    # Create posts
    for post in posts_with_embeddings:
        neo4j.execute_labeled(
            "CREATE (:__post__ {id: $id, text: $text, embedding: $emb})",
            {"post": "Post"},
            {"id": post["id"], "text": post["text"], "emb": post["embedding"]},
        )

    # Run clustering using Cypher projection
    graph_name = f"kmeans-post-communities-{neo4j.worker_label or 'master'}"
    gds_project_with_embedding(neo4j, graph_name, post_label)

    try:
        # Graph name is dynamic, use parameterized query
        neo4j.execute_and_fetch(
            "CALL gds.kmeans.write($graph_name, {"
            "nodeProperty: 'embedding', "
            "k: $k, "
            "randomSeed: 42, "
            "writeProperty: 'cluster_id'"
            "}) "
            "YIELD nodePropertiesWritten "
            "RETURN nodePropertiesWritten",
            {"graph_name": graph_name, "k": NUM_POST_CLUSTERS},
        )

        # Create PostCommunity nodes and BELONGS_TO relationships
        neo4j.execute_labeled(
            "MATCH (p:__post__) "
            "WHERE p.cluster_id IS NOT NULL "
            "WITH p, p.cluster_id AS cluster_id "
            "MERGE (pc:__pc__ {id: cluster_id}) "
            "MERGE (p)-[:BELONGS_TO]->(pc) "
            "RETURN count(*) AS count",
            {"post": "Post", "pc": "PostCommunity"},
        )

        # Calculate centroids for communities
        neo4j.execute_labeled(
            "MATCH (p:__post__)-[:BELONGS_TO]->(pc:__pc__) "
            "WITH pc, collect(p.embedding) AS embeddings, count(p) AS size "
            "SET pc.centroid = reduce(sum = [], emb IN embeddings | "
            "[i IN range(0, size(emb)-1) | "
            "CASE WHEN i < size(sum) "
            "THEN sum[i] + emb[i] / size "
            "ELSE emb[i] / size END] "
            ") "
            "SET pc.size = size "
            "RETURN count(pc) AS communities",
            {"post": "Post", "pc": "PostCommunity"},
        )

        # Verify PostCommunity nodes created
        communities = list(
            neo4j.execute_and_fetch_labeled(
                "MATCH (pc:__pc__) "
                "RETURN pc.id AS id, pc.size AS size, size(pc.centroid) AS centroid_dim "
                "ORDER BY pc.size DESC",
                {"pc": "PostCommunity"},
            )
        )

        assert len(communities) == NUM_POST_CLUSTERS
        assert all(coerce_int(c["size"]) >= MIN_CLUSTER_SIZE for c in communities)
        assert all(coerce_int(c["centroid_dim"]) == EMBEDDING_DIM for c in communities)

        # Verify BELONGS_TO relationships
        belongs = list(
            neo4j.execute_and_fetch_labeled(
                "MATCH (p:__post__)-[:BELONGS_TO]->(pc:__pc__) "
                "RETURN count(*) AS count",
                {"post": "Post", "pc": "PostCommunity"},
            )
        )
        assert coerce_int(belongs[0]["count"]) == NUM_POSTS

        print(f"✅ Created {len(communities)} PostCommunity nodes from REAL FastText embeddings:")
        for c in communities:
            community_id = coerce_int(c["id"])
            size = coerce_int(c["size"])
            centroid_dim = coerce_int(c["centroid_dim"])
            print(f"   Community {community_id}: {size} posts, centroid: {centroid_dim}-dim")

    finally:
        gds_drop_graph(neo4j, graph_name)
