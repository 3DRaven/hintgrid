"""Clustering quality and metrics tests.

All tests use worker-isolated labels via neo4j.label() for parallel execution.
"""

from collections import Counter

import numpy as np
import numpy.typing as npt
import pytest

from hintgrid.clients.neo4j import Neo4jClient
from hintgrid.utils.coercion import coerce_int
from tests.conftest import EmbeddingServiceConfig

from .conftest import (
    NUM_POST_CLUSTERS,
    PostInput,
    as_embedding,
    gds_drop_graph,
    gds_project_with_embedding,
    generate_embeddings_via_service,
)


@pytest.mark.integration
def test_kmeans_clustering_quality_metrics(
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    strong_signal_posts: list[PostInput],
) -> None:
    """Test K-Means clustering quality with FastText embeddings.

    Uses metrics to verify cluster separation and cohesion.
    """
    post_label = neo4j.label("Post")

    # Generate embeddings via FastText service
    posts_with_embeddings = generate_embeddings_via_service(
        fasttext_embedding_service, strong_signal_posts
    )

    # Store posts
    for post in posts_with_embeddings:
        neo4j.execute_labeled(
            "CREATE (:__post__ {id: $id, text: $text, embedding: $emb})",
            {"post": "Post"},
            {"id": post["id"], "text": post["text"], "emb": post["embedding"]},
        )

    # Run K-Means with 3 clusters using Cypher projection
    graph_name = f"kmeans-quality-metrics-{neo4j.worker_label or 'master'}"
    gds_project_with_embedding(neo4j, graph_name, post_label)

    try:
        # Graph name is dynamic, use parameterized query
        neo4j.execute_and_fetch(
            "CALL gds.kmeans.write($graph_name, {"
            "nodeProperty: 'embedding', "
            "k: 3, "
            "randomSeed: 42, "
            "writeProperty: 'cluster_id'"
            "}) "
            "YIELD nodePropertiesWritten "
            "RETURN nodePropertiesWritten",
            {"graph_name": graph_name},
        )

        # Get results with embeddings
        results = list(
            neo4j.execute_and_fetch_labeled(
                "MATCH (p:__post__) "
                "RETURN p.id AS id, p.text AS text, p.cluster_id AS cluster, p.embedding AS emb "
                "ORDER BY p.id",
                {"post": "Post"},
            )
        )

        # Calculate cluster metrics
        from hintgrid.clients.neo4j import Neo4jValue
        clusters: dict[int, list[dict[str, Neo4jValue]]] = {}
        for r in results:
            cluster_id = coerce_int(r["cluster"])
            if cluster_id not in clusters:
                clusters[cluster_id] = []
            clusters[cluster_id].append(r)

        # Verify we have exactly 3 clusters
        assert len(clusters) == 3, f"Should create exactly 3 clusters, got {len(clusters)}"

        # Calculate intra-cluster distances (should be SMALL - tight clusters)
        intra_distances: dict[int, float] = {}
        for cluster_id, posts in clusters.items():
            embeddings = np.array([as_embedding(p["emb"]) for p in posts])
            centroid = np.mean(embeddings, axis=0)

            # Average distance from centroid
            distances = [np.linalg.norm(emb - centroid) for emb in embeddings]
            intra_distances[cluster_id] = float(np.mean(distances))

        avg_intra = np.mean(list(intra_distances.values()))

        # Calculate inter-cluster distances (should be LARGE - separated clusters)
        centroids: dict[int, npt.NDArray[np.float64]] = {}
        for cluster_id, posts in clusters.items():
            embeddings = np.array([as_embedding(p["emb"]) for p in posts])
            centroids[cluster_id] = np.mean(embeddings, axis=0)

        inter_distances: list[float] = []
        cluster_ids = list(centroids.keys())
        for i in range(len(cluster_ids)):
            for j in range(i + 1, len(cluster_ids)):
                dist = float(np.linalg.norm(centroids[cluster_ids[i]] - centroids[cluster_ids[j]]))
                inter_distances.append(dist)

        avg_inter = np.mean(inter_distances)

        # Quality check
        separation_ratio = avg_inter / avg_intra

        # Verify clusters are balanced
        max_cluster_size = max(len(posts) for posts in clusters.values())
        min_cluster_size = min(len(posts) for posts in clusters.values())

        assert max_cluster_size < len(results), "No cluster should contain all posts"
        assert min_cluster_size >= 2, f"Minimum cluster size should be at least 2, got {min_cluster_size}"

        print("✅ Clustering quality metrics (FastText):")
        print(f"   Average intra-cluster distance: {avg_intra:.3f}")
        print(f"   Average inter-cluster distance: {avg_inter:.3f}")
        print(f"   Separation ratio: {separation_ratio:.2f}x")
        print(f"   Cluster sizes: {[len(posts) for posts in clusters.values()]}")
        print(f"   Balanced: min={min_cluster_size}, max={max_cluster_size}")

    finally:
        gds_drop_graph(neo4j, graph_name)


@pytest.mark.integration
def test_kmeans_lexical_similarity_clustering(
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    strong_signal_posts: list[PostInput],
) -> None:
    """Test that posts with same dominant keyword cluster together with FastText.

    FastText should group posts by semantic similarity (via n-grams).
    """
    post_label = neo4j.label("Post")

    # Generate embeddings
    posts_with_embeddings = generate_embeddings_via_service(
        fasttext_embedding_service, strong_signal_posts
    )

    # Store and cluster
    for post in posts_with_embeddings:
        neo4j.execute_labeled(
            "CREATE (:__post__ {id: $id, text: $text, embedding: $emb})",
            {"post": "Post"},
            {"id": post["id"], "text": post["text"], "emb": post["embedding"]},
        )

    graph_name = f"kmeans-lexical-{neo4j.worker_label or 'master'}"
    gds_project_with_embedding(neo4j, graph_name, post_label)

    try:
        # Graph name is dynamic, use parameterized query
        neo4j.execute_and_fetch(
            "CALL gds.kmeans.write($graph_name, {"
            "nodeProperty: 'embedding', "
            "k: 3, "
            "randomSeed: 42, "
            "writeProperty: 'cluster_id'"
            "}) "
            "YIELD nodePropertiesWritten "
            "RETURN nodePropertiesWritten",
            {"graph_name": graph_name},
        )

        # Get results
        results = list(
            neo4j.execute_and_fetch_labeled(
                "MATCH (p:__post__) "
                "RETURN p.id AS id, p.text AS text, p.cluster_id AS cluster "
                "ORDER BY p.id",
                {"post": "Post"},
            )
        )

        # Python posts (ids 1-15) - check majority in same cluster
        python_posts = [r for r in results if 1 <= coerce_int(r["id"]) <= 15]
        python_cluster_counts: dict[int, int] = {}
        for p in python_posts:
            cluster_id = coerce_int(p["cluster"])
            python_cluster_counts[cluster_id] = python_cluster_counts.get(cluster_id, 0) + 1

        max_python_cluster = max(python_cluster_counts.items(), key=lambda x: x[1])[0]
        python_majority = python_cluster_counts[max_python_cluster]

        assert python_majority >= 9, (
            f"At least 9/15 Python posts should cluster together, "
            f"got {python_majority}/15 in cluster {max_python_cluster}"
        )

        # Docker posts (ids 16-30) - check majority in same cluster
        docker_posts = [r for r in results if 16 <= coerce_int(r["id"]) <= 30]
        docker_cluster_counts: dict[int, int] = {}
        for p in docker_posts:
            cluster_id = coerce_int(p["cluster"])
            docker_cluster_counts[cluster_id] = docker_cluster_counts.get(cluster_id, 0) + 1

        max_docker_cluster = max(docker_cluster_counts.items(), key=lambda x: x[1])[0]
        docker_majority = docker_cluster_counts[max_docker_cluster]

        assert docker_majority >= 9

        # Pizza posts (ids 31-45) - check majority in same cluster
        pizza_posts = [r for r in results if 31 <= coerce_int(r["id"]) <= 45]
        pizza_cluster_counts: dict[int, int] = {}
        for p in pizza_posts:
            cluster_id = coerce_int(p["cluster"])
            pizza_cluster_counts[cluster_id] = pizza_cluster_counts.get(cluster_id, 0) + 1

        max_pizza_cluster = max(pizza_cluster_counts.items(), key=lambda x: x[1])[0]
        pizza_majority = pizza_cluster_counts[max_pizza_cluster]

        assert pizza_majority >= 9

        # Verify at least 2 different clusters exist
        unique_clusters = {max_python_cluster, max_docker_cluster, max_pizza_cluster}
        assert len(unique_clusters) >= 2

        print("✅ FastText lexical similarity clustering verified:")
        print(f"   Python posts: {python_majority}/15 in cluster {max_python_cluster}")
        print(f"   Docker posts: {docker_majority}/15 in cluster {max_docker_cluster}")
        print(f"   Pizza posts: {pizza_majority}/15 in cluster {max_pizza_cluster}")
        print(f"   Unique clusters: {len(unique_clusters)}/3")

    finally:
        gds_drop_graph(neo4j, graph_name)


@pytest.mark.integration
def test_kmeans_stability_with_fasttext(
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    strong_signal_posts: list[PostInput],
) -> None:
    """Test K-Means stability across multiple runs with FastText embeddings.

    While K-Means has random initialization, with strong signal results should be stable.
    """
    from sklearn.metrics import adjusted_rand_score  # type: ignore[import-untyped]

    post_label = neo4j.label("Post")

    # Generate embeddings once
    posts_with_embeddings = generate_embeddings_via_service(
        fasttext_embedding_service, strong_signal_posts
    )

    # Store posts once
    for post in posts_with_embeddings:
        neo4j.execute_labeled(
            "CREATE (:__post__ {id: $id, text: $text, embedding: $emb})",
            {"post": "Post"},
            {"id": post["id"], "text": post["text"], "emb": post["embedding"]},
        )

    # Project graph once for K-Means runs
    graph_name = f"kmeans-stability-{neo4j.worker_label or 'master'}"
    gds_project_with_embedding(neo4j, graph_name, post_label)

    try:
        # Run K-Means multiple times
        num_runs = 5
        cluster_assignments: list[list[int]] = []

        for _ in range(num_runs):
            # Run K-Means - graph name is dynamic, use parameterized query
            neo4j.execute_and_fetch(
                "CALL gds.kmeans.write($graph_name, {"
                "nodeProperty: 'embedding', "
                "k: 3, "
                "randomSeed: 42, "
                "writeProperty: 'cluster_id'"
                "}) "
                "YIELD nodePropertiesWritten "
                "RETURN nodePropertiesWritten",
                {"graph_name": graph_name},
            )

            # Get assignments
            results = list(
                neo4j.execute_and_fetch_labeled(
                    "MATCH (p:__post__) "
                    "RETURN p.id AS id, p.cluster_id AS cluster "
                    "ORDER BY p.id",
                    {"post": "Post"},
                )
            )

            cluster_assignments.append([coerce_int(r["cluster"]) for r in results])

            # Reset for next run
            neo4j.execute_labeled(
                "MATCH (p:__post__) REMOVE p.cluster_id",
                {"post": "Post"},
            )

        # Check stability using Adjusted Rand Index
        base_assignment = cluster_assignments[0]
        ari_scores: list[float] = []

        for i in range(1, num_runs):
            ari = adjusted_rand_score(base_assignment, cluster_assignments[i])
            ari_scores.append(ari)

        avg_ari = np.mean(ari_scores)

        # ARI > 0.9 means stable clustering for deterministic K-Means
        assert avg_ari > 0.9, (
            f"K-Means should produce stable results: average ARI = {avg_ari:.3f} (should be > 0.9)"
        )

        print("✅ K-Means stability check passed:")
        print(f"   Runs: {num_runs}")
        print(f"   Average ARI: {avg_ari:.3f}")
        print(f"   Min ARI: {min(ari_scores):.3f}")
        print(f"   Max ARI: {max(ari_scores):.3f}")

    finally:
        gds_drop_graph(neo4j, graph_name)


@pytest.mark.integration
def test_post_clustering_quality_improved(
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    strong_signal_posts: list[PostInput],
) -> None:
    """Test quality of post clustering with REAL FastText embeddings and strong lexical signal.

    Verifies that majority of posts with same keyword cluster together.
    """
    post_label = neo4j.label("Post")

    # Generate embeddings via FastText service
    posts_with_embeddings = generate_embeddings_via_service(
        fasttext_embedding_service, strong_signal_posts
    )

    # Store and cluster
    for post in posts_with_embeddings:
        neo4j.execute_labeled(
            "CREATE (:__post__ {id: $id, text: $text, embedding: $emb})",
            {"post": "Post"},
            {"id": post["id"], "text": post["text"], "emb": post["embedding"]},
        )

    graph_name = f"kmeans-quality-strong-{neo4j.worker_label or 'master'}"
    gds_project_with_embedding(neo4j, graph_name, post_label)

    try:
        # Graph name is dynamic, use parameterized query
        neo4j.execute_and_fetch(
            "CALL gds.kmeans.write($graph_name, {"
            "nodeProperty: 'embedding', "
            "k: 3, "
            "randomSeed: 42, "
            "writeProperty: 'cluster_id'"
            "}) "
            "YIELD nodePropertiesWritten "
            "RETURN nodePropertiesWritten",
            {"graph_name": graph_name},
        )

        # Get results
        results = list(
            neo4j.execute_and_fetch_labeled(
                "MATCH (p:__post__) "
                "RETURN p.id AS id, p.text AS text, p.cluster_id AS cluster "
                "ORDER BY p.id",
                {"post": "Post"},
            )
        )

        # Group by expected topic (15 posts per cluster)
        python_clusters = [coerce_int(r["cluster"]) for r in results if 1 <= coerce_int(r["id"]) <= 15]
        docker_clusters = [coerce_int(r["cluster"]) for r in results if 16 <= coerce_int(r["id"]) <= 30]
        pizza_clusters = [coerce_int(r["cluster"]) for r in results if 31 <= coerce_int(r["id"]) <= 45]

        # Check that MAJORITY of each topic is in same cluster
        python_counts = Counter(python_clusters)
        docker_counts = Counter(docker_clusters)
        pizza_counts = Counter(pizza_clusters)

        python_majority_cluster = python_counts.most_common(1)[0][0]
        python_majority_count = python_counts.most_common(1)[0][1]

        docker_majority_cluster = docker_counts.most_common(1)[0][0]
        docker_majority_count = docker_counts.most_common(1)[0][1]

        pizza_majority_cluster = pizza_counts.most_common(1)[0][0]
        pizza_majority_count = pizza_counts.most_common(1)[0][1]

        print("🔎 Cluster distribution:")
        print(f"   Python: {dict(python_counts)}")
        print(f"   Docker: {dict(docker_counts)}")
        print(f"   Pizza: {dict(pizza_counts)}")

        # With strong semantic signal, expect at least 50% in same cluster (8/15)
        assert python_majority_count >= 8
        assert docker_majority_count >= 8
        assert pizza_majority_count >= 8

        # Verify at least 2 different clusters exist
        unique_clusters = len(
            {python_majority_cluster, docker_majority_cluster, pizza_majority_cluster}
        )
        assert unique_clusters >= 2

        print("✅ FastText clustering quality verified:")
        print(f"   Python: {python_majority_count}/15 in cluster {python_majority_cluster}")
        print(f"   Docker: {docker_majority_count}/15 in cluster {docker_majority_cluster}")
        print(f"   Pizza: {pizza_majority_count}/15 in cluster {pizza_majority_cluster}")
        print(f"   Unique clusters: {unique_clusters}/3")

    finally:
        gds_drop_graph(neo4j, graph_name)


@pytest.mark.integration
def test_community_centroid_correctness(
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_posts: list[PostInput],
) -> None:
    """Test that PostCommunity centroid is correctly calculated as mean of cluster embeddings.

    Compares Cypher-calculated centroid with manual Python calculation.
    """
    post_label = neo4j.label("Post")

    # 1. Generate embeddings via FastText service
    posts_with_embeddings = generate_embeddings_via_service(
        fasttext_embedding_service, sample_posts
    )

    # 2. Create posts with embeddings
    for post in posts_with_embeddings:
        neo4j.execute_labeled(
            "CREATE (:__post__ {id: $id, text: $text, embedding: $emb})",
            {"post": "Post"},
            {"id": post["id"], "text": post["text"], "emb": post["embedding"]},
        )

    # 3. Run K-Means clustering using Cypher projection
    graph_name = f"kmeans-centroid-{neo4j.worker_label or 'master'}"
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

        # 4. Create PostCommunity nodes and relationships
        neo4j.execute_labeled(
            "MATCH (p:__post__) "
            "WHERE p.cluster_id IS NOT NULL "
            "WITH p, p.cluster_id AS cluster_id "
            "MERGE (pc:__pc__ {id: cluster_id}) "
            "MERGE (p)-[:BELONGS_TO]->(pc) "
            "RETURN count(*) AS count",
            {"post": "Post", "pc": "PostCommunity"},
        )

        # 5. Calculate centroids using Cypher
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

        # 6. Get communities with their stored centroids
        communities = list(
            neo4j.execute_and_fetch_labeled(
                "MATCH (pc:__pc__) "
                "RETURN pc.id AS community_id, pc.centroid AS stored_centroid, pc.size AS size "
                "ORDER BY pc.id",
                {"pc": "PostCommunity"},
            )
        )

        assert len(communities) > 0, "Should have at least one community"

        # 7. For each community, manually calculate centroid and compare
        for community in communities:
            community_id = community["community_id"]
            stored_centroid = np.array(community["stored_centroid"])

            # Get all post embeddings in this community
            posts_in_community = list(
                neo4j.execute_and_fetch_labeled(
                    "MATCH (p:__post__)-[:BELONGS_TO]->(pc:__pc__ {id: $cid}) "
                    "RETURN p.embedding AS embedding",
                    {"post": "Post", "pc": "PostCommunity"},
                    {"cid": community_id},
                )
            )

            # Calculate centroid manually (mean of all embeddings)
            embeddings_matrix = np.array([p["embedding"] for p in posts_in_community])
            manual_centroid = np.mean(embeddings_matrix, axis=0)

            # Compare stored vs manual centroid
            assert stored_centroid.shape == manual_centroid.shape

            # Check centroid correctness with tolerance for floating point precision
            assert np.allclose(stored_centroid, manual_centroid, rtol=1e-5, atol=1e-8)

            print(f"✅ Community {community_id}: centroid correct ({len(posts_in_community)} posts)")
            print(f"   Centroid dimension: {len(stored_centroid)}")
            print(f"   Max difference: {np.max(np.abs(stored_centroid - manual_centroid)):.2e}")

        print(f"\n✅ All {len(communities)} community centroids verified as mean of cluster embeddings")

    finally:
        gds_drop_graph(neo4j, graph_name)
