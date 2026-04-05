"""Neo4j storage and retrieval tests for embeddings.

All tests use worker-isolated labels via neo4j.label() for parallel execution.
"""


import pytest

from hintgrid.clients.neo4j import Neo4jClient
from hintgrid.utils.coercion import coerce_float, coerce_int, coerce_str, convert_dict_to_neo4j_value
from tests.conftest import EmbeddingServiceConfig

from .conftest import (
    EMBEDDING_DIM,
    NUM_POSTS,
    NUM_POST_CLUSTERS,
    PostInput,
    gds_drop_graph,
    gds_project_with_embedding,
    generate_embeddings_via_service,
)


@pytest.mark.integration
def test_store_posts_with_embeddings(
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_posts: list[PostInput],
) -> None:
    """Test storing posts with REAL FastText embeddings in Neo4j."""

    # Generate embeddings via FastText service
    posts_with_embeddings = generate_embeddings_via_service(
        fasttext_embedding_service, sample_posts
    )

    # Create posts with embeddings
    for post in posts_with_embeddings:
        neo4j.execute_labeled(
            "CREATE (p:__post__ {"
            "id: $id, "
            "text: $text, "
            "embedding: $embedding"
            "})",
            {"post": "Post"},
            {"id": post["id"], "text": post["text"], "embedding": post["embedding"]},
        )

    # Verify posts created
    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) RETURN count(p) AS count",
            {"post": "Post"},
        )
    )

    assert result[0]["count"] == NUM_POSTS, f"Expected {NUM_POSTS} posts"

    # Verify embeddings stored
    embeddings = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) "
            "WHERE p.embedding IS NOT NULL "
            "RETURN p.id AS id, size(p.embedding) AS dim "
            "ORDER BY p.id",
            {"post": "Post"},
        )
    )

    assert len(embeddings) == NUM_POSTS
    assert all(e["dim"] == EMBEDDING_DIM for e in embeddings)

    print(f"✅ Stored {len(embeddings)} posts with REAL FastText {EMBEDDING_DIM}-dim embeddings")


@pytest.mark.integration
def test_litellm_to_neo4j_pipeline(
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_posts: list[PostInput],
) -> None:
    """Test complete pipeline: fetch posts, generate embeddings via LiteLLM, store in Neo4j."""
    from litellm import embedding

    post_label = neo4j.label("Post")
    config = fasttext_embedding_service

    # Step 1: Generate embeddings using LiteLLM
    posts_with_embeddings: list[dict[str, object]] = []
    for post in sample_posts:
        response = embedding(
            model=config["model"],
            input=[post["text"]],
            api_base=config["api_base"],
        )
        emb = response["data"][0]["embedding"]
        posts_with_embeddings.append({"id": post["id"], "text": post["text"], "embedding": emb})

    # Step 2: Store in Neo4j
    for post_dict in posts_with_embeddings:
        neo4j.execute_labeled(
            "CREATE (p:__post__ {"
            "id: $id, "
            "text: $text, "
            "embedding: $embedding"
            "})",
            {"post": "Post"},
            convert_dict_to_neo4j_value({
                "id": post_dict["id"],
                "text": post_dict["text"],
                "embedding": post_dict["embedding"],
            }),
        )

    # Step 3: Verify storage
    stored_posts = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) "
            "WHERE p.embedding IS NOT NULL "
            "RETURN count(p) AS count",
            {"post": "Post"},
        )
    )

    assert stored_posts[0]["count"] == NUM_POSTS

    # Step 4: Run clustering using Cypher projection
    graph_name = f"kmeans-pipeline-{neo4j.worker_label}"
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

        # Step 5: Verify clustering
        clusters = list(
            neo4j.execute_and_fetch_labeled(
                "MATCH (p:__post__) RETURN DISTINCT p.cluster_id AS cluster_id",
                {"post": "Post"},
            )
        )

        assert len(clusters) == NUM_POST_CLUSTERS

        print(f"✅ Complete pipeline: {NUM_POSTS} posts → LiteLLM → Neo4j → {len(clusters)} clusters")

    finally:
        gds_drop_graph(neo4j, graph_name)


@pytest.mark.integration
def test_user_interests_based_on_post_clusters(
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_posts: list[PostInput],
) -> None:
    """Test learning user interests based on favourited posts and their clusters.

    Uses REAL FastText embeddings. Simulates UserCommunity → PostCommunity INTERESTED_IN.
    """
    post_label = neo4j.label("Post")

    # 1. Generate embeddings via FastText service
    posts_with_embeddings = generate_embeddings_via_service(
        fasttext_embedding_service, sample_posts
    )

    # 2. Create posts with embeddings FIRST
    for post in posts_with_embeddings:
        neo4j.execute_labeled(
            "CREATE (:__post__ {id: $id, text: $text, embedding: $emb})",
            {"post": "Post"},
            {"id": post["id"], "text": post["text"], "emb": post["embedding"]},
        )

    # 3. Cluster posts using Cypher projection
    graph_name = f"kmeans-user-interests-{neo4j.worker_label or 'master'}"
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

        # 5. NOW create users (after clustering is done)
        for user_id in range(1, 4):
            neo4j.execute_labeled(
                "CREATE (:__user__ {id: $id})",
                {"user": "User"},
                {"id": user_id},
            )

        # 6. Create UserCommunity
        neo4j.execute_labeled(
            "CREATE (:__uc__ {id: 1})",
            {"uc": "UserCommunity"},
        )
        neo4j.execute_labeled(
            "MATCH (u:__user__), (uc:__uc__ {id: 1}) "
            "CREATE (u)-[:BELONGS_TO]->(uc)",
            {"user": "User", "uc": "UserCommunity"},
        )

        # Users favourite tech posts (ids 1-3)
        for user_id in range(1, 4):
            for post_id in range(1, 4):
                neo4j.execute_labeled(
                    "MATCH (u:__user__ {id: $uid}), (p:__post__ {id: $pid}) "
                    "CREATE (u)-[:FAVOURITED]->(p)",
                    {"user": "User", "post": "Post"},
                    {"uid": user_id, "pid": post_id},
                )

        # Learn interests: UserCommunity → PostCommunity
        result = list(
            neo4j.execute_and_fetch_labeled(
                "MATCH (u:__user__)-[:BELONGS_TO]->(uc:__uc__), "
                "(u)-[:FAVOURITED]->(p:__post__)-[:BELONGS_TO]->(pc:__pc__) "
                "WITH uc, pc, count(*) AS favourites "
                "MERGE (uc)-[i:INTERESTED_IN]->(pc) "
                "SET i.score = toFloat(favourites) / 10.0, "
                "i.based_on = favourites "
                "RETURN uc.id AS uc_id, pc.id AS pc_id, i.score AS score, i.based_on AS favourites",
                {"user": "User", "uc": "UserCommunity", "post": "Post", "pc": "PostCommunity"},
            )
        )

        # Verify INTERESTED_IN relationship created
        assert len(result) >= 1, "Should create at least one INTERESTED_IN relationship"

        # Verify score calculation
        for r in result:
            expected_score = coerce_float(r["favourites"]) / 10.0
            score = coerce_float(r["score"])
            assert abs(score - expected_score) < 0.01

        print(f"✅ Learned {len(result)} INTERESTED_IN relationships from REAL FastText embeddings:")
        for r in result:
            uc_id = coerce_int(r["uc_id"])
            pc_id = coerce_int(r["pc_id"])
            score = coerce_float(r["score"])
            favourites = coerce_int(r["favourites"])
            print(
                f"   UserCommunity {uc_id} → PostCommunity {pc_id}: "
                f"score={score:.2f} (based on {favourites} favourites)"
            )

    finally:
        gds_drop_graph(neo4j, graph_name)


@pytest.mark.integration
def test_feed_generation_with_post_clusters(
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_posts: list[PostInput],
) -> None:
    """Test personalized feed generation using post clusters with REAL FastText embeddings.

    User belongs to UserCommunity → INTERESTED_IN → PostCommunity → Posts.
    """
    post_label = neo4j.label("Post")

    # 1. Generate embeddings via FastText service
    posts_with_embeddings = generate_embeddings_via_service(
        fasttext_embedding_service, sample_posts
    )

    # 2. Create posts with embeddings FIRST
    for post in posts_with_embeddings:
        neo4j.execute_labeled(
            "CREATE (:__post__ {id: $id, text: $text, embedding: $emb, createdAt: datetime()})",
            {"post": "Post"},
            {"id": post["id"], "text": post["text"], "emb": post["embedding"]},
        )

    # 3. Cluster posts into communities using Cypher projection
    graph_name = f"kmeans-feed-posts-{neo4j.worker_label or 'master'}"
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

        # 5. NOW create user and communities (after clustering is done)
        neo4j.execute_labeled(
            "CREATE (:__user__ {id: 1})",
            {"user": "User"},
        )
        neo4j.execute_labeled(
            "CREATE (:__uc__ {id: 1})",
            {"uc": "UserCommunity"},
        )
        neo4j.execute_labeled(
            "MATCH (u:__user__ {id: 1}), (uc:__uc__ {id: 1}) "
            "CREATE (u)-[:BELONGS_TO]->(uc)",
            {"user": "User", "uc": "UserCommunity"},
        )

        # Create INTERESTED_IN relationships
        neo4j.execute_labeled(
            "MATCH (uc:__uc__ {id: 1}), (pc:__pc__) "
            "MERGE (uc)-[i:INTERESTED_IN]->(pc) "
            "SET i.score = 0.8 "
            "RETURN count(i) AS count",
            {"uc": "UserCommunity", "pc": "PostCommunity"},
        )

        # Generate personalized feed
        feed = list(
            neo4j.execute_and_fetch_labeled(
                "MATCH (u:__user__ {id: 1})-[:BELONGS_TO]->(uc:__uc__) "
                "MATCH (uc)-[i:INTERESTED_IN]->(pc:__pc__) "
                "MATCH (p:__post__)-[:BELONGS_TO]->(pc) "
                "WHERE NOT (u)-[:FAVOURITED]->(p) "
                "AND NOT (u)-[:WROTE]->(p) "
                "RETURN p.id AS post_id, "
                "p.text AS text, "
                "i.score AS interest_score, "
                "pc.id AS community_id "
                "ORDER BY interest_score DESC "
                "LIMIT 5",
                {"user": "User", "uc": "UserCommunity", "post": "Post", "pc": "PostCommunity"},
            )
        )

        # Verify feed generated
        assert len(feed) >= 3, f"Should generate at least 3 posts in feed, got {len(feed)}"

        # Verify all posts have interest_score
        assert all(coerce_float(f["interest_score"]) > 0 for f in feed)

        print(f"✅ Generated personalized feed with {len(feed)} posts from REAL FastText embeddings:")
        for f in feed:
            post_id = coerce_int(f["post_id"])
            community_id = coerce_int(f["community_id"])
            score = coerce_float(f["interest_score"])
            text = coerce_str(f["text"])
            print(f"   Post {post_id} (community {community_id}): score={score:.2f} - {text[:50]}...")

    finally:
        gds_drop_graph(neo4j, graph_name)
