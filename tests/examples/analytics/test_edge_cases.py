"""Edge case and error handling tests for Neo4j GDS algorithms."""


import pytest

from hintgrid.clients.neo4j import Neo4jClient

from .conftest import (
    EMBEDDING_DIM_SMALL,
    KMEANS_EMPTY_EXCEPTIONS,
    KMEANS_INVALID_EXCEPTIONS,
    MINIMAL_GRAPH_NODES,
    NEGATIVE_CLUSTERS,
    NODES_PER_GROUP,
    NUM_CLUSTERS,
    SINGLE_NODE,
    THREE_NODES,
    ZERO_CLUSTERS,
    gds_project,
    gds_project_with_embedding,
)


@pytest.mark.integration
@pytest.mark.edge_case
def test_kmeans_empty_graph(neo4j: Neo4jClient) -> None:
    """K-Means on empty graph should handle error correctly."""
    with pytest.raises(
        KMEANS_EMPTY_EXCEPTIONS,
        match=r"(?i)(no|empty|not found|exist)",
    ) as exc_info:
        list(
            neo4j.execute_and_fetch("""
            CALL gds.kmeans.write('nonexistent-graph', {
                nodeProperty: 'embedding',
                k: 2,
                writeProperty: 'cluster_id'
            })
            YIELD nodePropertiesWritten
            RETURN count(*) AS count
        """)
        )

    print(f"✅ K-Means correctly handles empty graph: {exc_info.value}")


@pytest.mark.integration
@pytest.mark.edge_case
def test_kmeans_single_node(neo4j: Neo4jClient) -> None:
    """K-Means with one node should clamp clusters to 1."""
    user_label = neo4j.label("User")
    graph_name = f"{neo4j.worker_label}-kmeans-single" if neo4j.worker_label else "kmeans-single"

    neo4j.execute_labeled(
        "CREATE (:__label__ {id: 1, embedding: [0.1, 0.2]})",
        label_map={"label": user_label.split(":")[0]},
    )

    # Project graph with embedding
    gds_project_with_embedding(neo4j, graph_name, user_label)

    result = list(
        neo4j.execute_and_fetch(
            """
            CALL gds.kmeans.write($graph_name, {
                nodeProperty: 'embedding',
                k: $k,
                writeProperty: 'cluster_id'
            })
            YIELD nodePropertiesWritten
            RETURN nodePropertiesWritten
            """,
            {"graph_name": graph_name, "k": NUM_CLUSTERS},
        )
    )

    assert result[0]["nodePropertiesWritten"] == SINGLE_NODE
    num_clusters = neo4j.execute_and_fetch_labeled(
        """
        MATCH (u:__label__)
        WHERE u.cluster_id IS NOT NULL
        RETURN count(DISTINCT u.cluster_id) AS num_clusters
        """,
        label_map={"label": user_label.split(":")[0]},
    )[0]["num_clusters"]
    assert num_clusters == SINGLE_NODE

    # Cleanup
    neo4j.execute(
        "CALL gds.graph.drop($graph_name) YIELD graphName",
        {"graph_name": graph_name},
    )


@pytest.mark.integration
@pytest.mark.edge_case
def test_kmeans_more_clusters_than_nodes(neo4j: Neo4jClient) -> None:
    """K-Means should clamp clusters when k > node count."""
    user_label = neo4j.label("User")
    graph_name = f"{neo4j.worker_label}-kmeans-too-many" if neo4j.worker_label else "kmeans-too-many"

    # Create 3 nodes, request 5 clusters
    for i in range(1, THREE_NODES + 1):
        neo4j.execute_labeled(
            "CREATE (:__label__ {id: $id, embedding: $emb})",
            label_map={"label": user_label.split(":")[0]},
            params={"id": i, "emb": [i * 0.1, i * 0.2]},
        )

    # Project with embedding
    gds_project_with_embedding(neo4j, graph_name, user_label)

    result = list(
        neo4j.execute_and_fetch(
            """
            CALL gds.kmeans.write($graph_name, {
                nodeProperty: 'embedding',
                k: $k,
                writeProperty: 'cluster_id'
            })
            YIELD nodePropertiesWritten
            RETURN nodePropertiesWritten
            """,
            {"graph_name": graph_name, "k": NODES_PER_GROUP},
        )
    )

    assert result[0]["nodePropertiesWritten"] == THREE_NODES
    num_clusters = neo4j.execute_and_fetch_labeled(
        """
        MATCH (u:__label__)
        WHERE u.cluster_id IS NOT NULL
        RETURN count(DISTINCT u.cluster_id) AS num_clusters
        """,
        label_map={"label": user_label.split(":")[0]},
    )[0]["num_clusters"]
    assert num_clusters == THREE_NODES

    # Cleanup
    neo4j.execute(
        "CALL gds.graph.drop($graph_name) YIELD graphName",
        {"graph_name": graph_name},
    )


@pytest.mark.integration
@pytest.mark.edge_case
def test_fastRP_disconnected_graph(neo4j: Neo4jClient) -> None:
    """FastRP on completely disconnected graph (isolated nodes)."""
    user_label = neo4j.label("User")
    graph_name = (
        f"{neo4j.worker_label}-fastrp-disconnected"
        if neo4j.worker_label
        else "fastrp-disconnected"
    )

    # Create 3 isolated nodes (no edges)
    for i in range(1, THREE_NODES + 1):
        neo4j.execute_labeled(
            "CREATE (:__label__ {id: $id})",
            label_map={"label": user_label.split(":")[0]},
            params={"id": i},
        )

    # For disconnected nodes, use cypher projection that returns empty relationships
    neo4j.execute_labeled(
        """
        CALL gds.graph.project.cypher(
            $graph_name,
            'MATCH (n:__label__) RETURN id(n) AS id',
            'MATCH (n:__label__)-[r:FOLLOWS]->(m:__label__) RETURN id(n) AS source, id(m) AS target'
        )
        """,
        label_map={"label": user_label.split(":")[0]},
        params={"graph_name": graph_name},
    )

    result = list(
        neo4j.execute_and_fetch(
            """
            CALL gds.fastRP.write($graph_name, {
                embeddingDimension: $dim,
                iterationWeights: [0.0, 1.0, 1.0],
                writeProperty: 'embedding'
            })
            YIELD nodePropertiesWritten
            RETURN nodePropertiesWritten AS count
            """,
            {"graph_name": graph_name, "dim": EMBEDDING_DIM_SMALL},
        )
    )

    assert result[0]["count"] == THREE_NODES

    emb_count = neo4j.execute_and_fetch_labeled(
        """
        MATCH (u:__label__)
        WHERE u.embedding IS NOT NULL
        RETURN count(u) AS count
        """,
        label_map={"label": user_label.split(":")[0]},
    )[0]["count"]
    assert emb_count == THREE_NODES

    # Cleanup
    neo4j.execute(
        "CALL gds.graph.drop($graph_name) YIELD graphName",
        {"graph_name": graph_name},
    )


@pytest.mark.integration
@pytest.mark.edge_case
def test_fastRP_single_node(neo4j: Neo4jClient) -> None:
    """FastRP on a graph with one node should produce one embedding."""
    user_label = neo4j.label("User")
    graph_name = f"{neo4j.worker_label}-fastRP-single" if neo4j.worker_label else "fastRP-single"

    neo4j.execute_labeled(
        "CREATE (:__label__ {id: 1})",
        label_map={"label": user_label.split(":")[0]},
    )

    # Project single node (no relationships)
    neo4j.execute_labeled(
        """
        CALL gds.graph.project.cypher(
            $graph_name,
            'MATCH (n:__label__) RETURN id(n) AS id',
            'MATCH (n:__label__)-[r:FOLLOWS]->(m:__label__) RETURN id(n) AS source, id(m) AS target'
        )
        """,
        label_map={"label": user_label.split(":")[0]},
        params={"graph_name": graph_name},
    )

    result = list(
        neo4j.execute_and_fetch(
            """
            CALL gds.fastRP.write($graph_name, {
                embeddingDimension: $dim,
                iterationWeights: [0.0, 1.0, 1.0],
                writeProperty: 'embedding'
            })
            YIELD nodePropertiesWritten
            RETURN nodePropertiesWritten
            """,
            {"graph_name": graph_name, "dim": EMBEDDING_DIM_SMALL},
        )
    )

    assert result[0]["nodePropertiesWritten"] == SINGLE_NODE
    emb_count = neo4j.execute_and_fetch_labeled(
        """
        MATCH (u:__label__)
        WHERE u.embedding IS NOT NULL
        RETURN count(u) AS count
        """,
        label_map={"label": user_label.split(":")[0]},
    )[0]["count"]
    assert emb_count == SINGLE_NODE

    # Cleanup
    neo4j.execute(
        "CALL gds.graph.drop($graph_name) YIELD graphName",
        {"graph_name": graph_name},
    )


@pytest.mark.integration
@pytest.mark.edge_case
def test_fastRP_minimum_viable_graph(neo4j: Neo4jClient) -> None:
    """FastRP minimum viable graph: 2 nodes + 1 edge."""
    user_label = neo4j.label("User")
    graph_name = f"{neo4j.worker_label}-fastRP-min" if neo4j.worker_label else "fastRP-min"

    neo4j.execute_labeled(
        "CREATE (:__label__ {id: 1})",
        label_map={"label": user_label.split(":")[0]},
    )
    neo4j.execute_labeled(
        "CREATE (:__label__ {id: 2})",
        label_map={"label": user_label.split(":")[0]},
    )
    neo4j.execute_labeled(
        """
        MATCH (u1:__label__ {id: 1}), (u2:__label__ {id: 2})
        CREATE (u1)-[:FOLLOWS]->(u2)
        """,
        label_map={"label": user_label.split(":")[0]},
    )

    # Project graph
    gds_project(neo4j, graph_name, user_label)

    result = list(
        neo4j.execute_and_fetch(
            """
            CALL gds.fastRP.write($graph_name, {
                embeddingDimension: $dim,
                iterationWeights: [0.0, 1.0, 1.0],
                writeProperty: 'embedding'
            })
            YIELD nodePropertiesWritten
            RETURN nodePropertiesWritten AS count
            """,
            {"graph_name": graph_name, "dim": EMBEDDING_DIM_SMALL},
        )
    )

    assert result[0]["count"] == MINIMAL_GRAPH_NODES

    emb_count = neo4j.execute_and_fetch_labeled(
        """
        MATCH (u:__label__)
        WHERE u.embedding IS NOT NULL
        RETURN count(u) AS count
        """,
        label_map={"label": user_label.split(":")[0]},
    )[0]["count"]

    assert emb_count == MINIMAL_GRAPH_NODES
    print(f"✅ FastRP works on minimal graph ({MINIMAL_GRAPH_NODES} nodes + edge)")

    # Cleanup
    neo4j.execute(
        "CALL gds.graph.drop($graph_name) YIELD graphName",
        {"graph_name": graph_name},
    )


@pytest.mark.integration
@pytest.mark.edge_case
def test_kmeans_zero_clusters(neo4j: Neo4jClient) -> None:
    """K-Means with n_clusters=0 should raise error."""
    user_label = neo4j.label("User")
    graph_name = f"{neo4j.worker_label}-kmeans-zero" if neo4j.worker_label else "kmeans-zero"

    neo4j.execute_labeled(
        "CREATE (:__label__ {id: 1, embedding: [0.1, 0.2]})",
        label_map={"label": user_label.split(":")[0]},
    )

    # Project with embedding
    gds_project_with_embedding(neo4j, graph_name, user_label)

    with pytest.raises(KMEANS_INVALID_EXCEPTIONS) as exc_info:
        list(
            neo4j.execute_and_fetch(
                """
                CALL gds.kmeans.write($graph_name, {
                    nodeProperty: 'embedding',
                    k: $k,
                    writeProperty: 'cluster_id'
                })
                YIELD nodePropertiesWritten
                RETURN nodePropertiesWritten
                """,
                {"graph_name": graph_name, "k": ZERO_CLUSTERS},
            )
        )

    print(f"✅ K-Means correctly rejects n_clusters=0: {type(exc_info.value).__name__}")

    # Cleanup
    neo4j.execute(
        "CALL gds.graph.drop($graph_name) YIELD graphName",
        {"graph_name": graph_name},
    )


@pytest.mark.integration
@pytest.mark.edge_case
def test_kmeans_negative_clusters(neo4j: Neo4jClient) -> None:
    """K-Means with negative n_clusters should raise error."""
    user_label = neo4j.label("User")
    graph_name = f"{neo4j.worker_label}-kmeans-negative" if neo4j.worker_label else "kmeans-negative"

    neo4j.execute_labeled(
        "CREATE (:__label__ {id: 1, embedding: [0.1, 0.2]})",
        label_map={"label": user_label.split(":")[0]},
    )

    # Project with embedding
    gds_project_with_embedding(neo4j, graph_name, user_label)

    with pytest.raises(KMEANS_INVALID_EXCEPTIONS) as exc_info:
        list(
            neo4j.execute_and_fetch(
                """
                CALL gds.kmeans.write($graph_name, {
                    nodeProperty: 'embedding',
                    k: $k,
                    writeProperty: 'cluster_id'
                })
                YIELD nodePropertiesWritten
                RETURN nodePropertiesWritten
                """,
                {"graph_name": graph_name, "k": NEGATIVE_CLUSTERS},
            )
        )

    print(f"✅ K-Means correctly rejects n_clusters=-1: {type(exc_info.value).__name__}")

    # Cleanup
    neo4j.execute(
        "CALL gds.graph.drop($graph_name) YIELD graphName",
        {"graph_name": graph_name},
    )


@pytest.mark.integration
@pytest.mark.edge_case
def test_fastrp_invalid_embedding_dimension(neo4j: Neo4jClient) -> None:
    """FastRP with invalid embedding dimension should raise an error."""
    user_label = neo4j.label("User")
    graph_name = (
        f"{neo4j.worker_label}-fastrp-invalid-dim" if neo4j.worker_label else "fastrp-invalid-dim"
    )

    neo4j.execute_labeled(
        "CREATE (:__label__ {id: 1})-[:FOLLOWS]->(:__label__ {id: 2})",
        label_map={"label": user_label.split(":")[0]},
    )

    # Project graph
    gds_project(neo4j, graph_name, user_label)

    with pytest.raises(Exception) as exc_info:
        list(
            neo4j.execute_and_fetch(
                """
                CALL gds.fastRP.write($graph_name, {
                    embeddingDimension: 0,
                    iterationWeights: [0.0, 1.0, 1.0],
                    writeProperty: 'embedding'
                })
                YIELD nodePropertiesWritten
                RETURN nodePropertiesWritten
                """,
                {"graph_name": graph_name},
            )
        )

    print(f"✅ FastRP correctly rejects embeddingDimension=0: {type(exc_info.value).__name__}")

    # Cleanup
    neo4j.execute(
        "CALL gds.graph.drop($graph_name) YIELD graphName",
        {"graph_name": graph_name},
    )
