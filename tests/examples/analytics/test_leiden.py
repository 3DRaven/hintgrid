"""Leiden community detection tests for Neo4j GDS."""

import pytest

from hintgrid.clients.neo4j import Neo4jClient
from hintgrid.utils.coercion import coerce_int

from .conftest import gds_project_undirected


@pytest.mark.integration
@pytest.mark.smoke
def test_leiden_community_detection_basic(neo4j: Neo4jClient, neo4j_id_offset: int) -> None:
    """Basic Leiden community detection test."""
    o = neo4j_id_offset
    user_label = neo4j.label("User")
    graph_name = f"{neo4j.worker_label}-leiden-basic" if neo4j.worker_label else "leiden-basic"
    for i in range(1, 6):
        neo4j.execute_labeled(
            "CREATE (:__user__ {id: $id})", label_map={"user": "User"}, params={"id": o + i}
        )
    for i in range(1, 5):
        for j in range(i + 1, 6):
            neo4j.execute_labeled(
                "\n                MATCH (u1:__user__ {id: $i}), (u2:__user__ {id: $j})\n"
                "                CREATE (u1)-[:FOLLOWS]->(u2), (u2)-[:FOLLOWS]->(u1)\n            ",
                label_map={"user": "User"},
                params={"i": o + i, "j": o + j},
            )
    for i in range(6, 11):
        neo4j.execute_labeled(
            "CREATE (:__user__ {id: $id})", label_map={"user": "User"}, params={"id": o + i}
        )
    for i in range(6, 10):
        for j in range(i + 1, 11):
            neo4j.execute_labeled(
                "\n                MATCH (u1:__user__ {id: $i}), (u2:__user__ {id: $j})\n"
                "                CREATE (u1)-[:FOLLOWS]->(u2), (u2)-[:FOLLOWS]->(u1)\n            ",
                label_map={"user": "User"},
                params={"i": o + i, "j": o + j},
            )
    neo4j.execute_labeled(
        "\n        MATCH (u1:__user__ {id: $a}), (u2:__user__ {id: $b})\n"
        "        CREATE (u1)-[:FOLLOWS]->(u2)\n    ",
        label_map={"user": "User"},
        params={"a": o + 5, "b": o + 6},
    )
    print("✅ Created graph with 10 users, 2 communities")
    gds_project_undirected(neo4j, graph_name, user_label)
    neo4j.execute_labeled(
        "\n        CALL gds.leiden.write('__graph_name__', {\n"
        "            writeProperty: 'cluster_id'\n"
        "        })\n    ",
        ident_map={"graph_name": graph_name},
    )
    clustered_users = neo4j.execute_and_fetch_labeled(
        "\n        MATCH (u:__user__)\n"
        "        WHERE u.cluster_id IS NOT NULL\n"
        "        RETURN count(u) AS count\n    ",
        label_map={"user": "User"},
    )[0]["count"]
    assert clustered_users == 10
    print(f"✅ Leiden: {clustered_users} users clustered")
    community1_ids = list(
        neo4j.execute_and_fetch_labeled(
            "\n        MATCH (u:__user__)\n"
            "        WHERE u.id IN $ids\n"
            "        RETURN DISTINCT u.cluster_id AS cid\n    ",
            label_map={"user": "User"},
            params={"ids": [o + 1, o + 2, o + 3, o + 4, o + 5]},
        )
    )
    assert len(community1_ids) == 1, "Users 1-5 should be in same community"
    community2_ids = list(
        neo4j.execute_and_fetch_labeled(
            "\n        MATCH (u:__user__)\n"
            "        WHERE u.id IN $ids\n"
            "        RETURN DISTINCT u.cluster_id AS cid\n    ",
            label_map={"user": "User"},
            params={"ids": [o + 6, o + 7, o + 8, o + 9, o + 10]},
        )
    )
    assert len(community2_ids) == 1, "Users 6-10 should be in same community"
    assert community1_ids[0]["cid"] != community2_ids[0]["cid"]
    print(f"   Community 1: cluster_id={community1_ids[0]['cid']}")
    print(f"   Community 2: cluster_id={community2_ids[0]['cid']}")
    print("🎉 Leiden successfully separated communities!")
    neo4j.execute_labeled(
        "CALL gds.graph.drop('__graph_name__') YIELD graphName",
        ident_map={"graph_name": graph_name},
    )


@pytest.mark.integration
@pytest.mark.smoke
def test_neo4j_leiden_community_detection_basic(neo4j: Neo4jClient, neo4j_id_offset: int) -> None:
    """Basic Leiden community detection test with write mode."""
    o = neo4j_id_offset
    user_label = neo4j.label("User")
    graph_name = f"{neo4j.worker_label}-leiden-test" if neo4j.worker_label else "leiden-test"
    for i in range(1, 6):
        neo4j.execute_labeled(
            "CREATE (:__user__ {id: $id})", label_map={"user": "User"}, params={"id": o + i}
        )
    for i in range(1, 5):
        for j in range(i + 1, 6):
            neo4j.execute_labeled(
                "\n                MATCH (u1:__user__ {id: $i}), (u2:__user__ {id: $j})\n"
                "                CREATE (u1)-[:FOLLOWS]->(u2), (u2)-[:FOLLOWS]->(u1)\n            ",
                label_map={"user": "User"},
                params={"i": o + i, "j": o + j},
            )
    for i in range(6, 11):
        neo4j.execute_labeled(
            "CREATE (:__user__ {id: $id})", label_map={"user": "User"}, params={"id": o + i}
        )
    for i in range(6, 10):
        for j in range(i + 1, 11):
            neo4j.execute_labeled(
                "\n                MATCH (u1:__user__ {id: $i}), (u2:__user__ {id: $j})\n"
                "                CREATE (u1)-[:FOLLOWS]->(u2), (u2)-[:FOLLOWS]->(u1)\n            ",
                label_map={"user": "User"},
                params={"i": o + i, "j": o + j},
            )
    neo4j.execute_labeled(
        "\n        MATCH (u1:__user__ {id: $a}), (u2:__user__ {id: $b})\n"
        "        CREATE (u1)-[:FOLLOWS]->(u2)\n    ",
        label_map={"user": "User"},
        params={"a": o + 5, "b": o + 6},
    )
    print("✅ Created graph with 10 users, 2 communities")
    gds_project_undirected(neo4j, graph_name, user_label)
    neo4j.execute_labeled(
        "\n        CALL gds.leiden.write('__graph_name__', {\n"
        "            writeProperty: 'cluster_id'\n"
        "        })\n    ",
        ident_map={"graph_name": graph_name},
    )
    result = neo4j.execute_and_fetch_labeled(
        "\n        MATCH (u:__user__)\n"
        "        WHERE u.cluster_id IS NOT NULL\n"
        "        RETURN count(DISTINCT u.cluster_id) AS num_communities\n    ",
        label_map={"user": "User"},
    )[0]
    num_communities = result["num_communities"]
    assert num_communities == 2
    print(f"✅ Leiden detected {num_communities} communities")
    neo4j.execute_labeled(
        "CALL gds.graph.drop('__graph_name__') YIELD graphName",
        ident_map={"graph_name": graph_name},
    )


@pytest.mark.integration
@pytest.mark.smoke
def test_neo4j_leiden_stream_and_write(neo4j: Neo4jClient, neo4j_id_offset: int) -> None:
    """Leiden stream + write round-trip on a named graph."""
    o = neo4j_id_offset
    user_label = neo4j.label("User")
    graph_name = f"{neo4j.worker_label}-my_graph" if neo4j.worker_label else "my_graph"
    neo4j.execute_labeled(
        "\n        CREATE (a:__user__ {id: $id1, name: 'Alice'})\n"
        "        CREATE (b:__user__ {id: $id2, name: 'Bob'})\n"
        "        CREATE (c:__user__ {id: $id3, name: 'Carol'})\n"
        "        CREATE (d:__user__ {id: $id4, name: 'Dave'})\n"
        "        CREATE (a)-[:FOLLOWS]->(b)\n"
        "        CREATE (b)-[:FOLLOWS]->(a)\n"
        "        CREATE (c)-[:FOLLOWS]->(d)\n"
        "        CREATE (d)-[:FOLLOWS]->(c)\n    ",
        label_map={"user": "User"},
        params={"id1": o + 1, "id2": o + 2, "id3": o + 3, "id4": o + 4},
    )
    gds_project_undirected(neo4j, graph_name, user_label)
    streamed = list(
        neo4j.execute_and_fetch_labeled(
            "\n        CALL gds.leiden.stream('__graph_name__')\n"
            "        YIELD nodeId, communityId\n"
            "        RETURN gds.util.asNode(nodeId).name AS name, communityId\n"
            "        ORDER BY communityId ASC\n        ",
            ident_map={"graph_name": graph_name},
        )
    )
    assert len(streamed) == 4
    communities: dict[int, set[str]] = {}
    for row in streamed:
        community_id = coerce_int(row["communityId"])
        name = str(row["name"])
        communities.setdefault(community_id, set()).add(name)
    assert len(communities) == 2
    community_sets = list(communities.values())
    expected = [{"Alice", "Bob"}, {"Carol", "Dave"}]
    assert all(group in expected for group in community_sets)
    result = list(
        neo4j.execute_and_fetch_labeled(
            "\n        CALL gds.leiden.write('__graph_name__', {\n"
            "            writeProperty: 'community_id',\n"
            "            gamma: 1.0,\n"
            "            randomSeed: 42\n"
            "        })\n"
            "        YIELD communityCount, nodePropertiesWritten\n"
            "        RETURN communityCount, nodePropertiesWritten\n        ",
            ident_map={"graph_name": graph_name},
        )
    )
    assert result[0]["communityCount"] == 2
    assert result[0]["nodePropertiesWritten"] == 4
    written = list(
        neo4j.execute_and_fetch_labeled(
            "\n        MATCH (u:__user__)\n"
            "        RETURN u.name AS name, u.community_id AS community_id\n"
            "        ORDER BY u.name\n        ",
            label_map={"user": "User"},
        )
    )
    assert all(row["community_id"] is not None for row in written)
    neo4j.execute_labeled(
        "CALL gds.graph.drop('__graph_name__') YIELD graphName",
        ident_map={"graph_name": graph_name},
    )


@pytest.mark.integration
@pytest.mark.quality
def test_leiden_full_workflow_with_community_nodes(
    neo4j: Neo4jClient, neo4j_id_offset: int
) -> None:
    """Full Leiden workflow: clustering → UserCommunity nodes → BELONGS_TO relationships."""
    o = neo4j_id_offset
    user_label = neo4j.label("User")
    graph_name = f"{neo4j.worker_label}-leiden-full" if neo4j.worker_label else "leiden-full"
    for i in range(1, 5):
        neo4j.execute_labeled(
            "CREATE (:__user__ {id: $id})", label_map={"user": "User"}, params={"id": o + i}
        )
    for i in range(1, 4):
        for j in range(i + 1, 5):
            neo4j.execute_labeled(
                "\n                MATCH (u1:__user__ {id: $i}), (u2:__user__ {id: $j})\n"
                "                CREATE (u1)-[:FOLLOWS]->(u2), (u2)-[:FOLLOWS]->(u1)\n            ",
                label_map={"user": "User"},
                params={"i": o + i, "j": o + j},
            )
    for i in range(5, 9):
        neo4j.execute_labeled(
            "CREATE (:__user__ {id: $id})", label_map={"user": "User"}, params={"id": o + i}
        )
    for i in range(5, 8):
        for j in range(i + 1, 9):
            neo4j.execute_labeled(
                "\n                MATCH (u1:__user__ {id: $i}), (u2:__user__ {id: $j})\n"
                "                CREATE (u1)-[:FOLLOWS]->(u2), (u2)-[:FOLLOWS]->(u1)\n            ",
                label_map={"user": "User"},
                params={"i": o + i, "j": o + j},
            )
    for i in range(9, 13):
        neo4j.execute_labeled(
            "CREATE (:__user__ {id: $id})", label_map={"user": "User"}, params={"id": o + i}
        )
    for i in range(9, 12):
        neo4j.execute_labeled(
            "\n            MATCH (u1:__user__ {id: $i}), (u2:__user__ {id: $j})\n"
            "            CREATE (u1)-[:FOLLOWS]->(u2), (u2)-[:FOLLOWS]->(u1)\n        ",
            label_map={"user": "User"},
            params={"i": o + i, "j": o + i + 1},
        )
    print("✅ Created graph: 12 users in 3 communities")
    gds_project_undirected(neo4j, graph_name, user_label)
    neo4j.execute_labeled(
        "\n        CALL gds.leiden.write('__graph_name__', {\n"
        "            writeProperty: 'cluster_id'\n"
        "        })\n    ",
        ident_map={"graph_name": graph_name},
    )
    num_clusters = neo4j.execute_and_fetch_labeled(
        "\n        MATCH (u:__user__)\n"
        "        WHERE u.cluster_id IS NOT NULL\n"
        "        RETURN count(DISTINCT u.cluster_id) AS count\n    ",
        label_map={"user": "User"},
    )[0]["count"]
    print(f"✅ Leiden clustering → {num_clusters} clusters")
    neo4j.execute_labeled(
        "\n        MATCH (u:__user__)\n"
        "        WHERE u.cluster_id IS NOT NULL\n"
        "        WITH u, u.cluster_id AS cluster_id\n"
        "        MERGE (uc:__uc__ {id: cluster_id})\n"
        "        MERGE (u)-[:BELONGS_TO]->(uc)\n    ",
        label_map={"user": "User", "uc": "UserCommunity"},
    )
    communities_created = neo4j.execute_and_fetch_labeled(
        "\n        MATCH (uc:__uc__)\n        RETURN count(uc) AS count\n    ",
        label_map={"uc": "UserCommunity"},
    )[0]["count"]
    print(f"✅ Created {communities_created} UserCommunity nodes")
    users_without_community = neo4j.execute_and_fetch_labeled(
        "\n        MATCH (u:__user__)\n"
        "        WHERE NOT (u)-[:BELONGS_TO]->(:__uc__)\n"
        "        RETURN count(u) AS count\n    ",
        label_map={"user": "User", "uc": "UserCommunity"},
    )[0]["count"]
    assert users_without_community == 0
    print("🎉 Full Leiden workflow completed successfully!")
    neo4j.execute_labeled(
        "CALL gds.graph.drop('__graph_name__') YIELD graphName",
        ident_map={"graph_name": graph_name},
    )


@pytest.mark.integration
@pytest.mark.quality
def test_leiden_with_weighted_edges(neo4j: Neo4jClient, neo4j_id_offset: int) -> None:
    """Leiden with weighted edges example - uses unweighted for multi-label support."""
    o = neo4j_id_offset
    user_label = neo4j.label("User")
    graph_name = (
        f"{neo4j.worker_label}-leiden-weighted" if neo4j.worker_label else "leiden-weighted"
    )
    for i in range(1, 4):
        neo4j.execute_labeled(
            "CREATE (:__user__ {id: $id})", label_map={"user": "User"}, params={"id": o + i}
        )
    for i in range(1, 3):
        for j in range(i + 1, 4):
            neo4j.execute_labeled(
                "\n                MATCH (u1:__user__ {id: $i}), (u2:__user__ {id: $j})\n"
                "                CREATE (u1)-[:FOLLOWS]->(u2), (u2)-[:FOLLOWS]->(u1)\n            ",
                label_map={"user": "User"},
                params={"i": o + i, "j": o + j},
            )
    for i in range(4, 7):
        neo4j.execute_labeled(
            "CREATE (:__user__ {id: $id})", label_map={"user": "User"}, params={"id": o + i}
        )
    for i in range(4, 6):
        for j in range(i + 1, 7):
            neo4j.execute_labeled(
                "\n                MATCH (u1:__user__ {id: $i}), (u2:__user__ {id: $j})\n"
                "                CREATE (u1)-[:FOLLOWS]->(u2), (u2)-[:FOLLOWS]->(u1)\n            ",
                label_map={"user": "User"},
                params={"i": o + i, "j": o + j},
            )
    neo4j.execute_labeled(
        "\n        MATCH (u1:__user__ {id: $a}), (u2:__user__ {id: $b})\n"
        "        CREATE (u1)-[:FOLLOWS]->(u2)\n    ",
        label_map={"user": "User"},
        params={"a": o + 3, "b": o + 4},
    )
    print("✅ Created graph with 2 groups and weak bridge")
    gds_project_undirected(neo4j, graph_name, user_label)
    neo4j.execute_labeled(
        "\n        CALL gds.leiden.write('__graph_name__', {\n"
        "            writeProperty: 'cluster_id'\n"
        "        })\n    ",
        ident_map={"graph_name": graph_name},
    )
    num_communities = neo4j.execute_and_fetch_labeled(
        "\n        MATCH (u:__user__)\n        RETURN count(DISTINCT u.cluster_id) AS count\n    ",
        label_map={"user": "User"},
    )[0]["count"]
    assert num_communities == 2
    print(f"✅ Leiden detected {num_communities} communities")
    group1_clusters = list(
        neo4j.execute_and_fetch_labeled(
            "\n        MATCH (u:__user__)\n"
            "        WHERE u.id IN $ids\n"
            "        RETURN DISTINCT u.cluster_id AS cid\n    ",
            label_map={"user": "User"},
            params={"ids": [o + 1, o + 2, o + 3]},
        )
    )
    assert len(group1_clusters) == 1
    group2_clusters = list(
        neo4j.execute_and_fetch_labeled(
            "\n        MATCH (u:__user__)\n"
            "        WHERE u.id IN $ids\n"
            "        RETURN DISTINCT u.cluster_id AS cid\n    ",
            label_map={"user": "User"},
            params={"ids": [o + 4, o + 5, o + 6]},
        )
    )
    assert len(group2_clusters) == 1
    assert group1_clusters[0]["cid"] != group2_clusters[0]["cid"]
    print("🎉 Leiden correctly detected communities!")
    neo4j.execute_labeled(
        "CALL gds.graph.drop('__graph_name__') YIELD graphName",
        ident_map={"graph_name": graph_name},
    )
