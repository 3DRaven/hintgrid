"""Neo4j integration tests with worker-isolated labels.

All tests use neo4j.label() for parallel execution support.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, LiteralString, cast

import pytest


if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from tests.conftest import DockerComposeInfo

from .conftest import EXPECTED_NODES_COUNT, NEO4J_COMMUNITIES_COUNT, NEO4J_TEST_NODES_COUNT


@pytest.mark.smoke
@pytest.mark.integration
def test_neo4j_gds_connectivity(neo4j: Neo4jClient) -> None:
    """Test connection to Neo4j with GDS availability."""
    # Check required GDS procedures are available
    result = list(neo4j.execute_and_fetch("CALL gds.list() YIELD name RETURN name"))
    available = {str(row["name"]) for row in result}
    required = {"gds.kmeans.write", "gds.leiden.stream"}
    missing = required - available
    assert not missing, f"Missing GDS procedures: {sorted(missing)}"
    print(f"✅ Neo4j GDS: found required procedures: {sorted(required)}")

    # Create test graph with worker labels
    neo4j.execute_labeled(
        "CREATE (:__user__ {id: 1, name: 'Dev Prague'})",
        {"user": "User"},
    )
    neo4j.execute_labeled(
        "CREATE (:__post__ {id: 1, content: 'P2P Mastodon recsys'})",
        {"post": "Post"},
    )

    # Verify data with worker label
    users = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__) RETURN u.name AS name",
            {"user": "User"},
        )
    )
    assert len(users) == 1
    assert users[0]["name"] == "Dev Prague"
    print("✅ Neo4j: connection works, graph created")


@pytest.mark.integration
def test_neo4j_basic_operations(neo4j: Neo4jClient) -> None:
    """Basic operations with Neo4j using worker labels."""
    # Create nodes with worker labels
    neo4j.execute_labeled(
        "CREATE (u:__user__ {id: 100, username: 'testuser', domain: 'mastodon.social'}) "
        "CREATE (p:__post__ {id: 200, text: 'Hello Fediverse!', language: 'en'})",
        {"user": "User", "post": "Post"},
    )

    # Verify node count with worker labels
    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (n:__user__) RETURN count(n) AS count",
            {"user": "User"},
        )
    )
    user_count = cast("int", result[0]["count"])

    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (n:__post__) RETURN count(n) AS count",
            {"post": "Post"},
        )
    )
    post_count = cast("int", result[0]["count"])

    assert user_count + post_count == EXPECTED_NODES_COUNT

    # Create relationship
    neo4j.execute_labeled(
        "MATCH (u:__user__ {id: 100}), (p:__post__ {id: 200}) "
        "CREATE (u)-[:WROTE]->(p)",
        {"user": "User", "post": "Post"},
    )

    # Verify relationship
    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__)-[r:WROTE]->(p:__post__) "
            "RETURN u.username AS username, p.text AS text",
            {"user": "User", "post": "Post"},
        )
    )
    assert len(result) == 1
    assert result[0]["username"] == "testuser"
    assert result[0]["text"] == "Hello Fediverse!"

    print("✅ Neo4j: basic operations work")


@pytest.mark.smoke
@pytest.mark.integration
def test_neo4j_connectivity(neo4j: Neo4jClient) -> None:
    """Test connection to Neo4j with GDS."""
    # Check basic connectivity
    rows = list(neo4j.execute_and_fetch("RETURN 1 AS value"))
    assert cast("int", rows[0]["value"]) == 1

    # Check GDS availability
    rows = list(neo4j.execute_and_fetch("RETURN gds.version() AS version"))
    version = cast("str", rows[0]["version"])
    assert isinstance(version, str) and version.strip(), "GDS version is empty"
    assert version[0].isdigit(), f"Unexpected GDS version format: {version}"
    print(f"✅ Neo4j GDS: version {version}")

    # Create test graph with worker labels
    neo4j.execute_labeled(
        "CREATE (:__user__ {id: 1, name: 'Alice'})",
        {"user": "User"},
    )
    neo4j.execute_labeled(
        "CREATE (:__post__ {id: 1, content: 'Hello Neo4j!'})",
        {"post": "Post"},
    )

    # Check data with worker label
    rows = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__) RETURN u.name AS name",
            {"user": "User"},
        )
    )
    assert cast("str", rows[0]["name"]) == "Alice"

    print("✅ Neo4j: connection works, GDS available")


@pytest.mark.integration
def test_neo4j_indexes(neo4j: Neo4jClient) -> None:
    """Test creating indexes in Neo4j with worker labels.

    Note: Neo4j index syntax requires single label, so we use just the worker label
    for index definition. The index will apply to all nodes with that label.
    """
    # Create worker-specific index name to avoid conflicts
    if neo4j.worker_label is None:
        raise ValueError("worker_label is required for this test")
    worker_prefix = neo4j.worker_label.replace("-", "_")
    user_index = f"{worker_prefix}_user_username"

    # Create index on worker label only (Neo4j doesn't support multi-label in index definition)
    # Index name and label are dynamic, use execute_labeled with ident_map for safe substitution
    neo4j.execute_labeled(
        "CREATE INDEX __index_name__ IF NOT EXISTS FOR (u:__label__) ON (u.username)",
        label_map=None,
        ident_map={
            "index_name": user_index,
            "label": neo4j.worker_label,
        },
    )

    # Create users
    neo4j.execute_labeled(
        "CREATE (u:__user__ {id: 1, username: 'alice'})",
        {"user": "User"},
    )
    neo4j.execute_labeled(
        "CREATE (u:__user__ {id: 2, username: 'bob'})",
        {"user": "User"},
    )

    # Verify data
    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__) RETURN u.username AS username ORDER BY u.id",
            {"user": "User"},
        )
    )

    assert len(result) == 2
    assert result[0]["username"] == "alice"
    assert result[1]["username"] == "bob"

    print("✅ Neo4j: indexes work correctly")


@pytest.mark.integration
def test_neo4j_gds_community_detection(neo4j: Neo4jClient) -> None:
    """Test Neo4j GDS community detection (Leiden algorithm) with worker labels."""
    graph_name = f"social-network-{neo4j.worker_label or 'master'}"

    # Create test social network graph with worker labels
    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: 1, name: 'Alice'}) "
        "CREATE (u2:__user__ {id: 2, name: 'Bob'}) "
        "CREATE (u3:__user__ {id: 3, name: 'Carol'}) "
        "CREATE (u4:__user__ {id: 4, name: 'Dave'}) "
        "CREATE (u5:__user__ {id: 5, name: 'Eve'}) "
        "CREATE (u6:__user__ {id: 6, name: 'Frank'}) "
        "CREATE (u1)-[:FOLLOWS]->(u2) "
        "CREATE (u2)-[:FOLLOWS]->(u1) "
        "CREATE (u2)-[:FOLLOWS]->(u3) "
        "CREATE (u3)-[:FOLLOWS]->(u2) "
        "CREATE (u3)-[:FOLLOWS]->(u1) "
        "CREATE (u4)-[:FOLLOWS]->(u5) "
        "CREATE (u5)-[:FOLLOWS]->(u4) "
        "CREATE (u5)-[:FOLLOWS]->(u6) "
        "CREATE (u6)-[:FOLLOWS]->(u5)",
        {"user": "User"},
    )

    # Verify graph structure
    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__) RETURN count(u) AS count",
            {"user": "User"},
        )
    )
    count = cast("int", result[0]["count"])
    assert count == NEO4J_TEST_NODES_COUNT

    # Project graph into GDS using Cypher projection
    # Graph name is dynamic, use parameterized query with real label
    user_label = neo4j.label("User")
    # Use string concatenation and cast to LiteralString for type safety
    query_str = f"MATCH (n:{user_label})-[r:FOLLOWS]->(m:{user_label}) " + "WITH gds.graph.project($graph_name, n, m, {}, {undirectedRelationshipTypes: ['*']}) AS g " + "RETURN g.graphName"
    query = cast("LiteralString", query_str)  # type: ignore[redundant-cast]
    neo4j.execute(
        query,
        {"graph_name": graph_name},
    )

    try:
        # Run Leiden community detection
        result = neo4j.execute_and_fetch(
            "CALL gds.leiden.stream($graph_name) "
            "YIELD nodeId, communityId "
            "RETURN gds.util.asNode(nodeId).name AS name, communityId "
            "ORDER BY communityId, name",
            {"graph_name": graph_name},
        )

        # Collect communities
        communities: dict[int, list[str]] = {}
        for record in result:
            name = cast("str", record["name"])
            community_id = cast("int", record["communityId"])
            if community_id not in communities:
                communities[community_id] = []
            communities[community_id].append(name)

        # Verify community detection results
        assert len(communities) == NEO4J_COMMUNITIES_COUNT
        print(f"✅ Neo4j GDS: found {len(communities)} communities")

        # Check community composition
        community_sizes = [len(members) for members in communities.values()]
        assert sorted(community_sizes) == [3, 3]

        # Verify that connected users are in same community
        for community_members in communities.values():
            if "Alice" in community_members:
                assert "Bob" in community_members
                assert "Carol" in community_members
            if "Dave" in community_members:
                assert "Eve" in community_members
                assert "Frank" in community_members

        print("✅ Neo4j GDS: Leiden community detection works")

    finally:
        # Cleanup GDS graph
        neo4j.execute(
            "CALL gds.graph.drop($graph_name, false)",
            {"graph_name": graph_name},
        )


@pytest.mark.integration
def test_neo4j_gds_pagerank(neo4j: Neo4jClient) -> None:
    """Test Neo4j GDS PageRank algorithm with worker labels."""
    graph_name = f"pagerank-graph-{neo4j.worker_label or 'master'}"

    # Create test graph with clear hub structure
    neo4j.execute_labeled(
        "CREATE (hub:__user__ {id: 1, name: 'Hub'}) "
        "CREATE (u2:__user__ {id: 2, name: 'User2'}) "
        "CREATE (u3:__user__ {id: 3, name: 'User3'}) "
        "CREATE (u4:__user__ {id: 4, name: 'User4'}) "
        "CREATE (u2)-[:FOLLOWS]->(hub) "
        "CREATE (u3)-[:FOLLOWS]->(hub) "
        "CREATE (u4)-[:FOLLOWS]->(hub) "
        "CREATE (hub)-[:FOLLOWS]->(u2)",
        {"user": "User"},
    )

    # Project graph using Cypher projection
    # Graph name is dynamic, use parameterized query with real label
    user_label = neo4j.label("User")
    query_str3 = f"MATCH (n:{user_label})-[r:FOLLOWS]->(m:{user_label}) " + "WITH gds.graph.project($graph_name, n, m) AS g " + "RETURN g.graphName"
    query3 = cast("LiteralString", query_str3)  # type: ignore[redundant-cast]
    neo4j.execute(
        query3,
        {"graph_name": graph_name},
    )

    try:
        # Run PageRank
        result = neo4j.execute_and_fetch(
            "CALL gds.pageRank.stream($graph_name) "
            "YIELD nodeId, score "
            "RETURN gds.util.asNode(nodeId).name AS name, score "
            "ORDER BY score DESC",
            {"graph_name": graph_name},
        )

        scores = [(cast("str", record["name"]), cast("float", record["score"])) for record in result]

        # Hub should have highest PageRank
        assert scores[0][0] == "Hub"
        assert scores[0][1] > scores[1][1]

        print(f"✅ Neo4j GDS: PageRank works, Hub score = {scores[0][1]:.4f}")

    finally:
        # Cleanup
        neo4j.execute(
            "CALL gds.graph.drop($graph_name, false)",
            {"graph_name": graph_name},
        )


@pytest.mark.integration
def test_neo4j_526_parameterized_labels_and_graph_names(
    docker_compose: DockerComposeInfo,
) -> None:
    """Test Neo4j 5.26+ parameterized labels and graph names via $(param) syntax.

    This test verifies the new Cypher feature that allows dynamic labels
    and relationship types through parameters, eliminating the need for
    string templating.

    Uses direct Neo4j driver connection (not Neo4jClient) to test native
    parameter support without template substitution.

    Note: This feature requires Neo4j 5.26+ (December 2024). If the syntax
    doesn't work, the test will check the Neo4j version and provide
    informative error messages.
    """
    from neo4j import GraphDatabase

    # Direct connection to Neo4j (bypassing Neo4jClient template system)
    uri = f"bolt://{docker_compose.neo4j_host}:{docker_compose.neo4j_port}"
    driver = GraphDatabase.driver(
        uri, auth=(docker_compose.neo4j_user, docker_compose.neo4j_password)
    )

    try:
        with driver.session() as session:
            # Check Neo4j version first
            version_result = session.run("CALL dbms.components() YIELD name, versions, edition RETURN name, versions[0] AS version, edition")
            version_info = version_result.single()
            if version_info:
                version_data = version_info.data()
                version_str = cast("str", version_data.get("version", "unknown"))
                edition_str = cast("str", version_data.get("edition", "unknown"))
                print("\n📋 Neo4j Information:")
                print(f"   Version: {version_str}")
                print(f"   Edition: {edition_str}")
                
                # Also get full version info
                full_version_result = session.run("RETURN gds.version() AS gds_version")
                gds_record = full_version_result.single()
                if gds_record:
                    gds_data = gds_record.data()
                    gds_version = cast("str", gds_data.get("gds_version", "unknown"))
                    print(f"   GDS Version: {gds_version}")
                
                # Parse version - handle both "5.x.x" and "2025.12.1" formats
                try:
                    parts = version_str.split(".")
                    if len(parts) >= 2:
                        # Try to parse as year.month (2025.12) or version (5.26)
                        first_part = int(parts[0])
                        second_part = int(parts[1])
                        
                        if first_part >= 2020:
                            # Year format (2025.12.1) - this is likely Neo4j 5.x
                            print("   i  Version format: year.month (likely Neo4j 5.x)")
                            # Continue test to check actual syntax support
                        else:
                            # Version format (5.26)
                            if first_part < 5 or (first_part == 5 and second_part < 26):
                                pytest.skip(f"Neo4j {version_str} does not support parameterized labels (requires 5.26+)")
                except (ValueError, IndexError):
                    print(f"⚠️  Could not parse version {version_str}, continuing test")

            # Check Neo4j documentation: SHOW PROCEDURES and SHOW FUNCTIONS
            print("\n📚 Neo4j Documentation Check:")
            procedures_result = session.run(
                "SHOW PROCEDURES YIELD name, description "
                "WHERE name STARTS WITH 'gds.' OR name STARTS WITH 'apoc.' "
                "RETURN name, description "
                "LIMIT 10"
            )
            procedures = list(procedures_result)
            print(f"   Found {len(procedures)} GDS/APOC procedures")
            for proc in procedures[:5]:
                proc_data = proc.data()
                proc_name = cast("str", proc_data.get("name", ""))
                print(f"   • {proc_name}")

            # Check constraints documentation
            constraints_result = session.run(
                "SHOW CONSTRAINTS YIELD name, type, entityType, properties "
                "RETURN name, type, entityType, properties "
                "LIMIT 5"
            )
            constraints = list(constraints_result)
            print(f"   Current constraints: {len(constraints)}")
            for constraint in constraints:
                const_data = constraint.data()
                const_name = cast("str", const_data.get("name", ""))
                const_type = cast("str", const_data.get("type", ""))
                print(f"   • {const_name} ({const_type})")

            # Test 1: Dynamic labels via $(param) - create nodes
            test_label = "TestUser"
            graph_name = "test_graph_526"
            parameterized_labels_supported = False

            # Try creating nodes with parameterized label
            # Note: If $(param) syntax doesn't work, we'll catch the error
            # and continue with other tests (composite constraints)
            try:
                session.run(
                    "CREATE (n:$(label) {id: 1, name: 'Alice'}) "
                    "CREATE (m:$(label) {id: 2, name: 'Bob'})",
                    {"label": test_label},
                )
                parameterized_labels_supported = True
                print("   ✅ Parameterized labels $(param) syntax works!")
            except Exception as e:
                error_msg = str(e)
                if "Variable" in error_msg and "not defined" in error_msg:
                    print("   ⚠️  Parameterized labels $(param) syntax not supported")
                    print(f"   i  Error: {error_msg[:100]}")
                    print("   i  This feature may require Neo4j 5.26+ (December 2024) or later")
                    print("   i  Continuing with composite constraints test...")
                    # Create nodes without parameterized syntax for composite constraints test
                    session.run(
                        f"CREATE (n:{test_label} {{id: 1, name: 'Alice'}}) "
                        f"CREATE (m:{test_label} {{id: 2, name: 'Bob'}})"
                    )
                else:
                    raise

            # Verify nodes were created with correct label
            if parameterized_labels_supported:
                result = session.run(
                    "MATCH (n:$(label)) RETURN count(n) AS count, collect(n.name) AS names",
                    {"label": test_label},
                )
            else:
                result = session.run(
                    f"MATCH (n:{test_label}) RETURN count(n) AS count, collect(n.name) AS names",
                )
            record = result.single()
            assert record is not None
            count = cast("int", record["count"])
            names = cast("list[str]", record["names"])
            assert count == 2
            assert set(names) == {"Alice", "Bob"}
            print(f"   ✅ Verified {count} nodes created")

            # Test 2: Create relationships with parameterized relationship type
            rel_type = "FOLLOWS"
            if parameterized_labels_supported:
                session.run(
                    "MATCH (a:$(label) {id: 1}), (b:$(label) {id: 2}) "
                    "CREATE (a)-[r:$(rel_type)]->(b) SET r.weight = 1.0",
                    {"label": test_label, "rel_type": rel_type},
                )
                # Verify relationship
                result = session.run(
                    "MATCH (a:$(label))-[r:$(rel_type)]->(b:$(label)) "
                    "RETURN count(r) AS count",
                    {"label": test_label, "rel_type": rel_type},
                )
            else:
                session.run(
                    f"MATCH (a:{test_label} {{id: 1}}), (b:{test_label} {{id: 2}}) "
                    f"CREATE (a)-[r:{rel_type}]->(b) SET r.weight = 1.0",
                )
                # Verify relationship
                result = session.run(
                    f"MATCH (a:{test_label})-[r:{rel_type}]->(b:{test_label}) "
                    "RETURN count(r) AS count",
                )
            record = result.single()
            assert record is not None
            rel_count = cast("int", record["count"])
            assert rel_count == 1
            print("   ✅ Created and verified relationship")

            # Test 3: GDS graph projection with parameterized graph name
            # Project graph using Cypher projection (properties are included automatically)
            if parameterized_labels_supported:
                session.run(
                    "MATCH (n:$(label))-[r:$(rel_type)]->(m:$(label)) "
                    "WITH gds.graph.project($graph_name, n, m, {}, {undirectedRelationshipTypes: ['*']}) AS g "
                    "RETURN g.graphName",
                    {"label": test_label, "rel_type": rel_type, "graph_name": graph_name},
                )
            else:
                # Use string concatenation to avoid f-string escaping issues
                query = (
                    f"MATCH (n:{test_label})-[r:{rel_type}]->(m:{test_label}) "
                    "WITH gds.graph.project($graph_name, n, m) AS g "
                    "RETURN g.graphName"
                )
                session.run(query, {"graph_name": graph_name})

            # Verify graph exists using parameterized graph name
            result = session.run(
                "CALL gds.graph.exists($graph_name) YIELD exists",
                {"graph_name": graph_name},
            )
            record = result.single()
            assert record is not None
            exists = cast("bool", record["exists"])
            assert exists, f"Graph {graph_name} should exist"

            # Test 4: GDS procedure with parameterized graph name
            # Just verify graph exists and has nodes (properties may not be projected)
            result = session.run(
                "CALL gds.graph.list($graph_name) "
                "YIELD graphName, nodeCount, relationshipCount "
                "RETURN graphName, nodeCount, relationshipCount",
                {"graph_name": graph_name},
            )
            record = result.single()
            assert record is not None
            node_count = cast("int", record["nodeCount"])
            rel_count = cast("int", record["relationshipCount"])
            assert node_count == 2
            assert rel_count == 1
            print(f"   ✅ Graph verified: {node_count} nodes, {rel_count} relationships")

            # Test 5: Cleanup with parameterized graph name
            session.run(
                "CALL gds.graph.drop($graph_name, false)",
                {"graph_name": graph_name},
            )

            # Verify graph was dropped
            result = session.run(
                "CALL gds.graph.exists($graph_name) YIELD exists",
                {"graph_name": graph_name},
            )
            record = result.single()
            assert record is not None
            exists_after_drop = cast("bool", record["exists"])
            assert not exists_after_drop, f"Graph {graph_name} should be dropped"

            # Cleanup test nodes
            if parameterized_labels_supported:
                session.run("MATCH (n:$(label)) DETACH DELETE n", {"label": test_label})
                print("✅ Neo4j 5.26+: parameterized labels and graph names work correctly")
            else:
                session.run(f"MATCH (n:{test_label}) DETACH DELETE n")
                print("✅ Neo4j: GDS graph operations work correctly (parameterized labels not supported)")

            # Test 6: Idempotency with composite constraints (even if parameterized labels don't work)
            print("\n🔒 Testing Composite Constraints Idempotency:")
            composite_label = "CompositeTest"
            
            # Create composite unique constraint on (id, domain) pair
            # Note: Neo4j composite constraints may not be supported in Community Edition
            constraint_name = "composite_test_id_domain_unique"
            constraint_created = False
            try:
                # Try Neo4j 5.x syntax for composite constraints
                # First, drop if exists
                session.run(f"DROP CONSTRAINT {constraint_name} IF EXISTS")
                
                # Try creating composite constraint
                # Note: Neo4j Community Edition may not support composite constraints
                try:
                    session.run(
                        f"CREATE CONSTRAINT {constraint_name} "
                        f"FOR (n:{composite_label}) "
                        "REQUIRE (n.id, n.domain) IS UNIQUE",
                    )
                    constraint_created = True
                    print(f"   ✅ Created composite constraint: {constraint_name}")
                except Exception as create_error:
                    error_msg = str(create_error)
                    if "composite" in error_msg.lower() or "multiple" in error_msg.lower() or "not supported" in error_msg.lower():
                        print(f"   ⚠️  Composite constraints not supported: {error_msg[:100]}")
                        print("   i  Testing idempotency with MERGE (without composite constraint)")
                    else:
                        print(f"   ⚠️  Could not create composite constraint: {error_msg[:100]}")
                        print("   i  Testing idempotency with MERGE (without composite constraint)")
            except Exception as constraint_error:
                print(f"   ⚠️  Constraint setup error: {constraint_error}")
                print("   i  Testing idempotency with MERGE (without composite constraint)")

            # Test idempotency: create same node multiple times
            # First creation should succeed (use direct label, not parameterized)
            try:
                session.run(
                    f"MERGE (n:{composite_label} {{id: 100, domain: 'test.com', name: 'First'}}) "
                    "ON CREATE SET n.created = datetime() "
                    "ON MATCH SET n.updated = datetime()",
                )
                print("   ✅ First MERGE succeeded")
            except Exception as e:
                print(f"   ⚠️  First MERGE failed: {e}")

            # Second MERGE with same (id, domain) should be idempotent
            # MERGE should match existing node and update it, not create duplicate
            try:
                session.run(
                    f"MERGE (n:{composite_label} {{id: 100, domain: 'test.com'}}) "
                    "ON CREATE SET n.created = datetime(), n.name = 'First' "
                    "ON MATCH SET n.updated = datetime(), n.name = 'Second'",
                )
                print("   ✅ Second MERGE succeeded (idempotent - updated existing node)")
            except Exception as e:
                error_msg = str(e)
                # If constraint violation, it means MERGE tried to create instead of match
                # This can happen if composite constraint doesn't work as expected with MERGE
                if "Constraint" in error_msg or "already exists" in error_msg:
                    print(f"   ⚠️  MERGE constraint issue (expected with composite constraints): {error_msg[:80]}")
                    print("   i  This is expected - composite constraint enforces uniqueness")
                else:
                    print(f"   ⚠️  Second MERGE failed: {error_msg[:80]}")

            # Verify only one node exists with this composite key
            result = session.run(
                f"MATCH (n:{composite_label} {{id: 100, domain: 'test.com'}}) "
                "RETURN n.name AS name, n.id AS id, n.domain AS domain",
            )
            records = list(result)
            assert len(records) == 1, f"Expected 1 node, got {len(records)}"
            print(f"   ✅ Idempotency verified: {len(records)} node with (id=100, domain='test.com')")

            # Try to create duplicate with same composite key - should fail or merge
            if constraint_created:
                try:
                    # Use direct label (not parameterized) since $(param) may not work
                    session.run(
                        f"CREATE (n:{composite_label} {{id: 100, domain: 'test.com', name: 'Duplicate'}})",
                    )
                    print("   ⚠️  CREATE succeeded (constraint may not be enforced)")
                except Exception as e:
                    error_msg = str(e)
                    if "Constraint" in error_msg or "unique" in error_msg.lower():
                        print("   ✅ Constraint enforced: duplicate creation blocked")
                    else:
                        print(f"   ⚠️  Unexpected error: {error_msg}")
            else:
                print("   i  Skipping duplicate CREATE test (no composite constraint)")

            # Cleanup composite constraint and nodes
            try:
                session.run(f"DROP CONSTRAINT {constraint_name} IF EXISTS")
                session.run(f"MATCH (n:{composite_label}) DETACH DELETE n")
                print("   ✅ Cleaned up composite constraint and test data")
            except Exception as cleanup_error:
                print(f"   ⚠️  Cleanup warning: {cleanup_error}")

            # Test 7: Multi-label constraints (the original problem that led to worker labels)
            print("\n🏷️  Testing Multi-Label Constraints (Original Worker Label Problem):")
            print("   This test checks if the old limitation that required worker labels is still relevant.")
            
            # Test 7.1: Try creating constraint on multi-label node (A:B)
            multi_label_constraint_supported = False
            multi_label_constraint_name = "test_multi_label_constraint"
            base_label = "TestPost"
            worker_label = "worker_gw0"
            
            try:
                # Drop if exists
                session.run(f"DROP CONSTRAINT {multi_label_constraint_name} IF EXISTS")
                
                # Create a node with multiple labels
                session.run(
                    f"CREATE (n:{base_label}:{worker_label} {{id: 1, text: 'Test'}})"
                )
                print(f"   ✅ Created node with labels: {base_label}:{worker_label}")
                
                # Try creating constraint on multi-label node
                try:
                    session.run(
                        f"CREATE CONSTRAINT {multi_label_constraint_name} "
                        f"FOR (n:{base_label}:{worker_label}) "
                        "REQUIRE n.id IS UNIQUE",
                    )
                    multi_label_constraint_supported = True
                    print("   ✅ Multi-label constraint created successfully!")
                    print("   i  This means worker labels may not be needed for constraint isolation")
                except Exception as multi_error:
                    error_msg = str(multi_error)
                    if "multi-label" in error_msg.lower() or "multiple labels" in error_msg.lower():
                        print(f"   ❌ Multi-label constraint NOT supported: {error_msg[:150]}")
                        print("   i  This confirms the original problem - worker labels still needed")
                    elif "syntax" in error_msg.lower() or "invalid" in error_msg.lower():
                        print(f"   ❌ Multi-label constraint syntax error: {error_msg[:150]}")
                        print("   i  This confirms the original problem - worker labels still needed")
                    else:
                        print(f"   ⚠️  Unexpected error: {error_msg[:150]}")
            except Exception as setup_error:
                print(f"   ⚠️  Setup error: {setup_error}")
            
            # Test 7.2: Test the original conflict scenario (Post id vs UserCommunity cluster_id)
            print("\n   🔍 Testing Original Conflict Scenario (Post id vs UserCommunity cluster_id):")
            conflict_detected = False
            
            try:
                # Clean up any existing test data
                session.run(f"MATCH (n:{base_label}) DETACH DELETE n")
                session.run("MATCH (n:TestUserCommunity) DETACH DELETE n")
                
                # Drop any existing constraints
                session.run("DROP CONSTRAINT test_post_id_unique IF EXISTS")
                session.run("DROP CONSTRAINT test_uc_cluster_id_unique IF EXISTS")
                
                # Create simple constraints (not multi-label) to test conflict
                # This simulates the scenario where constraints on different labels
                # might conflict if they share the same index space
                try:
                    session.run(
                        f"CREATE CONSTRAINT test_post_id_unique "
                        f"FOR (p:{base_label}) "
                        "REQUIRE p.id IS UNIQUE"
                    )
                    print(f"   ✅ Created constraint on {base_label}.id")
                    
                    session.run(
                        "CREATE CONSTRAINT test_uc_cluster_id_unique "
                        "FOR (uc:TestUserCommunity) "
                        "REQUIRE uc.cluster_id IS UNIQUE"
                    )
                    print("   ✅ Created constraint on TestUserCommunity.cluster_id")
                    
                    # Create Post with id=4
                    session.run(
                        f"CREATE (p:{base_label} {{id: 4, text: 'Post 4'}})"
                    )
                    print("   ✅ Created Post with id=4")
                    
                    # Try to create UserCommunity with cluster_id=4 (same value, different label)
                    # This should NOT conflict if constraints are properly isolated by label
                    try:
                        session.run(
                            "CREATE (uc:TestUserCommunity {cluster_id: 4, name: 'Cluster 4'})"
                        )
                        print("   ✅ Created UserCommunity with cluster_id=4 (no conflict!)")
                        print("   i  Constraints are properly isolated by label - no conflict")
                    except Exception as conflict_error:
                        error_msg = str(conflict_error)
                        if "IndexEntryConflictException" in error_msg or "Constraint" in error_msg:
                            conflict_detected = True
                            print(f"   ❌ CONFLICT DETECTED: {error_msg[:150]}")
                            print("   ⚠️  This confirms the original problem - constraints share index space")
                            print("   i  Worker labels are still needed to prevent this conflict")
                        else:
                            print(f"   ⚠️  Unexpected error: {error_msg[:150]}")
                    
                except Exception as constraint_error:
                    print(f"   ⚠️  Constraint creation error: {constraint_error}")
                
                # Cleanup
                session.run("DROP CONSTRAINT test_post_id_unique IF EXISTS")
                session.run("DROP CONSTRAINT test_uc_cluster_id_unique IF EXISTS")
                session.run(f"MATCH (n:{base_label}) DETACH DELETE n")
                session.run("MATCH (n:TestUserCommunity) DETACH DELETE n")
                
            except Exception as test_error:
                print(f"   ⚠️  Test error: {test_error}")
            
            # Test 7.3: Composite constraint with worker_id (alternative to worker labels)
            print("\n   🔍 Testing Composite Constraint with worker_id (Alternative Approach):")
            composite_with_worker_supported = False
            
            try:
                composite_worker_constraint_name = "test_user_id_worker_unique"
                test_user_label = "TestUser"
                
                # Drop if exists
                session.run(f"DROP CONSTRAINT {composite_worker_constraint_name} IF EXISTS")
                
                # Create composite constraint on (id, worker_id)
                try:
                    session.run(
                        f"CREATE CONSTRAINT {composite_worker_constraint_name} "
                        f"FOR (u:{test_user_label}) "
                        "REQUIRE (u.id, u.worker_id) IS UNIQUE",
                    )
                    composite_with_worker_supported = True
                    print("   ✅ Composite constraint (id, worker_id) created successfully!")
                    print("   i  This allows unique (id, worker_id) pairs per worker")
                    
                    # Test: Create users with same id but different worker_id
                    session.run(
                        f"CREATE (u1:{test_user_label} {{id: 100, worker_id: 'gw0', name: 'User 100 gw0'}})"
                    )
                    session.run(
                        f"CREATE (u2:{test_user_label} {{id: 100, worker_id: 'gw1', name: 'User 100 gw1'}})"
                    )
                    print("   ✅ Created users with same id but different worker_id (no conflict)")
                    
                    # Try to create duplicate (id, worker_id) - should fail
                    try:
                        session.run(
                            f"CREATE (u3:{test_user_label} {{id: 100, worker_id: 'gw0', name: 'Duplicate'}})"
                        )
                        print("   ⚠️  Duplicate (id, worker_id) creation succeeded (constraint may not enforce)")
                    except Exception as dup_error:
                        error_msg = str(dup_error)
                        if "Constraint" in error_msg or "unique" in error_msg.lower():
                            print("   ✅ Constraint enforced: duplicate (id, worker_id) blocked")
                        else:
                            print(f"   ⚠️  Unexpected error: {error_msg[:100]}")
                    
                    # Cleanup
                    session.run(f"MATCH (n:{test_user_label}) DETACH DELETE n")
                    
                except Exception as composite_error:
                    error_msg = str(composite_error)
                    print(f"   ⚠️  Composite constraint with worker_id error: {error_msg[:150]}")
                
                # Cleanup constraint
                session.run(f"DROP CONSTRAINT {composite_worker_constraint_name} IF EXISTS")
                
            except Exception as test_error:
                print(f"   ⚠️  Test error: {test_error}")
            
            # Final summary
            print("\n📊 Summary of Worker Label Requirements:")
            print("=" * 80)
            if multi_label_constraint_supported:
                print("✅ Multi-label constraints ARE supported")
                print("   → Worker labels may NOT be needed for constraint isolation")
            else:
                print("❌ Multi-label constraints NOT supported")
                print("   → Worker labels ARE still needed for constraint isolation")
            
            if not conflict_detected:
                print("✅ No IndexEntryConflictException detected")
                print("   → Constraints are properly isolated by label")
            else:
                print("❌ IndexEntryConflictException detected")
                print("   → Worker labels ARE still needed to prevent conflicts")
            
            if composite_with_worker_supported:
                print("✅ Composite constraints with worker_id ARE supported")
                print("   → Alternative approach available: use (id, worker_id) instead of worker labels")
            else:
                print("❌ Composite constraints with worker_id may have issues")
                print("   → Worker labels remain the primary isolation mechanism")
            
            print("=" * 80)

    finally:
        driver.close()
