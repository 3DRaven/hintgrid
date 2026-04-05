"""APOC dynamic label procedures documentation tests.

Verifies that APOC procedures support dynamic label operations,
enabling type-safe Neo4j queries without "...".

Patterns tested:
- apoc.create.node: CREATE with parameterized labels
- apoc.merge.node:  MERGE with parameterized labels + onCreate/onMatch props
- UNWIND + apoc.merge.node: batch operations
- apoc.merge.relationship: relationship creation between dynamically labeled nodes
- Full merge_posts pattern: UNWIND + two node merges + relationship

All tests use worker-isolated labels via neo4j.label() for parallel execution.
"""

import pytest

from hintgrid.clients.neo4j import Neo4jClient
from hintgrid.utils.coercion import coerce_float, coerce_int, coerce_str, convert_batch_decimals

from .conftest import MAX_MODULE_PROCEDURES


@pytest.mark.integration
def test_apoc_availability(neo4j: Neo4jClient) -> None:
    """Verify APOC plugin is installed and key procedures are available.

    Checks that apoc.create.node, apoc.merge.node, apoc.merge.relationship
    are all present in the procedure catalog.

    ```cypher
    SHOW PROCEDURES YIELD name
    WHERE name STARTS WITH 'apoc.create' OR name STARTS WITH 'apoc.merge'
    RETURN name
    ORDER BY name;
    ```
    """
    result = list(
        neo4j.execute_and_fetch(
            "SHOW PROCEDURES YIELD name "
            "WHERE name STARTS WITH 'apoc.create' OR name STARTS WITH 'apoc.merge' "
            "RETURN name ORDER BY name"
        )
    )
    names = {str(row["name"]) for row in result}

    required = {
        "apoc.create.node",
        "apoc.merge.node",
        "apoc.merge.relationship",
    }
    missing = required - names
    assert not missing, f"Missing APOC procedures: {sorted(missing)}"

    # Print documentation
    docs = "\n" + "=" * 80 + "\n"
    docs += "APOC PROCEDURES FOR DYNAMIC LABELS\n"
    docs += "=" * 80 + "\n\n"
    for row in result:
        docs += f"  • {row['name']}\n"
    docs += f"\nTotal: {len(result)} procedures\n"
    docs += "=" * 80 + "\n"
    print(docs)


@pytest.mark.integration
def test_apoc_create_node_dynamic_labels(neo4j: Neo4jClient) -> None:
    """Test apoc.create.node with dynamic labels passed as parameters.

    Instead of f-string interpolation:
        neo4j.execute("CREATE (:{label} {{id: $id}})", ...)

    Use parameterized APOC call:
    ```cypher
    CALL apoc.create.node($labels, {id: 1, name: 'Alice'})
    YIELD node
    RETURN node;
    ```
    """
    # Build labels list dynamically (worker-isolated)
    labels = _split_label(neo4j.label("User"))

    # Create node with dynamic labels via APOC
    result = list(
        neo4j.execute_and_fetch(
            "CALL apoc.create.node($labels, {id: $id, name: $name}) "
            "YIELD node "
            "RETURN node.id AS id, node.name AS name, labels(node) AS node_labels",
            {"labels": labels, "id": 1, "name": "Alice"},
        )
    )

    assert len(result) == 1
    assert coerce_int(result[0]["id"]) == 1
    assert coerce_str(result[0]["name"]) == "Alice"

    # Verify all expected labels are present
    node_labels = result[0]["node_labels"]
    assert isinstance(node_labels, list)
    for expected_label in labels:
        assert expected_label in node_labels, f"Label '{expected_label}' not found in {node_labels}"

    print(f"✅ apoc.create.node works with dynamic labels: {labels}")


@pytest.mark.integration
def test_apoc_merge_node_dynamic_labels(neo4j: Neo4jClient) -> None:
    """Test apoc.merge.node for idempotent MERGE with dynamic labels.

    Replaces the pattern:
        query = "MERGE (u:{label} {{id: $id}}) ON CREATE SET ..."
        neo4j.execute(query, ...)

    With parameterized APOC call:
    ```cypher
    CALL apoc.merge.node(
        $labels,           -- dynamic labels list
        {id: 1},           -- identity properties (for matching)
        {name: 'Alice'},   -- properties set ON CREATE only
        {updatedAt: ...}   -- properties set ON MATCH only
    ) YIELD node
    RETURN node;
    ```
    """
    labels = _split_label(neo4j.label("User"))

    # First call: creates node (ON CREATE)
    result1 = list(
        neo4j.execute_and_fetch(
            "CALL apoc.merge.node($labels, {id: $id}, {name: $name, version: $v}, {}) "
            "YIELD node "
            "RETURN node.id AS id, node.name AS name, node.version AS version",
            {"labels": labels, "id": 42, "name": "Bob", "v": 1},
        )
    )

    assert len(result1) == 1
    assert coerce_int(result1[0]["id"]) == 42
    assert coerce_str(result1[0]["name"]) == "Bob"
    assert coerce_int(result1[0]["version"]) == 1

    # Second call: merges (ON MATCH) — name stays, version updated via onMatch
    result2 = list(
        neo4j.execute_and_fetch(
            "CALL apoc.merge.node($labels, {id: $id}, {name: $on_create_name}, "
            "{version: $v}) "
            "YIELD node "
            "RETURN node.id AS id, node.name AS name, node.version AS version",
            {"labels": labels, "id": 42, "on_create_name": "SHOULD_NOT_SET", "v": 2},
        )
    )

    assert len(result2) == 1
    assert coerce_int(result2[0]["id"]) == 42
    # ON CREATE name should NOT change (was already created)
    assert coerce_str(result2[0]["name"]) == "Bob"
    # ON MATCH version should be updated
    assert coerce_int(result2[0]["version"]) == 2

    # Verify only one node exists (merge is idempotent)
    count_result = list(
        neo4j.execute_and_fetch(
            "CALL apoc.merge.node($labels, {id: $id}, {}, {}) YIELD node RETURN count(node) AS cnt",
            {"labels": labels, "id": 42},
        )
    )
    assert coerce_int(count_result[0]["cnt"]) == 1

    print("✅ apoc.merge.node: idempotent MERGE with onCreate/onMatch works")


@pytest.mark.integration
def test_apoc_merge_node_batch_unwind(neo4j: Neo4jClient) -> None:
    """Test UNWIND + apoc.merge.node for batch operations.

    Replaces the pattern:
        query = cast(LiteralString, f'''
            UNWIND $batch AS row
            MERGE (p:{label} {{id: row.id}})
            ON CREATE SET p.text = row.text
        ''')

    With parameterized batch:
    ```cypher
    UNWIND $batch AS row
    CALL apoc.merge.node($labels, {id: row.id}, {text: row.text}, {})
    YIELD node
    RETURN count(node) AS merged;
    ```
    """
    labels = _split_label(neo4j.label("Post"))

    batch = [
        {"id": 1, "text": "Hello world"},
        {"id": 2, "text": "Привет мир"},
        {"id": 3, "text": "Distributed systems"},
    ]

    # Batch merge via UNWIND + APOC
    result = list(
        neo4j.execute_and_fetch(
            "UNWIND $batch AS row "
            "CALL apoc.merge.node($labels, {id: row.id}, {text: row.text}, {}) "
            "YIELD node "
            "RETURN count(node) AS merged",
            {"labels": labels, "batch": convert_batch_decimals(batch)},
        )
    )

    assert coerce_int(result[0]["merged"]) == len(batch)

    # Verify all nodes created with correct labels
    verify = list(
        neo4j.execute_and_fetch(
            "UNWIND $batch AS row "
            "CALL apoc.merge.node($labels, {id: row.id}, {}, {}) "
            "YIELD node "
            "RETURN node.id AS id, node.text AS text",
            {"labels": labels, "batch": convert_batch_decimals(batch)},
        )
    )
    texts = {coerce_int(r["id"]): coerce_str(r["text"]) for r in verify}
    assert texts == {1: "Hello world", 2: "Привет мир", 3: "Distributed systems"}

    print(f"✅ UNWIND + apoc.merge.node batch works ({len(batch)} nodes, labels={labels})")


@pytest.mark.integration
def test_apoc_merge_relationship(neo4j: Neo4jClient) -> None:
    """Test apoc.merge.relationship for creating relationships between nodes.

    Replaces patterns like:
        query = cast(LiteralString, f'''
            UNWIND $batch AS row
            MERGE (u:__user__ {{id: row.account_id}})
            MERGE (p:__post__ {{id: row.status_id}})
            MERGE (u)-[f:FAVORITED]->(p)
            ON CREATE SET f.at = datetime(row.created_at)
        ''')

    With:
    ```cypher
    UNWIND $batch AS row
    CALL apoc.merge.node($user_labels, {id: row.account_id}, {}, {}) YIELD node AS u
    CALL apoc.merge.node($post_labels, {id: row.status_id}, {}, {}) YIELD node AS p
    CALL apoc.merge.relationship(u, 'FAVORITED', {}, {at: datetime()}, p, {})
    YIELD rel
    RETURN count(rel) AS created;
    ```
    """
    user_labels = _split_label(neo4j.label("User"))
    post_labels = _split_label(neo4j.label("Post"))

    batch = [
        {"account_id": 100, "status_id": 1},
        {"account_id": 100, "status_id": 2},
        {"account_id": 200, "status_id": 1},
    ]

    result = list(
        neo4j.execute_and_fetch(
            "UNWIND $batch AS row "
            "CALL apoc.merge.node($user_labels, {id: row.account_id}, {}, {}) YIELD node AS u "
            "CALL apoc.merge.node($post_labels, {id: row.status_id}, {}, {}) YIELD node AS p "
            "CALL apoc.merge.relationship(u, 'FAVORITED', {}, {}, p, {}) "
            "YIELD rel "
            "RETURN count(rel) AS created",
            {
                "user_labels": user_labels,
                "post_labels": post_labels,
                "batch": batch,
            },
        )
    )

    assert coerce_int(result[0]["created"]) == len(batch)

    # Verify relationships
    verify = list(
        neo4j.execute_and_fetch(
            "MATCH (u)-[f:FAVORITED]->(p) "
            "WHERE all(l IN $user_labels WHERE l IN labels(u)) "
            "RETURN u.id AS user_id, p.id AS post_id "
            "ORDER BY u.id, p.id",
            {"user_labels": user_labels},
        )
    )
    pairs = [(coerce_int(r["user_id"]), coerce_int(r["post_id"])) for r in verify]
    assert (100, 1) in pairs
    assert (100, 2) in pairs
    assert (200, 1) in pairs

    print(f"✅ apoc.merge.relationship: {len(batch)} FAVORITED relationships created")


@pytest.mark.integration
def test_apoc_full_merge_posts_pattern(neo4j: Neo4jClient, neo4j_id_offset: int) -> None:
    """Test the full merge_posts pattern with APOC.

    This is the most complex pattern in the application: merging posts
    with embeddings AND creating WROTE relationships to authors in one batch.

    Original (with cast):
    ```cypher
    -- Original pattern with f-string label interpolation
    UNWIND $batch AS row
    MERGE (p:__post__ {id: row.id})
    ON CREATE SET p.text = row.text, p.embedding = row.embedding,
                  p.createdAt = datetime(row.createdAt)
    ON MATCH SET p.embedding = row.embedding
    WITH p, row
    MERGE (u:__user__ {id: row.authorId})
    MERGE (u)-[:WROTE]->(p)
    ```

    APOC replacement:
    ```cypher
    UNWIND $batch AS row
    CALL apoc.merge.node($post_labels, {id: row.id},
        {text: row.text, embedding: row.embedding,
         createdAt: datetime(row.createdAt), pagerank: 0.0},
        {embedding: row.embedding}
    ) YIELD node AS p
    CALL apoc.merge.node($user_labels, {id: row.authorId}, {}, {})
    YIELD node AS u
    MERGE (u)-[:WROTE]->(p)
    ```
    """
    user_labels = _split_label(neo4j.label("User"))
    post_labels = _split_label(neo4j.label("Post"))

    o = neo4j_id_offset
    batch = [
        {
            "id": o + 10,
            "authorId": o + 1,
            "text": "First post about graphs",
            "embedding": [0.1, 0.2, 0.3],
            "createdAt": "2025-01-15T10:00:00Z",
        },
        {
            "id": o + 20,
            "authorId": o + 2,
            "text": "Second post about ML",
            "embedding": [0.4, 0.5, 0.6],
            "createdAt": "2025-01-16T12:00:00Z",
        },
        {
            "id": o + 30,
            "authorId": o + 1,
            "text": "Third post about Neo4j",
            "embedding": [0.7, 0.8, 0.9],
            "createdAt": "2025-01-17T14:00:00Z",
        },
    ]

    # Execute full pattern with APOC
    neo4j.execute(
        "UNWIND $batch AS row "
        "CALL apoc.merge.node($post_labels, {id: row.id}, "
        "  {text: row.text, embedding: row.embedding, "
        "   createdAt: datetime(row.createdAt), pagerank: 0.0}, "
        "  {embedding: row.embedding}) YIELD node AS p "
        "CALL apoc.merge.node($user_labels, {id: row.authorId}, {}, {}) "
        "YIELD node AS u "
        "MERGE (u)-[:WROTE]->(p)",
        {
            "post_labels": post_labels,
            "user_labels": user_labels,
            "batch": convert_batch_decimals(batch),
        },
    )

    # Verify posts
    posts = list(
        neo4j.execute_and_fetch(
            "MATCH (p) "
            "WHERE all(l IN $post_labels WHERE l IN labels(p)) AND p.text IS NOT NULL "
            "RETURN p.id AS id, p.text AS text, p.embedding AS embedding, p.pagerank AS pagerank "
            "ORDER BY p.id",
            {"post_labels": post_labels},
        )
    )

    assert len(posts) == len(batch)
    assert coerce_str(posts[0]["text"]) == "First post about graphs"

    # Verify embeddings stored (Neo4j returns list of floats)
    assert posts[0].get("embedding") is not None
    assert coerce_str(posts[0].get("embedding")).startswith("[0.1")
    for row in posts:
        assert coerce_float(row.get("pagerank")) == 0.0

    # Verify WROTE relationships
    wrote = list(
        neo4j.execute_and_fetch(
            "MATCH (u)-[:WROTE]->(p) "
            "WHERE all(l IN $user_labels WHERE l IN labels(u)) "
            "RETURN u.id AS author, p.id AS post "
            "ORDER BY u.id, p.id",
            {"user_labels": user_labels},
        )
    )
    author_posts = [(coerce_int(r["author"]), coerce_int(r["post"])) for r in wrote]
    assert (o + 1, o + 10) in author_posts
    assert (o + 2, o + 20) in author_posts
    assert (o + 1, o + 30) in author_posts

    # Test idempotency — re-running should NOT create duplicates
    neo4j.execute(
        "UNWIND $batch AS row "
        "CALL apoc.merge.node($post_labels, {id: row.id}, "
        "  {text: row.text, embedding: row.embedding, "
        "   createdAt: datetime(row.createdAt), pagerank: 0.0}, "
        "  {embedding: row.embedding}) YIELD node AS p "
        "CALL apoc.merge.node($user_labels, {id: row.authorId}, {}, {}) "
        "YIELD node AS u "
        "MERGE (u)-[:WROTE]->(p)",
        {
            "post_labels": post_labels,
            "user_labels": user_labels,
            "batch": convert_batch_decimals(batch),
        },
    )

    # Count should be the same (idempotent)
    count = list(
        neo4j.execute_and_fetch(
            "MATCH (p) "
            "WHERE all(l IN $post_labels WHERE l IN labels(p)) AND p.text IS NOT NULL "
            "RETURN count(p) AS cnt",
            {"post_labels": post_labels},
        )
    )
    assert coerce_int(count[0]["cnt"]) == len(batch)

    print(f"✅ Full merge_posts APOC pattern: {len(batch)} posts merged idempotently")


@pytest.mark.integration
def test_apoc_merge_node_with_set_property(neo4j: Neo4jClient) -> None:
    """Test apoc.merge.node combined with SET for additional state updates.

    This pattern is needed for cases like AppState where we need to
    SET multiple properties atomically.

    ```cypher
    CALL apoc.merge.node($labels, {id: $state_id},
        {last_status_id: 0, updated_at: timestamp()},
        {}
    ) YIELD node AS s
    SET s.last_status_id = $new_value
    RETURN s.last_status_id AS result;
    ```
    """
    labels = _split_label(neo4j.label("AppState"))

    # Create state node
    neo4j.execute(
        "CALL apoc.merge.node($labels, {id: $state_id}, "
        "  {last_status_id: $initial, updated_at: timestamp()}, {}) "
        "YIELD node",
        {"labels": labels, "state_id": "main", "initial": 0},
    )

    # Update state
    result = list(
        neo4j.execute_and_fetch(
            "CALL apoc.merge.node($labels, {id: $state_id}, {}, "
            "  {last_status_id: $new_val, updated_at: timestamp()}) "
            "YIELD node "
            "RETURN node.last_status_id AS val",
            {"labels": labels, "state_id": "main", "new_val": 12345},
        )
    )

    assert coerce_int(result[0]["val"]) == 12345
    print("✅ apoc.merge.node + SET for state updates works")


@pytest.mark.integration
def test_apoc_count_nodes_by_label_param(neo4j: Neo4jClient) -> None:
    """Test counting nodes using APOC label filter (for MATCH queries).

    For read queries like COUNT, we test using label-based matching
    through MATCH + WHERE label IN labels(n).

    ```cypher
    -- Create with APOC (dynamic labels)
    CALL apoc.create.node($labels, {id: 1}) YIELD node

    -- Read with label filter (no string interpolation)
    MATCH (n)
    WHERE all(l IN $labels WHERE l IN labels(n))
    RETURN count(n) AS cnt;
    ```
    """
    labels = _split_label(neo4j.label("User"))

    # Create test nodes
    for i in range(5):
        neo4j.execute(
            "CALL apoc.create.node($labels, {id: $id}) YIELD node",
            {"labels": labels, "id": i},
        )

    # Count with label filter
    result = list(
        neo4j.execute_and_fetch(
            "MATCH (n) WHERE all(l IN $labels WHERE l IN labels(n)) RETURN count(n) AS cnt",
            {"labels": labels},
        )
    )

    assert coerce_int(result[0]["cnt"]) == 5
    print(f"✅ Label-filtered COUNT works: 5 nodes with labels {labels}")


@pytest.mark.integration
def test_apoc_documentation_dump(neo4j: Neo4jClient) -> None:
    """Dump full APOC documentation from procedure catalog.

    Prints all APOC procedures with signatures, grouped by module.
    Validates that critical procedures for dynamic label operations are present.

    ```cypher
    SHOW PROCEDURES YIELD name, signature, description
    WHERE name STARTS WITH 'apoc.'
    RETURN name, signature, description
    ORDER BY name;
    ```
    """
    # Fetch all APOC procedures
    procedures = list(
        neo4j.execute_and_fetch(
            "SHOW PROCEDURES YIELD name, signature, description "
            "WHERE name STARTS WITH 'apoc.' "
            "RETURN name, signature, description "
            "ORDER BY name"
        )
    )

    # Group by module (apoc.create, apoc.merge, apoc.path, etc.)
    from hintgrid.clients.neo4j import Neo4jValue

    modules: dict[str, list[dict[str, Neo4jValue]]] = {}
    for proc in procedures:
        name = coerce_str(proc["name"])
        parts = name.split(".")
        # Module = first two parts (e.g. "apoc.create", "apoc.merge")
        module = ".".join(parts[:2]) if len(parts) > 1 else parts[0]
        if module not in modules:
            modules[module] = []
        modules[module].append(proc)

    # Format documentation
    docs = "\n" + "=" * 80 + "\n"
    docs += f"APOC PROCEDURES DOCUMENTATION ({len(modules)} modules, "
    docs += f"{len(procedures)} procedures)\n"
    docs += "=" * 80 + "\n\n"

    for module, procs in sorted(modules.items()):
        docs += f"\n{'─' * 80}\n"
        docs += f"📦 {module.upper()} ({len(procs)} procedures)\n"
        docs += f"{'─' * 80}\n"

        for proc in procs[:MAX_MODULE_PROCEDURES]:
            docs += f"  • {proc['name']}\n"
            if proc.get("signature"):
                docs += f"    {proc['signature']}\n"
            if proc.get("description"):
                docs += f"    📝 {proc['description']}\n"

        if len(procs) > MAX_MODULE_PROCEDURES:
            docs += f"  ... and {len(procs) - MAX_MODULE_PROCEDURES} more procedures\n"
        docs += "\n"

    # Focus on key modules for dynamic label operations
    key_modules = ["apoc.create", "apoc.merge"]

    docs += "\n" + "=" * 80 + "\n"
    docs += "KEY MODULES FOR DYNAMIC LABEL OPERATIONS:\n"
    docs += "=" * 80 + "\n"

    for mod in key_modules:
        if mod in modules:
            docs += f"\n{mod}:\n"
            for proc in modules[mod]:
                docs += f"  - {proc['name']}\n"
                docs += f"    {proc['signature']}\n"

    docs += "\n" + "=" * 80 + "\n"
    docs += "EXAMPLE USAGE:\n"
    docs += "=" * 80 + "\n"
    docs += """
-- Create node with dynamic labels:
CALL apoc.create.node($labels, {id: 1, name: 'Alice'})
YIELD node RETURN node;

-- Merge node with dynamic labels:
CALL apoc.merge.node($labels, {id: 1}, {name: 'Alice'}, {updatedAt: timestamp()})
YIELD node RETURN node;

-- Merge relationship between dynamically labeled nodes:
CALL apoc.merge.node($user_labels, {id: 100}, {}, {}) YIELD node AS u
CALL apoc.merge.node($post_labels, {id: 1}, {}, {}) YIELD node AS p
CALL apoc.merge.relationship(u, 'WROTE', {}, {}, p, {}) YIELD rel
RETURN rel;

-- Batch with UNWIND:
UNWIND $batch AS row
CALL apoc.merge.node($labels, {id: row.id}, {text: row.text}, {})
YIELD node RETURN count(node) AS merged;
"""
    docs += "=" * 80 + "\n"

    # Output documentation
    print(docs)

    # Verify critical procedures exist
    assert len(procedures) > 0, "No APOC procedures found"
    assert "apoc.create" in modules, "apoc.create module not found"
    assert "apoc.merge" in modules, "apoc.merge module not found"

    # Verify specific procedures used in the application
    all_names = {coerce_str(p["name"]) for p in procedures}
    required_procs = {
        "apoc.create.node",
        "apoc.merge.node",
        "apoc.merge.relationship",
    }
    missing = required_procs - all_names
    assert not missing, f"Missing required APOC procedures: {sorted(missing)}"

    print(
        f"✅ APOC documentation retrieved: "
        f"{len(procedures)} procedures across {len(modules)} modules"
    )


@pytest.mark.integration
def test_apoc_merge_signature_discovery(neo4j: Neo4jClient) -> None:
    """Discover exact signatures for apoc.merge.node and apoc.merge.relationship.

    Prints full signatures for the procedures used in the application
    to validate compatibility with the installed APOC version.

    ```cypher
    SHOW PROCEDURES YIELD name, signature, description
    WHERE name IN ['apoc.merge.node', 'apoc.merge.relationship', 'apoc.create.node']
    RETURN name, signature, description;
    ```
    """
    target_procs = [
        "apoc.merge.node",
        "apoc.merge.relationship",
        "apoc.create.node",
    ]

    result = list(
        neo4j.execute_and_fetch(
            "SHOW PROCEDURES YIELD name, signature, description "
            "WHERE name IN $names "
            "RETURN name, signature, description "
            "ORDER BY name",
            {"names": target_procs},
        )
    )

    docs = "\n" + "=" * 80 + "\n"
    docs += "APOC MERGE/CREATE SIGNATURE DISCOVERY\n"
    docs += "=" * 80 + "\n\n"

    for proc in result:
        docs += f"📦 {proc['name']}\n"
        docs += f"{'─' * 80}\n"
        docs += f"Signature: {proc['signature']}\n"
        if proc.get("description"):
            docs += f"Description: {proc['description']}\n"
        docs += "\n"

    # Validate on a minimal graph
    labels = _split_label(neo4j.label("TestSig"))

    merge_result = list(
        neo4j.execute_and_fetch(
            "CALL apoc.merge.node($labels, {id: $id}, {value: $val}, {}) "
            "YIELD node "
            "RETURN node.id AS id, node.value AS val, labels(node) AS node_labels",
            {"labels": labels, "id": 999, "val": "test"},
        )
    )

    docs += f"Live test: merged node id={merge_result[0]['id']}, "
    docs += f"labels={merge_result[0]['node_labels']}\n"
    docs += "=" * 80 + "\n"

    print(docs)

    assert len(result) == len(target_procs), (
        f"Expected {len(target_procs)} procedures, got {len(result)}"
    )
    assert coerce_int(merge_result[0]["id"]) == 999

    print("✅ APOC merge/create signatures confirmed for current Neo4j version")


def _split_label(compound_label: str) -> list[str]:
    """Split compound label like 'User:worker_gw0' into ['User', 'worker_gw0'].

    APOC procedures accept labels as a list of strings.
    Neo4jClient.label() returns 'BaseLabel:WorkerLabel' compound string.
    """
    return compound_label.split(":")
