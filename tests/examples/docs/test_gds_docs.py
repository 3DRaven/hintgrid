"""GDS documentation dump tests.

All tests use worker-isolated labels via neo4j.label() for parallel execution.
"""


import pytest

from hintgrid.clients.neo4j import Neo4jClient

from .conftest import (
    MAX_DOCS_PARAMS,
    MAX_MODULE_PROCEDURES,
    gds_drop_graph,
    gds_project_with_embedding,
)


@pytest.mark.integration
def test_kmeans_documentation_dump(neo4j: Neo4jClient) -> None:
    """Test dumping K-Means documentation from the Neo4j procedure catalog.

    Prints procedure signatures and validates availability.
    """
    # Fetch K-Means signatures from the procedure catalog
    all_procs = list(
        neo4j.execute_and_fetch("""
        SHOW PROCEDURES YIELD name, signature
        RETURN name, signature;
    """)
    )

    kmeans_procs = [p for p in all_procs if "gds.kmeans" in str(p["name"]).lower()]

    # Format documentation
    docs = "\n" + "=" * 80 + "\n"
    docs += "KMEANS PROCEDURES DOCUMENTATION\n"
    docs += "=" * 80 + "\n\n"

    for proc in kmeans_procs:
        docs += f"{proc['name']}:\n{proc['signature']}\n\n"

    docs += "=" * 80 + "\n"
    docs += "EXAMPLE USAGE:\n"
    docs += "=" * 80 + "\n"
    docs += """
CALL gds.kmeans.write('graph-name', {
    nodeProperty: 'embedding',
    k: 2,
    writeProperty: 'cluster_id'
})
YIELD nodePropertiesWritten
RETURN nodePropertiesWritten;
"""
    docs += "=" * 80 + "\n"

    # Output documentation
    print(docs)

    # Verify we got documentation
    assert len(kmeans_procs) > 0, "GDS K-Means procedures were not returned"
    print(f"✅ GDS K-Means documentation retrieved ({len(kmeans_procs)} procedures)")


@pytest.mark.integration
def test_fastrp_documentation_dump(neo4j: Neo4jClient) -> None:
    """Test dumping full FastRP documentation.

    Prints documentation and validates availability.
    """
    # Fetch full FastRP documentation
    help_info = list(
        neo4j.execute_and_fetch("""
        SHOW PROCEDURES YIELD name, signature
        WHERE name STARTS WITH 'gds.fastRP'
        RETURN name, signature AS value
        ORDER BY name;
    """)
    )

    # Format documentation
    docs = "\n" + "=" * 80 + "\n"
    docs += "GDS.FASTRP DOCUMENTATION\n"
    docs += "=" * 80 + "\n\n"

    for info in help_info[:MAX_DOCS_PARAMS]:  # First 30 parameters
        docs += f"{info['name']}:\n{info['value']}\n\n"

    if len(help_info) > MAX_DOCS_PARAMS:
        docs += f"... and {len(help_info) - MAX_DOCS_PARAMS} more parameters\n\n"

    docs += "=" * 80 + "\n"
    docs += "EXAMPLE USAGE:\n"
    docs += "=" * 80 + "\n"
    docs += """
CALL gds.fastRP.write('graph-name', {
    embeddingDimension: 16,
    iterationWeights: [0.0, 1.0, 1.0],
    writeProperty: 'embedding'
})
YIELD nodePropertiesWritten
RETURN nodePropertiesWritten;
"""
    docs += "=" * 80 + "\n"

    # Output documentation
    print(docs)

    # Verify we got documentation
    assert len(help_info) > 0, "FastRP documentation was not returned"
    print(f"✅ FastRP documentation retrieved ({len(help_info)} entries)")


@pytest.mark.integration
def test_all_gds_procedures_dump(neo4j: Neo4jClient) -> None:
    """Test to output ALL GDS procedures.

    Outputs list through print and passes successfully.
    """
    # Fetch all procedures
    procedures = list(
        neo4j.execute_and_fetch("""
        SHOW PROCEDURES YIELD name, signature
        WHERE name STARTS WITH 'gds.'
        RETURN name, signature
        ORDER BY name;
    """)
    )

    # Group by module
    from hintgrid.clients.neo4j import Neo4jValue
    modules: dict[str, list[dict[str, Neo4jValue]]] = {}
    for proc in procedures:
        name = str(proc["name"])
        parts = name.split(".")
        module = (parts[1] if len(parts) > 1 else parts[0]).lower()
        if module not in modules:
            modules[module] = []
        modules[module].append(proc)

    # Format output
    docs = "\n" + "=" * 80 + "\n"
    docs += f"AVAILABLE GDS MODULES ({len(modules)} modules, {len(procedures)} procedures)\n"
    docs += "=" * 80 + "\n\n"

    for module, procs in sorted(modules.items()):
        docs += f"\n{'─' * 80}\n"
        docs += f"📦 {module.upper()} ({len(procs)} procedures)\n"
        docs += f"{'─' * 80}\n"

        for proc in procs[:MAX_MODULE_PROCEDURES]:  # First 5 procedures per module
            docs += f"  • {proc['name']}\n"
            if proc.get("signature"):
                docs += f"    {proc['signature']}\n"

        if len(procs) > MAX_MODULE_PROCEDURES:
            docs += f"  ... and {len(procs) - MAX_MODULE_PROCEDURES} more procedures\n"
        docs += "\n"

    # Key modules to display
    interesting_modules = ["fastrp", "kmeans", "pagerank", "leiden"]

    docs += "\n" + "=" * 80 + "\n"
    docs += "KEY MODULES FOR GRAPH ANALYTICS:\n"
    docs += "=" * 80 + "\n"

    for mod in interesting_modules:
        if mod in modules:
            docs += f"\n{mod}:\n"
            for proc in modules[mod]:
                docs += f"  - {proc['name']}\n"

    # Output documentation
    print(docs)

    # Verify we got procedures
    assert len(procedures) > 0, "GDS procedures were not returned"
    assert "kmeans" in modules, "kmeans module not found"
    assert "fastrp" in modules, "fastrp module not found"
    print(
        f"✅ GDS procedures list retrieved: "
        f"{len(procedures)} procedures across {len(modules)} modules",
    )


@pytest.mark.integration
def test_kmeans_signature_discovery(neo4j: Neo4jClient) -> None:
    """Special test to discover the gds.kmeans.write signature.

    Ensures the procedure is available and runs on a minimal graph.
    Uses worker-isolated labels for parallel execution.
    """
    user_label = neo4j.label("User")
    graph_name = f"kmeans-signature-{neo4j.worker_label}"

    # Create test data with worker label
    for i in range(1, 6):
        emb = [0.1 + i * 0.01, 0.2 + i * 0.01]
        neo4j.execute_labeled(
            "CREATE (:__user__ {id: $id, embedding: $emb})",
            {"user": "User"},
            {"id": i, "emb": emb},
        )

    # Fetch signature from the procedure catalog
    all_procs = list(
        neo4j.execute_and_fetch("""
        SHOW PROCEDURES YIELD name, signature
        RETURN name, signature;
    """)
    )
    signature_info = [p for p in all_procs if str(p["name"]) == "gds.kmeans.write"]

    output = "\n" + "=" * 80 + "\n"
    output += "GDS.KMEANS.WRITE SIGNATURE DISCOVERY\n"
    output += "=" * 80 + "\n\n"

    if signature_info:
        output += f"Official signature:\n{signature_info[0]['signature']}\n\n"

    # Projection using Cypher projection for worker isolation
    gds_project_with_embedding(neo4j, graph_name, user_label)

    try:
        # Graph name is dynamic, use parameterized query
        result = list(
            neo4j.execute_and_fetch(
                "CALL gds.kmeans.write($graph_name, {"
                "nodeProperty: 'embedding', "
                "k: 2, "
                "randomSeed: 42, "
                "writeProperty: 'cluster_id'"
                "}) "
                "YIELD nodePropertiesWritten "
                "RETURN nodePropertiesWritten",
                {"graph_name": graph_name},
            )
        )

        output += f"✅ SUCCESS: {result}\n\n"
        print(output)

        assert len(result) == 1
        print("✅ GDS K-Means signature confirmed")

    finally:
        gds_drop_graph(neo4j, graph_name)


@pytest.mark.integration
def test_leiden_documentation_dump(neo4j: Neo4jClient) -> None:
    """Test to output Leiden community detection documentation (Neo4j GDS).

    Shows signature and usage examples through print and passes successfully.
    """
    # Get Leiden signature from procedure catalog
    all_procs = list(
        neo4j.execute_and_fetch("""
        SHOW PROCEDURES YIELD name, signature
        RETURN name, signature;
    """)
    )

    leiden_procs = [p for p in all_procs if "gds.leiden" in str(p["name"]).lower()]

    # Format documentation
    docs = "\n" + "=" * 80 + "\n"
    docs += "LEIDEN COMMUNITY DETECTION PROCEDURES DOCUMENTATION\n"
    docs += "=" * 80 + "\n\n"

    for proc in leiden_procs:
        docs += f"{proc['name']}:\n{proc['signature']}\n\n"

    docs += "=" * 80 + "\n"
    docs += "EXAMPLE USAGE FOR gds.leiden.stream:\n"
    docs += "=" * 80 + "\n"
    docs += """
# Project graph and run Leiden:
CALL gds.graph.project('graph-name', 'User', 'FOLLOWS');
CALL gds.leiden.stream('graph-name')
YIELD nodeId, communityId
RETURN nodeId, communityId;

# With weight property:
CALL gds.leiden.stream('graph-name', {relationshipWeightProperty: 'weight'})
YIELD nodeId, communityId
RETURN count(DISTINCT communityId) AS num_communities;
"""
    docs += "=" * 80 + "\n"

    # Output documentation
    print(docs)

    # Verify we got documentation
    assert len(leiden_procs) > 0, "Leiden procedures not found"
    leiden_main = [p for p in leiden_procs if p["name"] == "gds.leiden.stream"]
    assert len(leiden_main) > 0, "gds.leiden.stream not found"
    print(f"✅ Leiden documentation successfully retrieved ({len(leiden_procs)} procedures)")
