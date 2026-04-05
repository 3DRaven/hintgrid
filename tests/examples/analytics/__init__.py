"""Analytics tests for Neo4j GDS algorithms.

Tests are organized by algorithm:
- test_fastrp.py - FastRP embedding tests
- test_kmeans.py - K-Means clustering tests
- test_leiden.py - Leiden community detection tests
- test_fasttext.py - FastText model training and inference tests
- test_workflows.py - Complex multi-algorithm workflows
- test_edge_cases.py - Edge cases and error handling

All tests use worker-isolated labels via neo4j.label() for parallel execution.
"""
