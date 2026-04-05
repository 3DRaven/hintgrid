"""Fixtures specific to examples/documentation tests.

Example tests demonstrate API usage and serve as living documentation.
They focus on verifying that documented patterns work correctly.

All example tests use worker-isolated labels via neo4j.label() for parallel execution.
"""

from __future__ import annotations

# Test data constants shared across example tests
EXPECTED_NODES_COUNT = 2  # User + Post nodes
TOP_POSTS_LIMIT = 3  # Number of top-scored posts to retrieve
TOTAL_FEED_ITEMS = 4  # Total items in feed
EXPECTED_STATUSES_COUNT = 3  # Number of test statuses
FAVOURITES_COUNT = 2  # Number of favourites in test
PIPELINE_BATCH_SIZE = 100  # Batch size for pipeline operations
PAGINATION_BATCH_SIZE = 5  # Items per page for pagination
TOTAL_PAGINATION_BATCHES = 4  # Expected number of batches
MAX_DOCS_PARAMS = 30  # Maximum documentation parameters to show
MAX_MODULE_PROCEDURES = 5  # Maximum procedures to show per module
POOL_CONNECTIONS_COUNT = 3  # Number of connections to test in pool
NEO4J_TEST_NODES_COUNT = 6  # Number of nodes in Neo4j test graph
NEO4J_COMMUNITIES_COUNT = 2  # Expected number of communities

# Neo4j GDS algorithm constants
NODE2VEC_EMBEDDING_DIM = 16  # Embedding dimension for Node2Vec
NODE2VEC_WALK_LENGTH = 10  # Walk length for Node2Vec
NODE2VEC_WALKS_PER_NODE = 5  # Number of walks per node
SMALL_GRAPH_NODES = 5  # Small test graph size
MEDIUM_GRAPH_NODES = 10  # Medium test graph size
KMEANS_ITERATIONS = 10  # K-Means max iterations
MIN_CLUSTER_COHESION = 0.6  # Minimum cluster cohesion for quality tests
