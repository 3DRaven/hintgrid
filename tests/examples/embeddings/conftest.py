"""Shared fixtures, types and helpers for embeddings tests.

All tests use worker-isolated labels via neo4j.label() for parallel execution.
"""

from typing import TYPE_CHECKING, TypedDict, cast

import numpy as np
import pytest

from hintgrid.config import DEFAULT_FASTTEXT_VECTOR_SIZE

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient


# =============================================================================
# Typed Records
# =============================================================================


class PostInput(TypedDict):
    """Input post data without embedding."""

    id: int
    text: str


class PostWithEmbedding(TypedDict):
    """Post data with generated embedding."""

    id: int
    text: str
    embedding: list[float]


class EmbeddingServiceConfig(TypedDict):
    """Configuration for embedding service."""

    api_base: str
    model: str
    port: int


# =============================================================================
# Test Constants
# =============================================================================

# Use FastText default vector size (configurable via HINTGRID_FASTTEXT_VECTOR_SIZE)
EMBEDDING_DIM = DEFAULT_FASTTEXT_VECTOR_SIZE
EMBEDDING_DIM_OPENAI = 3072  # text-embedding-3-large dimension
NUM_POSTS = 10  # Number of test posts
NUM_POST_CLUSTERS = 3  # Number of post clusters
BATCH_SIZE = 32  # LiteLLM batch size
MIN_CLUSTER_SIZE = 1  # Minimum posts per cluster (relaxed for small datasets)
SIMILARITY_THRESHOLD = 0.7  # Cosine similarity threshold for same cluster


# =============================================================================
# Helper Functions
# =============================================================================


def as_embedding(value: object) -> list[float]:
    """Cast value to embedding list."""
    return cast("list[float]", value)


def cosine_similarity(a: list[float], b: list[float]) -> float:
    """Calculate cosine similarity between two vectors."""
    a_np = np.array(a)
    b_np = np.array(b)
    return float(np.dot(a_np, b_np) / (np.linalg.norm(a_np) * np.linalg.norm(b_np)))


def generate_embeddings_via_service(
    mock_config: EmbeddingServiceConfig, posts: list[PostInput]
) -> list[PostWithEmbedding]:
    """Generate embeddings using REAL FastText service via LiteLLM.

    Args:
        mock_config: Configuration dict with api_base, model
        posts: List of dicts with 'id' and 'text' keys

    Returns:
        List of dicts with 'id', 'text', and 'embedding' keys
    """
    from litellm import embedding

    texts = [post["text"] for post in posts]
    response = embedding(
        model=mock_config["model"],
        input=texts,
        api_base=mock_config["api_base"],
    )
    embeddings = [item["embedding"] for item in response["data"]]
    posts_with_embeddings: list[PostWithEmbedding] = []
    for post, emb in zip(posts, embeddings, strict=False):
        posts_with_embeddings.append(
            {
                "id": post["id"],
                "text": post["text"],
                "embedding": emb,
            }
        )

    return posts_with_embeddings


# =============================================================================
# GDS Projection Helpers
# =============================================================================


def gds_project_with_embedding(neo4j: "Neo4jClient", graph_name: str, label: str) -> None:
    """Create GDS graph with embedding property for K-Means clustering.

    Uses Cypher projection to correctly handle node properties.
    """
    # Graph name and label are dynamic, use parameterized query
    node_query = f"MATCH (n:{label}) WHERE n.embedding IS NOT NULL RETURN id(n) AS id, n.embedding AS embedding"
    neo4j.execute(
        "CALL gds.graph.project.cypher($graph_name, $node_query, 'RETURN null AS source, null AS target LIMIT 0')",
        {"graph_name": graph_name, "node_query": node_query},
    )


def gds_drop_graph(neo4j: "Neo4jClient", graph_name: str) -> None:
    """Drop GDS graph if exists."""
    neo4j.execute("CALL gds.graph.drop($name, false)", {"name": graph_name})


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_posts() -> list[PostInput]:
    """Sample posts WITHOUT embeddings (text only).

    Embeddings should be generated dynamically via FastText service in tests.
    """
    return [
        # Tech cluster
        {"id": 1, "text": "Python programming language features"},
        {"id": 2, "text": "Docker container microservices"},
        {"id": 3, "text": "Kubernetes cluster management"},
        # Food cluster
        {"id": 4, "text": "Pizza recipe with cheese"},
        {"id": 5, "text": "Italian pasta carbonara"},
        {"id": 6, "text": "Coffee brewing methods"},
        # Travel cluster
        {"id": 7, "text": "Beach vacation in Thailand"},
        {"id": 8, "text": "Paris museums and attractions"},
        {"id": 9, "text": "Budget travel Europe"},
        # Random
        {"id": 10, "text": "Random thoughts"},
    ]


@pytest.fixture
def strong_signal_posts() -> list[PostInput]:
    """Posts with STRONG semantic signal for FastText clustering.

    Keywords repeated multiple times with related terms for n-gram learning.
    Each cluster has 15 posts with recurring keywords and semantic context.
    More data = better FastText vocabulary and stable embeddings.
    """
    return [
        # Python cluster (15 posts)
        {"id": 1, "text": "Python Python programming code software development best practices"},
        {"id": 2, "text": "Python coding Python syntax Python fundamentals tutorial guide"},
        {"id": 3, "text": "Python developer Python programmer Python jobs software career"},
        {"id": 4, "text": "Python data science Python machine learning Python analytics"},
        {"id": 5, "text": "Python Django Python Flask Python web frameworks backend"},
        {"id": 6, "text": "Python testing Python pytest Python unittest automation code"},
        {"id": 7, "text": "Python numpy Python pandas Python matplotlib data libraries"},
        {"id": 8, "text": "Python scripting Python automation Python DevOps tasks code"},
        {"id": 9, "text": "Python interview Python questions Python coding challenges"},
        {"id": 10, "text": "Python community Python documentation Python tutorials learning"},
        {"id": 11, "text": "Python asyncio Python concurrency Python multithreading code"},
        {"id": 12, "text": "Python pip Python packages Python virtual environment setup"},
        {"id": 13, "text": "Python decorators Python generators Python advanced features"},
        {"id": 14, "text": "Python API Python REST Python FastAPI web services code"},
        {"id": 15, "text": "Python debugging Python profiling Python performance code"},
        # Docker cluster (15 posts)
        {"id": 16, "text": "Docker Docker containerization Docker microservices deployment"},
        {"id": 17, "text": "Docker compose Docker orchestration Docker services networking"},
        {"id": 18, "text": "Docker images Docker registry Docker hub repository storage"},
        {"id": 19, "text": "Docker kubernetes Docker integration Docker cloud native apps"},
        {"id": 20, "text": "Docker build Docker optimize Docker layers cache performance"},
        {"id": 21, "text": "Docker swarm Docker clustering Docker high availability"},
        {"id": 22, "text": "Docker security Docker scanning Docker vulnerabilities best"},
        {"id": 23, "text": "Docker networking Docker bridge Docker overlay configuration"},
        {"id": 24, "text": "Docker volumes Docker persistent Docker storage management"},
        {"id": 25, "text": "Docker debugging Docker logs Docker troubleshooting monitoring"},
        {"id": 26, "text": "Docker container Docker runtime Docker daemon configuration"},
        {"id": 27, "text": "Docker Dockerfile Docker multi-stage Docker build optimization"},
        {"id": 28, "text": "Docker healthcheck Docker restart Docker policy container"},
        {"id": 29, "text": "Docker secrets Docker config Docker environment variables"},
        {"id": 30, "text": "Docker prune Docker cleanup Docker disk space management"},
        # Pizza cluster (15 posts)
        {"id": 31, "text": "Pizza Pizza recipe Pizza homemade Pizza dough italian cooking"},
        {"id": 32, "text": "Pizza toppings Pizza cheese Pizza mushrooms Pizza pepperoni"},
        {"id": 33, "text": "Pizza baking Pizza oven Pizza temperature Pizza crispy crust"},
        {"id": 34, "text": "Pizza margherita Pizza napoletana Pizza traditional authentic"},
        {"id": 35, "text": "Pizza delivery Pizza restaurants Pizza ordering Pizza menu"},
        {"id": 36, "text": "Pizza sauce Pizza tomato Pizza basil Pizza garlic herbs"},
        {"id": 37, "text": "Pizza mozzarella Pizza cheese Pizza quality Pizza fresh"},
        {"id": 38, "text": "Pizza styles Pizza Chicago Pizza New York Pizza regional"},
        {"id": 39, "text": "Pizza party Pizza catering Pizza events Pizza celebrations"},
        {"id": 40, "text": "Pizza history Pizza origin Pizza Italy Pizza Naples food"},
        {"id": 41, "text": "Pizza dough Pizza flour Pizza yeast Pizza fermentation"},
        {"id": 42, "text": "Pizza slice Pizza portion Pizza serving Pizza eating food"},
        {"id": 43, "text": "Pizza restaurant Pizza pizzeria Pizza italian Pizza dining"},
        {"id": 44, "text": "Pizza frozen Pizza homemade Pizza comparison Pizza taste"},
        {"id": 45, "text": "Pizza calories Pizza nutrition Pizza healthy Pizza diet food"},
    ]
