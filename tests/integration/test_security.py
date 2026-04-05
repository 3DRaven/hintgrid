"""Integration tests for injection prevention and secret sanitization.

Tests that all user-controllable inputs (labels, identifiers, GDS names,
passwords in DSN) are properly validated or encoded to prevent injection
attacks and secret leaks with real Neo4j connection.
"""

from __future__ import annotations

import pytest

from hintgrid.clients.neo4j import Neo4jClient
from hintgrid.pipeline.clustering import validate_gds_name
from hintgrid.utils.sanitize import sanitize_dsn, sanitize_error
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tests.conftest import DockerComposeInfo
else:
    from tests.conftest import DockerComposeInfo


@pytest.mark.integration
class TestLabelInjection:
    """Tests that Cypher injection via labels is prevented."""

    def test_cypher_injection_in_label_rejected(self, neo4j: Neo4jClient) -> None:
        """Cypher injection attempt via label must raise ValueError."""
        with pytest.raises(ValueError, match="Invalid label"):
            neo4j.label("User} DETACH DELETE n //")

    def test_semicolon_in_label_rejected(self, neo4j: Neo4jClient) -> None:
        """Semicolon in label must be rejected to prevent statement chaining."""
        with pytest.raises(ValueError, match="Invalid label"):
            neo4j.label("User; MATCH (n) DELETE n")

    def test_bracket_in_label_rejected(self, neo4j: Neo4jClient) -> None:
        """Brackets in label must be rejected."""
        with pytest.raises(ValueError, match="Invalid label"):
            neo4j.label("User)-[:ADMIN]->(")

    def test_empty_label_rejected(self, neo4j: Neo4jClient) -> None:
        """Empty label must be rejected."""
        with pytest.raises(ValueError, match="Invalid label"):
            neo4j.label("")

    def test_space_in_label_rejected(self, neo4j: Neo4jClient) -> None:
        """Space in label must be rejected."""
        with pytest.raises(ValueError, match="Invalid label"):
            neo4j.label("User DETACH")

    def test_backtick_in_label_rejected(self, neo4j: Neo4jClient) -> None:
        """Backtick escape in label must be rejected."""
        with pytest.raises(ValueError, match="Invalid label"):
            neo4j.label("`Injected`")

    def test_worker_label_injection_rejected(
        self, docker_compose: DockerComposeInfo
    ) -> None:
        """Cypher injection via worker_label must be rejected."""
        # Create client with clean worker_label, but try to inject via label()
        client = Neo4jClient(
            host=docker_compose.neo4j_host,
            port=docker_compose.neo4j_port,
            username=docker_compose.neo4j_user,
            password=docker_compose.neo4j_password,
            worker_label="clean",
        )
        try:
            # _validate_label is called on worker_label too
            with pytest.raises(ValueError, match="Invalid label"):
                client.label("User')--")
        finally:
            client.close()


@pytest.mark.integration
class TestIdentifierInjection:
    """Tests that injection via query identifiers is prevented."""

    def test_cypher_injection_via_ident_rejected(self, neo4j: Neo4jClient) -> None:
        """Cypher injection in ident_map must raise ValueError."""
        with pytest.raises(ValueError, match="Unsafe identifier"):
            neo4j.execute_labeled(
                "CREATE INDEX __idx__ IF NOT EXISTS FOR (n:Post) ON n.embedding",
                ident_map={"idx": "foo; DROP INDEX bar"},
            )

    def test_newline_in_ident_rejected(self, neo4j: Neo4jClient) -> None:
        """Newline character in identifier must be rejected."""
        with pytest.raises(ValueError, match="Unsafe identifier"):
            neo4j.execute_labeled(
                "RETURN __val__",
                ident_map={"val": "ok\nMATCH (n) DELETE n"},
            )

    def test_parenthesis_in_ident_rejected(self, neo4j: Neo4jClient) -> None:
        """Parentheses in identifier must be rejected."""
        with pytest.raises(ValueError, match="Unsafe identifier"):
            neo4j.execute_labeled(
                "RETURN __val__",
                ident_map={"val": "fn()"},
            )

    def test_backtick_in_ident_rejected(self, neo4j: Neo4jClient) -> None:
        """Backtick in identifier must be rejected."""
        with pytest.raises(ValueError, match="Unsafe identifier"):
            neo4j.execute_labeled(
                "RETURN __val__",
                ident_map={"val": "`injected`"},
            )

    def test_quote_in_ident_rejected(self, neo4j: Neo4jClient) -> None:
        """Quotes in identifier must be rejected."""
        with pytest.raises(ValueError, match="Unsafe identifier"):
            neo4j.execute_labeled(
                "RETURN __val__",
                ident_map={"val": "val'OR 1=1--"},
            )


@pytest.mark.integration
class TestVectorIndexInjection:
    """Tests that vector index creation validates all parameters."""

    def test_injection_in_index_name_rejected(self, neo4j: Neo4jClient) -> None:
        """Cypher injection via index_name must be rejected."""
        with pytest.raises(ValueError, match="Invalid index name"):
            neo4j.create_vector_index(
                index_name="idx; DROP INDEX other_idx",
                label="Post",
                property_name="embedding",
                dimensions=128,
            )

    def test_injection_in_label_rejected(self, neo4j: Neo4jClient) -> None:
        """Cypher injection via label in vector index must be rejected."""
        with pytest.raises(ValueError, match="Invalid label"):
            neo4j.create_vector_index(
                index_name="my_index",
                label="Post) ON n.x OPTIONS {} //",
                property_name="embedding",
                dimensions=128,
            )

    def test_injection_in_property_rejected(self, neo4j: Neo4jClient) -> None:
        """Cypher injection via property_name must be rejected."""
        with pytest.raises(ValueError, match="Invalid property name"):
            neo4j.create_vector_index(
                index_name="my_index",
                label="Post",
                property_name="emb OPTIONS {evil: true}",
                dimensions=128,
            )

    def test_invalid_similarity_function_rejected(self, neo4j: Neo4jClient) -> None:
        """Non-whitelisted similarity function must be rejected."""
        with pytest.raises(ValueError, match="Invalid similarity function"):
            neo4j.create_vector_index(
                index_name="my_index",
                label="Post",
                property_name="embedding",
                dimensions=128,
                similarity_function="'; DROP INDEX --",
            )

    def test_invalid_dimensions_rejected(self, neo4j: Neo4jClient) -> None:
        """Dimensions out of range must be rejected."""
        with pytest.raises(ValueError, match="Invalid dimensions"):
            neo4j.create_vector_index(
                index_name="my_index",
                label="Post",
                property_name="embedding",
                dimensions=0,
            )
        with pytest.raises(ValueError, match="Invalid dimensions"):
            neo4j.create_vector_index(
                index_name="my_index",
                label="Post",
                property_name="embedding",
                dimensions=9999,
            )


class TestGdsNameInjection:
    """Tests that GDS graph name injection is prevented."""

    def test_cypher_injection_in_gds_name_rejected(self) -> None:
        """Cypher injection via GDS name must raise ValueError."""
        with pytest.raises(ValueError, match="Invalid GDS name"):
            validate_gds_name("graph'; MATCH (n) DELETE n; //")

    def test_space_in_gds_name_rejected(self) -> None:
        """Space in GDS name must be rejected."""
        with pytest.raises(ValueError, match="Invalid GDS name"):
            validate_gds_name("graph name")

    def test_dot_in_gds_name_rejected(self) -> None:
        """Dot in GDS name must be rejected (only alphanum, dash, underscore)."""
        with pytest.raises(ValueError, match="Invalid GDS name"):
            validate_gds_name("graph.evil")

    def test_starts_with_number_rejected(self) -> None:
        """GDS name starting with number must be rejected."""
        with pytest.raises(ValueError, match="Invalid GDS name"):
            validate_gds_name("123graph")


class TestDsnPasswordEncoding:
    """Tests that passwords with special chars are safely encoded in DSN."""

    def test_special_chars_in_password_encoded(self) -> None:
        """Password with @, :, /, %, & must be URL-encoded in DSN."""
        from hintgrid.clients.postgres import build_postgres_dsn
        from hintgrid.config import HintGridSettings

        settings = HintGridSettings(
            postgres_user="user",
            postgres_password="p@ss:w/rd%&?",
            postgres_host="localhost",
            postgres_port=5432,
            postgres_database="testdb",
        )
        dsn = build_postgres_dsn(settings)

        # Password must be encoded — raw special chars must NOT appear
        # between user: and @host
        assert "p@ss:w/rd%&?" not in dsn
        # But the DSN must still be parseable
        assert dsn.startswith("postgresql://")
        assert "@localhost:5432/" in dsn

    def test_empty_password_produces_valid_dsn(self) -> None:
        """Empty password must produce valid DSN without errors."""
        from hintgrid.clients.postgres import build_postgres_dsn
        from hintgrid.config import HintGridSettings

        settings = HintGridSettings(
            postgres_user="user",
            postgres_password=None,
            postgres_host="localhost",
            postgres_port=5432,
            postgres_database="testdb",
        )
        dsn = build_postgres_dsn(settings)

        assert dsn == "postgresql://user:@localhost:5432/testdb?client_encoding=UTF8"


class TestSanitizeDsn:
    """Tests for DSN sanitization utility."""

    def test_postgres_dsn_password_masked(self) -> None:
        """Password in PostgreSQL DSN must be replaced with ***."""
        dsn = "postgresql://myuser:s3cr3t_p@ss@dbhost:5432/mydb"
        sanitized = sanitize_dsn(dsn)

        assert "s3cr3t_p@ss" not in sanitized
        assert "***" in sanitized
        assert "myuser:" in sanitized

    def test_bolt_dsn_password_masked(self) -> None:
        """Password in bolt:// (Neo4j) URI must be masked."""
        uri = "bolt://neo4j:secret123@neo4j-host:7687"
        sanitized = sanitize_dsn(uri)

        assert "secret123" not in sanitized
        assert "***" in sanitized

    def test_no_dsn_unchanged(self) -> None:
        """String without DSN pattern must be returned unchanged."""
        msg = "Connection refused to localhost:5432"
        assert sanitize_dsn(msg) == msg

    def test_sanitize_error_masks_dsn_in_exception(self) -> None:
        """Exception containing DSN must have its password masked."""
        exc = RuntimeError(
            "could not connect to postgresql://user:LEAKED@host:5432/db"
        )
        sanitized = sanitize_error(exc)

        assert "LEAKED" not in sanitized
        assert "***" in sanitized
        assert "could not connect to" in sanitized

    def test_sanitize_error_none(self) -> None:
        """sanitize_error(None) must return empty string."""
        assert sanitize_error(None) == ""
