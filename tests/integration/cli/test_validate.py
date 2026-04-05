"""CLI integration tests for validate command."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid import app as app_module

from .conftest import run_cli, set_cli_env

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.config import HintGridSettings
    from pathlib import Path
    from tests.conftest import DockerComposeInfo, EmbeddingServiceConfig


@pytest.mark.integration
def test_cli_validate_success(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test 'hintgrid validate' command with valid configuration.

    The validate command should:
    1. Verify all database connections are working
    2. Print configuration tree
    3. Show embedding signature status
    4. Return exit code 0 on success
    """
    log_file = tmp_path / "validate.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(monkeypatch, ["validate"])
    assert exit_code == 0

    output = capsys.readouterr().out
    # Should print confirmation of valid configuration
    assert "valid" in output.lower() or "Configuration" in output


@pytest.mark.integration
def test_cli_validate_shows_settings(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test 'hintgrid validate' displays configuration settings.

    Should show database connections, embedding config, and pipeline settings.
    """
    log_file = tmp_path / "validate_settings.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(monkeypatch, ["validate"])
    assert exit_code == 0

    output = capsys.readouterr().out
    # Verify settings tree is printed with expected sections
    assert "PostgreSQL" in output or "Neo4j" in output or "Redis" in output
