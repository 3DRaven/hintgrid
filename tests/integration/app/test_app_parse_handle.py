"""Unit tests for parse_account_handle and clean_models.

Tests parsing of Mastodon handles in various formats
and model file cleanup edge cases.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from hintgrid.app import parse_account_handle
from hintgrid.config import HintGridSettings


class TestParseAccountHandle:
    """Tests for parse_account_handle function."""

    def test_handle_without_at_prefix(self) -> None:
        """Test parsing handle without leading '@'."""
        username, domain = parse_account_handle("username")
        assert username == "username"
        assert domain is None

    def test_handle_with_at_prefix(self) -> None:
        """Test parsing handle with leading '@'."""
        username, domain = parse_account_handle("@username")
        assert username == "username"
        assert domain is None

    def test_handle_with_domain_no_at(self) -> None:
        """Test parsing 'username@domain' format."""
        username, domain = parse_account_handle("username@example.com")
        assert username == "username"
        assert domain == "example.com"

    def test_handle_with_domain_and_at(self) -> None:
        """Test parsing '@username@domain' format."""
        username, domain = parse_account_handle("@username@example.com")
        assert username == "username"
        assert domain == "example.com"

    def test_empty_handle_raises(self) -> None:
        """Test that empty handle raises ValueError."""
        with pytest.raises(ValueError, match="Handle is empty"):
            parse_account_handle("")

    def test_whitespace_only_handle_raises(self) -> None:
        """Test that whitespace-only handle raises ValueError."""
        with pytest.raises(ValueError, match="Handle is empty"):
            parse_account_handle("   ")

    def test_at_sign_only_raises(self) -> None:
        """Test that '@' only raises ValueError."""
        with pytest.raises(ValueError, match="Handle is empty"):
            parse_account_handle("@")

    def test_invalid_format_multiple_at_signs(self) -> None:
        """Test that handle with too many '@' raises ValueError."""
        with pytest.raises(ValueError, match="Invalid handle format"):
            parse_account_handle("user@domain@extra")

    def test_invalid_format_empty_username(self) -> None:
        """Test that '@domain' without username raises ValueError."""
        with pytest.raises(ValueError, match="Invalid handle format"):
            parse_account_handle("@@domain")

    def test_invalid_format_empty_domain(self) -> None:
        """Test that 'user@' with empty domain raises ValueError."""
        with pytest.raises(ValueError, match="Invalid handle format"):
            parse_account_handle("user@")

    def test_handle_with_whitespace_stripped(self) -> None:
        """Test that leading/trailing whitespace is stripped."""
        username, domain = parse_account_handle("  @username  ")
        assert username == "username"
        assert domain is None


@pytest.mark.integration
class TestCleanModelFiles:
    """Tests for clean_models using real app with tmp_path."""

    def test_nonexistent_path_returns_early(
        self, tmp_path: Path, settings: HintGridSettings
    ) -> None:
        """Test that clean_models returns early for non-existent path."""
        from hintgrid.app import HintGridApp

        # Create app with non-existent model path (clean_models doesn't need clients)
        settings.fasttext_model_path = str(tmp_path / "nonexistent_dir")
        app = HintGridApp.__new__(HintGridApp)
        app.settings = settings

        # Should not raise - just returns early
        app.clean_models()

    def test_deletes_model_files(
        self, tmp_path: Path, settings: HintGridSettings
    ) -> None:
        """Test that clean_models deletes matching files."""
        from hintgrid.app import HintGridApp

        # Create some model files
        (tmp_path / "phrases_v1.pkl").write_text("data")
        (tmp_path / "fasttext_v1.bin").write_text("data")
        (tmp_path / "fasttext_v1.bin.wv.vectors_ngrams.npy").write_text("data")
        (tmp_path / "other_file.txt").write_text("keep me")

        settings.fasttext_model_path = str(tmp_path)
        app = HintGridApp.__new__(HintGridApp)
        app.settings = settings

        app.clean_models()

        # Model files should be deleted
        assert not (tmp_path / "phrases_v1.pkl").exists()
        assert not (tmp_path / "fasttext_v1.bin").exists()
        assert not (tmp_path / "fasttext_v1.bin.wv.vectors_ngrams.npy").exists()
        # Other files should remain
        assert (tmp_path / "other_file.txt").exists()

    def test_handles_permission_error(
        self, tmp_path: Path, settings: HintGridSettings, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that clean_models handles permission errors gracefully."""
        from unittest.mock import patch
        from hintgrid.app import HintGridApp

        # Create a model file
        model_file = tmp_path / "phrases_v1.pkl"
        model_file.write_text("data")

        settings.fasttext_model_path = str(tmp_path)
        app = HintGridApp.__new__(HintGridApp)
        app.settings = settings

        # Patch Path.unlink to raise PermissionError
        original_unlink = Path.unlink

        def mock_unlink(self: Path, missing_ok: bool = False) -> None:
            if self.name == "phrases_v1.pkl":
                raise PermissionError("Permission denied")
            original_unlink(self, missing_ok=missing_ok)

        with patch.object(Path, "unlink", mock_unlink):
            app.clean_models()

        # File should still exist (deletion failed)
        assert model_file.exists()
        # Warning should be logged
        assert "Failed to delete model file" in caplog.text
