"""Integration tests for model bundle export/import.

Tests the public API only: export_bundle and import_bundle.
All internal logic (checksums, manifest parsing, file collection,
compatibility checks) is verified indirectly through these two functions.
"""

from __future__ import annotations

import json
import tarfile

import pytest

from hintgrid.config import HintGridSettings
from hintgrid.embeddings.bundle import (
    MANIFEST_FILENAME,
    export_bundle,
    import_bundle,
)
from hintgrid.embeddings.fasttext_service import STATE_NODE_ID
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path
    from hintgrid.clients.neo4j import Neo4jClient


@pytest.fixture
def model_dir(tmp_path: Path) -> Path:
    """Create a temporary model directory for fake model files."""
    model_path = tmp_path / "models"
    model_path.mkdir()
    return model_path


@pytest.fixture
def settings(model_dir: Path) -> HintGridSettings:
    """Create settings pointing to temporary model directory."""
    return HintGridSettings(
        fasttext_model_path=str(model_dir),
        fasttext_vector_size=128,
        fasttext_window=3,
        fasttext_min_count=10,
        fasttext_bucket=10000,
        fasttext_epochs=5,
        fasttext_max_vocab_size=500_000,
    )


def _create_fake_model_files(
    model_dir: Path, version: int, *, quantized: bool = False
) -> None:
    """Create fake model files for testing."""
    for name in [
        f"phrases_v{version}.pkl",
        f"phraser_v{version}.pkl",
        f"fasttext_v{version}.bin",
        f"fasttext_v{version}.bin.wv.vectors_ngrams.npy",
    ]:
        (model_dir / name).write_bytes(b"fake model data for " + name.encode())

    if quantized:
        (model_dir / f"fasttext_v{version}.q.bin").write_bytes(
            b"fake quantized data"
        )


def _setup_fasttext_state(
    neo4j: Neo4jClient,
    version: int = 1,
    last_trained_post_id: int = 500,
    vocab_size: int = 3000,
    corpus_size: int = 8000,
) -> None:
    """Create FastTextState in Neo4j for testing."""
    neo4j.label("FastTextState")
    neo4j.execute_labeled(
        "MERGE (s:__label__ {id: $id}) "
        "SET s.version = $version, "
        "    s.last_trained_post_id = $last_trained_post_id, "
        "    s.vocab_size = $vocab_size, "
        "    s.corpus_size = $corpus_size, "
        "    s.updated_at = timestamp()",
        {"label": "FastTextState"},
        {
            "id": STATE_NODE_ID,
            "version": version,
            "last_trained_post_id": last_trained_post_id,
            "vocab_size": vocab_size,
            "corpus_size": corpus_size,
        },
    )


# ---------------------------------------------------------------------------
# export_bundle
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestExportBundle:
    """Test export_bundle public API."""

    def test_full_mode_includes_all_model_files(
        self,
        model_dir: Path,
        settings: HintGridSettings,
        tmp_path: Path,
        neo4j: Neo4jClient,
    ) -> None:
        _create_fake_model_files(model_dir, 1, quantized=True)
        _setup_fasttext_state(neo4j, version=1)
        archive = tmp_path / "bundle.tar.gz"

        result = export_bundle(settings, neo4j, archive, mode="full")

        assert result.exists()
        with tarfile.open(result, "r:gz") as tar:
            names = set(tar.getnames())
            assert MANIFEST_FILENAME in names
            assert "phraser_v1.pkl" in names
            assert "phrases_v1.pkl" in names
            assert "fasttext_v1.bin" in names
            assert "fasttext_v1.q.bin" in names
            assert "fasttext_v1.bin.wv.vectors_ngrams.npy" in names

    def test_inference_mode_prefers_quantized(
        self,
        model_dir: Path,
        settings: HintGridSettings,
        tmp_path: Path,
        neo4j: Neo4jClient,
    ) -> None:
        _create_fake_model_files(model_dir, 1, quantized=True)
        _setup_fasttext_state(neo4j, version=1)
        archive = tmp_path / "bundle.tar.gz"

        export_bundle(settings, neo4j, archive, mode="inference")

        with tarfile.open(archive, "r:gz") as tar:
            names = set(tar.getnames())
            assert "phraser_v1.pkl" in names
            assert "fasttext_v1.q.bin" in names
            assert "phrases_v1.pkl" not in names

    def test_inference_falls_back_to_full_bin(
        self,
        model_dir: Path,
        settings: HintGridSettings,
        tmp_path: Path,
        neo4j: Neo4jClient,
    ) -> None:
        _create_fake_model_files(model_dir, 1, quantized=False)
        _setup_fasttext_state(neo4j, version=1)
        archive = tmp_path / "bundle.tar.gz"

        export_bundle(settings, neo4j, archive, mode="inference")

        with tarfile.open(archive, "r:gz") as tar:
            names = set(tar.getnames())
            assert "fasttext_v1.bin" in names
            assert "fasttext_v1.q.bin" not in names

    def test_no_model_files_raises_file_not_found(
        self,
        model_dir: Path,
        settings: HintGridSettings,
        tmp_path: Path,
        neo4j: Neo4jClient,
    ) -> None:
        _setup_fasttext_state(neo4j, version=1)
        archive = tmp_path / "bundle.tar.gz"

        with pytest.raises(FileNotFoundError, match="No model files"):
            export_bundle(settings, neo4j, archive, mode="full")

    def test_version_zero_raises_value_error(
        self,
        model_dir: Path,
        settings: HintGridSettings,
        tmp_path: Path,
        neo4j: Neo4jClient,
    ) -> None:
        _setup_fasttext_state(neo4j, version=0)
        archive = tmp_path / "bundle.tar.gz"

        with pytest.raises(ValueError, match="No trained model"):
            export_bundle(settings, neo4j, archive)

    def test_no_state_in_neo4j_raises_value_error(
        self,
        model_dir: Path,
        settings: HintGridSettings,
        tmp_path: Path,
        neo4j: Neo4jClient,
    ) -> None:
        # Don't create state - should raise error
        archive = tmp_path / "bundle.tar.gz"

        with pytest.raises(ValueError, match="No FastTextState"):
            export_bundle(settings, neo4j, archive)

    def test_manifest_contains_correct_metadata(
        self,
        model_dir: Path,
        settings: HintGridSettings,
        tmp_path: Path,
        neo4j: Neo4jClient,
    ) -> None:
        _create_fake_model_files(model_dir, 1)
        _setup_fasttext_state(
            neo4j,
            version=1,
            vocab_size=3000,
            corpus_size=8000,
            last_trained_post_id=500,
        )
        archive = tmp_path / "bundle.tar.gz"

        export_bundle(settings, neo4j, archive, mode="full")

        with tarfile.open(archive, "r:gz") as tar:
            manifest_member = tar.getmember(MANIFEST_FILENAME)
            manifest_file = tar.extractfile(manifest_member)
            assert manifest_file is not None
            data: dict[str, object] = json.load(manifest_file)

        assert data["version"] == 1
        assert data["mode"] == "full"

        tp = data.get("training_params")
        assert isinstance(tp, dict)
        assert tp["vector_size"] == 128

        stats = data.get("statistics")
        assert isinstance(stats, dict)
        assert stats["vocab_size"] == 3000
        assert stats["corpus_size"] == 8000
        assert stats["last_trained_post_id"] == 500

        compat = data.get("compatibility")
        assert isinstance(compat, dict)
        assert "hintgrid_version" in compat
        assert "gensim_version" in compat


# ---------------------------------------------------------------------------
# import_bundle
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestImportBundle:
    """Test import_bundle public API."""

    def test_roundtrip_restores_files(
        self,
        model_dir: Path,
        settings: HintGridSettings,
        tmp_path: Path,
        neo4j: Neo4jClient,
    ) -> None:
        """Export → clear → import restores all model files."""
        _create_fake_model_files(model_dir, 1, quantized=True)
        _setup_fasttext_state(neo4j, version=1)
        archive = tmp_path / "bundle.tar.gz"
        export_bundle(settings, neo4j, archive, mode="full")

        # Remove all model files
        for f in model_dir.iterdir():
            f.unlink()

        result = import_bundle(settings, neo4j, archive, force=True)

        assert result.version == 1
        assert result.mode == "full"
        assert result.files_installed >= 4
        assert result.state_updated is True
        assert (model_dir / "phraser_v1.pkl").exists()
        assert (model_dir / "fasttext_v1.bin").exists()
        assert (model_dir / "phrases_v1.pkl").exists()

    def test_roundtrip_preserves_file_contents(
        self,
        model_dir: Path,
        settings: HintGridSettings,
        tmp_path: Path,
        neo4j: Neo4jClient,
    ) -> None:
        """File contents must survive export → import cycle."""
        _create_fake_model_files(model_dir, 1)
        _setup_fasttext_state(neo4j, version=1)
        archive = tmp_path / "bundle.tar.gz"
        export_bundle(settings, neo4j, archive, mode="full")

        # Save original contents
        phraser_original = (model_dir / "phraser_v1.pkl").read_bytes()
        fasttext_original = (model_dir / "fasttext_v1.bin").read_bytes()

        # Clear and reimport
        for f in model_dir.iterdir():
            f.unlink()

        import_bundle(settings, neo4j, archive, force=True)

        assert (model_dir / "phraser_v1.pkl").read_bytes() == phraser_original
        assert (model_dir / "fasttext_v1.bin").read_bytes() == fasttext_original

    def test_nonexistent_archive_raises_file_not_found(
        self,
        settings: HintGridSettings,
        tmp_path: Path,
        neo4j: Neo4jClient,
    ) -> None:
        _setup_fasttext_state(neo4j, version=1)
        fake = tmp_path / "nonexistent.tar.gz"

        with pytest.raises(FileNotFoundError, match="not found"):
            import_bundle(settings, neo4j, fake)

    def test_without_force_conflicts_on_existing_files(
        self,
        model_dir: Path,
        settings: HintGridSettings,
        tmp_path: Path,
        neo4j: Neo4jClient,
    ) -> None:
        """Import without --force should reject if model files already exist."""
        _create_fake_model_files(model_dir, 1, quantized=True)
        _setup_fasttext_state(neo4j, version=1)
        archive = tmp_path / "bundle.tar.gz"
        export_bundle(settings, neo4j, archive, mode="full")

        with pytest.raises(ValueError, match="already exist"):
            import_bundle(settings, neo4j, archive, force=False)

    def test_with_force_overwrites_existing_files(
        self,
        model_dir: Path,
        settings: HintGridSettings,
        tmp_path: Path,
        neo4j: Neo4jClient,
    ) -> None:
        """Import with --force should succeed even if files exist."""
        _create_fake_model_files(model_dir, 1)
        _setup_fasttext_state(neo4j, version=1)
        archive = tmp_path / "bundle.tar.gz"
        export_bundle(settings, neo4j, archive, mode="full")

        result = import_bundle(settings, neo4j, archive, force=True)
        assert result.version == 1
        assert result.state_updated is True

    def test_updates_neo4j_state(
        self,
        model_dir: Path,
        settings: HintGridSettings,
        tmp_path: Path,
        neo4j: Neo4jClient,
    ) -> None:
        """Import should update FastTextState in Neo4j."""
        _create_fake_model_files(model_dir, 1)
        _setup_fasttext_state(
            neo4j,
            version=1,
            vocab_size=3000,
            corpus_size=8000,
            last_trained_post_id=500,
        )
        archive = tmp_path / "bundle.tar.gz"
        export_bundle(settings, neo4j, archive, mode="full")

        for f in model_dir.iterdir():
            f.unlink()

        import_bundle(settings, neo4j, archive, force=True)

        # Verify state was updated in Neo4j
        neo4j.label("FastTextState")
        result = list(
            neo4j.execute_and_fetch_labeled(
                "MATCH (s:__label__ {id: $id}) "
                "RETURN s.version AS version, "
                "       s.vocab_size AS vocab_size, "
                "       s.corpus_size AS corpus_size",
                {"label": "FastTextState"},
                {"id": STATE_NODE_ID},
            )
        )
        assert len(result) == 1
        assert result[0]["version"] == 1
        assert result[0]["vocab_size"] == 3000
        assert result[0]["corpus_size"] == 8000

    def test_mismatched_vector_size_raises_value_error(
        self,
        model_dir: Path,
        tmp_path: Path,
        neo4j: Neo4jClient,
    ) -> None:
        """Import should reject a bundle with different vector_size."""
        # Export with vector_size=128
        settings_128 = HintGridSettings(
            fasttext_model_path=str(model_dir),
            fasttext_vector_size=128,
        )
        _create_fake_model_files(model_dir, 1)
        _setup_fasttext_state(neo4j, version=1)
        archive = tmp_path / "bundle.tar.gz"
        export_bundle(settings_128, neo4j, archive, mode="full")

        for f in model_dir.iterdir():
            f.unlink()

        # Import with vector_size=256 should fail
        settings_256 = HintGridSettings(
            fasttext_model_path=str(model_dir),
            fasttext_vector_size=256,
        )
        with pytest.raises(ValueError, match="Incompatible vector_size"):
            import_bundle(settings_256, neo4j, archive, force=True)

    def test_corrupted_archive_raises_value_error(
        self,
        model_dir: Path,
        settings: HintGridSettings,
        tmp_path: Path,
        neo4j: Neo4jClient,
    ) -> None:
        """Import should detect corrupted files via checksum mismatch."""
        _create_fake_model_files(model_dir, 1)
        _setup_fasttext_state(neo4j, version=1)
        archive = tmp_path / "bundle.tar.gz"
        export_bundle(settings, neo4j, archive, mode="full")

        for f in model_dir.iterdir():
            f.unlink()

        # Corrupt the archive: rewrite a model file inside with wrong content
        corrupted_archive = tmp_path / "corrupted.tar.gz"
        with tarfile.open(archive, "r:gz") as src_tar:
            with tarfile.open(corrupted_archive, "w:gz") as dst_tar:
                for member in src_tar.getmembers():
                    if member.name == "phraser_v1.pkl":
                        # Replace with different content
                        data = b"CORRUPTED DATA THAT WONT MATCH CHECKSUM"
                        member.size = len(data)
                        import io

                        dst_tar.addfile(member, io.BytesIO(data))
                    else:
                        extracted = src_tar.extractfile(member)
                        if extracted is not None:
                            dst_tar.addfile(member, extracted)
                        else:
                            dst_tar.addfile(member)

        with pytest.raises(ValueError, match="Checksum mismatch"):
            import_bundle(settings, neo4j, corrupted_archive, force=True)

    def test_archive_missing_manifest_raises_value_error(
        self,
        model_dir: Path,
        settings: HintGridSettings,
        tmp_path: Path,
        neo4j: Neo4jClient,
    ) -> None:
        """Import should reject an archive without manifest.json."""
        _setup_fasttext_state(neo4j, version=1)
        bad_archive = tmp_path / "no_manifest.tar.gz"
        with tarfile.open(bad_archive, "w:gz") as tar:
            dummy = tmp_path / "dummy.bin"
            dummy.write_bytes(b"no manifest here")
            tar.add(str(dummy), arcname="dummy.bin")

        with pytest.raises(ValueError, match="manifest.json not found"):
            import_bundle(settings, neo4j, bad_archive, force=True)

    def test_inference_roundtrip_with_quantized(
        self,
        model_dir: Path,
        settings: HintGridSettings,
        tmp_path: Path,
        neo4j: Neo4jClient,
    ) -> None:
        """Inference bundle should export/import quantized model correctly."""
        _create_fake_model_files(model_dir, 1, quantized=True)
        _setup_fasttext_state(neo4j, version=1)
        archive = tmp_path / "bundle.tar.gz"
        export_bundle(settings, neo4j, archive, mode="inference")

        for f in model_dir.iterdir():
            f.unlink()

        result = import_bundle(settings, neo4j, archive, force=True)

        assert result.mode == "inference"
        assert (model_dir / "phraser_v1.pkl").exists()
        assert (model_dir / "fasttext_v1.q.bin").exists()
        # phrases should NOT be present in inference bundle
        assert not (model_dir / "phrases_v1.pkl").exists()
