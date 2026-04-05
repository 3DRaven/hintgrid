"""Model bundle export/import for distributing pretrained FastText+Phraser models.

Packs all model files and metadata into a single .tar.gz archive
for easy distribution to other HintGrid installations.

Two bundle modes:
- inference: minimal (Phraser + quantized FastText) for embedding only
- full: complete (Phrases + full FastText + quantized) for continued training
"""

from __future__ import annotations

import hashlib
import logging
import os
import platform
import sys
import tarfile
import tempfile
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from datetime import datetime, UTC
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import gensim

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.config import HintGridSettings

from hintgrid.utils.coercion import coerce_int, coerce_str

import msgspec

logger = logging.getLogger(__name__)

MANIFEST_FILENAME = "manifest.json"
MANIFEST_SCHEMA_VERSION = 1
VALID_BUNDLE_MODES = ("inference", "full")

BundleMode = Literal["inference", "full"]


@dataclass
class TrainingParams:
    """Training parameters stored in the manifest for compatibility checks."""

    vector_size: int
    window: int
    min_count: int
    bucket: int
    epochs: int
    max_vocab_size: int


@dataclass
class BundleStatistics:
    """Model statistics from the FastTextState node."""

    vocab_size: int
    corpus_size: int
    last_trained_post_id: int


@dataclass
class Compatibility:
    """Version information for compatibility warnings."""

    hintgrid_version: str
    gensim_version: str
    python_version: str


@dataclass
class BundleManifest:
    """Metadata for a model bundle archive."""

    schema_version: int
    version: int
    mode: BundleMode
    training_params: TrainingParams
    statistics: BundleStatistics
    compatibility: Compatibility
    files: dict[str, str]
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())


def _sha256(path: Path) -> str:
    """Compute SHA-256 hash of a file."""
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _model_files_for_version(
    model_path: Path,
    version: int,
    mode: BundleMode,
) -> list[Path]:
    """Collect model files for the given version and mode.

    Args:
        model_path: Directory containing model files
        version: Model version number
        mode: Bundle mode (inference or full)

    Returns:
        List of existing model file paths to include in bundle
    """
    # Files always included (if they exist)
    candidates: list[Path] = [
        model_path / f"phraser_v{version}.pkl",
    ]

    if mode == "full":
        # Full mode: include everything for continued training
        candidates.extend([
            model_path / f"phrases_v{version}.pkl",
            model_path / f"fasttext_v{version}.bin",
            model_path / f"fasttext_v{version}.q.bin",
            model_path / f"fasttext_v{version}.bin.wv.vectors_ngrams.npy",
        ])
    else:
        # Inference mode: prefer quantized, fall back to full
        quantized = model_path / f"fasttext_v{version}.q.bin"
        full_bin = model_path / f"fasttext_v{version}.bin"
        if quantized.exists():
            candidates.append(quantized)
        else:
            candidates.append(full_bin)
        # Ngrams are needed for inference too
        candidates.append(
            model_path / f"fasttext_v{version}.bin.wv.vectors_ngrams.npy",
        )

    return [p for p in candidates if p.exists()]


def _build_manifest(
    files: list[Path],
    version: int,
    mode: BundleMode,
    settings: HintGridSettings,
    state_version: int,
    state_last_trained_post_id: int,
    state_vocab_size: int,
    state_corpus_size: int,
) -> BundleManifest:
    """Build manifest with checksums and metadata."""
    from hintgrid import __version__

    file_checksums: dict[str, str] = {}
    for path in files:
        file_checksums[path.name] = _sha256(path)

    return BundleManifest(
        schema_version=MANIFEST_SCHEMA_VERSION,
        version=version,
        mode=mode,
        training_params=TrainingParams(
            vector_size=settings.fasttext_vector_size,
            window=settings.fasttext_window,
            min_count=settings.fasttext_min_count,
            bucket=settings.fasttext_bucket,
            epochs=settings.fasttext_epochs,
            max_vocab_size=settings.fasttext_max_vocab_size,
        ),
        statistics=BundleStatistics(
            vocab_size=state_vocab_size,
            corpus_size=state_corpus_size,
            last_trained_post_id=state_last_trained_post_id,
        ),
        compatibility=Compatibility(
            hintgrid_version=__version__,
            gensim_version=getattr(gensim, "__version__", "unknown"),
            python_version=f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        ),
        files=file_checksums,
    )


def export_bundle(
    settings: HintGridSettings,
    neo4j: Neo4jClient,
    output_path: Path,
    mode: BundleMode = "inference",
) -> Path:
    """Export pretrained models as a single .tar.gz bundle.

    Args:
        settings: Application settings (for model path and training params)
        neo4j: Neo4j client (for reading FastTextState)
        output_path: Path for the output .tar.gz archive
        mode: Bundle mode - 'inference' (minimal) or 'full' (for retraining)

    Returns:
        Path to the created archive

    Raises:
        FileNotFoundError: If no model files found for current version
        ValueError: If no trained model exists (version 0)
    """
    from hintgrid.embeddings.fasttext_service import (
        INITIAL_VERSION,
        STATE_NODE_ID,
    )

    model_path = Path(os.path.expanduser(settings.fasttext_model_path))

    # Load state from Neo4j
    rows = list(neo4j.execute_and_fetch_labeled(
        "MATCH (s:__label__ {id: $id}) "
        "RETURN s.version AS version, "
        "       s.last_trained_post_id AS last_trained_post_id, "
        "       s.vocab_size AS vocab_size, "
        "       s.corpus_size AS corpus_size",
        {"label": "FastTextState"},
        {"id": STATE_NODE_ID},
    ))

    if not rows:
        raise ValueError(
            "No FastTextState found in Neo4j. Train a model first with: hintgrid train --full"
        )

    row = rows[0]
    version = coerce_int(row.get("version"), INITIAL_VERSION)
    if version == INITIAL_VERSION:
        raise ValueError(
            "No trained model found (version=0). Train a model first with: hintgrid train --full"
        )

    last_trained_post_id = coerce_int(row.get("last_trained_post_id"), 0)
    vocab_size = coerce_int(row.get("vocab_size"), 0)
    corpus_size = coerce_int(row.get("corpus_size"), 0)

    # Collect model files
    files = _model_files_for_version(model_path, version, mode)
    if not files:
        raise FileNotFoundError(
            f"No model files found for version {version} in {model_path}"
        )

    # Build manifest
    manifest = _build_manifest(
        files=files,
        version=version,
        mode=mode,
        settings=settings,
        state_version=version,
        state_last_trained_post_id=last_trained_post_id,
        state_vocab_size=vocab_size,
        state_corpus_size=corpus_size,
    )

    # Pack archive
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False,
    ) as manifest_file:
        # Use msgspec for type-safe JSON encoding
        manifest_dict = asdict(manifest)
        manifest_json = msgspec.json.encode(manifest_dict).decode("utf-8")
        manifest_file.write(manifest_json)
        manifest_tmp = Path(manifest_file.name)

    try:
        with tarfile.open(output_path, "w:gz") as tar:
            tar.add(str(manifest_tmp), arcname=MANIFEST_FILENAME)
            for file_path in files:
                tar.add(str(file_path), arcname=file_path.name)
    finally:
        manifest_tmp.unlink(missing_ok=True)

    archive_size_mb = output_path.stat().st_size / (1024 * 1024)
    logger.info(
        "Exported %s bundle v%d (%d files, %.1f MB) to %s",
        mode,
        version,
        len(files),
        archive_size_mb,
        output_path,
    )

    return output_path


@dataclass
class ImportResult:
    """Result of a bundle import operation."""

    version: int
    mode: BundleMode
    files_installed: int
    state_updated: bool


def import_bundle(
    settings: HintGridSettings,
    neo4j: Neo4jClient,
    archive_path: Path,
    *,
    force: bool = False,
) -> ImportResult:
    """Import a model bundle from a .tar.gz archive.

    Validates checksums, checks compatibility, copies files to model_path,
    and updates FastTextState in Neo4j.

    Args:
        settings: Application settings (for model path and vector_size check)
        neo4j: Neo4j client (for updating FastTextState)
        archive_path: Path to the .tar.gz bundle
        force: If True, overwrite existing model files for the same version

    Returns:
        ImportResult with details about what was imported

    Raises:
        FileNotFoundError: If archive does not exist
        ValueError: If manifest is invalid, checksums fail, or vector_size mismatch
    """
    from hintgrid.embeddings.fasttext_service import (
        INITIAL_VERSION,
        STATE_NODE_ID,
    )

    if not archive_path.exists():
        raise FileNotFoundError(f"Bundle archive not found: {archive_path}")

    model_path = Path(os.path.expanduser(settings.fasttext_model_path))
    model_path.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        # Extract archive safely
        with tarfile.open(archive_path, "r:gz") as tar:
            tar.extractall(path=tmpdir)

        # Read and parse manifest
        manifest_path = tmp_path / MANIFEST_FILENAME
        if not manifest_path.exists():
            raise ValueError(
                "Invalid bundle: manifest.json not found in archive"
            )

        # Use msgspec for type-safe JSON decoding
        manifest_bytes = manifest_path.read_bytes()
        manifest_data: dict[str, object] = msgspec.json.decode(manifest_bytes, type=dict[str, object])

        manifest = _parse_manifest(manifest_data)

        # Validate checksums
        _validate_checksums(tmp_path, manifest)

        # Validate compatibility
        _validate_compatibility(settings, manifest)

        # Check version conflict
        if not force:
            _check_version_conflict(model_path, manifest.version)

        # Install files
        files_installed = _install_files(tmp_path, model_path, manifest)

        # Update Neo4j state
        _update_neo4j_state(neo4j, manifest, STATE_NODE_ID, INITIAL_VERSION)
        state_updated = True

    logger.info(
        "Imported %s bundle v%d (%d files) from %s",
        manifest.mode,
        manifest.version,
        files_installed,
        archive_path,
    )

    return ImportResult(
        version=manifest.version,
        mode=manifest.mode,
        files_installed=files_installed,
        state_updated=state_updated,
    )


def _get_int(data: dict[str, object], key: str, default: int) -> int:
    """Safely extract an int from a dict with object values."""
    return coerce_int(data.get(key, default), default)


def _get_str(data: dict[str, object], key: str, default: str) -> str:
    """Safely extract a str from a dict with object values."""
    return coerce_str(data.get(key, default), default)


def _narrow_dict(raw: object) -> dict[str, object]:
    """Narrow a raw object to dict[str, object] using msgspec validation.

    Uses msgspec for type-safe validation instead of isinstance.
    """
    # Use msgspec for type validation - converts to dict[str, object] if valid
    try:
        validated = msgspec.convert(raw, type=dict[str, object])
        return validated
    except msgspec.ValidationError as e:
        raise ValueError(f"Expected dict, got {type(raw).__name__}: {e}") from e


def _parse_manifest(data: dict[str, object]) -> BundleManifest:
    """Parse and validate manifest data from JSON."""
    try:
        tp = _narrow_dict(data.get("training_params"))
        stats = _narrow_dict(data.get("statistics"))
        compat = _narrow_dict(data.get("compatibility"))

        files_dict = _narrow_dict(data.get("files"))

        mode_val = _get_str(data, "mode", "inference")
        if mode_val not in VALID_BUNDLE_MODES:
            raise ValueError(f"Invalid mode: {mode_val}")

        # Narrow mode_val type for Literal
        bundle_mode: BundleMode = "full" if mode_val == "full" else "inference"

        files_typed: dict[str, str] = {
            k: coerce_str(v) for k, v in files_dict.items()
        }

        return BundleManifest(
            schema_version=_get_int(data, "schema_version", 1),
            version=_get_int(data, "version", 0),
            mode=bundle_mode,
            training_params=TrainingParams(
                vector_size=_get_int(tp, "vector_size", 128),
                window=_get_int(tp, "window", 3),
                min_count=_get_int(tp, "min_count", 10),
                bucket=_get_int(tp, "bucket", 10000),
                epochs=_get_int(tp, "epochs", 5),
                max_vocab_size=_get_int(tp, "max_vocab_size", 500000),
            ),
            statistics=BundleStatistics(
                vocab_size=_get_int(stats, "vocab_size", 0),
                corpus_size=_get_int(stats, "corpus_size", 0),
                last_trained_post_id=_get_int(stats, "last_trained_post_id", 0),
            ),
            compatibility=Compatibility(
                hintgrid_version=_get_str(compat, "hintgrid_version", "unknown"),
                gensim_version=_get_str(compat, "gensim_version", "unknown"),
                python_version=_get_str(compat, "python_version", "unknown"),
            ),
            files=files_typed,
            created_at=_get_str(data, "created_at", ""),
        )
    except (TypeError, KeyError) as exc:
        raise ValueError(f"Invalid manifest format: {exc}") from exc


def _validate_checksums(
    extract_dir: Path,
    manifest: BundleManifest,
) -> None:
    """Verify SHA-256 checksums for all files in the manifest."""
    for filename, expected_hash in manifest.files.items():
        file_path = extract_dir / filename
        if not file_path.exists():
            raise ValueError(
                f"Missing file from bundle: {filename}"
            )
        actual_hash = _sha256(file_path)
        if actual_hash != expected_hash:
            raise ValueError(
                f"Checksum mismatch for {filename}: "
                f"expected {expected_hash[:16]}..., got {actual_hash[:16]}..."
            )
    logger.debug("All %d file checksums verified", len(manifest.files))


def _validate_compatibility(
    settings: HintGridSettings,
    manifest: BundleManifest,
) -> None:
    """Check compatibility between bundle and current settings.

    Raises ValueError if vector_size does not match (critical).
    Logs warnings for gensim version mismatches (non-critical).
    """
    bundle_vector_size = manifest.training_params.vector_size
    current_vector_size = settings.fasttext_vector_size

    if bundle_vector_size != current_vector_size:
        raise ValueError(
            f"Incompatible vector_size: bundle has {bundle_vector_size}, "
            f"but current settings use {current_vector_size}. "
            f"Set HINTGRID_FASTTEXT_VECTOR_SIZE={bundle_vector_size} "
            f"or use a compatible bundle."
        )

    bundle_gensim = manifest.compatibility.gensim_version
    current_gensim: str = getattr(gensim, "__version__", "unknown")
    if bundle_gensim != current_gensim:
        logger.warning(
            "Gensim version mismatch: bundle was created with %s, "
            "current installation has %s. Models may not load correctly.",
            bundle_gensim,
            current_gensim,
        )

    bundle_python = manifest.compatibility.python_version
    current_python = platform.python_version()
    bundle_major_minor = ".".join(bundle_python.split(".")[:2])
    current_major_minor = ".".join(current_python.split(".")[:2])
    if bundle_major_minor != current_major_minor:
        logger.warning(
            "Python version mismatch: bundle was created with %s, "
            "current installation has %s. Pickle files may not load.",
            bundle_python,
            current_python,
        )


def _check_version_conflict(
    model_path: Path,
    version: int,
) -> None:
    """Check if model files for this version already exist."""
    existing_patterns = [
        f"phrases_v{version}.pkl",
        f"phraser_v{version}.pkl",
        f"fasttext_v{version}.bin",
        f"fasttext_v{version}.q.bin",
    ]
    existing_files = [
        model_path / name
        for name in existing_patterns
        if (model_path / name).exists()
    ]
    if existing_files:
        names = ", ".join(p.name for p in existing_files)
        raise ValueError(
            f"Model files for version {version} already exist: {names}. "
            f"Use --force to overwrite."
        )


def _install_files(
    extract_dir: Path,
    model_path: Path,
    manifest: BundleManifest,
) -> int:
    """Copy model files from extract directory to model_path."""
    import shutil

    installed = 0
    for filename in manifest.files:
        src = extract_dir / filename
        dst = model_path / filename
        shutil.copy2(str(src), str(dst))
        installed += 1
        logger.debug("Installed %s -> %s", src.name, dst)

    return installed


def _update_neo4j_state(
    neo4j: Neo4jClient,
    manifest: BundleManifest,
    state_node_id: str,
    initial_version: int,
) -> None:
    """Update FastTextState in Neo4j from manifest data.

    Creates the state node if it doesn't exist, then updates it.
    """
    # Ensure state node exists
    neo4j.execute(
        "CALL apoc.merge.node($labels, {id: $id}, "
        "{version: $version, last_trained_post_id: 0, "
        " vocab_size: 0, corpus_size: 0, "
        " updated_at: timestamp()}, {}) "
        "YIELD node",
        {
            "labels": neo4j.labels_list("FastTextState"),
            "id": state_node_id,
            "version": initial_version,
        },
    )

    # Update with bundle data
    neo4j.execute_labeled(
        "MATCH (s:__label__ {id: $id}) "
        "SET s.version = $version, "
        "    s.last_trained_post_id = $last_trained_post_id, "
        "    s.vocab_size = $vocab_size, "
        "    s.corpus_size = $corpus_size, "
        "    s.updated_at = timestamp()",
        {"label": "FastTextState"},
        {
            "id": state_node_id,
            "version": manifest.version,
            "last_trained_post_id": manifest.statistics.last_trained_post_id,
            "vocab_size": manifest.statistics.vocab_size,
            "corpus_size": manifest.statistics.corpus_size,
        },
    )
    logger.info(
        "Updated FastTextState: version=%d, vocab_size=%d, corpus_size=%d",
        manifest.version,
        manifest.statistics.vocab_size,
        manifest.statistics.corpus_size,
    )
