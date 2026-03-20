"""Upload and workspace storage helpers for the web interface."""

from __future__ import annotations

import gzip
import io
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename


class UploadValidationError(ValueError):
    """Raised when a web upload cannot be accepted safely."""


_VCF_HEADER_PREFIXES = ("##fileformat=VCF", "#CHROM\t")


@dataclass(slots=True)
class RunWorkspace:
    """Filesystem layout for one submitted web run."""

    job_id: str
    run_root: Path
    upload_dir: Path
    output_dir: Path
    uploaded_vcf_path: Path


def ensure_storage_roots(upload_root: Path, run_output_root: Path) -> None:
    """Create the configured storage roots if they do not already exist."""
    upload_root.mkdir(parents=True, exist_ok=True)
    run_output_root.mkdir(parents=True, exist_ok=True)


def _validate_upload_filename(filename: str) -> str:
    if not filename:
        raise UploadValidationError("A VCF or VCF.GZ file is required.")

    lowered = filename.lower()
    if not (lowered.endswith(".vcf") or lowered.endswith(".vcf.gz")):
        raise UploadValidationError("Uploaded file must end with .vcf or .vcf.gz.")

    safe_name = secure_filename(Path(filename).name)
    if not safe_name:
        raise UploadValidationError("Uploaded filename is not usable.")
    return safe_name


def _read_upload_prefix(upload: FileStorage, size: int = 4096) -> bytes:
    stream = upload.stream
    current_position = stream.tell()
    prefix = stream.read(size)
    stream.seek(current_position)
    return prefix


def _looks_like_vcf(upload: FileStorage, filename: str) -> None:
    prefix = _read_upload_prefix(upload)
    lowered = filename.lower()

    if lowered.endswith(".vcf.gz"):
        if not prefix.startswith(b"\x1f\x8b"):
            raise UploadValidationError("Uploaded .vcf.gz file is not gzip-compressed.")
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(prefix)) as handle:
                header = handle.read(512).decode("utf-8", errors="ignore")
        except OSError as error:
            raise UploadValidationError("Uploaded .vcf.gz file could not be read as gzip data.") from error
    else:
        header = prefix.decode("utf-8", errors="ignore")

    if not any(marker in header for marker in _VCF_HEADER_PREFIXES):
        raise UploadValidationError("Uploaded file does not appear to be a VCF.")


def create_run_workspace(*, job_id: str, upload_root: Path, run_output_root: Path) -> RunWorkspace:
    """Create isolated upload and output directories for a submitted run."""
    run_root = run_output_root / job_id
    upload_dir = upload_root / job_id
    output_dir = run_root / "outputs"
    upload_dir.mkdir(parents=True, exist_ok=False)
    output_dir.mkdir(parents=True, exist_ok=False)
    return RunWorkspace(
        job_id=job_id,
        run_root=run_root,
        upload_dir=upload_dir,
        output_dir=output_dir,
        uploaded_vcf_path=upload_dir / "pending-upload",
    )


def save_uploaded_vcf(*, upload: FileStorage | None, workspace: RunWorkspace) -> RunWorkspace:
    """Persist an uploaded VCF into the run-specific upload directory."""
    safe_name = _validate_upload_filename(upload.filename if upload is not None else "")
    assert upload is not None
    try:
        _looks_like_vcf(upload, safe_name)
        destination = workspace.upload_dir / safe_name
        upload.save(destination)
        return RunWorkspace(
            job_id=workspace.job_id,
            run_root=workspace.run_root,
            upload_dir=workspace.upload_dir,
            output_dir=workspace.output_dir,
            uploaded_vcf_path=destination,
        )
    finally:
        upload.close()


def save_demo_vcf(*, demo_vcf_path: Path, workspace: RunWorkspace) -> RunWorkspace:
    """Copy a configured demo VCF into the run-specific upload directory."""
    if not demo_vcf_path.exists():
        raise UploadValidationError("Configured demo sample is not available.")
    if not demo_vcf_path.is_file():
        raise UploadValidationError("Configured demo sample path is not a file.")

    destination = workspace.upload_dir / secure_filename(demo_vcf_path.name)
    shutil.copy2(demo_vcf_path, destination)
    return RunWorkspace(
        job_id=workspace.job_id,
        run_root=workspace.run_root,
        upload_dir=workspace.upload_dir,
        output_dir=workspace.output_dir,
        uploaded_vcf_path=destination,
    )


def cleanup_expired_run_directories(*, upload_root: Path, run_output_root: Path, retention_hours: int) -> None:
    """Remove old run directories beyond the configured retention window."""
    expiry_cutoff = datetime.now(UTC) - timedelta(hours=retention_hours)
    for root in (upload_root, run_output_root):
        if not root.exists():
            continue
        for child in root.iterdir():
            if not child.is_dir():
                continue
            modified_at = datetime.fromtimestamp(child.stat().st_mtime, tz=UTC)
            if modified_at < expiry_cutoff:
                shutil.rmtree(child, ignore_errors=True)
