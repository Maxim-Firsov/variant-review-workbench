"""Standalone entrypoint for preparing the processed ClinVar cache."""

from __future__ import annotations

import argparse
from pathlib import Path

from .app_service import PipelineUsageError
from .clinvar_index import prepare_processed_clinvar_cache


def build_parser() -> argparse.ArgumentParser:
    """Create the command-line parser for standalone cache bootstrap."""
    parser = argparse.ArgumentParser(
        description="Build or refresh the processed ClinVar SQLite cache without running a report.",
    )
    parser.add_argument("--variant-summary", required=True, help="Path to ClinVar variant_summary.txt.gz.")
    parser.add_argument(
        "--conflict-summary",
        default=None,
        help="Optional path to summary_of_conflicting_interpretations.txt.",
    )
    parser.add_argument(
        "--submission-summary",
        default=None,
        help="Optional path to submission_summary.txt.gz.",
    )
    parser.add_argument(
        "--clinvar-cache-db",
        default=None,
        help="Optional output path for the processed ClinVar SQLite cache.",
    )
    parser.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Delete any existing processed cache and stale temp rebuild artifacts before rebuilding.",
    )
    return parser


def _validate_existing_file(path: Path, label: str) -> None:
    """Validate that an expected cache-bootstrap input exists as a file."""
    if not path.exists():
        raise PipelineUsageError(f"{label} was not found: {path}")
    if not path.is_file():
        raise PipelineUsageError(f"{label} must be a file path: {path}")


def main() -> None:
    """Parse CLI arguments and prepare a processed ClinVar cache."""
    parser = build_parser()
    args = parser.parse_args()

    try:
        variant_summary_path = Path(args.variant_summary)
        _validate_existing_file(variant_summary_path, "ClinVar variant summary")

        conflict_summary_path = Path(args.conflict_summary) if args.conflict_summary else None
        if conflict_summary_path is not None:
            _validate_existing_file(conflict_summary_path, "ClinVar conflict summary")

        submission_summary_path = Path(args.submission_summary) if args.submission_summary else None
        if submission_summary_path is not None:
            _validate_existing_file(submission_summary_path, "ClinVar submission summary")

        cache_db_path = Path(args.clinvar_cache_db) if args.clinvar_cache_db else None
        if cache_db_path is not None and cache_db_path.exists() and cache_db_path.is_dir():
            raise PipelineUsageError(f"ClinVar cache path must be a file path, not a directory: {cache_db_path}")

        resolved_cache_db_path = prepare_processed_clinvar_cache(
            variant_summary_path=variant_summary_path,
            conflict_summary_path=conflict_summary_path,
            submission_summary_path=submission_summary_path,
            cache_db_path=cache_db_path,
            force_rebuild=bool(args.force_rebuild),
        )
    except PipelineUsageError as error:
        parser.exit(2, f"Error: {error}\n")
    except ValueError as error:
        parser.exit(2, f"Error: {error}\n")
    except OSError as error:
        parser.exit(1, f"Runtime error: {error}\n")

    print(f"Processed ClinVar cache ready: {resolved_cache_db_path.resolve()}")


if __name__ == "__main__":
    main()
