"""ClinVar snapshot loading and indexing helpers."""

from __future__ import annotations

import csv
import gzip
import json
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Iterator, TextIO

import pandas as pd

from .models import (
    ClinVarMatch,
    ConflictSummary,
    DataProvenance,
    GenomeAssembly,
    InputVariant,
    MatchStrategy,
    SubmissionEvidence,
)
from .vcf_parser import normalize_allele, normalize_chromosome

VARIANT_SUMMARY_COLUMNS = [
    "#AlleleID",
    "Type",
    "Name",
    "GeneSymbol",
    "ClinicalSignificance",
    "LastEvaluated",
    "PhenotypeList",
    "ReviewStatus",
    "Origin",
    "Assembly",
    "Chromosome",
    "VariationID",
    "PositionVCF",
    "ReferenceAlleleVCF",
    "AlternateAlleleVCF",
    "RCVaccession",
]
VariantKey = tuple[str, str, int, str, str]
CACHE_DB_FILENAME = "clinvar_lookup_cache.sqlite3"


def _configure_csv_field_limit() -> None:
    """Raise the CSV parser field limit to tolerate large ClinVar support-file cells."""
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit //= 10


_configure_csv_field_limit()


def review_status_to_stars(review_status: str | None) -> int | None:
    """Convert ClinVar review status text into the familiar star count."""
    if not review_status:
        return None

    normalized = review_status.strip().lower()
    if normalized == "practice guideline":
        return 4
    if normalized == "reviewed by expert panel":
        return 3
    if normalized == "criteria provided, multiple submitters, no conflicts":
        return 2
    if normalized == "criteria provided, single submitter":
        return 1
    return 0


def _open_text_stream(path: Path) -> TextIO:
    """Open a plain-text or gzipped tabular ClinVar file."""
    if path.suffix.lower() == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def _parse_assembly(value: str | None) -> GenomeAssembly:
    """Normalize assembly labels from ClinVar files into supported enum values."""
    if value == GenomeAssembly.GRCH37.value:
        return GenomeAssembly.GRCH37
    if value == GenomeAssembly.GRCH38.value:
        return GenomeAssembly.GRCH38
    return GenomeAssembly.UNKNOWN


def _split_pipe_values(value: str | None) -> list[str]:
    """Split ClinVar multi-value fields while removing empty sentinels."""
    if value is None:
        return []
    results = []
    for part in value.split("|"):
        normalized = part.strip()
        if normalized and normalized.lower() not in {"-", "na", "not provided"}:
            results.append(normalized)
    return results


def _choose_preferred_match(existing: ClinVarMatch, candidate: ClinVarMatch) -> ClinVarMatch:
    """Prefer the stronger ClinVar aggregate when duplicate coordinate keys appear."""
    existing_score = (
        existing.review_stars or -1,
        1 if existing.clinical_significance else 0,
        existing.variation_id or -1,
    )
    candidate_score = (
        candidate.review_stars or -1,
        1 if candidate.clinical_significance else 0,
        candidate.variation_id or -1,
    )
    return candidate if candidate_score > existing_score else existing


def _iter_submission_rows(submission_summary_path: Path) -> Iterator[dict[str, str]]:
    """Yield parsed submission-summary rows after skipping the explanatory preamble."""
    with _open_text_stream(submission_summary_path) as handle:
        header_line = None
        for line in handle:
            if line.startswith("#VariationID\t"):
                header_line = line.lstrip("#").rstrip("\n")
                break

        if header_line is None:
            raise ValueError("Submission summary file does not contain a tabular header.")

        fieldnames = header_line.split("\t")
        reader = csv.DictReader(handle, fieldnames=fieldnames, delimiter="\t")
        for row in reader:
            yield {key: (value or "").strip() for key, value in row.items()}


def _build_provenance(source_name: str, source_kind: str, source_path: Path) -> DataProvenance:
    """Construct provenance metadata from a local file path."""
    return DataProvenance(
        source_name=source_name,
        source_kind=source_kind,
        source_path=str(source_path),
    )


def _default_cache_db_path(variant_summary_path: Path) -> Path:
    """Choose a stable processed-cache location near the raw ClinVar files."""
    parent = variant_summary_path.parent
    if parent.name == "raw":
        processed_dir = parent.parent / "processed"
        processed_dir.mkdir(parents=True, exist_ok=True)
        return processed_dir / CACHE_DB_FILENAME
    return parent / CACHE_DB_FILENAME


def _source_signature(path: Path | None) -> dict[str, object] | None:
    """Capture a lightweight signature used to invalidate processed cache state."""
    if path is None:
        return None
    stat = path.stat()
    return {
        "path": str(path.resolve()),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def _connect_cache_db(cache_db_path: Path) -> sqlite3.Connection:
    """Open the local processed ClinVar cache database."""
    cache_db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(cache_db_path)
    connection.row_factory = sqlite3.Row
    return connection


def _configure_cache_connection(connection: sqlite3.Connection) -> None:
    """Apply pragmatic SQLite settings for local processed ClinVar cache usage."""
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA temp_store=MEMORY")


def _configure_cache_build_connection(connection: sqlite3.Connection) -> None:
    """Use a simpler journal mode while building a fresh cache from raw sources."""
    connection.execute("PRAGMA journal_mode=DELETE")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA temp_store=MEMORY")


def _cache_rebuild_artifacts(cache_db_path: Path) -> list[Path]:
    """List temporary files produced while rebuilding a processed cache."""
    temporary_path = cache_db_path.with_suffix(cache_db_path.suffix + ".tmp")
    return [
        temporary_path,
        temporary_path.with_name(temporary_path.name + "-shm"),
        temporary_path.with_name(temporary_path.name + "-wal"),
    ]


def _cleanup_stale_cache_rebuild_artifacts(cache_db_path: Path) -> None:
    """Remove stale temp rebuild artifacts left behind by interrupted cache bootstrap."""
    for artifact in _cache_rebuild_artifacts(cache_db_path):
        if artifact.exists():
            artifact.unlink()


def _initialize_cache_schema(connection: sqlite3.Connection) -> None:
    """Create the processed ClinVar cache schema if it does not already exist."""
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS variant_matches (
            assembly TEXT NOT NULL,
            chromosome TEXT NOT NULL,
            position INTEGER NOT NULL,
            reference_allele TEXT NOT NULL,
            alternate_allele TEXT NOT NULL,
            variation_id INTEGER NOT NULL,
            allele_id INTEGER NOT NULL,
            accession TEXT,
            preferred_name TEXT,
            gene TEXT,
            condition_names TEXT NOT NULL,
            clinical_significance TEXT,
            review_status TEXT,
            review_stars INTEGER,
            interpretation_origin TEXT,
            last_evaluated TEXT,
            review_stars_sort INTEGER NOT NULL,
            significance_rank INTEGER NOT NULL,
            PRIMARY KEY (assembly, chromosome, position, reference_allele, alternate_allele)
        );

        CREATE TABLE IF NOT EXISTS conflicts (
            variation_id INTEGER PRIMARY KEY,
            conflict_significance TEXT NOT NULL,
            submitter_count INTEGER,
            summary_text TEXT
        );

        CREATE TABLE IF NOT EXISTS submissions (
            variation_id INTEGER PRIMARY KEY,
            total_submissions INTEGER,
            submitter_names TEXT NOT NULL,
            review_statuses TEXT NOT NULL,
            clinical_significances TEXT NOT NULL
        );
        """
    )


def _read_cache_metadata(connection: sqlite3.Connection, key: str) -> dict[str, object] | None:
    """Read a JSON metadata value from the processed ClinVar cache."""
    row = connection.execute("SELECT value FROM metadata WHERE key = ?", (key,)).fetchone()
    if row is None:
        return None
    return json.loads(row["value"])


def _cache_is_current(
    connection: sqlite3.Connection,
    *,
    variant_summary_path: Path,
    conflict_summary_path: Path | None,
    submission_summary_path: Path | None,
) -> bool:
    """Return whether the processed ClinVar cache matches the current raw files."""
    expected = {
        "variant_summary": _source_signature(variant_summary_path),
        "conflict_summary": _source_signature(conflict_summary_path),
        "submission_summary": _source_signature(submission_summary_path),
    }
    observed = {
        "variant_summary": _read_cache_metadata(connection, "variant_summary"),
        "conflict_summary": _read_cache_metadata(connection, "conflict_summary"),
        "submission_summary": _read_cache_metadata(connection, "submission_summary"),
    }
    return observed == expected


def _write_cache_metadata(
    connection: sqlite3.Connection,
    *,
    variant_summary_path: Path,
    conflict_summary_path: Path | None,
    submission_summary_path: Path | None,
) -> None:
    """Persist source signatures for cache invalidation."""
    payloads = {
        "variant_summary": _source_signature(variant_summary_path),
        "conflict_summary": _source_signature(conflict_summary_path),
        "submission_summary": _source_signature(submission_summary_path),
    }
    connection.executemany(
        "INSERT OR REPLACE INTO metadata(key, value) VALUES(?, ?)",
        [(key, json.dumps(value, sort_keys=True)) for key, value in payloads.items()],
    )


def _parse_int_field(value: str | None) -> int | None:
    """Parse an integer field from ClinVar text, returning None for blanks."""
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    try:
        return int(normalized)
    except ValueError:
        return None


def _build_match_from_variant_summary_row(row: dict[str, str]) -> ClinVarMatch | None:
    """Convert a variant_summary row into a normalized exact-match candidate."""
    assembly = _parse_assembly(row.get("Assembly"))
    if assembly == GenomeAssembly.UNKNOWN:
        return None

    position_vcf = (row.get("PositionVCF") or "").strip()
    reference_allele = normalize_allele((row.get("ReferenceAlleleVCF") or "").strip())
    alternate_allele = normalize_allele((row.get("AlternateAlleleVCF") or "").strip())
    chromosome = normalize_chromosome((row.get("Chromosome") or "").strip())
    if not position_vcf or not chromosome:
        return None
    if reference_allele in {"", "-"} or alternate_allele in {"", "-"}:
        return None

    variation_id = _parse_int_field(row.get("VariationID"))
    allele_id = _parse_int_field(row.get("#AlleleID"))
    parsed_position = _parse_int_field(position_vcf)
    if variation_id is None or allele_id is None or parsed_position is None:
        return None

    return ClinVarMatch(
        matched=True,
        match_strategy=MatchStrategy.EXACT,
        assembly=assembly,
        chromosome=chromosome,
        position=parsed_position,
        reference_allele=reference_allele,
        alternate_allele=alternate_allele,
        variation_id=variation_id,
        allele_id=allele_id,
        accession=(row.get("RCVaccession") or "").split("|", 1)[0] or None,
        preferred_name=(row.get("Name") or "").strip() or None,
        gene=(row.get("GeneSymbol") or "").strip() or None,
        condition_names=_split_pipe_values((row.get("PhenotypeList") or "").strip()),
        clinical_significance=(row.get("ClinicalSignificance") or "").strip() or None,
        review_status=(row.get("ReviewStatus") or "").strip() or None,
        review_stars=review_status_to_stars((row.get("ReviewStatus") or "").strip() or None),
        interpretation_origin=(row.get("Origin") or "").strip() or None,
        last_evaluated=(row.get("LastEvaluated") or "").strip() or None,
    )


def _serialize_json_list(items: list[str]) -> str:
    """Persist a list-valued field into the processed cache."""
    return json.dumps(items, separators=(",", ":"))


def _deserialize_json_list(payload: str | None) -> list[str]:
    """Read a cached list-valued field from the processed cache."""
    if not payload:
        return []
    return list(json.loads(payload))


def _variant_match_row_from_candidate(candidate: ClinVarMatch) -> tuple[object, ...]:
    """Convert a normalized ClinVar candidate into a cache-table row."""
    return (
        candidate.assembly.value,
        candidate.chromosome,
        candidate.position,
        candidate.reference_allele,
        candidate.alternate_allele,
        candidate.variation_id,
        candidate.allele_id,
        candidate.accession,
        candidate.preferred_name,
        candidate.gene,
        _serialize_json_list(candidate.condition_names),
        candidate.clinical_significance,
        candidate.review_status,
        candidate.review_stars,
        candidate.interpretation_origin,
        candidate.last_evaluated,
        candidate.review_stars if candidate.review_stars is not None else -1,
        1 if candidate.clinical_significance else 0,
    )


def _variant_match_from_cache_row(row: sqlite3.Row) -> ClinVarMatch:
    """Rehydrate a ClinVar exact match from the processed cache."""
    return ClinVarMatch(
        matched=True,
        match_strategy=MatchStrategy.EXACT,
        assembly=GenomeAssembly(row["assembly"]),
        chromosome=row["chromosome"],
        position=row["position"],
        reference_allele=row["reference_allele"],
        alternate_allele=row["alternate_allele"],
        variation_id=row["variation_id"],
        allele_id=row["allele_id"],
        accession=row["accession"],
        preferred_name=row["preferred_name"],
        gene=row["gene"],
        condition_names=_deserialize_json_list(row["condition_names"]),
        clinical_significance=row["clinical_significance"],
        review_status=row["review_status"],
        review_stars=row["review_stars"],
        interpretation_origin=row["interpretation_origin"],
        last_evaluated=row["last_evaluated"],
    )


def _conflict_from_cache_row(row: sqlite3.Row) -> ConflictSummary:
    """Rehydrate a cached conflict summary."""
    return ConflictSummary(
        has_conflict=True,
        conflict_significance=_deserialize_json_list(row["conflict_significance"]),
        submitter_count=row["submitter_count"],
        summary_text=row["summary_text"],
    )


def _submission_from_cache_row(row: sqlite3.Row) -> SubmissionEvidence:
    """Rehydrate cached submission aggregates."""
    return SubmissionEvidence(
        total_submissions=row["total_submissions"],
        submitter_names=_deserialize_json_list(row["submitter_names"]),
        review_statuses=_deserialize_json_list(row["review_statuses"]),
        clinical_significances=_deserialize_json_list(row["clinical_significances"]),
    )


@dataclass(slots=True)
class ClinVarIndex:
    """In-memory ClinVar lookup tables used by the annotation stage."""

    exact_matches: dict[tuple[str, str, int, str, str], ClinVarMatch]
    conflicts_by_variation_id: dict[int, ConflictSummary] = field(default_factory=dict)
    submissions_by_variation_id: dict[int, SubmissionEvidence] = field(default_factory=dict)
    provenance: list[DataProvenance] = field(default_factory=list)

    def lookup(self, input_variant: InputVariant) -> ClinVarMatch:
        """Return the best exact ClinVar match for a normalized input variant."""
        match = self.exact_matches.get(input_variant.variant_key)
        if match is None:
            return ClinVarMatch()

        resolved = match.model_copy(deep=True)
        variation_id = resolved.variation_id
        if variation_id is not None:
            conflict = self.conflicts_by_variation_id.get(variation_id)
            if conflict is not None:
                resolved.conflict = conflict.model_copy(deep=True)
            submission = self.submissions_by_variation_id.get(variation_id)
            if submission is not None:
                resolved.submissions = submission.model_copy(deep=True)
        return resolved


def _rebuild_cache_db(
    cache_db_path: Path,
    *,
    variant_summary_path: Path,
    conflict_summary_path: Path | None,
    submission_summary_path: Path | None,
) -> None:
    """Build a processed SQLite cache from the staged raw ClinVar files."""
    temporary_path = cache_db_path.with_suffix(cache_db_path.suffix + ".tmp")
    _cleanup_stale_cache_rebuild_artifacts(cache_db_path)

    connection = _connect_cache_db(temporary_path)
    try:
        _configure_cache_build_connection(connection)
        _initialize_cache_schema(connection)
        connection.execute("BEGIN")
        connection.execute("DELETE FROM variant_matches")
        connection.execute("DELETE FROM conflicts")
        connection.execute("DELETE FROM submissions")
        connection.execute("DELETE FROM metadata")

        with _open_text_stream(variant_summary_path) as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            batch: list[tuple[object, ...]] = []
            for raw_row in reader:
                row = {key: (value or "").strip() for key, value in raw_row.items()}
                candidate = _build_match_from_variant_summary_row(row)
                if candidate is None:
                    continue
                batch.append(_variant_match_row_from_candidate(candidate))
                if len(batch) >= 5_000:
                    connection.executemany(
                        """
                        INSERT INTO variant_matches(
                            assembly, chromosome, position, reference_allele, alternate_allele,
                            variation_id, allele_id, accession, preferred_name, gene,
                            condition_names, clinical_significance, review_status, review_stars,
                            interpretation_origin, last_evaluated, review_stars_sort, significance_rank
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(assembly, chromosome, position, reference_allele, alternate_allele) DO UPDATE SET
                            variation_id = excluded.variation_id,
                            allele_id = excluded.allele_id,
                            accession = excluded.accession,
                            preferred_name = excluded.preferred_name,
                            gene = excluded.gene,
                            condition_names = excluded.condition_names,
                            clinical_significance = excluded.clinical_significance,
                            review_status = excluded.review_status,
                            review_stars = excluded.review_stars,
                            interpretation_origin = excluded.interpretation_origin,
                            last_evaluated = excluded.last_evaluated,
                            review_stars_sort = excluded.review_stars_sort,
                            significance_rank = excluded.significance_rank
                        WHERE
                            excluded.review_stars_sort > variant_matches.review_stars_sort OR
                            (
                                excluded.review_stars_sort = variant_matches.review_stars_sort AND
                                excluded.significance_rank > variant_matches.significance_rank
                            ) OR
                            (
                                excluded.review_stars_sort = variant_matches.review_stars_sort AND
                                excluded.significance_rank = variant_matches.significance_rank AND
                                excluded.variation_id > variant_matches.variation_id
                            )
                        """,
                        batch,
                    )
                    batch.clear()
            if batch:
                connection.executemany(
                    """
                    INSERT INTO variant_matches(
                        assembly, chromosome, position, reference_allele, alternate_allele,
                        variation_id, allele_id, accession, preferred_name, gene,
                        condition_names, clinical_significance, review_status, review_stars,
                        interpretation_origin, last_evaluated, review_stars_sort, significance_rank
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(assembly, chromosome, position, reference_allele, alternate_allele) DO UPDATE SET
                        variation_id = excluded.variation_id,
                        allele_id = excluded.allele_id,
                        accession = excluded.accession,
                        preferred_name = excluded.preferred_name,
                        gene = excluded.gene,
                        condition_names = excluded.condition_names,
                        clinical_significance = excluded.clinical_significance,
                        review_status = excluded.review_status,
                        review_stars = excluded.review_stars,
                        interpretation_origin = excluded.interpretation_origin,
                        last_evaluated = excluded.last_evaluated,
                        review_stars_sort = excluded.review_stars_sort,
                        significance_rank = excluded.significance_rank
                    WHERE
                        excluded.review_stars_sort > variant_matches.review_stars_sort OR
                        (
                            excluded.review_stars_sort = variant_matches.review_stars_sort AND
                            excluded.significance_rank > variant_matches.significance_rank
                        ) OR
                        (
                            excluded.review_stars_sort = variant_matches.review_stars_sort AND
                            excluded.significance_rank = variant_matches.significance_rank AND
                            excluded.variation_id > variant_matches.variation_id
                        )
                    """,
                    batch,
                )

        if conflict_summary_path is not None:
            conflicts, _ = load_conflict_lookup(conflict_summary_path)
            connection.executemany(
                """
                INSERT OR REPLACE INTO conflicts(variation_id, conflict_significance, submitter_count, summary_text)
                VALUES (?, ?, ?, ?)
                """,
                [
                    (
                        variation_id,
                        _serialize_json_list(conflict.conflict_significance),
                        conflict.submitter_count,
                        conflict.summary_text,
                    )
                    for variation_id, conflict in conflicts.items()
                ],
            )

        if submission_summary_path is not None:
            submissions, _ = load_submission_lookup(submission_summary_path)
            connection.executemany(
                """
                INSERT OR REPLACE INTO submissions(
                    variation_id, total_submissions, submitter_names, review_statuses, clinical_significances
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (
                        variation_id,
                        submission.total_submissions,
                        _serialize_json_list(submission.submitter_names),
                        _serialize_json_list(submission.review_statuses),
                        _serialize_json_list(submission.clinical_significances),
                    )
                    for variation_id, submission in submissions.items()
                ],
            )

        _write_cache_metadata(
            connection,
            variant_summary_path=variant_summary_path,
            conflict_summary_path=conflict_summary_path,
            submission_summary_path=submission_summary_path,
        )
        connection.commit()
    finally:
        connection.close()

    temporary_path.replace(cache_db_path)


def _ensure_cache_db(
    cache_db_path: Path,
    *,
    variant_summary_path: Path,
    conflict_summary_path: Path | None,
    submission_summary_path: Path | None,
) -> None:
    """Ensure a fresh processed SQLite cache exists for the provided raw ClinVar files."""
    if cache_db_path.exists():
        connection = _connect_cache_db(cache_db_path)
        try:
            _initialize_cache_schema(connection)
            if _cache_is_current(
                connection,
                variant_summary_path=variant_summary_path,
                conflict_summary_path=conflict_summary_path,
                submission_summary_path=submission_summary_path,
            ):
                _cleanup_stale_cache_rebuild_artifacts(cache_db_path)
                return
        finally:
            connection.close()

    _rebuild_cache_db(
        cache_db_path,
        variant_summary_path=variant_summary_path,
        conflict_summary_path=conflict_summary_path,
        submission_summary_path=submission_summary_path,
    )


def prepare_processed_clinvar_cache(
    *,
    variant_summary_path: Path,
    conflict_summary_path: Path | None = None,
    submission_summary_path: Path | None = None,
    cache_db_path: Path | None = None,
    force_rebuild: bool = False,
) -> Path:
    """Build or refresh the processed ClinVar cache independently of a report run."""
    resolved_cache_db_path = cache_db_path or _default_cache_db_path(variant_summary_path)
    if force_rebuild and resolved_cache_db_path.exists():
        resolved_cache_db_path.unlink()
    if force_rebuild:
        _cleanup_stale_cache_rebuild_artifacts(resolved_cache_db_path)
    _ensure_cache_db(
        resolved_cache_db_path,
        variant_summary_path=variant_summary_path,
        conflict_summary_path=conflict_summary_path,
        submission_summary_path=submission_summary_path,
    )
    return resolved_cache_db_path


def _load_variant_summary_index_from_cache(
    cache_db_path: Path,
    *,
    variant_summary_path: Path,
    target_variant_keys: set[VariantKey],
) -> ClinVarIndex:
    """Query a processed SQLite cache for the requested exact ClinVar matches."""
    exact_matches: dict[VariantKey, ClinVarMatch] = {}
    connection = _connect_cache_db(cache_db_path)
    try:
        for key in target_variant_keys:
            row = connection.execute(
                """
                SELECT
                    assembly, chromosome, position, reference_allele, alternate_allele,
                    variation_id, allele_id, accession, preferred_name, gene,
                    condition_names, clinical_significance, review_status, review_stars,
                    interpretation_origin, last_evaluated
                FROM variant_matches
                WHERE assembly = ? AND chromosome = ? AND position = ? AND reference_allele = ? AND alternate_allele = ?
                """,
                key,
            ).fetchone()
            if row is None:
                continue
            exact_matches[key] = _variant_match_from_cache_row(row)
    finally:
        connection.close()

    return ClinVarIndex(
        exact_matches=exact_matches,
        provenance=[
            _build_provenance("ClinVar variant summary", "file", variant_summary_path),
            _build_provenance("ClinVar processed cache", "file", cache_db_path),
        ],
    )


def load_variant_summary_index(
    variant_summary_path: Path,
    chunk_size: int = 50_000,
    target_variant_keys: set[VariantKey] | None = None,
) -> ClinVarIndex:
    """Stream `variant_summary.txt.gz` into an exact coordinate lookup index."""
    exact_matches: dict[tuple[str, str, int, str, str], ClinVarMatch] = {}

    if target_variant_keys is not None:
        with _open_text_stream(variant_summary_path) as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            for raw_row in reader:
                row = {key: (value or "").strip() for key, value in raw_row.items()}
                candidate = _build_match_from_variant_summary_row(row)
                if candidate is None:
                    continue
                key = candidate.variant_key
                if key not in target_variant_keys:
                    continue
                existing = exact_matches.get(key)
                exact_matches[key] = candidate if existing is None else _choose_preferred_match(existing, candidate)

        return ClinVarIndex(
            exact_matches=exact_matches,
            provenance=[_build_provenance("ClinVar variant summary", "file", variant_summary_path)],
        )

    reader = pd.read_csv(
        variant_summary_path,
        sep="\t",
        compression="gzip" if variant_summary_path.suffix.lower() == ".gz" else None,
        usecols=VARIANT_SUMMARY_COLUMNS,
        dtype=str,
        chunksize=chunk_size,
        low_memory=False,
    )

    for chunk in reader:
        chunk = chunk.fillna("")
        for row in chunk.itertuples(index=False):
            candidate = _build_match_from_variant_summary_row(
                {
                    "#AlleleID": row[0],
                    "Name": row.Name,
                    "GeneSymbol": row.GeneSymbol,
                    "ClinicalSignificance": row.ClinicalSignificance,
                    "LastEvaluated": row.LastEvaluated,
                    "PhenotypeList": row.PhenotypeList,
                    "ReviewStatus": row.ReviewStatus,
                    "Origin": row.Origin,
                    "Assembly": row.Assembly,
                    "Chromosome": row.Chromosome,
                    "VariationID": row.VariationID,
                    "PositionVCF": row.PositionVCF,
                    "ReferenceAlleleVCF": row.ReferenceAlleleVCF,
                    "AlternateAlleleVCF": row.AlternateAlleleVCF,
                    "RCVaccession": row.RCVaccession,
                }
            )
            if candidate is None:
                continue
            key = candidate.variant_key
            existing = exact_matches.get(key)
            exact_matches[key] = candidate if existing is None else _choose_preferred_match(existing, candidate)

    return ClinVarIndex(
        exact_matches=exact_matches,
        provenance=[_build_provenance("ClinVar variant summary", "file", variant_summary_path)],
    )


def load_conflict_lookup(
    conflict_summary_path: Path,
    target_variation_ids: set[int] | None = None,
) -> tuple[dict[int, ConflictSummary], DataProvenance]:
    """Load conflict summaries keyed by ClinVar VariationID."""
    accumulators: dict[int, dict[str, set[str]]] = {}

    with _open_text_stream(conflict_summary_path) as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            variation_text = (row.get("NCBI_Variation_ID") or "").strip()
            if not variation_text:
                continue

            variation_id = _parse_int_field(variation_text)
            if variation_id is None:
                continue
            if target_variation_ids is not None and variation_id not in target_variation_ids:
                continue

            bucket = accumulators.setdefault(
                variation_id,
                {"submitters": set(), "significances": set(), "preferred_names": set()},
            )
            for key in ("Submitter1", "Submitter2"):
                value = (row.get(key) or "").strip()
                if value:
                    bucket["submitters"].add(value)
            for key in ("Submitter1_ClinSig", "Submitter2_ClinSig"):
                value = (row.get(key) or "").strip()
                if value:
                    bucket["significances"].add(value)
            preferred = (row.get("ClinVar_Preferred") or "").strip()
            if preferred:
                bucket["preferred_names"].add(preferred)

    conflicts = {}
    for variation_id, bucket in accumulators.items():
        summary_text = None
        if bucket["preferred_names"]:
            summary_text = "; ".join(sorted(bucket["preferred_names"]))
        conflicts[variation_id] = ConflictSummary(
            has_conflict=True,
            conflict_significance=sorted(bucket["significances"]),
            submitter_count=len(bucket["submitters"]) or None,
            summary_text=summary_text,
        )
    return conflicts, _build_provenance("ClinVar conflicting interpretations", "file", conflict_summary_path)


def load_submission_lookup(
    submission_summary_path: Path,
    target_variation_ids: set[int] | None = None,
) -> tuple[dict[int, SubmissionEvidence], DataProvenance]:
    """Load submission aggregates keyed by ClinVar VariationID."""
    accumulators: dict[int, dict[str, int | set[str]]] = {}
    for row in _iter_submission_rows(submission_summary_path):
        variation_text = (row.get("VariationID") or "").strip()
        if not variation_text:
            continue

        variation_id = _parse_int_field(variation_text)
        if variation_id is None:
            continue
        if target_variation_ids is not None and variation_id not in target_variation_ids:
            continue

        bucket = accumulators.setdefault(
            variation_id,
            {
                "total_submissions": 0,
                "submitter_names": set(),
                "review_statuses": set(),
                "clinical_significances": set(),
            },
        )
        bucket["total_submissions"] = int(bucket["total_submissions"]) + 1

        submitter = (row.get("Submitter") or "").strip()
        if submitter:
            bucket["submitter_names"].add(submitter)

        review_status = (row.get("ReviewStatus") or "").strip()
        if review_status:
            bucket["review_statuses"].add(review_status)

        clinical_significance = (row.get("ClinicalSignificance") or "").strip()
        if clinical_significance:
            bucket["clinical_significances"].add(clinical_significance)

    submissions = {}
    for variation_id, bucket in accumulators.items():
        submissions[variation_id] = SubmissionEvidence(
            total_submissions=int(bucket["total_submissions"]),
            submitter_names=sorted(bucket["submitter_names"]),
            review_statuses=sorted(bucket["review_statuses"]),
            clinical_significances=sorted(bucket["clinical_significances"]),
        )
    return submissions, _build_provenance("ClinVar submission summary", "file", submission_summary_path)


def _iter_int_chunks(values: Iterable[int], chunk_size: int = 500) -> Iterator[list[int]]:
    """Yield integer values in predictable query-sized chunks."""
    chunk: list[int] = []
    for value in values:
        chunk.append(value)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _load_conflict_lookup_from_cache(
    cache_db_path: Path,
    *,
    conflict_summary_path: Path,
    target_variation_ids: set[int],
) -> tuple[dict[int, ConflictSummary], DataProvenance]:
    """Read conflict summaries for the requested VariationIDs from the processed cache."""
    conflicts: dict[int, ConflictSummary] = {}
    connection = _connect_cache_db(cache_db_path)
    try:
        for chunk in _iter_int_chunks(sorted(target_variation_ids)):
            placeholders = ",".join("?" for _ in chunk)
            rows = connection.execute(
                f"""
                SELECT variation_id, conflict_significance, submitter_count, summary_text
                FROM conflicts
                WHERE variation_id IN ({placeholders})
                """,
                chunk,
            ).fetchall()
            for row in rows:
                conflicts[row["variation_id"]] = _conflict_from_cache_row(row)
    finally:
        connection.close()
    return conflicts, _build_provenance("ClinVar conflicting interpretations", "file", conflict_summary_path)


def _load_submission_lookup_from_cache(
    cache_db_path: Path,
    *,
    submission_summary_path: Path,
    target_variation_ids: set[int],
) -> tuple[dict[int, SubmissionEvidence], DataProvenance]:
    """Read submission aggregates for the requested VariationIDs from the processed cache."""
    submissions: dict[int, SubmissionEvidence] = {}
    connection = _connect_cache_db(cache_db_path)
    try:
        for chunk in _iter_int_chunks(sorted(target_variation_ids)):
            placeholders = ",".join("?" for _ in chunk)
            rows = connection.execute(
                f"""
                SELECT variation_id, total_submissions, submitter_names, review_statuses, clinical_significances
                FROM submissions
                WHERE variation_id IN ({placeholders})
                """,
                chunk,
            ).fetchall()
            for row in rows:
                submissions[row["variation_id"]] = _submission_from_cache_row(row)
    finally:
        connection.close()
    return submissions, _build_provenance("ClinVar submission summary", "file", submission_summary_path)


def enrich_index_with_supporting_data(
    index: ClinVarIndex,
    conflict_summary_path: Path | None = None,
    submission_summary_path: Path | None = None,
    target_variation_ids: Iterable[int] | None = None,
    cache_db_path: Path | None = None,
) -> ClinVarIndex:
    """Attach conflict and submission layers to an existing exact-match index."""
    allowed_variation_ids = set(target_variation_ids) if target_variation_ids is not None else {
        match.variation_id for match in index.exact_matches.values() if match.variation_id is not None
    }
    if not allowed_variation_ids:
        return index

    if conflict_summary_path is not None:
        if cache_db_path is not None:
            conflicts, provenance = _load_conflict_lookup_from_cache(
                cache_db_path,
                conflict_summary_path=conflict_summary_path,
                target_variation_ids=allowed_variation_ids,
            )
        else:
            conflicts, provenance = load_conflict_lookup(conflict_summary_path, allowed_variation_ids)
        index.conflicts_by_variation_id.update(conflicts)
        if provenance not in index.provenance:
            index.provenance.append(provenance)

    if submission_summary_path is not None:
        if cache_db_path is not None:
            submissions, provenance = _load_submission_lookup_from_cache(
                cache_db_path,
                submission_summary_path=submission_summary_path,
                target_variation_ids=allowed_variation_ids,
            )
        else:
            submissions, provenance = load_submission_lookup(submission_summary_path, allowed_variation_ids)
        index.submissions_by_variation_id.update(submissions)
        if provenance not in index.provenance:
            index.provenance.append(provenance)

    return index


def load_clinvar_index(
    variant_summary_path: Path,
    conflict_summary_path: Path | None = None,
    submission_summary_path: Path | None = None,
    target_variation_ids: Iterable[int] | None = None,
    target_variant_keys: set[VariantKey] | None = None,
    cache_db_path: Path | None = None,
    use_processed_cache: bool = True,
    chunk_size: int = 50_000,
) -> ClinVarIndex:
    """Build the exact-match index and optionally attach supporting evidence layers."""
    resolved_cache_db_path = cache_db_path or _default_cache_db_path(variant_summary_path)
    use_cache = use_processed_cache and target_variant_keys is not None
    if use_cache:
        _ensure_cache_db(
            resolved_cache_db_path,
            variant_summary_path=variant_summary_path,
            conflict_summary_path=conflict_summary_path,
            submission_summary_path=submission_summary_path,
        )
        index = _load_variant_summary_index_from_cache(
            resolved_cache_db_path,
            variant_summary_path=variant_summary_path,
            target_variant_keys=target_variant_keys,
        )
    else:
        index = load_variant_summary_index(
            variant_summary_path,
            chunk_size=chunk_size,
            target_variant_keys=target_variant_keys,
        )
    return enrich_index_with_supporting_data(
        index,
        conflict_summary_path=conflict_summary_path,
        submission_summary_path=submission_summary_path,
        target_variation_ids=target_variation_ids,
        cache_db_path=resolved_cache_db_path if use_cache else None,
    )
