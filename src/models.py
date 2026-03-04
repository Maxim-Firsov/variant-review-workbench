"""Core data models for variant-review-workbench."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class GenomeAssembly(StrEnum):
    """Supported reference assemblies used for variant matching."""

    GRCH37 = "GRCh37"
    GRCH38 = "GRCh38"
    UNKNOWN = "unknown"


class MatchStrategy(StrEnum):
    """How a user variant was associated with a reference record."""

    EXACT = "exact"
    NORMALIZED = "normalized"
    IDENTIFIER = "identifier"
    UNMATCHED = "unmatched"


class ReviewPriorityTier(StrEnum):
    """High-level review bucket emitted by the ranking stage."""

    HIGH_REVIEW_PRIORITY = "high_review_priority"
    REVIEW = "review"
    CONTEXT_ONLY = "context_only"


class DataProvenance(BaseModel):
    """Source metadata for a loaded reference file or remote API payload."""

    model_config = ConfigDict(extra="forbid")

    source_name: str
    source_kind: str
    source_path: str | None = None
    source_url: str | None = None
    release_label: str | None = None
    downloaded_at: datetime | None = None
    accessed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class InputVariant(BaseModel):
    """Normalized user-supplied variant record parsed from a VCF row."""

    model_config = ConfigDict(extra="forbid")

    record_id: str
    assembly: GenomeAssembly
    chromosome: str
    position: int
    reference_allele: str
    alternate_allele: str
    quality: str | None = None
    filter_status: str | None = None
    variant_id: str | None = None
    gene: str | None = None
    transcript: str | None = None
    consequence: str | None = None
    impact: str | None = None
    info: dict[str, str] = Field(default_factory=dict)

    @property
    def variant_key(self) -> tuple[str, str, int, str, str]:
        """Stable coordinate key used for local reference matching."""
        return (
            self.assembly.value,
            self.chromosome,
            self.position,
            self.reference_allele,
            self.alternate_allele,
        )


class ConflictSummary(BaseModel):
    """Conflict-specific ClinVar context for a matched variant."""

    model_config = ConfigDict(extra="forbid")

    has_conflict: bool = False
    conflict_significance: list[str] = Field(default_factory=list)
    submitter_count: int | None = None
    summary_text: str | None = None


class SubmissionEvidence(BaseModel):
    """Optional aggregate evidence derived from submitter-level ClinVar data."""

    model_config = ConfigDict(extra="forbid")

    total_submissions: int | None = None
    submitter_names: list[str] = Field(default_factory=list)
    review_statuses: list[str] = Field(default_factory=list)
    clinical_significances: list[str] = Field(default_factory=list)


class ClinVarMatch(BaseModel):
    """Interpretation layer contributed by the local ClinVar snapshot."""

    model_config = ConfigDict(extra="forbid")

    matched: bool = False
    match_strategy: MatchStrategy = MatchStrategy.UNMATCHED
    assembly: GenomeAssembly = GenomeAssembly.UNKNOWN
    chromosome: str | None = None
    position: int | None = None
    reference_allele: str | None = None
    alternate_allele: str | None = None
    variation_id: int | None = None
    allele_id: int | None = None
    accession: str | None = None
    preferred_name: str | None = None
    gene: str | None = None
    condition_names: list[str] = Field(default_factory=list)
    clinical_significance: str | None = None
    review_status: str | None = None
    review_stars: int | None = None
    interpretation_origin: str | None = None
    last_evaluated: str | None = None
    conflict: ConflictSummary = Field(default_factory=ConflictSummary)
    submissions: SubmissionEvidence | None = None

    @property
    def variant_key(self) -> tuple[str, str | None, int | None, str | None, str | None]:
        """Coordinate key aligned with InputVariant matching semantics."""
        return (
            self.assembly.value,
            self.chromosome,
            self.position,
            self.reference_allele,
            self.alternate_allele,
        )


class PharmGKBAnnotation(BaseModel):
    """Optional pharmacogenomics context collected from public PharmGKB endpoints."""

    model_config = ConfigDict(extra="forbid")

    queried: bool = False
    matched: bool = False
    from_cache: bool = False
    gene_symbols: list[str] = Field(default_factory=list)
    pharmgkb_gene_ids: list[str] = Field(default_factory=list)
    pharmgkb_variant_ids: list[str] = Field(default_factory=list)
    chemicals: list[str] = Field(default_factory=list)
    clinical_annotation_ids: list[str] = Field(default_factory=list)
    guideline_annotation_ids: list[str] = Field(default_factory=list)
    evidence_notes: list[str] = Field(default_factory=list)


class AnnotatedVariant(BaseModel):
    """Merged analysis object carried into ranking and reporting stages."""

    model_config = ConfigDict(extra="forbid")

    input_variant: InputVariant
    clinvar: ClinVarMatch = Field(default_factory=ClinVarMatch)
    pharmgkb: PharmGKBAnnotation | None = None
    flags: list[str] = Field(default_factory=list)

    @property
    def has_clinvar_match(self) -> bool:
        """Whether ClinVar annotation was attached successfully."""
        return self.clinvar.matched

    @property
    def has_conflict(self) -> bool:
        """Whether the matched ClinVar record is conflict-flagged."""
        return self.clinvar.conflict.has_conflict


class RankedVariant(BaseModel):
    """Annotated variant plus review prioritization output."""

    model_config = ConfigDict(extra="forbid")

    annotated_variant: AnnotatedVariant
    priority_score: float
    priority_tier: ReviewPriorityTier
    ranking_rationale: list[str] = Field(default_factory=list)


class RunStatistics(BaseModel):
    """Machine-readable aggregate counts produced by a completed run."""

    model_config = ConfigDict(extra="forbid")

    input_variant_count: int = 0
    clinvar_matched_count: int = 0
    clinvar_unmatched_count: int = 0
    conflict_flagged_count: int = 0
    pharmgkb_enriched_count: int = 0


class RunMetadata(BaseModel):
    """Reproducibility metadata captured alongside output artifacts."""

    model_config = ConfigDict(extra="forbid")

    project_name: str = "variant-review-workbench"
    run_started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    input_path: str
    output_dir: str
    assembly: GenomeAssembly
    pharmgkb_enabled: bool = False
    sources: list[DataProvenance] = Field(default_factory=list)
    statistics: RunStatistics = Field(default_factory=RunStatistics)
