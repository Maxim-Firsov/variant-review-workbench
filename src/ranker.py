"""Variant review prioritization logic."""

from __future__ import annotations

from .models import AnnotatedVariant, RankedVariant, ReviewPriorityTier

CLINICAL_SIGNIFICANCE_SCORES = {
    "pathogenic": 10.0,
    "likely pathogenic": 8.0,
    "pathogenic/likely pathogenic": 9.0,
    "uncertain significance": 4.0,
    "conflicting classifications of pathogenicity": 6.0,
    "risk factor": 3.0,
    "drug response": 3.0,
    "protective": 2.0,
    "affects": 2.0,
    "association": 2.0,
    "benign/likely benign": 1.0,
    "likely benign": 1.0,
    "benign": 0.0,
}

IMPACT_SCORES = {
    "HIGH": 3.0,
    "MODERATE": 1.5,
    "LOW": 0.5,
    "MODIFIER": 0.0,
}


def _score_clinical_significance(value: str | None) -> tuple[float, str | None]:
    """Score the aggregate ClinVar significance label."""
    if not value:
        return 0.0, None

    normalized = value.strip().lower()
    score = CLINICAL_SIGNIFICANCE_SCORES.get(normalized, 2.0)
    return score, f"ClinVar significance '{value}' contributed {score:.1f} points."


def _score_review_strength(review_stars: int | None) -> tuple[float, str | None]:
    """Score the ClinVar review strength."""
    if review_stars is None:
        return 0.0, None

    score = float(review_stars) * 1.5
    return score, f"ClinVar review strength ({review_stars} stars) contributed {score:.1f} points."


def _score_conflict(has_conflict: bool) -> tuple[float, str | None]:
    """Surface conflict-flagged variants without overpowering pathogenic evidence."""
    if not has_conflict:
        return 0.0, None

    score = 2.5
    return score, "Conflicting ClinVar interpretations increased review priority by 2.5 points."


def _score_variant_impact(impact: str | None) -> tuple[float, str | None]:
    """Use pre-existing consequence severity only as a secondary boost."""
    if not impact:
        return 0.0, None

    normalized = impact.strip().upper()
    score = IMPACT_SCORES.get(normalized, 0.0)
    if score <= 0.0:
        return 0.0, None
    return score, f"Input impact '{impact}' contributed {score:.1f} points."


def _score_pharmgkb_context(annotated_variant: AnnotatedVariant) -> tuple[float, str | None]:
    """Provide a modest boost when pharmacogenomics enrichment is present."""
    pharmgkb = annotated_variant.pharmgkb
    if pharmgkb is None or not pharmgkb.matched:
        return 0.0, None

    score = 0.0
    evidence_parts: list[str] = []
    if pharmgkb.clinical_annotation_ids:
        score += 2.0
        evidence_parts.append("clinical annotations")
    if pharmgkb.guideline_annotation_ids:
        score += 2.0
        evidence_parts.append("guideline annotations")
    if pharmgkb.chemicals:
        score += 1.0
        evidence_parts.append("drug associations")

    if score == 0.0:
        score = 1.0
        evidence_parts.append("gene/variant PharmGKB context")

    return score, f"PharmGKB {' + '.join(evidence_parts)} contributed {score:.1f} points."


def _score_annotation_flags(flags: list[str]) -> tuple[float, list[str]]:
    """Apply small penalties or boosts from workflow flags."""
    score = 0.0
    rationale: list[str] = []
    if "gene_symbol_mismatch" in flags:
        score -= 1.5
        rationale.append("Input gene symbol mismatched ClinVar gene symbol, reducing confidence by 1.5 points.")
    return score, rationale


def determine_priority_tier(priority_score: float, has_clinvar_match: bool) -> ReviewPriorityTier:
    """Map the numeric score into a stable review tier."""
    if not has_clinvar_match:
        return ReviewPriorityTier.CONTEXT_ONLY
    if priority_score >= 12.0:
        return ReviewPriorityTier.HIGH_REVIEW_PRIORITY
    if priority_score >= 5.0:
        return ReviewPriorityTier.REVIEW
    return ReviewPriorityTier.CONTEXT_ONLY


def rank_variant(annotated_variant: AnnotatedVariant) -> RankedVariant:
    """Assign a transparent review score and rationale to a single annotated variant."""
    if not annotated_variant.has_clinvar_match:
        return RankedVariant(
            annotated_variant=annotated_variant,
            priority_score=0.0,
            priority_tier=ReviewPriorityTier.CONTEXT_ONLY,
            ranking_rationale=["No ClinVar match was found, so the variant remains context-only until reviewed manually."],
        )

    score = 0.0
    rationale: list[str] = []

    components = [
        _score_clinical_significance(annotated_variant.clinvar.clinical_significance),
        _score_review_strength(annotated_variant.clinvar.review_stars),
        _score_conflict(annotated_variant.has_conflict),
        _score_variant_impact(annotated_variant.input_variant.impact),
        _score_pharmgkb_context(annotated_variant),
    ]
    for component_score, component_reason in components:
        score += component_score
        if component_reason:
            rationale.append(component_reason)

    flag_score, flag_reasons = _score_annotation_flags(annotated_variant.flags)
    score += flag_score
    rationale.extend(flag_reasons)

    final_score = round(max(score, 0.0), 2)
    return RankedVariant(
        annotated_variant=annotated_variant,
        priority_score=final_score,
        priority_tier=determine_priority_tier(final_score, annotated_variant.has_clinvar_match),
        ranking_rationale=rationale,
    )


def rank_variants(annotated_variants: list[AnnotatedVariant]) -> list[RankedVariant]:
    """Rank a batch of annotated variants from highest to lowest review priority."""
    ranked_variants = [rank_variant(annotated_variant) for annotated_variant in annotated_variants]
    return sorted(
        ranked_variants,
        key=lambda ranked: (
            ranked.priority_score,
            ranked.annotated_variant.clinvar.review_stars or -1,
            ranked.annotated_variant.clinvar.variation_id or -1,
        ),
        reverse=True,
    )
