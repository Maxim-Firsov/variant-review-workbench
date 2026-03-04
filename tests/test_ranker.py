from __future__ import annotations

import unittest

from src.models import (
    AnnotatedVariant,
    ClinVarMatch,
    ConflictSummary,
    GenomeAssembly,
    InputVariant,
    PharmGKBAnnotation,
    ReviewPriorityTier,
    SubmissionEvidence,
)
from src.ranker import rank_variant, rank_variants


def build_input_variant(**overrides: object) -> InputVariant:
    base = {
        "record_id": "record-1",
        "assembly": GenomeAssembly.GRCH38,
        "chromosome": "17",
        "position": 43045702,
        "reference_allele": "A",
        "alternate_allele": "G",
        "gene": "TP53",
        "impact": "HIGH",
    }
    base.update(overrides)
    return InputVariant(**base)


def build_annotated_variant(**overrides: object) -> AnnotatedVariant:
    clinvar_defaults = {
        "matched": True,
        "assembly": GenomeAssembly.GRCH38,
        "chromosome": "17",
        "position": 43045702,
        "reference_allele": "A",
        "alternate_allele": "G",
        "variation_id": 1234,
        "clinical_significance": "Pathogenic",
        "review_stars": 3,
        "gene": "TP53",
    }
    clinvar_overrides = overrides.pop("clinvar", {})
    clinvar_defaults.update(clinvar_overrides)

    base = {
        "input_variant": build_input_variant(),
        "clinvar": ClinVarMatch(**clinvar_defaults),
        "flags": [],
    }
    base.update(overrides)
    return AnnotatedVariant(**base)


class RankerTests(unittest.TestCase):
    def test_rank_variant_marks_unmatched_as_context_only(self) -> None:
        annotated = AnnotatedVariant(input_variant=build_input_variant())

        ranked = rank_variant(annotated)

        self.assertEqual(ranked.priority_score, 0.0)
        self.assertEqual(ranked.priority_tier, ReviewPriorityTier.CONTEXT_ONLY)
        self.assertEqual(len(ranked.ranking_rationale), 1)

    def test_rank_variant_prioritizes_pathogenic_reviewed_variants(self) -> None:
        annotated = build_annotated_variant()

        ranked = rank_variant(annotated)

        self.assertEqual(ranked.priority_tier, ReviewPriorityTier.HIGH_REVIEW_PRIORITY)
        self.assertGreaterEqual(ranked.priority_score, 12.0)
        self.assertTrue(any("ClinVar significance" in reason for reason in ranked.ranking_rationale))
        self.assertTrue(any("review strength" in reason for reason in ranked.ranking_rationale))

    def test_rank_variant_includes_conflict_and_pharmgkb_context(self) -> None:
        annotated = build_annotated_variant(
            clinvar={
                "conflict": ConflictSummary(
                    has_conflict=True,
                    conflict_significance=["Pathogenic", "Uncertain significance"],
                    submitter_count=2,
                ),
                "submissions": SubmissionEvidence(total_submissions=2),
            },
            pharmgkb=PharmGKBAnnotation(
                queried=True,
                matched=True,
                chemicals=["warfarin"],
                clinical_annotation_ids=["CA1"],
                guideline_annotation_ids=["GA1"],
            ),
            flags=["clinvar_conflict", "submission_evidence_available"],
        )

        ranked = rank_variant(annotated)

        self.assertGreater(ranked.priority_score, 15.0)
        self.assertTrue(any("Conflicting ClinVar interpretations" in reason for reason in ranked.ranking_rationale))
        self.assertTrue(any("PharmGKB" in reason for reason in ranked.ranking_rationale))

    def test_rank_variant_penalizes_gene_mismatch(self) -> None:
        annotated = build_annotated_variant(
            input_variant=build_input_variant(gene="WRONG1"),
            flags=["gene_symbol_mismatch"],
        )

        ranked = rank_variant(annotated)

        self.assertTrue(any("reducing confidence" in reason for reason in ranked.ranking_rationale))
        self.assertLess(ranked.priority_score, 17.5)

    def test_rank_variants_sorts_by_priority_descending(self) -> None:
        high = build_annotated_variant()
        low = build_annotated_variant(
            clinvar={
                "variation_id": 2000,
                "clinical_significance": "Benign",
                "review_stars": 1,
            },
            input_variant=build_input_variant(record_id="record-2", position=43045703, impact="LOW"),
        )
        unmatched = AnnotatedVariant(input_variant=build_input_variant(record_id="record-3", position=43045704))

        ranked = rank_variants([low, unmatched, high])

        self.assertEqual(ranked[0].annotated_variant.input_variant.record_id, "record-1")
        self.assertEqual(ranked[-1].annotated_variant.input_variant.record_id, "record-3")


if __name__ == "__main__":
    unittest.main()
