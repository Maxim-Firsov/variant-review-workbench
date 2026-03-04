from __future__ import annotations

import csv
import gzip
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.cache_bootstrap import main
from src.clinvar_index import prepare_processed_clinvar_cache


VARIANT_SUMMARY_HEADER = [
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


class CacheBootstrapTests(unittest.TestCase):
    def test_prepare_processed_clinvar_cache_builds_full_cache_and_cleans_stale_temp_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw_dir = root / "data" / "clinvar" / "raw"
            processed_dir = root / "data" / "clinvar" / "processed"
            raw_dir.mkdir(parents=True)
            processed_dir.mkdir(parents=True)

            variant_summary = raw_dir / "variant_summary.txt.gz"
            with gzip.open(variant_summary, "wt", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle, delimiter="\t")
                writer.writerow(VARIANT_SUMMARY_HEADER)
                writer.writerow(
                    [
                        "10",
                        "single nucleotide variant",
                        "TP53 example",
                        "TP53",
                        "Pathogenic",
                        "Jan 01, 2025",
                        "Li-Fraumeni syndrome",
                        "reviewed by expert panel",
                        "germline",
                        "GRCh38",
                        "17",
                        "1234",
                        "43045702",
                        "A",
                        "G",
                        "RCV000000001",
                    ]
                )

            conflict_path = raw_dir / "summary_of_conflicting_interpretations.txt"
            with conflict_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle, delimiter="\t")
                writer.writerow(
                    [
                        "#Gene_Symbol",
                        "NCBI_Variation_ID",
                        "ClinVar_Preferred",
                        "Submitter1",
                        "Submitter1_SCV",
                        "Submitter1_ClinSig",
                        "Submitter1_LastEval",
                        "Submitter1_ReviewStatus",
                        "Submitter1_Sub_Condition",
                        "Submitter1_Description",
                        "Submitter2",
                        "Submitter2_SCV",
                        "Submitter2_ClinSig",
                        "Submitter2_LastEval",
                        "Submitter2_ReviewStatus",
                        "Submitter2_Sub_Condition",
                        "Submitter2_Description",
                        "Rank_diff",
                        "Conflict_Reported",
                        "Variant_type",
                        "Submitter1_Method",
                        "Submitter2_Method",
                    ]
                )
                writer.writerow(["TP53", "1234", "var", "A", "SCV1", "Pathogenic", "", "", "", "", "B", "SCV2", "Benign", "", "", "", "", "1", "yes", "SNV", "", ""])

            submission_path = raw_dir / "submission_summary.txt.gz"
            with gzip.open(submission_path, "wt", encoding="utf-8", newline="") as handle:
                handle.write("##Overview\n")
                handle.write("#VariationID\tClinicalSignificance\tDateLastEvaluated\tDescription\tSubmittedPhenotypeInfo\tReportedPhenotypeInfo\tReviewStatus\tCollectionMethod\tOriginCounts\tSubmitter\tSCV\tSubmittedGeneSymbol\tExplanationOfInterpretation\tSomaticClinicalImpact\tOncogenicity\tContributesToAggregateClassification\n")
                handle.write("1234\tPathogenic\tJan 01, 2025\t-\t-\t-\treviewed by expert panel\tclinical testing\tgermline:1\tLab A\tSCV1\tTP53\t-\t-\t-\tyes\n")

            cache_db = processed_dir / "clinvar_lookup_cache.sqlite3"
            stale_tmp = processed_dir / "clinvar_lookup_cache.sqlite3.tmp"
            stale_tmp_wal = processed_dir / "clinvar_lookup_cache.sqlite3.tmp-wal"
            stale_tmp_shm = processed_dir / "clinvar_lookup_cache.sqlite3.tmp-shm"
            stale_tmp.write_text("stale", encoding="utf-8")
            stale_tmp_wal.write_text("stale", encoding="utf-8")
            stale_tmp_shm.write_text("stale", encoding="utf-8")

            resolved = prepare_processed_clinvar_cache(
                variant_summary_path=variant_summary,
                conflict_summary_path=conflict_path,
                submission_summary_path=submission_path,
                cache_db_path=cache_db,
            )

            self.assertEqual(resolved, cache_db)
            self.assertTrue(cache_db.exists())
            self.assertFalse(stale_tmp.exists())
            self.assertFalse(stale_tmp_wal.exists())
            self.assertFalse(stale_tmp_shm.exists())

    def test_cache_bootstrap_main_reports_resolved_cache_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            variant_summary = root / "variant_summary.txt.gz"
            with gzip.open(variant_summary, "wt", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle, delimiter="\t")
                writer.writerow(VARIANT_SUMMARY_HEADER)
                writer.writerow(
                    [
                        "10",
                        "single nucleotide variant",
                        "TP53 example",
                        "TP53",
                        "Pathogenic",
                        "Jan 01, 2025",
                        "Li-Fraumeni syndrome",
                        "reviewed by expert panel",
                        "germline",
                        "GRCh38",
                        "17",
                        "1234",
                        "43045702",
                        "A",
                        "G",
                        "RCV000000001",
                    ]
                )

            cache_db = root / "clinvar_lookup_cache.sqlite3"
            argv = [
                "prog",
                "--variant-summary",
                str(variant_summary),
                "--clinvar-cache-db",
                str(cache_db),
            ]

            with patch("sys.argv", argv):
                with patch("sys.stdout", new_callable=io.StringIO) as stdout:
                    main()

        output = stdout.getvalue()
        self.assertIn("Processed ClinVar cache ready:", output)
        self.assertIn("clinvar_lookup_cache.sqlite3", output)


if __name__ == "__main__":
    unittest.main()
