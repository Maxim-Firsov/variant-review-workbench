from __future__ import annotations

import csv
import gzip
import tempfile
import unittest
from pathlib import Path

from src.clinvar_index import load_clinvar_index
from src.models import GenomeAssembly
from src.vcf_parser import parse_vcf


class FoundationPipelineTests(unittest.TestCase):
    def test_parse_vcf_normalizes_chromosome_and_allele_case(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "input.vcf"
            input_path.write_text(
                (
                    "##fileformat=VCFv4.2\n"
                    "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
                    "chr17\t43045702\t.\ta\tg\t100\tPASS\tGENE=TP53;IMPACT=HIGH\n"
                ),
                encoding="utf-8",
            )

            variants = parse_vcf(input_path, GenomeAssembly.GRCH38)

        self.assertEqual(len(variants), 1)
        self.assertEqual(variants[0].chromosome, "17")
        self.assertEqual(variants[0].reference_allele, "A")
        self.assertEqual(variants[0].alternate_allele, "G")

    def test_clinvar_index_matches_lowercase_input_and_filters_placeholder_conditions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_path = root / "input.vcf"
            input_path.write_text(
                (
                    "##fileformat=VCFv4.2\n"
                    "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
                    "chr17\t43045702\t.\ta\tg\t100\tPASS\tGENE=TP53;IMPACT=HIGH\n"
                ),
                encoding="utf-8",
            )

            variant_summary = root / "variant_summary.txt.gz"
            with gzip.open(variant_summary, "wt", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle, delimiter="\t")
                writer.writerow(
                    [
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
                )
                writer.writerow(
                    [
                        "10",
                        "single nucleotide variant",
                        "TP53 example",
                        "TP53",
                        "Pathogenic",
                        "Jan 01, 2025",
                        "not provided|Li-Fraumeni syndrome",
                        "reviewed by expert panel",
                        "germline",
                        "GRCh38",
                        "chr17",
                        "1234",
                        "43045702",
                        "a",
                        "g",
                        "RCV000000001",
                    ]
                )

            index = load_clinvar_index(variant_summary)
            variants = parse_vcf(input_path, GenomeAssembly.GRCH38)
            match = index.lookup(variants[0])

        self.assertTrue(match.matched)
        self.assertEqual(match.reference_allele, "A")
        self.assertEqual(match.alternate_allele, "G")
        self.assertEqual(match.condition_names, ["Li-Fraumeni syndrome"])


if __name__ == "__main__":
    unittest.main()
