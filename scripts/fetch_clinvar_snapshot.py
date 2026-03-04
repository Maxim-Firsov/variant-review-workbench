"""ClinVar source locations and local snapshot helpers."""

from __future__ import annotations

from pathlib import Path


CLINVAR_FTP_ROOT = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/"
CLINVAR_DOCS_URL = "https://www.ncbi.nlm.nih.gov/clinvar/docs/maintenance_use/"
CLINVAR_FTP_PRIMER_URL = "https://www.ncbi.nlm.nih.gov/clinvar/docs/ftp_primer/"

CLINVAR_FILES = {
    "variant_summary": {
        "url": f"{CLINVAR_FTP_ROOT}tab_delimited/variant_summary.txt.gz",
        "local_path": Path("data/clinvar/raw/variant_summary.txt.gz"),
    },
    "submission_summary": {
        "url": f"{CLINVAR_FTP_ROOT}tab_delimited/submission_summary.txt.gz",
        "local_path": Path("data/clinvar/raw/submission_summary.txt.gz"),
    },
    "conflicting_interpretations": {
        "url": f"{CLINVAR_FTP_ROOT}tab_delimited/summary_of_conflicting_interpretations.txt.gz",
        "local_path": Path("data/clinvar/raw/summary_of_conflicting_interpretations.txt"),
    },
    "vcf_grch38": {
        "url": f"{CLINVAR_FTP_ROOT}vcf_GRCh38/clinvar.vcf.gz",
        "local_path": Path("data/clinvar/raw/clinvar_grch38.vcf.gz"),
    },
    "vcf_grch37": {
        "url": f"{CLINVAR_FTP_ROOT}vcf_GRCh37/clinvar.vcf.gz",
        "local_path": Path("data/clinvar/raw/clinvar_grch37.vcf.gz"),
    },
}
