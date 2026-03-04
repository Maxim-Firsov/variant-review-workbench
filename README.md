# Variant Review Workbench

ClinVar-first bioinformatics project scaffold for a research-oriented small-variant triage and reporting tool.

## Goal

Build a workbench that accepts a VCF, annotates variants against a local ClinVar snapshot, enriches results with optional PharmGKB data, ranks findings for review, and emits analyst-friendly reports.

## Local Data Layout

```text
data/
|-- clinvar/
|   |-- raw/
|   `-- processed/
|-- pharmgkb/
|   `-- cache/
|-- references/
|   `-- external_sources.md
|-- gene_panels/
`-- demo.vcf
```

## Staged Public Data

The following ClinVar snapshot files have been placed under `data/clinvar/raw/`:

- `variant_summary.txt.gz`
- `submission_summary.txt.gz`
- `summary_of_conflicting_interpretations.txt`
- `clinvar_grch38.vcf.gz`
- `clinvar_grch37.vcf.gz`

These raw files are ignored by git so the repository stays publishable.

## Public Endpoints

- ClinVar source metadata and file URLs are defined in `scripts/fetch_clinvar_snapshot.py`.
- PharmGKB API endpoints are defined in `src/pgx_enrichment.py`.
- Human-readable source links are listed in `data/references/external_sources.md`.

## Status

Initial repository scaffold is complete and external source files are staged for implementation work.
