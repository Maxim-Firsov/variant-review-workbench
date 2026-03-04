# Variant Review Workbench

ClinVar-first small-variant triage and reporting tool for research-oriented review workflows.

The workbench accepts a VCF, matches variants against a local ClinVar snapshot, highlights conflicting interpretations, optionally enriches findings with PharmGKB, ranks the review queue, and emits analyst-friendly HTML and machine-readable outputs.

## Problem Statement

VCFs are compact and machine-friendly, but they are not ideal review artifacts. Analysts often need to answer a narrower question first:

- Which variants matched a known ClinVar record?
- Which findings have conflicting interpretations?
- Which records should be reviewed first?
- Which variants may have pharmacogenomics context worth surfacing?

This repository focuses on that gap. It is not a full annotation platform or a clinical interpretation engine. It is a reproducible, inspectable triage workbench for small-variant review.

## What The Tool Does

- reads `.vcf` and `.vcf.gz` inputs
- normalizes one record per alternate allele
- matches variants to a local ClinVar snapshot using assembly-aware coordinate and allele keys
- attaches conflict and submission context when available
- optionally enriches variants with PharmGKB gene, variant, clinical annotation, and guideline data
- ranks findings with transparent heuristics
- writes HTML, JSON, CSV, and run metadata outputs

## Product Boundary

This project is:

- a research triage workbench
- a reproducible annotation and reporting tool
- a portfolio-quality bioinformatics-adjacent engineering project

This project is not:

- a clinical decision support system
- an ACMG classifier
- a star-allele caller
- a treatment recommendation engine

## Architecture

```text
VCF / VCF.GZ
    |
    v
VCF parser
    |
    v
Normalized InputVariant records
    |
    v
ClinVar exact-match index
    |
    +--> conflict attachment
    |
    +--> submission evidence attachment
    |
    v
AnnotatedVariant records
    |
    +--> optional PharmGKB enrichment
    |
    v
RankedVariant records
    |
    +--> HTML report
    +--> prioritized_variants.json
    +--> annotated_variants.csv
    +--> summary.json
    `--> run_metadata.json
```

## Repository Layout

```text
variant-review-workbench/
|-- src/
|   |-- annotator.py
|   |-- cli.py
|   |-- clinvar_index.py
|   |-- models.py
|   |-- pgx_enrichment.py
|   |-- ranker.py
|   |-- report_builder.py
|   `-- vcf_parser.py
|-- templates/
|   `-- report.html.j2
|-- data/
|   |-- clinvar/
|   |-- pharmgkb/
|   |-- references/
|   `-- demo.vcf
|-- tests/
|-- README.md
`-- pyproject.toml
```

## Inputs

### Required

- input VCF or VCF.GZ
- ClinVar `variant_summary.txt.gz`
- reference assembly: `GRCh37` or `GRCh38`

### Optional

- `summary_of_conflicting_interpretations.txt`
- `submission_summary.txt.gz`
- PharmGKB enrichment via `--enable-pharmgkb`

## Outputs

Each run writes:

- `annotated_variants.csv`
  - stable CSV export of ranked variant records
  - list-valued fields are serialized as JSON arrays inside each cell
- `prioritized_variants.json`
  - machine-readable prioritized variant artifact with `schema_version`, `artifact_type`, and `records`
- `summary.json`
  - machine-readable summary artifact with stable count fields and priority-tier counts
- `run_metadata.json`
  - reproducibility metadata, source provenance, and counts
- `report.html`
  - analyst-facing HTML report with top findings, conflicts, methods, and limitations

## Important Runtime Note

This tool now uses a persistent processed ClinVar cache.

- by default the CLI builds and reuses a SQLite cache at `data/clinvar/processed/clinvar_lookup_cache.sqlite3`
- the first run against a new raw ClinVar snapshot is a preprocessing run and can take a long time
- after that cache exists, repeated runs against the same snapshot should be much faster

Observed timing on the staged repository data:

- first run with cache build: about `1402` seconds, about `23 minutes 22 seconds`
- warm run reusing the cache: about `2.83` seconds

What this means in practice:

- if the tool appears slow on the first real run, that is expected
- that first run is building a local queryable index from the raw ClinVar files
- the warm-run path is the intended day-to-day workflow

Cache controls:

- default cache location: `data/clinvar/processed/clinvar_lookup_cache.sqlite3`
- override location: `--clinvar-cache-db <path>`
- disable cache and force raw-file reads: `--disable-clinvar-cache`

## Output Contract

The machine-readable outputs are intentionally separate from the human-oriented HTML report.

- `prioritized_variants.json` is the canonical structured export for downstream scripts
- `annotated_variants.csv` uses the same field set as the JSON records where practical
- list-valued fields such as `condition_names`, `flags`, and `ranking_rationale` remain lists in JSON and are encoded as JSON arrays in CSV cells
- `summary.json` uses stable count names:
  - `input_variant_count`
  - `clinvar_matched_count`
  - `clinvar_unmatched_count`
  - `conflict_flagged_count`
  - `pharmgkb_enriched_count`
  - `review_priority_tier_counts`

## Setup

```powershell
python -m pip install -r requirements.txt
```

## Example Usage

### Base ClinVar Run

This command will build the processed ClinVar cache on first use if it does not already exist.

```powershell
python -m src.cli `
  --input data\demo.vcf `
  --assembly GRCh38 `
  --variant-summary data\clinvar\raw\variant_summary.txt.gz `
  --conflict-summary data\clinvar\raw\summary_of_conflicting_interpretations.txt `
  --submission-summary data\clinvar\raw\submission_summary.txt.gz `
  --out-dir outputs\demo_run
```

### Cache Control Examples

Use a specific cache path:

```powershell
python -m src.cli `
  --input data\demo.vcf `
  --assembly GRCh38 `
  --variant-summary data\clinvar\raw\variant_summary.txt.gz `
  --conflict-summary data\clinvar\raw\summary_of_conflicting_interpretations.txt `
  --submission-summary data\clinvar\raw\submission_summary.txt.gz `
  --clinvar-cache-db data\clinvar\processed\clinvar_lookup_cache.sqlite3 `
  --out-dir outputs\demo_run
```

Disable the processed cache entirely:

```powershell
python -m src.cli `
  --input data\demo.vcf `
  --assembly GRCh38 `
  --variant-summary data\clinvar\raw\variant_summary.txt.gz `
  --conflict-summary data\clinvar\raw\summary_of_conflicting_interpretations.txt `
  --submission-summary data\clinvar\raw\submission_summary.txt.gz `
  --disable-clinvar-cache `
  --out-dir outputs\demo_run_no_cache
```

### Run With PharmGKB Enrichment

```powershell
python -m src.cli `
  --input data\demo.vcf `
  --assembly GRCh38 `
  --variant-summary data\clinvar\raw\variant_summary.txt.gz `
  --conflict-summary data\clinvar\raw\summary_of_conflicting_interpretations.txt `
  --submission-summary data\clinvar\raw\submission_summary.txt.gz `
  --out-dir outputs\demo_run_pgx `
  --enable-pharmgkb
```

## Ranking Approach

Ranking is heuristic and intentionally transparent.

The current score uses:

- ClinVar clinical significance
- ClinVar review strength
- conflict surfacing
- input impact severity
- optional PharmGKB context
- gene-symbol mismatch penalty

The system emits a numeric score, a priority tier, and a rationale list for every ranked variant.

Priority tiers:

- `high_review_priority`
- `review`
- `context_only`

## Example Review Questions This Tool Helps Answer

- Which variants have strong ClinVar support and should be reviewed first?
- Which findings are conflict-flagged and require closer inspection?
- Which unmatched records remain context-only?
- Which variants have optional PGx context worth surfacing for downstream review?

## Data Sources

### ClinVar

Used as the core local reference layer for variant matching and conflict attachment.

- FTP root: `https://ftp.ncbi.nlm.nih.gov/pub/clinvar/`
- maintenance and release notes: `https://www.ncbi.nlm.nih.gov/clinvar/docs/maintenance_use/`
- FTP primer: `https://www.ncbi.nlm.nih.gov/clinvar/docs/ftp_primer/`

Primary files used by this workbench:

- `variant_summary.txt.gz`
- `summary_of_conflicting_interpretations.txt`
- `submission_summary.txt.gz`

### PharmGKB / ClinPGx

Used only as optional enrichment.

- API base: `https://api.pharmgkb.org/v1`
- docs: `https://api.pharmgkb.org/`

Current integration uses stable public queries for:

- gene lookup by symbol
- variant lookup by symbol
- clinical annotations by gene symbol
- guideline annotations by gene symbol

### Source Reference Index

Human-readable source links are also maintained in:

- [external_sources.md](C:/Code/GitPortfolio/variant-review-workbench/data/references/external_sources.md)

## Licensing And Attribution

This repository contains code written for the workbench itself. Upstream datasets and APIs remain governed by their respective providers.

- ClinVar data usage and redistribution expectations should be reviewed through NCBI documentation.
- PharmGKB API usage should follow PharmGKB terms and public API guidance.

Users of this repository should verify current upstream licensing and attribution requirements before redistributing derived datasets or packaging source snapshots.

## Testing

Run the current unit suite:

```powershell
python -m unittest discover -s tests -v
```

GitHub Actions also runs syntax checks and the unit suite on push and pull request.

The implemented system currently has coverage for:

- VCF parsing
- ClinVar index loading
- conflict and submission attachment
- annotation behavior
- ranking behavior
- report generation
- CLI orchestration
- PharmGKB caching, failure handling, and integration

## Current Status

Implemented and tested:

- ClinVar-first local annotation pipeline
- HTML report generation
- CSV and JSON exports
- optional PharmGKB enrichment
- end-to-end CLI orchestration

Current automated test count:

- `43` passing unit tests

## Limitations

- matching is exact and assembly-aware, but does not yet perform deeper variant normalization beyond the current key strategy
- unmatched variants are intentionally left as context-only rather than force-interpreted
- PharmGKB enrichment is optional and network-dependent when enabled
- this is a focused small-variant review tool, not a full-scale annotation framework

## Disclaimer

This tool is for research triage and educational review only. It is not intended for diagnosis, treatment selection, or other clinical decision-making.
