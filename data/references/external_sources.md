# External Sources

This file centralizes the public data and API endpoints used by `variant-review-workbench`.

## ClinVar Data

- FTP root: `https://ftp.ncbi.nlm.nih.gov/pub/clinvar/`
- Maintenance and release notes: `https://www.ncbi.nlm.nih.gov/clinvar/docs/maintenance_use/`
- FTP primer: `https://www.ncbi.nlm.nih.gov/clinvar/docs/ftp_primer/`

### Local ClinVar Snapshot Files

- `data/clinvar/raw/variant_summary.txt.gz`
- `data/clinvar/raw/submission_summary.txt.gz`
- `data/clinvar/raw/summary_of_conflicting_interpretations.txt`

## PharmGKB / ClinPGx

- API base: `https://api.pharmgkb.org/v1`
- Public API docs landing page: `https://api.pharmgkb.org/`
- Example endpoints:
  - `https://api.pharmgkb.org/v1/data/chemical`
  - `https://api.pharmgkb.org/v1/data/gene`
  - `https://api.pharmgkb.org/v1/data/variant`
  - `https://api.pharmgkb.org/v1/data/clinicalAnnotation`

This repository's current runtime path uses the tab-delimited ClinVar snapshot files above plus optional PharmGKB API calls. It does not currently depend on Ensembl REST in the shipped pipeline.
