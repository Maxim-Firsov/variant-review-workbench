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
- `data/clinvar/raw/clinvar_grch38.vcf.gz`
- `data/clinvar/raw/clinvar_grch37.vcf.gz`

## PharmGKB / ClinPGx

- API base: `https://api.pharmgkb.org/v1`
- Public API docs landing page: `https://api.pharmgkb.org/`
- Example endpoints:
  - `https://api.pharmgkb.org/v1/data/chemical`
  - `https://api.pharmgkb.org/v1/data/gene`
  - `https://api.pharmgkb.org/v1/data/variant`
  - `https://api.pharmgkb.org/v1/data/clinicalAnnotation`

## Ensembl REST

- API base: `https://rest.ensembl.org`
- Docs: `https://rest.ensembl.org/`
