"""Optional pharmacogenomics enrichment helpers."""

from __future__ import annotations

PHARMGKB_API_BASE = "https://api.pharmgkb.org/v1"
PHARMGKB_DOCS_URL = "https://api.pharmgkb.org/"

PHARMGKB_ENDPOINTS = {
    "genes": f"{PHARMGKB_API_BASE}/data/gene",
    "chemicals": f"{PHARMGKB_API_BASE}/data/chemical",
    "variants": f"{PHARMGKB_API_BASE}/data/variant",
    "clinical_annotations": f"{PHARMGKB_API_BASE}/data/clinicalAnnotation",
    "guideline_annotations": f"{PHARMGKB_API_BASE}/data/guidelineAnnotation",
}
