# Policy extractor v1

Minimal tax-law Data Flow Control policy extractor for the TaxAgent demo.

## Setup

Install dependencies:

python -m pip install -r requirements.txt

Provide OPENAI_API_KEY through the local environment or a local .env file. Do not commit local environment files.

## Build Title 26 input

python build_title26_input.py

This creates:

inputs/title26_sections.jsonl

The generated Title 26 input is local build data.

## Run section 274 extraction

PowerShell:

$env:INPUT="inputs/title26_sections.jsonl"
$env:TARGETS="274"
$env:OUTPUT="outputs/policies_274.json"
python extract_v1.py

Expected output:

outputs/policies_274.json

Each policy candidate includes citations, a constraint, an explanation, wrapped DFC policy text, local subset validation, and a confidence label.

This is a review and iteration checkpoint, not a final full tax-law extractor.

### Section 274 reference audit

Run:

    python .\reference_audit_274.py
    python .\filter_reference_audit_274.py

Outputs:

- outputs/reference_audit_274.csv: raw section 274 reference scan
- outputs/reference_audit_274_annotated.csv: raw scan with main-text vs notes/amendments labels
- outputs/reference_audit_274_main.csv: references from main section 274 text only
- outputs/reference_audit_274_part_summary.csv: count of main-text vs notes/amendments references

Current main-text result:

- 89 main-text references
- 39 resolved Title 26 section references
- 49 local structural references needing hierarchy-aware resolution
- 1 external reference to section 16(a) of the Securities Exchange Act
- 9 section 212 references relevant to the seminar policy

