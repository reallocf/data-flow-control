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

### Section 274 REJ16-style reference layer
Run:
    python .\rej16_reference_audit.py
    python .\evaluate_reference_274_exact.py
Reviewed section 274 result:
- 123 gold reference occurrences
- 123 predicted occurrences
- 123 exact surface, span, and target matches
- 0 wrong matches
- 0 missing occurrences
- 0 extra occurrences
This is an in-sample result. Section 274 was used while adapting the rules, so this result does not establish performance on unseen law.
The implementation is a Title 26 adaptation of the REJ16 pattern-and-hierarchy approach. It is not a full reproduction of REJ16.
The project use of this layer is to form resolved cross-reference edges that can supply referenced legal text to the policy extractor.
