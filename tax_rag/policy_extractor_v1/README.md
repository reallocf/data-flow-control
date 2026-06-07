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