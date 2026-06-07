import json
import os
import re
from pathlib import Path
from typing import Literal

from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel

load_dotenv()

INPUT = Path(os.getenv("INPUT", "inputs/title26_sections.jsonl"))
OUTPUT = Path(os.getenv("OUTPUT", "outputs/policies_274.json"))
TARGETS = {x.strip() for x in os.getenv("TARGETS", "274").split(",") if x.strip()}
MODEL = os.getenv("MODEL", "gpt-4.1-mini")

client = OpenAI()

class Candidate(BaseModel):
    source_citation: str
    supporting_citations: list[str]
    constraint: str
    explanation: str

class CandidateBatch(BaseModel):
    policies: list[Candidate]

class Judgment(BaseModel):
    confidence: Literal["HIGH", "LOW"]

def load_sections():
    if not INPUT.exists():
        raise FileNotFoundError(f"Missing {INPUT}")
    return [json.loads(line) for line in INPUT.read_text(encoding="utf-8-sig").splitlines() if line.strip()]

def section_number(citation):
    match = re.search(r"§\s*(\d+[A-Za-z0-9-]*)", citation)
    return match.group(1) if match else None

def detect_refs(text):
    refs = set()
    for match in re.finditer(r"(?:section|§)\s*(\d+[A-Za-z0-9-]*)", text, re.I):
        ref = match.group(1)
        if len(ref) <= 4 or re.match(r"^\d+[A-Z]$", ref):
            refs.add(ref)
    return sorted(refs)

def valid_dfc(policy):
    upper = policy.upper()
    if not all(part in upper for part in ["SOURCE", "SINK", "CONSTRAINT", "ON FAIL"]):
        return False

    blocked = ["SELECT ", " EXISTS ", " WHERE "]
    if any(term in upper for term in blocked):
        return False

    constraint = upper.split("CONSTRAINT", 1)[1].split("ON FAIL", 1)[0].strip()
    if re.search(r"(^|\bAND\b|\bOR\b|\()\s*E\.DEDUCT\s*($|\)|\bAND\b|\bOR\b)", constraint):
        return False

    return True

def parsed(prompt, schema):
    response = client.responses.parse(
        model=MODEL,
        input=[{"role": "user", "content": prompt}],
        text_format=schema,
        temperature=0,
    )
    return response.output_parsed

def generate_policy(section, context):
    prompt = f"""
Extract candidate Data Flow Control policies from U.S. tax law.

Return every policy clearly supported by the main legal text and referenced context.
Only fill the SQL-like boolean constraint.

The fixed policy wrapper is:
SOURCE Receipt AS R SINK Expense AS E
CONSTRAINT <constraint>
ON FAIL KILL

Reference Receipt fields with alias R and Expense fields with alias E.
Prefer simple fields such as R.category, R.type, R.purpose, R.cost, R.qual, E.cost, and E.deduct.
Use E.deduct as a numeric deduction fraction, for example E.deduct <= 0.5.
Use decimal fractions for percentages, for example 0.5 instead of 50%.

Every constraint must be implication-style:
<receipt does not match this rule> OR <deduction is allowed>

Examples:
R.category != 'food' OR E.deduct <= 0.5
R.category != 'gift' OR SUM(E.cost * E.deduct) <= 25
R.category != 'club dues' OR E.deduct = 0

Do not use SELECT subqueries.
Do not use EXISTS.
Do not use WHERE.
Do not use a bare boolean E.deduct.
Do not create constraints that reject unrelated receipts.
Pay special attention to subsection 274(h), including convention, seminar, and similar meeting rules that reference section 212.
For section 274(h)(7), extract a policy like: R.type != 'investment' OR R.category != 'seminar' OR E.deduct = 0, if supported by the text and referenced context.
Do not skip a policy only because it requires context from a referenced section.
If no DFC policy is clearly supported, return an empty policies list.

Main legal text:
{section["citation"]}
{section["text"]}

Referenced context:
{context}
"""
    return parsed(prompt, CandidateBatch).policies

def judge_policy(policy_obj, legal_text):
    prompt = f"""
Decide whether the DFC constraint is clearly supported by the legal text.

Return HIGH only if the constraint follows directly from the text.
Return LOW if it is plausible but needs manual review.

Legal text:
{legal_text}

Constraint:
{policy_obj["constraint"]}
"""
    return parsed(prompt, Judgment).confidence

def main():
    sections = load_sections()

    by_number = {}
    for section in sections:
        number = section_number(section["citation"])
        if number:
            by_number[number] = section

    results = []

    for section in sections:
        number = section_number(section["citation"])
        if TARGETS and number not in TARGETS:
            continue

        refs = detect_refs(section["text"])
        context_parts = []
        for ref in refs:
            ref_section = by_number.get(ref)
            if ref_section:
                context_parts.append(ref_section["citation"] + "\n" + ref_section["text"])

        context = "\n\n".join(context_parts)
        candidates = generate_policy(section, context)

        for candidate in candidates:
            row = candidate.model_dump()
            row["policy"] = (
                "SOURCE Receipt AS R SINK Expense AS E\n"
                f"CONSTRAINT {row['constraint']}\n"
                "ON FAIL KILL"
            )
            row["detected_refs"] = refs
            row["valid_dfc_subset"] = valid_dfc(row["policy"])
            row["confidence"] = judge_policy(row, section["text"]) if row["valid_dfc_subset"] else "LOW"
            results.append(row)

    OUTPUT.parent.mkdir(exist_ok=True)
    OUTPUT.write_text(json.dumps(results, indent=2), encoding="utf-8-sig")
    print(f"Wrote {OUTPUT} with {len(results)} policies")

if __name__ == "__main__":
    main()