import json
import os
import re
from pathlib import Path
from typing import Literal
from reference_context import load_graph, render_context, scoped_context

from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel

load_dotenv()

INPUT = Path(os.getenv("INPUT", "inputs/title26_sections.jsonl"))
OUTPUT = Path(os.getenv("OUTPUT", "outputs/policies_274.json"))
TARGETS = {x.strip() for x in os.getenv("TARGETS", "274").split(",") if x.strip()}
MODEL = os.getenv("MODEL", "gpt-4.1-mini")
CONTEXT_MODE = os.getenv(
    "CONTEXT_MODE",
    "scoped",
).strip().lower()

if CONTEXT_MODE not in {"none", "detected", "scoped"}:
    raise ValueError(
        f"invalid CONTEXT_MODE: {CONTEXT_MODE}"
    )

RUN_JUDGE = os.getenv(
    "RUN_JUDGE",
    "1",
).strip().lower() not in {
    "0",
    "false",
    "no",
}

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

section_re = re.compile(r"§\s*(\d+[A-Za-z0-9-]*)")
explicit_ref_re = re.compile(r"\b(?:sections?|§{1,2})\s+([0-9][0-9A-Za-z-]*)(?P<trail>(?:\s*\([A-Za-z0-9-]+\))*)", re.I)
local_ref_re = re.compile(r"\b(?:this\s+|such\s+)?(subsection|paragraph|subparagraph|clause)s?\s+(?P<trail>(?:\([A-Za-z0-9-]+\)\s*)+)", re.I)
part_re = re.compile(r"\(([A-Za-z0-9-]+)\)")

def load_sections():
    if not INPUT.exists():
        raise FileNotFoundError(f"Missing {INPUT}")
    return [json.loads(line) for line in INPUT.read_text(encoding="utf-8-sig").splitlines() if line.strip()]

def section_number(citation):
    match = section_re.search(citation)
    return match.group(1) if match else None

def section_main_cut(text):
    markers = ["Editorial Notes", "Source Credit", "Statutory Notes and Related Subsidiaries", "Executive Documents"]
    positions = [text.find(marker) for marker in markers if text.find(marker) > 0]
    return min(positions) if positions else len(text)

def ref_path(text):
    return ".".join(part_re.findall(text or ""))

def reference_records(source, text, known_sections):
    cut = section_main_cut(text)
    rows = []
    for match in explicit_ref_re.finditer(text):
        target = match.group(1)
        part = "main_text" if match.start() < cut else "notes_or_amendments"
        status = "resolved_section" if target in known_sections else "unresolved_missing_section"
        rows.append({
            "source_section": source,
            "span_start": match.start(),
            "span_end": match.end(),
            "surface": match.group(0),
            "ref_class": "explicit",
            "target_section": target,
            "target_path": ref_path(match.group("trail")),
            "status": status,
            "section_part": part,
        })
    for match in local_ref_re.finditer(text):
        part = "main_text" if match.start() < cut else "notes_or_amendments"
        rows.append({
            "source_section": source,
            "span_start": match.start(),
            "span_end": match.end(),
            "surface": match.group(0),
            "ref_class": "local_structural",
            "target_section": source,
            "target_path": ref_path(match.group("trail")),
            "status": "needs_structural_resolution",
            "section_part": part,
        })
    return sorted(rows, key=lambda row: (row["span_start"], row["span_end"], row["surface"]))

def detected_section_refs(records):
    refs = {
        row["target_section"]
        for row in records
        if row["section_part"] == "main_text"
        and row["ref_class"] == "explicit"
        and row["status"] == "resolved_section"
    }
    return sorted(refs)

def reference_summary(records):
    summary = {
        "main_text": 0,
        "notes_or_amendments": 0,
        "resolved_section": 0,
        "needs_structural_resolution": 0,
        "unresolved_missing_section": 0,
    }
    for row in records:
        if row["section_part"] in summary:
            summary[row["section_part"]] += 1
        if row["section_part"] == "main_text" and row["status"] in summary:
            summary[row["status"]] += 1
    return summary

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


Do not use SELECT subqueries.
Do not use EXISTS.
Do not use WHERE.
Do not use a bare boolean E.deduct.
Do not create constraints that reject unrelated receipts.
Use referenced context only when it is supplied.
Do not infer a legal rule that is not supported by the supplied text.
If no DFC policy is clearly supported, return an empty policies list.

Main legal text:
{section["citation"]}
{section["text"][:section_main_cut(section["text"])]}

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
    reference_graph = load_graph()

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

        records = reference_records(number, section["text"], by_number)
        detected_refs = detected_section_refs(records)
        context_blocks = []
        context_refs = []
        context_identifiers = []
        context = ""

        if CONTEXT_MODE == "scoped":
            context_blocks = scoped_context(
                number,
                by_number,
                reference_graph,
            )

            if context_blocks:
                context_refs = sorted({
                    block["section"]
                    for block in context_blocks
                })
                context_identifiers = [
                    block["identifier"]
                    for block in context_blocks
                ]
                context = render_context(
                    context_blocks
                )

        use_detected = (
            CONTEXT_MODE == "detected"
            or (
                CONTEXT_MODE == "scoped"
                and not context_blocks
            )
        )

        if use_detected:
            context_refs = detected_refs
            context_parts = []

            for ref in context_refs:
                ref_section = by_number.get(ref)
                if ref_section:
                    ref_text = ref_section["text"]
                    ref_text = ref_text[
                        :section_main_cut(ref_text)
                    ]
                    context_parts.append(
                        ref_section["citation"]
                        + "\n"
                        + ref_text
                    )

            context = "\n\n".join(context_parts)

        source_main = section["text"][
            :section_main_cut(section["text"])
        ]
        legal_context = source_main
        if context:
            legal_context += (
                "\n\nReferenced context:\n"
                + context
            )
        candidates = generate_policy(section, context)
        main_records = [row for row in records if row["section_part"] == "main_text"]
        summary = reference_summary(records)

        for candidate in candidates:
            row = candidate.model_dump()
            row["policy"] = (
                "SOURCE Receipt AS R SINK Expense AS E\n"
                f"CONSTRAINT {row['constraint']}\n"
                "ON FAIL KILL"
            )
            row["detected_refs"] = detected_refs
            row["context_refs"] = context_refs
            row["context_identifiers"] = context_identifiers
            row["context_chars"] = len(context)
            row["context_mode"] = CONTEXT_MODE
            row["detected_ref_records"] = main_records
            row["reference_summary"] = summary
            row["valid_dfc_subset"] = valid_dfc(row["policy"])
            if row["valid_dfc_subset"] and RUN_JUDGE:
                row["confidence"] = judge_policy(
                    row,
                    legal_context,
                )
                row["judge_ran"] = True
            elif row["valid_dfc_subset"]:
                row["confidence"] = "NOT_RUN"
                row["judge_ran"] = False
            else:
                row["confidence"] = "LOW"
                row["judge_ran"] = False
            results.append(row)

    OUTPUT.parent.mkdir(exist_ok=True)
    OUTPUT.write_text(json.dumps(results, indent=2), encoding="utf-8-sig")
    print(f"Wrote {OUTPUT} with {len(results)} policies")

if __name__ == "__main__":
    main()
