import json
import os
import re
import time
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

if CONTEXT_MODE not in {
    "none",
    "detected",
    "scoped",
    "direct",
    "bounded",
}:
    raise ValueError(
        f"invalid CONTEXT_MODE: {CONTEXT_MODE}"
    )

DIRECT_CONTEXT_PATH = Path(
    os.getenv(
        "DIRECT_CONTEXT_PATH",
        "outputs/section_274_direct_context.txt",
    )
)

DIRECT_MANIFEST_PATH = Path(
    os.getenv(
        "DIRECT_MANIFEST_PATH",
        "outputs/section_274_direct_context_manifest.json",
    )
)

DIRECT_METRICS_PATH = Path(
    os.getenv(
        "DIRECT_METRICS_PATH",
        "outputs/section_274_direct_prompt_metrics.json",
    )
)

BOUNDED_CONTEXT_PATH = Path(
    os.getenv(
        "BOUNDED_CONTEXT_PATH",
        "outputs/section_274_bounded_context.txt",
    )
)

BOUNDED_MANIFEST_PATH = Path(
    os.getenv(
        "BOUNDED_MANIFEST_PATH",
        "outputs/section_274_bounded_context_manifest.json",
    )
)

BOUNDED_METRICS_PATH = Path(
    os.getenv(
        "BOUNDED_METRICS_PATH",
        "outputs/section_274_bounded_prompt_metrics.json",
    )
)

DRY_RUN = os.getenv(
    "DRY_RUN",
    "0",
).strip().lower() in {
    "1",
    "true",
    "yes",
}

DRY_RUN_PROMPT_OUTPUT = Path(
    os.getenv(
        "DRY_RUN_PROMPT_OUTPUT",
        "outputs/section_274_bounded_prompt_from_extract.txt",
    )
)

RUN_JUDGE = os.getenv(
    "RUN_JUDGE",
    "1",
).strip().lower() not in {
    "0",
    "false",
    "no",
}

client = None

API_CALL_LOG = []

API_LOG_OUTPUT = os.getenv(
    "API_LOG_OUTPUT",
    "",
).strip()

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
    global client

    if client is None:
        client = OpenAI()

    started = time.perf_counter()

    response = client.responses.parse(
        model=MODEL,
        input=[{"role": "user", "content": prompt}],
        text_format=schema,
        temperature=0,
    )

    elapsed = (
        time.perf_counter()
        - started
    )

    usage = getattr(
        response,
        "usage",
        None,
    )

    if hasattr(
        usage,
        "model_dump",
    ):
        usage = usage.model_dump(
            mode="json"
        )

    elif usage is not None:
        usage = str(usage)

    API_CALL_LOG.append({
        "response_id":
            getattr(
                response,
                "id",
                None,
            ),
        "response_model":
            getattr(
                response,
                "model",
                MODEL,
            ),
        "elapsed_seconds":
            round(
                elapsed,
                6,
            ),
        "usage":
            usage,
    })

    return response.output_parsed

def generate_policy(
    section,
    context,
    return_prompt=False,
):
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
    if return_prompt:
        return prompt

    return parsed(
        prompt,
        CandidateBatch,
    ).policies

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
        context_external_citations = []
        context = ""

        if CONTEXT_MODE == "direct":
            if number != "274":
                raise RuntimeError(
                    "The current direct-context "
                    "artifact is defined only for "
                    "Section 274."
                )

            required = [
                DIRECT_CONTEXT_PATH,
                DIRECT_MANIFEST_PATH,
                DIRECT_METRICS_PATH,
            ]

            missing = [
                str(path)
                for path in required
                if not path.exists()
            ]

            if missing:
                raise FileNotFoundError(
                    "Missing direct-context "
                    "artifact(s): "
                    + ", ".join(missing)
                )

            context = (
                DIRECT_CONTEXT_PATH.read_text(
                    encoding="utf-8"
                ).rstrip()
            )

            manifest = json.loads(
                DIRECT_MANIFEST_PATH.read_text(
                    encoding="utf-8"
                )
            )

            metrics = json.loads(
                DIRECT_METRICS_PATH.read_text(
                    encoding="utf-8"
                )
            )

            if (
                metrics.get("status")
                != "DIRECT_MATERIALIZATION"
            ):
                raise RuntimeError(
                    "Unexpected direct-context "
                    "status: "
                    + str(
                        metrics.get("status")
                    )
                )

            if (
                int(
                    metrics.get(
                        "dependency_layer",
                        -1,
                    )
                )
                != 1
            ):
                raise RuntimeError(
                    "Direct context is not "
                    "dependency layer 1."
                )

            if (
                int(
                    metrics.get(
                        "tax_code_units",
                        -1,
                    )
                )
                != 19
            ):
                raise RuntimeError(
                    "Unexpected direct Title-26 "
                    "unit count."
                )

            if (
                int(
                    metrics.get(
                        "cross_title_provisions",
                        -1,
                    )
                )
                != 3
            ):
                raise RuntimeError(
                    "Unexpected direct cross-title "
                    "provision count."
                )

            context_refs = sorted({
                str(unit["section"])
                for unit
                in manifest.get(
                    "tax_code_units",
                    [],
                )
            })

            context_identifiers = [
                str(unit["identifier"])
                for unit
                in manifest.get(
                    "tax_code_units",
                    [],
                )
            ]

            context_external_citations = [
                str(item["citation"])
                for item
                in manifest.get(
                    "cross_title",
                    [],
                )
            ]

        elif CONTEXT_MODE == "bounded":
            if number != "274":
                raise RuntimeError(
                    "The current bounded-context "
                    "artifact is defined only for "
                    "Section 274."
                )

            required = [
                BOUNDED_CONTEXT_PATH,
                BOUNDED_MANIFEST_PATH,
                BOUNDED_METRICS_PATH,
            ]

            missing = [
                str(path)
                for path in required
                if not path.exists()
            ]

            if missing:
                raise FileNotFoundError(
                    "Missing bounded-context "
                    "artifact(s): "
                    + ", ".join(missing)
                )

            context = (
                BOUNDED_CONTEXT_PATH.read_text(
                    encoding="utf-8"
                ).rstrip()
            )

            manifest = json.loads(
                BOUNDED_MANIFEST_PATH.read_text(
                    encoding="utf-8"
                )
            )

            metrics = json.loads(
                BOUNDED_METRICS_PATH.read_text(
                    encoding="utf-8"
                )
            )

            allowed_status = {
                "CONDITIONAL_MATERIALIZATION",
                "READY_FOR_POLICY_GENERATION",
            }

            if (
                metrics.get("status")
                not in allowed_status
            ):
                raise RuntimeError(
                    "Unexpected bounded prompt "
                    "status: "
                    + str(
                        metrics.get("status")
                    )
                )

            if (
                metrics.get(
                    "overlay_entries"
                )
                != metrics.get(
                    "overlay_applied"
                )
            ):
                raise RuntimeError(
                    "Bounded overlay is not "
                    "fully applied."
                )

            if (
                int(
                    metrics.get(
                        "last_complete_layer",
                        -1,
                    )
                )
                != 3
            ):
                raise RuntimeError(
                    "Unexpected bounded "
                    "last-complete layer."
                )

            if (
                int(
                    metrics.get(
                        "first_layer_beyond_capacity",
                        -1,
                    )
                )
                != 4
            ):
                raise RuntimeError(
                    "Unexpected bounded "
                    "first-over-capacity layer."
                )

            context_refs = sorted({
                str(unit["section"])
                for unit
                in manifest.get(
                    "tax_code_units",
                    [],
                )
            })

            context_identifiers = [
                str(unit["identifier"])
                for unit
                in manifest.get(
                    "tax_code_units",
                    [],
                )
            ]

            context_external_citations = [
                str(item["citation"])
                for item
                in manifest.get(
                    "cross_title",
                    [],
                )
            ]

        elif CONTEXT_MODE == "scoped":
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
        if DRY_RUN:
            prompt = generate_policy(
                section,
                context,
                return_prompt=True,
            )

            output_path = (
                DRY_RUN_PROMPT_OUTPUT
            )

            if len(TARGETS) > 1:
                output_path = (
                    output_path.parent
                    / (
                        output_path.stem
                        + "_"
                        + number
                        + output_path.suffix
                    )
                )

            output_path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            output_path.write_bytes(
                prompt.encode("utf-8")
            )

            print(
                "DRY_RUN_PROMPT="
                + str(output_path)
            )

            continue

        candidates = generate_policy(
            section,
            context,
        )

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
            row["context_external_citations"] = (
                context_external_citations
            )
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

    if DRY_RUN:
        print(
            "DRY_RUN_COMPLETE=1"
        )
        print(
            "API_CALLED=0"
        )
        return

    if API_LOG_OUTPUT:
        api_log_path = Path(
            API_LOG_OUTPUT
        )

        api_log_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        api_log_path.write_text(
            json.dumps(
                {
                    "model":
                        MODEL,
                    "context_mode":
                        CONTEXT_MODE,
                    "run_judge":
                        RUN_JUDGE,
                    "calls":
                        API_CALL_LOG,
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    OUTPUT.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    OUTPUT.write_text(
        json.dumps(
            results,
            indent=2,
        ),
        encoding="utf-8-sig",
    )

    print(
        f"Wrote {OUTPUT} "
        f"with {len(results)} policies"
    )

if __name__ == "__main__":
    main()
