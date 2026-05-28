import json
import os
import re
from pathlib import Path
from openai import OpenAI
from dotenv import load_dotenv

INPUT = Path("inputs/sections.jsonl")
OUTPUT = Path("outputs/policies.json")
MODEL = os.getenv("MODEL", "gpt-4.1-mini")

load_dotenv()
client = OpenAI()

def load_sections():
    if not INPUT.exists():
        raise FileNotFoundError("Missing inputs/sections.jsonl")
    return [json.loads(line) for line in INPUT.read_text(encoding="utf-8-sig").splitlines() if line.strip()]

def detect_refs(text):
    refs = set()
    for m in re.finditer(r"(?:section|§)\s*(\d+[A-Za-z0-9-]*)", text, re.I):
        refs.add(m.group(1))
    return sorted(refs)

def clean_json(text):
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?", "", text).strip()
        text = re.sub(r"```$", "", text).strip()
    return json.loads(text)

def llm(prompt):
    response = client.responses.create(
        model=MODEL,
        input=prompt,
    )
    return response.output_text

def valid_dfc(policy):
    upper = policy.upper()
    return all(x in upper for x in ["SOURCE", "SINK", "CONSTRAINT", "ON FAIL"])

def generate_policy(section, context):
    prompt = f"""
You extract candidate Data Flow Control policies from U.S. tax law.

Return JSON only. Return a list of objects.
Each object must have:
- source_citation
- supporting_citations
- policy
- explanation

Use only this DFC shape:
SOURCE Receipt AS R SINK Expense AS E
CONSTRAINT <SQL-like boolean condition>
ON FAIL KILL

If no DFC policy is clearly supported, return [].

Example:
[
  {{
    "source_citation": "26 U.S.C. § 274(n)",
    "supporting_citations": ["26 U.S.C. § 274(n)"],
    "policy": "SOURCE Receipt AS R SINK Expense AS E\\nCONSTRAINT R.category != 'Meal' OR E.deduct <= 0.5\\nON FAIL KILL",
    "explanation": "Meal expenses may be deducted only up to 50 percent."
  }}
]

Main legal text:
{section["citation"]}
{section["text"]}

Referenced context:
{context}
"""
    return clean_json(llm(prompt))

def judge_policy(policy_obj, legal_text):
    prompt = f"""
Return only HIGH or LOW.

Is this DFC policy clearly supported by the legal text?

Legal text:
{legal_text}

Policy:
{policy_obj["policy"]}
"""
    answer = llm(prompt).strip().upper()
    return "HIGH" if "HIGH" in answer else "LOW"

def main():
    sections = load_sections()

    by_number = {}
    for s in sections:
        m = re.search(r"§\s*(\d+[A-Za-z0-9-]*)", s["citation"])
        if m:
            by_number[m.group(1)] = s

    results = []

    for section in sections:
        refs = detect_refs(section["text"])
        context_parts = []
        for ref in refs:
            if ref in by_number:
                ref_section = by_number[ref]
                context_parts.append(ref_section["citation"] + "\n" + ref_section["text"])
        context = "\n\n".join(context_parts)

        candidates = generate_policy(section, context)

        for p in candidates:
            p["detected_refs"] = refs
            p["valid_dfc_subset"] = valid_dfc(p.get("policy", ""))
            p["confidence"] = judge_policy(p, section["text"]) if p["valid_dfc_subset"] else "LOW"
            results.append(p)

    OUTPUT.write_text(json.dumps(results, indent=2), encoding="utf-8-sig")
    print(f"Wrote {OUTPUT} with {len(results)} policies")

if __name__ == "__main__":
    main()


