import csv
import json
import re
from pathlib import Path

from bedrock_ref_client import complete_json

base = Path(__file__).resolve().parent
sections_path = base / "inputs" / "title26_sections.jsonl"
output_path = base / "outputs" / "llm_reference_audit_274.csv"

section_re = re.compile(r"\u00a7\s*([0-9][0-9A-Za-z-]*)")
id_re = re.compile(r"(?:section|usc)[:_\s-]*([0-9][0-9A-Za-z-]*)", re.I)


def read_jsonl(path):
    rows = []
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def pick(row, keys):
    for key in keys:
        value = row.get(key)
        if value:
            return str(value)
    return ""


def section_number(row):
    direct = pick(row, ["section", "section_number", "num", "number"])
    if direct:
        return direct.strip().lstrip("§")

    text = pick(row, ["citation", "cite", "source_citation", "id"])
    found = section_re.search(text)
    if found:
        return found.group(1)

    found = id_re.search(text)
    return found.group(1) if found else ""


def section_text(row):
    return pick(row, ["text", "body", "content", "section_text"])


def main_text(text):
    markers = [
        "Editorial Notes",
        "Source Credit",
        "Statutory Notes and Related Subsidiaries",
        "Executive Documents",
    ]
    positions = [text.find(marker) for marker in markers if text.find(marker) > 0]
    return text[: min(positions)] if positions else text


def chunks(text, size=3500):
    start = 0
    while start < len(text):
        end = min(len(text), start + size)
        split = text.rfind("\n", start, end)
        if split > start + 1200:
            end = split
        yield start, text[start:end]
        start = end


def prompt_for(piece):
    return f"""
Return only valid JSON. Do not explain.

Find legal cross-reference expressions in this excerpt from 26 U.S.C. section 274.

Include exact phrases like:
- section 162
- section 212
- section 132(f)
- subsection (a)
- paragraph (1)
- subparagraph (A)
- § 274

Exclude headings, editorial notes, source credits, and explanations.

Use this exact JSON shape:
{{
  "references": [
    {{
      "surface": "section 212",
      "ref_class": "explicit",
      "target_section": "212",
      "target_path": "",
      "confidence": "high"
    }}
  ]
}}

For local references inside section 274, set:
"ref_class": "local_structural"
"target_section": "274"

Excerpt:
{piece}
""".strip()


def normalize_class(value):
    value = str(value or "").strip().lower()
    if value in {"explicit", "local_structural", "external"}:
        return value
    if value in {"local", "structural"}:
        return "local_structural"
    return "explicit"


def find_span(text, surface, start_hint):
    if not surface:
        return -1, -1

    start = text.find(surface, start_hint)
    if start < 0:
        start = text.find(surface)
    if start < 0:
        return -1, -1

    return start, start + len(surface)


def main():
    sections = read_jsonl(sections_path)
    known = {section_number(row): row for row in sections if section_number(row)}

    row = known.get("274")
    if not row:
        raise RuntimeError("section 274 not found")

    text = main_text(section_text(row))
    rows = []
    seen = set()

    for chunk_start, piece in chunks(text):
        data = complete_json(prompt_for(piece), max_tokens=16000)

        for item in data.get("references", []):
            surface = str(item.get("surface", "")).strip()
            start, end = find_span(text, surface, chunk_start)

            key = (start, end, surface)
            if key in seen:
                continue
            seen.add(key)

            ref_class = normalize_class(item.get("ref_class"))
            target = str(item.get("target_section", "")).strip().lstrip("§")

            if ref_class == "local_structural" and not target:
                target = "274"

            rows.append(
                {
                    "source_section": "274",
                    "span_start": start,
                    "span_end": end,
                    "surface": surface,
                    "ref_class": ref_class,
                    "target_section": target,
                    "target_path": str(item.get("target_path", "")).strip(),
                    "status": "needs_review",
                    "method": "llm",
                    "confidence": str(item.get("confidence", "")).strip().lower(),
                }
            )

    output_path.parent.mkdir(exist_ok=True)

    fields = [
        "source_section",
        "span_start",
        "span_end",
        "surface",
        "ref_class",
        "target_section",
        "target_path",
        "status",
        "method",
        "confidence",
    ]

    rows.sort(key=lambda row: int(row["span_start"]) if str(row["span_start"]).isdigit() else 999999999)

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    print(f"wrote {output_path} with {len(rows)} rows")


if __name__ == "__main__":
    main()
