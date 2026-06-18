import csv
import json
import os
import re
from pathlib import Path

base = Path(__file__).resolve().parent
input_path = Path(os.getenv("INPUT", base / "inputs" / "title26_sections.jsonl"))
output_path = Path(os.getenv("OUTPUT", base / "outputs" / "rej16_reference_audit.csv"))
targets = {x.strip() for x in os.getenv("TARGETS", "274").split(",") if x.strip()}

section_re = re.compile(r"\u00a7\s*([0-9][0-9A-Za-z-]*)")
id_re = re.compile(r"(?:section|usc)[:_\s-]*([0-9][0-9A-Za-z-]*)", re.I)
explicit_re = re.compile(r"\b(?:sections?|\u00a7{1,2})\s+([0-9][0-9A-Za-z-]*)(?P<trail>(?:\s*\([A-Za-z0-9-]+\))*)", re.I)
local_re = re.compile(r"\b(?:this\s+|such\s+)?(subsection|paragraph|subparagraph|clause)s?\s+(?P<trail>(?:\([A-Za-z0-9-]+\)\s*)+)", re.I)
part_re = re.compile(r"\(([A-Za-z0-9-]+)\)")

def read_jsonl(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]

def pick(row, keys):
    for key in keys:
        value = row.get(key)
        if value:
            return str(value)
    return ""

def section_number(row):
    direct = pick(row, ["section", "section_number", "num", "number"])
    if direct:
        return direct
    text = pick(row, ["citation", "cite", "source_citation", "id"])
    found = section_re.search(text)
    if found:
        return found.group(1)
    found = id_re.search(text)
    return found.group(1) if found else ""

def section_text(row):
    return pick(row, ["text", "body", "content", "section_text"])

def main_cut(text):
    markers = ["Editorial Notes", "Source Credit", "Statutory Notes and Related Subsidiaries", "Executive Documents"]
    positions = [text.find(marker) for marker in markers if text.find(marker) > 0]
    return min(positions) if positions else len(text)

def path_parts(text):
    return ".".join(part_re.findall(text or ""))

def scan(text, source, known):
    text = text[:main_cut(text)]
    rows = []
    for match in explicit_re.finditer(text):
        target = match.group(1)
        rows.append({
            "source_section": source,
            "span_start": match.start(),
            "span_end": match.end(),
            "surface": match.group(0),
            "ref_class": "explicit",
            "target_section": target,
            "target_path": path_parts(match.group("trail")),
            "status": "resolved_section" if target in known else "unresolved_missing_section",
            "method": "rej16",
        })
    for match in local_re.finditer(text):
        rows.append({
            "source_section": source,
            "span_start": match.start(),
            "span_end": match.end(),
            "surface": match.group(0),
            "ref_class": "local_structural",
            "target_section": source,
            "target_path": path_parts(match.group("trail")),
            "status": "needs_structural_resolution",
            "method": "rej16",
        })
    return rows

def main():
    sections = read_jsonl(input_path)
    known = {section_number(row): row for row in sections if section_number(row)}

    rows = []
    for number in sorted(known):
        if number in targets:
            rows.extend(scan(section_text(known[number]), number, known))

    output_path.parent.mkdir(exist_ok=True)
    fields = ["source_section", "span_start", "span_end", "surface", "ref_class", "target_section", "target_path", "status", "method"]
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda r: (r["source_section"], int(r["span_start"]), int(r["span_end"]))))

    print(f"wrote {output_path} with {len(rows)} rows")

if __name__ == "__main__":
    main()
