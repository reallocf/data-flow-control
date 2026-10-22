import csv
import json
import re
from pathlib import Path

base = Path(__file__).resolve().parent
sections_path = base / "inputs" / "title26_sections.jsonl"
policies_path = base / "outputs" / "policies_274.json"
out_json = base / "outputs" / "reference_audit_274.json"
out_csv = base / "outputs" / "reference_audit_274.csv"

explicit_re = re.compile(r"\b(?:sections?|§{1,2})\s+([0-9][0-9A-Za-z-]*)(?P<trail>(?:\s*\([A-Za-z0-9-]+\))*)", re.I)
local_re = re.compile(r"\b(?:this\s+|such\s+)?(subsection|paragraph|subparagraph|clause)s?\s+(?P<trail>(?:\([A-Za-z0-9-]+\)\s*)+)", re.I)
section_re = re.compile(r"§\s*([0-9][0-9A-Za-z-]*)")
part_re = re.compile(r"\(([A-Za-z0-9-]+)\)")


def read_json(path):
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ["policies", "items", "results"]:
            if isinstance(data.get(key), list):
                return data[key]
        return [data]
    return []


def read_jsonl(path):
    rows = []
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def value(row, keys):
    for key in keys:
        item = row.get(key)
        if item:
            return str(item)
    return ""


def section_number(row):
    direct = value(row, ["section", "section_number", "num", "number"])
    if direct:
        return direct
    text = value(row, ["citation", "cite", "source_citation", "id"])
    found = section_re.search(text)
    if found:
        return found.group(1)
    found = re.search(r"(?:section|usc)[:_\s-]*([0-9][0-9A-Za-z-]*)", text, re.I)
    return found.group(1) if found else ""


def section_text(row):
    return value(row, ["text", "body", "content", "section_text"])


def section_citation(row):
    return value(row, ["citation", "cite", "source_citation", "id"])


def parts(text):
    return ".".join(part_re.findall(text or ""))


def source_from_policy(policy):
    found = section_re.search(str(policy.get("source_citation", "")))
    return found.group(1) if found else ""


def scan(text, source, known):
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
            "target_path": parts(match.group("trail")),
            "status": "resolved_section" if target in known else "unresolved_missing_section",
        })
    for match in local_re.finditer(text):
        rows.append({
            "source_section": source,
            "span_start": match.start(),
            "span_end": match.end(),
            "surface": match.group(0),
            "ref_class": "local_structural",
            "target_section": source,
            "target_path": parts(match.group("trail")),
            "status": "needs_structural_resolution",
        })
    return sorted(rows, key=lambda row: (row["span_start"], row["span_end"], row["surface"]))


def main():
    sections = read_jsonl(sections_path)
    policies = read_json(policies_path)

    known = {}
    for row in sections:
        number = section_number(row)
        if number:
            known[number] = row

    sources = {"274"}
    for policy in policies:
        source = source_from_policy(policy)
        if source:
            sources.add(source)

    audit = []
    for source in sorted(sources):
        row = known.get(source)
        if row:
            audit.append({
                "source_section": source,
                "source_citation": section_citation(row),
                "reference_mentions": scan(section_text(row), source, known),
            })

    detected = []
    for index, policy in enumerate(policies):
        for ref in policy.get("detected_refs", []):
            ref = str(ref)
            detected.append({
                "policy_index": index,
                "source_citation": policy.get("source_citation", ""),
                "detected_ref": ref,
                "target_exists": ref in known,
            })

    report = {
        "sections_file": str(sections_path),
        "policies_file": str(policies_path),
        "audit": audit,
        "policy_detected_refs": detected,
    }

    out_json.write_text(json.dumps(report, indent=2), encoding="utf-8")

    with out_csv.open("w", newline="", encoding="utf-8") as handle:
        fields = ["source_section", "span_start", "span_end", "surface", "ref_class", "target_section", "target_path", "status", "manual_note"]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for item in audit:
            for row in item["reference_mentions"]:
                writer.writerow({**row, "manual_note": ""})

    print(out_json)
    print(out_csv)


if __name__ == "__main__":
    main()
