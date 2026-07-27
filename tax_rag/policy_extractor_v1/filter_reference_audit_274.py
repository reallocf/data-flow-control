import csv
import json
import re
from pathlib import Path

base = Path(".")
sections_path = base / "inputs" / "title26_sections.jsonl"
in_csv = base / "outputs" / "reference_audit_274.csv"
main_csv = base / "outputs" / "reference_audit_274_main.csv"
annotated_csv = base / "outputs" / "reference_audit_274_annotated.csv"
summary_csv = base / "outputs" / "reference_audit_274_part_summary.csv"

section_re = re.compile(r"§\s*([0-9][0-9A-Za-z-]*)")

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
        return direct
    text = pick(row, ["citation", "cite", "source_citation", "id"])
    found = section_re.search(text)
    if found:
        return found.group(1)
    found = re.search(r"(?:section|usc)[:_\s-]*([0-9][0-9A-Za-z-]*)", text, re.I)
    return found.group(1) if found else ""

def section_text(row):
    return pick(row, ["text", "body", "content", "section_text"])

sections = read_jsonl(sections_path)
text = ""

for row in sections:
    if section_number(row) == "274":
        text = section_text(row)
        break

if not text:
    raise RuntimeError("section 274 text not found")

markers = ["Editorial Notes", "Source Credit", "Statutory Notes and Related Subsidiaries", "Executive Documents"]
positions = [text.find(marker) for marker in markers if text.find(marker) > 0]
main_cut = min(positions) if positions else len(text)

rows = list(csv.DictReader(in_csv.open(newline="", encoding="utf-8-sig")))

fields = list(rows[0].keys())
if "section_part" not in fields:
    fields.append("section_part")

annotated = []
main_rows = []
counts = {}

for row in rows:
    start = int(row["span_start"])
    row["section_part"] = "main_text" if start < main_cut else "notes_or_amendments"
    counts[row["section_part"]] = counts.get(row["section_part"], 0) + 1
    annotated.append(row)
    if row["section_part"] == "main_text":
        main_rows.append(row)

with annotated_csv.open("w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=fields)
    writer.writeheader()
    writer.writerows(annotated)

with main_csv.open("w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=fields)
    writer.writeheader()
    writer.writerows(main_rows)

with summary_csv.open("w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=["section_part", "count"])
    writer.writeheader()
    for key in sorted(counts):
        writer.writerow({"section_part": key, "count": counts[key]})

print("main_cut", main_cut)
print("main_rows", len(main_rows))
print("wrote", main_csv)
print("wrote", annotated_csv)
print("wrote", summary_csv)
