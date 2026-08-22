import csv
import re
from collections import Counter, defaultdict
from pathlib import Path
from xml.etree import ElementTree as ET

import cornell_reference_audit as cx
from rej16_reference_audit import read_jsonl, scan, section_number, section_text

base = Path(__file__).resolve().parent
input_path = base / "inputs" / "title26_sections.jsonl"
xml_path = base / "inputs" / "usc26.xml"
diff_path = base / "outputs" / "rej16_title26_diff_clean.csv"
raw_summary_path = base / "outputs" / "rej16_title26_summary_clean.csv"
audit_path = base / "outputs" / "rej16_title26_disagreement_audit.csv"
category_path = base / "outputs" / "rej16_title26_disagreement_categories.csv"
adjudicated_path = base / "outputs" / "rej16_title26_adjudicated_summary.csv"
meeting_path = base / "outputs" / "rej16_title26_meeting_summary.txt"

def norm(value):
    return re.sub(r"\s+", "", str(value)).replace("\u2013", "-").replace("\u2014", "-").replace("\u2212", "-").replace("§", "")

def local(tag):
    return tag.rsplit("}", 1)[-1]

rows = read_jsonl(input_path)
known = {norm(section_number(row)): row for row in rows if section_number(row)}

scan_rows = []
for number, row in known.items():
    scan_rows.extend(scan(section_text(row), number, known))

by_source = defaultdict(list)
for row in scan_rows:
    by_source[norm(row["source_section"])].append(row)

root = ET.parse(xml_path).getroot()
xml_text = {}

for sec in root.iter():
    if local(sec.tag) != "section":
        continue

    match = re.fullmatch(
        r"/us/usc/t26/s([^/]+)",
        sec.attrib.get("identifier", ""),
    )

    if match:
        xml_text[norm(match.group(1))] = cx.statute_text(sec)

def xml_hits(source, target):
    text = xml_text[source]
    out = []

    for match in cx.SECTION_EXPR_RE.finditer(text):
        items = []

        for item in re.finditer(cx.ATOM, match.group("items"), re.I):
            head = cx.SECTION_HEAD_RE.match(item.group(0))
            if head:
                items.append(norm(head.group(0)))

        if target not in items:
            continue

        suffix = text[match.end():match.end() + 140]

        out.append({
            "items": items,
            "external": bool(cx.EXTERNAL_SUFFIX_RE.match(suffix)),
        })

    return out

def classify(source, target, status):
    source_rows = by_source[source]
    hits = xml_hits(source, target)

    if status == "rej16_only":
        if any(hit["external"] for hit in hits):
            return "rej16_external_law"

        raw_variant = target.replace("-", "\u2013")

        if raw_variant != target and raw_variant in xml_text[source]:
            return "xml_typographic_dash"

        return "unclassified"

    if status == "xml_only":
        if any(
            row["status"] == "resolved_external"
            and norm(row["target_section"]) == target
            for row in source_rows
        ):
            return "xml_external_reference"

        if any(
            row["status"] == "unresolved_missing_section"
            and norm(row["target_section"]).endswith("-")
            and norm(row["target_section"]).rstrip("-") == target
            for row in source_rows
        ):
            return "rej16_dash_delimiter"

        for hit in hits:
            if hit["external"]:
                continue

            positions = [
                i
                for i, item in enumerate(hit["items"])
                if item == target
            ]

            if len(hit["items"]) > 1 and any(i > 0 for i in positions):
                return "rej16_multi_section_list"

        return "unclassified"

    return "agreement"

with diff_path.open(encoding="utf-8") as handle:
    diff_rows = list(csv.DictReader(handle))

audit_rows = []
counts = Counter()

for row in diff_rows:
    if row["status"] == "both":
        continue

    source = norm(row["source_section"])
    target = norm(row["target_section"])
    category = classify(source, target, row["status"])

    counts[category] += 1

    audit_rows.append({
        "source_section": source,
        "target_section": target,
        "comparison_status": row["status"],
        "category": category,
    })

expected = {
    "xml_external_reference": 19,
    "rej16_external_law": 111,
    "xml_typographic_dash": 3,
}

if counts.get("unclassified", 0):
    raise RuntimeError(
        f"unclassified disagreements: {counts['unclassified']}"
    )

if dict(counts) != expected:
    raise RuntimeError(
        f"unexpected categories: {dict(counts)}"
    )

with audit_path.open("w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(
        handle,
        fieldnames=[
            "source_section",
            "target_section",
            "comparison_status",
            "category",
        ],
    )
    writer.writeheader()
    writer.writerows(
        sorted(
            audit_rows,
            key=lambda row: (
                row["category"],
                row["source_section"],
                row["target_section"],
            ),
        )
    )

with category_path.open("w", newline="", encoding="utf-8") as handle:
    writer = csv.writer(handle)
    writer.writerow(["category", "count"])

    for category, count in sorted(counts.items()):
        writer.writerow([category, count])

with raw_summary_path.open(encoding="utf-8") as handle:
    raw = next(csv.DictReader(handle))

raw_tp = int(raw["tp"])
adj_tp = raw_tp + counts["xml_typographic_dash"]
adj_fp = counts["rej16_external_law"]
adj_fn = (
    counts["rej16_multi_section_list"]
    + counts["rej16_dash_delimiter"]
)

precision = adj_tp / (adj_tp + adj_fp)
recall = adj_tp / (adj_tp + adj_fn)
f1 = 2 * precision * recall / (precision + recall)

adjudicated = {
    "sections": len(known),
    "tp": adj_tp,
    "fp": adj_fp,
    "fn": adj_fn,
    "precision": f"{precision:.4f}",
    "recall": f"{recall:.4f}",
    "f1": f"{f1:.4f}",
}

with adjudicated_path.open(
    "w",
    newline="",
    encoding="utf-8",
) as handle:
    writer = csv.DictWriter(
        handle,
        fieldnames=adjudicated.keys(),
    )
    writer.writeheader()
    writer.writerow(adjudicated)

meeting_lines = [
    "Title 26 REJ16 baseline",
    f"Sections: {len(known)}",
    f"Raw XML comparison: precision {raw['precision']}, recall {raw['recall']}, F1 {raw['f1']}",
    f"Disagreements: {len(audit_rows)}",
    f"Multi-section list misses: {counts['rej16_multi_section_list']}",
    f"Dash-delimiter misses: {counts['rej16_dash_delimiter']}",
    f"External-law false positives: {counts['rej16_external_law']}",
    f"XML external-reference false positives: {counts['xml_external_reference']}",
    f"XML typographic-dash omissions: {counts['xml_typographic_dash']}",
    f"Rule-adjudicated estimate: precision {precision:.4f}, recall {recall:.4f}, F1 {f1:.4f}",
    "Caveat: the comparator is parser-derived rather than an independent hand-labeled gold set.",
]

meeting_path.write_text(
    "\n".join(meeting_lines) + "\n",
    encoding="utf-8",
)

print(dict(counts))
print(adjudicated)
print(meeting_path)
