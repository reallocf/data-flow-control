import csv
import re
from pathlib import Path
from cornell_reference_audit import load_xml
from rej16_reference_audit import read_jsonl, scan, section_number, section_text

base = Path(__file__).resolve().parent
input_path = base / "inputs" / "title26_sections.jsonl"
summary_path = base / "outputs" / "rej16_title26_summary_clean.csv"
diff_path = base / "outputs" / "rej16_title26_diff_clean.csv"

def norm(value):
    return re.sub(r"\s+", "", str(value)).replace("\u2013", "-").replace("\u2014", "-").replace("\u2212", "-").replace("§", "")

rows = read_jsonl(input_path)

known = {}
for row in rows:
    number = norm(section_number(row))
    if number:
        known[number] = row

pred_rows = []

for number in known:
    pred_rows.extend(scan(section_text(known[number]), number, known))

xml_sections, raw_xml_pairs = load_xml()
xml_known = {norm(x) for x in xml_sections}
eligible = set(known) & xml_known

pred_pairs = {
    (norm(row["source_section"]), norm(row["target_section"]))
    for row in pred_rows
    if row["status"] == "resolved_section"
    and norm(row["source_section"]) != norm(row["target_section"])
    and norm(row["source_section"]) in eligible
    and norm(row["target_section"]) in eligible
}

xml_pairs = {
    (norm(source), norm(target))
    for source, target in raw_xml_pairs
    if norm(source) != norm(target)
    and norm(source) in eligible
    and norm(target) in eligible
}

tp = pred_pairs & xml_pairs
fp = pred_pairs - xml_pairs
fn = xml_pairs - pred_pairs

precision = len(tp) / (len(tp) + len(fp)) if tp or fp else 0.0
recall = len(tp) / (len(tp) + len(fn)) if tp or fn else 0.0
f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0

summary = {
    "input_sections": len(known),
    "xml_sections": len(xml_known),
    "eligible_sections": len(eligible),
    "rej16_pairs": len(pred_pairs),
    "xml_pairs": len(xml_pairs),
    "tp": len(tp),
    "fp": len(fp),
    "fn": len(fn),
    "precision": f"{precision:.4f}",
    "recall": f"{recall:.4f}",
    "f1": f"{f1:.4f}",
}

summary_path.parent.mkdir(exist_ok=True)

with summary_path.open("w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=summary.keys())
    writer.writeheader()
    writer.writerow(summary)

with diff_path.open("w", newline="", encoding="utf-8") as handle:
    writer = csv.writer(handle)
    writer.writerow(["source_section", "target_section", "status"])

    for source, target in sorted(tp):
        writer.writerow([source, target, "both"])

    for source, target in sorted(fp):
        writer.writerow([source, target, "rej16_only"])

    for source, target in sorted(fn):
        writer.writerow([source, target, "xml_only"])

print(summary)
