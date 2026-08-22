import csv
from pathlib import Path
from cornell_reference_audit import load_xml as load_uslm, section_key
from rej16_reference_audit import read_jsonl, scan, section_number, section_text

base = Path(__file__).resolve().parent
input_path = base / "inputs" / "title26_sections.jsonl"
pred_path = base / "outputs" / "rej16_reference_audit_title26.csv"
summary_path = base / "outputs" / "rej16_title26_summary.csv"
diff_path = base / "outputs" / "rej16_title26_diff.csv"

rows = read_jsonl(input_path)
known = {section_number(row): row for row in rows if section_number(row)}
pred_rows = []

for number in sorted(known, key=section_key):
    pred_rows.extend(scan(section_text(known[number]), number, known))

pred_path.parent.mkdir(exist_ok=True)

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
]

with pred_path.open("w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=fields)
    writer.writeheader()
    writer.writerows(
        sorted(
            pred_rows,
            key=lambda row: (
                section_key(row["source_section"]),
                int(row["span_start"]),
                int(row["span_end"]),
            ),
        )
    )

uslm_sections, uslm_pairs = load_uslm()
uslm_known = set(uslm_sections)

pred_pairs = {
    (row["source_section"], row["target_section"])
    for row in pred_rows
    if row["status"] == "resolved_section"
    and row["source_section"] != row["target_section"]
    and row["source_section"] in uslm_known
    and row["target_section"] in uslm_known
}

tp = pred_pairs & uslm_pairs
fp = pred_pairs - uslm_pairs
fn = uslm_pairs - pred_pairs

precision = len(tp) / (len(tp) + len(fp)) if tp or fp else 0.0
recall = len(tp) / (len(tp) + len(fn)) if tp or fn else 0.0
f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0

summary = {
    "input_sections": len(known),
    "uslm_sections": len(uslm_known),
    "rej16_rows": len(pred_rows),
    "rej16_pairs": len(pred_pairs),
    "uslm_pairs": len(uslm_pairs),
    "tp": len(tp),
    "fp": len(fp),
    "fn": len(fn),
    "precision": f"{precision:.4f}",
    "recall": f"{recall:.4f}",
    "f1": f"{f1:.4f}",
}

with summary_path.open("w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=summary.keys())
    writer.writeheader()
    writer.writerow(summary)

with diff_path.open("w", newline="", encoding="utf-8") as handle:
    writer = csv.writer(handle)
    writer.writerow(["source_section", "target_section", "status"])
    for pair in sorted(
        tp | fp | fn,
        key=lambda pair: (section_key(pair[0]), section_key(pair[1])),
    ):
        status = "both" if pair in tp else "rej16_only" if pair in fp else "uslm_only"
        writer.writerow([pair[0], pair[1], status])

print(summary)
