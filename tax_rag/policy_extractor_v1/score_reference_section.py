import csv
import os
from pathlib import Path

base = Path(__file__).resolve().parent
section = os.getenv("SECTION", "212").strip()

gold_path = base / "outputs" / f"reference_union_gold_{section}.csv"
summary_path = base / "outputs" / f"reference_eval_summary_{section}.csv"


def read_csv(path):
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def score(rows, method):
    tp = sum(1 for row in rows if row[method] == "1" and row["gold"] == "1")
    fp = sum(1 for row in rows if row[method] == "1" and row["gold"] == "0")
    fn = sum(1 for row in rows if row[method] == "0" and row["gold"] == "1")

    precision = tp / (tp + fp) if tp + fp else 0
    recall = tp / (tp + fn) if tp + fn else 0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0

    return {
        "section": section,
        "method": method,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": f"{precision:.4f}",
        "recall": f"{recall:.4f}",
        "f1": f"{f1:.4f}",
    }


rows = read_csv(gold_path)
blanks = [row for row in rows if row.get("gold", "") == ""]
if blanks:
    raise RuntimeError(f"{len(blanks)} blank gold cells remain")

summary = [score(rows, "rej16"), score(rows, "llm")]
fields = ["section", "method", "tp", "fp", "fn", "precision", "recall", "f1"]

with summary_path.open("w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=fields)
    writer.writeheader()
    writer.writerows(summary)

for row in summary:
    print(row)
