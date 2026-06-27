import csv
from pathlib import Path

base = Path(__file__).resolve().parent
sections = ["274", "212", "162"]
summary_path = base / "outputs" / "reference_eval_summary_integrated.csv"


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
        "scope": "integrated",
        "method": method,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": f"{precision:.4f}",
        "recall": f"{recall:.4f}",
        "f1": f"{f1:.4f}",
    }


rows = []
for section in sections:
    path = base / "outputs" / f"reference_union_gold_{section}.csv"
    part = read_csv(path)
    blanks = [row for row in part if row.get("gold", "") == ""]
    if blanks:
        raise RuntimeError(f"{path} has {len(blanks)} blank gold cells")
    rows.extend(part)

summary = [score(rows, "rej16"), score(rows, "llm")]
fields = ["scope", "method", "tp", "fp", "fn", "precision", "recall", "f1"]

with summary_path.open("w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=fields)
    writer.writeheader()
    writer.writerows(summary)

for row in summary:
    print(row)
