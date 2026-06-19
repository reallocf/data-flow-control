import csv
from pathlib import Path

gold_path = Path("outputs/reference_union_gold_274.csv")
out_path = Path("outputs/reference_eval_summary_274.csv")

rows = list(csv.DictReader(gold_path.open(encoding="utf-8-sig")))

if any((row.get("gold") or "") == "" for row in rows):
    raise SystemExit("gold column still has blank cells")


def score(method):
    tp = fp = fn = 0
    for row in rows:
        predicted = row.get(method) == "1"
        actual = row.get("gold") == "1"

        if predicted and actual:
            tp += 1
        elif predicted and not actual:
            fp += 1
        elif not predicted and actual:
            fn += 1

    precision = tp / (tp + fp) if tp + fp else 0
    recall = tp / (tp + fn) if tp + fn else 0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0

    return {
        "method": method,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": f"{precision:.4f}",
        "recall": f"{recall:.4f}",
        "f1": f"{f1:.4f}",
    }


summary = [score("rej16"), score("llm")]

with out_path.open("w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(
        handle,
        fieldnames=["method", "tp", "fp", "fn", "precision", "recall", "f1"],
    )
    writer.writeheader()
    writer.writerows(summary)

for row in summary:
    print(row)
