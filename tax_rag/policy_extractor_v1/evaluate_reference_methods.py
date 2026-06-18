import csv
from pathlib import Path

base = Path(__file__).resolve().parent
rej16_path = base / "outputs" / "rej16_reference_audit.csv"
gold_path = base / "outputs" / "reference_gold.csv"
seed_path = base / "outputs" / "reference_gold_seed.csv"
summary_path = base / "outputs" / "reference_eval_summary.csv"

def read_csv(path):
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))

def norm(row):
    return {
        "source_section": row.get("source_section", ""),
        "span_start": row.get("span_start", ""),
        "span_end": row.get("span_end", ""),
        "surface": " ".join(row.get("surface", "").split()),
        "ref_class": row.get("ref_class", ""),
        "target_section": row.get("target_section", ""),
        "target_path": row.get("target_path", ""),
    }

def main():
    rows = read_csv(rej16_path)

    if not gold_path.exists():
        fields = ["source_section", "span_start", "span_end", "surface", "ref_class", "target_section", "target_path", "rej16", "gold", "note"]
        with seed_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            for row in rows:
                item = norm(row)
                item["rej16"] = "1"
                item["gold"] = ""
                item["note"] = ""
                writer.writerow(item)
        print(f"wrote {seed_path}")
        print("copy to outputs/reference_gold.csv, fill gold with 1 or 0, then run again")
        return

    gold_rows = read_csv(gold_path)
    tp = fp = 0
    for row in gold_rows:
        if row.get("gold", "").strip() == "1":
            tp += 1
        elif row.get("gold", "").strip() == "0":
            fp += 1

    precision = tp / (tp + fp) if tp + fp else 0
    result = [{"method": "rej16", "tp": tp, "fp": fp, "fn": "", "precision": round(precision, 4), "recall": "", "f1": ""}]

    with summary_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["method", "tp", "fp", "fn", "precision", "recall", "f1"])
        writer.writeheader()
        writer.writerows(result)

    print(f"wrote {summary_path}")
    print(result[0])

if __name__ == "__main__":
    main()
