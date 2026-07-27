import csv
from pathlib import Path
base = Path(__file__).resolve().parent
gold_path = base / "outputs" / "reference_fact_274.csv"
pred_path = base / "outputs" / "rej16_reference_audit.csv"
out_path = base / "outputs" / "reference_eval_exact_274.csv"
def read(path):
    with path.open(newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))
def norm(value):
    return str(value or "").strip().lower()
gold = read(gold_path)
pred = read(pred_path)
gold_map = {
    (row["span_start"], row["span_end"]): row
    for row in gold
    if row.get("span_start") and row.get("span_end")
}
pred_map = {
    (row["span_start"], row["span_end"]): row
    for row in pred
}
exact = 0
wrong = 0
missing = 0
for key, g in gold_map.items():
    p = pred_map.get(key)
    if p is None:
        missing += 1
        continue
    fields = ["surface", "target_section", "target_path"]
    if all(norm(g[x]) == norm(p[x]) for x in fields):
        exact += 1
    else:
        wrong += 1
extra = sum(key not in gold_map for key in pred_map)
result = {
    "method": "rej16_exact",
    "gold": len(gold_map),
    "pred": len(pred_map),
    "exact": exact,
    "wrong": wrong,
    "missing": missing,
    "extra": extra,
}
with out_path.open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=result.keys())
    writer.writeheader()
    writer.writerow(result)
print(result)
