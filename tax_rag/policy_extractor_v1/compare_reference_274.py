import csv
import re
from pathlib import Path

base = Path(__file__).resolve().parent

llm_path = base / "outputs" / "llm_reference_audit_274.csv"
rej16_candidates = [
    base / "outputs" / "rej16_reference_audit.csv",
    base / "outputs" / "reference_audit_274_main.csv",
    base / "outputs" / "reference_audit_274.csv",
]
union_path = base / "outputs" / "reference_union_gold_274.csv"


def first_existing(paths):
    for path in paths:
        if path.exists():
            return path
    raise RuntimeError("no rej16 output file found")


def read_csv(path):
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def clean(value):
    return re.sub(r"\s+", " ", str(value or "").strip())


def key(row):
    surface = clean(row.get("surface"))
    target = clean(row.get("target_section") or row.get("target") or row.get("resolved_section"))
    path = clean(row.get("target_path") or row.get("path") or row.get("target_subpath"))
    kind = clean(row.get("ref_class") or row.get("reference_class") or row.get("type"))
    return (surface.lower(), target.lower(), path.lower(), kind.lower())


def convert(row, method):
    return {
        "surface": clean(row.get("surface")),
        "target_section": clean(row.get("target_section") or row.get("target") or row.get("resolved_section")),
        "target_path": clean(row.get("target_path") or row.get("path") or row.get("target_subpath")),
        "ref_class": clean(row.get("ref_class") or row.get("reference_class") or row.get("type")),
        "span_start": clean(row.get("span_start")),
        "span_end": clean(row.get("span_end")),
        "rej16": "1" if method == "rej16" else "0",
        "llm": "1" if method == "llm" else "0",
        "gold": "",
        "note": "",
    }


def main():
    rej16_path = first_existing(rej16_candidates)

    rows = {}

    for row in read_csv(rej16_path):
        item = convert(row, "rej16")
        rows.setdefault(key(item), item)["rej16"] = "1"

    for row in read_csv(llm_path):
        item = convert(row, "llm")
        rows.setdefault(key(item), item)["llm"] = "1"

    fields = [
        "surface",
        "target_section",
        "target_path",
        "ref_class",
        "span_start",
        "span_end",
        "rej16",
        "llm",
        "gold",
        "note",
    ]

    out = sorted(rows.values(), key=lambda row: (row["surface"].lower(), row["target_section"], row["target_path"]))

    with union_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(out)

    print(f"rej16 file: {rej16_path}")
    print(f"wrote {union_path} with {len(out)} rows")


if __name__ == "__main__":
    main()
