import csv
from pathlib import Path

from reference_normalization import clean, normalize_target

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
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def raw_target(row):
    return (
        clean(row.get("target_section") or row.get("target") or row.get("resolved_section")),
        clean(row.get("target_path") or row.get("path") or row.get("target_subpath")),
    )


def key(row):
    target_section, target_path = normalize_target(*raw_target(row))
    surface = clean(row.get("surface"))
    kind = clean(row.get("ref_class") or row.get("reference_class") or row.get("type"))
    return (surface.lower(), target_section, target_path, kind.lower())


def old_key(row):
    surface = clean(row.get("surface"))
    target = clean(row.get("target_section") or row.get("target") or row.get("resolved_section"))
    path = clean(row.get("target_path") or row.get("path") or row.get("target_subpath"))
    kind = clean(row.get("ref_class") or row.get("reference_class") or row.get("type"))
    return (surface.lower(), target.lower(), path.lower(), kind.lower())


def convert(row, method):
    target_section, target_path = normalize_target(*raw_target(row))
    return {
        "surface": clean(row.get("surface")),
        "target_section": target_section,
        "target_path": target_path,
        "ref_class": clean(row.get("ref_class") or row.get("reference_class") or row.get("type")),
        "span_start": clean(row.get("span_start")),
        "span_end": clean(row.get("span_end")),
        "rej16": "1" if method == "rej16" else "0",
        "llm": "1" if method == "llm" else "0",
        "gold": "",
        "note": "",
    }


def prior_labels():
    result = {}
    for row in read_csv(union_path):
        value = clean(row.get("gold"))
        note = clean(row.get("note"))
        if value:
            result.setdefault(key(row), set()).add((value, note))
            result.setdefault(old_key(row), set()).add((value, note))
    return result


def apply_prior_labels(rows):
    labels = prior_labels()
    for row in rows:
        found = labels.get(key(row)) or labels.get(old_key(row)) or set()
        values = {value for value, _ in found if value}
        notes = sorted({note for _, note in found if note})
        if len(values) == 1:
            row["gold"] = next(iter(values))
            row["note"] = "; ".join(notes)
        elif len(values) > 1:
            row["note"] = "review merged labels"


def main():
    rej16_path = first_existing(rej16_candidates)
    rows = {}

    for row in read_csv(rej16_path):
        item = convert(row, "rej16")
        rows.setdefault(key(item), item)["rej16"] = "1"

    for row in read_csv(llm_path):
        item = convert(row, "llm")
        rows.setdefault(key(item), item)["llm"] = "1"

    out = sorted(rows.values(), key=lambda row: (row["surface"].lower(), row["target_section"], row["target_path"]))
    apply_prior_labels(out)

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

    with union_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(out)

    blanks = sum(1 for row in out if not row["gold"])
    print(f"rej16 file: {rej16_path}")
    print(f"wrote {union_path} with {len(out)} rows")
    print(f"blank gold cells: {blanks}")


if __name__ == "__main__":
    main()
