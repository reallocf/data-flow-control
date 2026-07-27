import csv
import os
import re
from pathlib import Path

from reference_normalization import normalize_target

base = Path(__file__).resolve().parent
section = os.getenv("SECTION", "212").strip()

llm_path = base / "outputs" / f"llm_reference_audit_{section}.csv"
rej16_path = base / "outputs" / f"rej16_reference_audit_{section}.csv"
union_path = base / "outputs" / f"reference_union_gold_{section}.csv"


def tidy(value):
    return re.sub(r"\s+", " ", str(value or "").strip())


def read_csv(path):
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def raw_target(row):
    return (
        tidy(row.get("target_section") or row.get("target") or row.get("resolved_section")),
        tidy(row.get("target_path") or row.get("path") or row.get("target_subpath")),
    )


def key(row):
    target_section, target_path = normalize_target(*raw_target(row))
    surface = tidy(row.get("surface"))
    kind = tidy(row.get("ref_class") or row.get("reference_class") or row.get("type"))
    return (surface.lower(), target_section, target_path, kind.lower())


def old_key(row):
    surface = tidy(row.get("surface"))
    target = tidy(row.get("target_section") or row.get("target") or row.get("resolved_section"))
    path = tidy(row.get("target_path") or row.get("path") or row.get("target_subpath"))
    kind = tidy(row.get("ref_class") or row.get("reference_class") or row.get("type"))
    return (surface.lower(), target.lower(), path.lower(), kind.lower())


def convert(row, method):
    target_section, target_path = normalize_target(*raw_target(row))
    return {
        "surface": tidy(row.get("surface")),
        "target_section": target_section,
        "target_path": target_path,
        "ref_class": tidy(row.get("ref_class") or row.get("reference_class") or row.get("type")),
        "span_start": tidy(row.get("span_start")),
        "span_end": tidy(row.get("span_end")),
        "rej16": "1" if method == "rej16" else "0",
        "llm": "1" if method == "llm" else "0",
        "gold": "",
        "note": "",
    }


def prior_labels():
    result = {}
    for row in read_csv(union_path):
        value = tidy(row.get("gold"))
        note = tidy(row.get("note"))
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
