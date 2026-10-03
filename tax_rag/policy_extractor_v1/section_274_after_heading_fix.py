import csv
import json
import zipfile
from pathlib import Path

base = Path(__file__).resolve().parent
outputs = base / "outputs"
notes = base / "notes"

cross = json.loads(
    (
        outputs
        / "section_274_cross_title_summary.json"
    ).read_text(
        encoding="utf-8"
    )
)

extra_tokens = int(
    cross["cross_title_tokens"]
)

rows = list(
    csv.DictReader(
        (
            outputs
            / "section_274_structural_layers.csv"
        ).open(
            encoding="utf-8-sig",
            newline="",
        )
    )
)

combined = []
last_fit = 0
first_over = None

for row in rows:
    layer = int(row["layer"])
    tax_tokens = int(
        row["context_tokens"]
    )
    capacity = int(
        row["legal_context_capacity"]
    )
    total = tax_tokens + extra_tokens
    fits = total <= capacity

    combined.append({
        "layer": layer,
        "new_units":
            int(row["new_units"]),
        "cumulative_units":
            int(row["cumulative_units"]),
        "tax_code_tokens":
            tax_tokens,
        "cross_code_tokens":
            extra_tokens,
        "combined_tokens":
            total,
        "capacity":
            capacity,
        "fits":
            str(fits).lower(),
        "external_items":
            int(row["resolved_external"]),
        "structural_items":
            int(row["needs_structural_resolution"]),
        "missing_sections":
            int(row["unresolved_missing_section"]),
    })

    if fits:
        last_fit = layer
    elif first_over is None:
        first_over = layer

csv_path = (
    outputs
    / "section_274_capacity_after_heading_fix.csv"
)

with csv_path.open(
    "w",
    encoding="utf-8",
    newline="",
) as fh:
    writer = csv.DictWriter(
        fh,
        fieldnames=list(
            combined[0]
        ),
    )
    writer.writeheader()
    writer.writerows(combined)

summary = {
    "cross_code_tokens":
        extra_tokens,
    "last_complete_layer":
        last_fit,
    "first_layer_beyond_capacity":
        first_over,
    "layers":
        combined,
}

summary_path = (
    outputs
    / "section_274_after_heading_fix.json"
)

summary_path.write_text(
    json.dumps(
        summary,
        indent=2,
    ),
    encoding="utf-8",
)

note = [
    "# Section 274 hierarchy-filter check",
    "",
    f"Cross-code context: {extra_tokens:,} tokens.",
    f"Last complete layer within capacity: {last_fit}.",
    f"First complete layer beyond capacity: {first_over}.",
    "",
    "The hierarchy filter was checked against the XML structure for sections 267 and 274 before replacement.",
    "",
    "The XML comparison serves only as an audit benchmark. The resolver still derives its hierarchy from plain statutory text.",
    "",
    "Later-layer diagnostic counts remain relevant when interpreting the capacity trace.",
]

note_path = (
    notes
    / "section_274_after_heading_fix.md"
)

note_path.write_text(
    "\n".join(note) + "\n",
    encoding="utf-8",
)

pack = (
    base
    / "section_274_heading_check.zip"
)

if pack.exists():
    pack.unlink()

items = [
    outputs / "heading_filter_check.json",
    outputs / "section_274_structural_layers.csv",
    outputs / "section_274_resolution_items.csv",
    outputs / "section_274_structural_trace.json",
    outputs / "section_274_capacity_after_heading_fix.csv",
    outputs / "section_274_after_heading_fix.json",
    notes / "section_274_after_heading_fix.md",
]

with zipfile.ZipFile(
    pack,
    "w",
    zipfile.ZIP_DEFLATED,
) as archive:
    for path in items:
        archive.write(
            path,
            arcname=path.name,
        )

print(
    json.dumps(
        summary,
        indent=2,
    )
)

print(pack)