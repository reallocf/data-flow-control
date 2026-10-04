import csv
import json
from pathlib import Path

base = Path(__file__).resolve().parent
outputs = base / "outputs"
notes = base / "notes"

src = outputs / "section_274_diag_review.csv"

rows = list(
    csv.DictReader(
        src.open(
            encoding="utf-8-sig",
            newline="",
        )
    )
)

layer2 = [
    row
    for row in rows
    if row["layer"] == "2"
    and row["status"] == "needs_structural_resolution"
]

if len(layer2) != 5:
    raise RuntimeError(
        f"Expected 5 Layer-2 structural cases, found {len(layer2)}"
    )

expectations = {
    (
        "162",
        "e.5.c",
        "clause (i)",
    ): {
        "expected_source_path": "e.5.d.iii",
        "expected_target_section": "162",
        "expected_target_path": "e.5.d.i",
        "closure_class": "consecutive_nested_markers",
        "reason": (
            "The text is inside section 162(e)(5)(D)(iii). "
            "Clause (i) refers to sibling clause (D)(i). "
            "The current hierarchy misses the (D) -> (i)/(ii)/(iii) nesting."
        ),
    },
    (
        "162",
        "m.5.d.ii",
        "clause (i)(I)",
    ): {
        "expected_source_path": "m.5.d.ii",
        "expected_target_section": "162",
        "expected_target_path": "m.5.d.i.i",
        "closure_class": "nested_subclause",
        "reason": (
            "Clause (i)(I) identifies subclause (I) inside sibling "
            "clause (i) of section 162(m)(5)(D)."
        ),
    },
    (
        "162",
        "o.3.b",
        "subparagraph (A)(ii)",
    ): {
        "expected_source_path": "o.3.b",
        "expected_target_section": "1",
        "expected_target_path": "f.3.a.ii",
        "closure_class": "thereof_antecedent",
        "reason": (
            "The phrase follows section 1(f)(3) and says "
            "'subparagraph (A)(ii) thereof'; 'thereof' points to "
            "section 1(f)(3), not to section 162."
        ),
    },
    (
        "217",
        "d",
        "subsection (c)(2) (1)",
    ): {
        "expected_source_path": "d",
        "expected_target_section": "217",
        "expected_target_path": "c.2",
        "closure_class": "heading_boundary",
        "reason": (
            "The reference is subsection (c)(2). The following (1) "
            "starts paragraph (1) under subsection (d) and must not "
            "be absorbed into the citation."
        ),
    },
}

out_rows = []

for row in layer2:
    key = (
        row["source_section"],
        row["source_path"],
        row["surface"],
    )

    if key not in expectations:
        raise RuntimeError(
            "Unexpected Layer-2 case: "
            + json.dumps(key)
        )

    exp = expectations[key]

    out_rows.append({
        "layer": row["layer"],
        "span_start": row.get("span_start", ""),
        "span_end": row.get("span_end", ""),
        "observed_source_section": row["source_section"],
        "observed_source_path": row["source_path"],
        "surface": row["surface"],
        "observed_target_section": row["target_section"],
        "observed_target_path": row["target_path"],
        "candidate_state": row["candidate_state"],
        "candidate_path": row["candidate_path"],
        "expected_source_path": exp["expected_source_path"],
        "expected_target_section": exp["expected_target_section"],
        "expected_target_path": exp["expected_target_path"],
        "closure_class": exp["closure_class"],
        "status": "ADJUDICATED_NOT_APPLIED",
        "reason": exp["reason"],
        "context_excerpt": row["context_excerpt"],
    })

out_csv = outputs / "section_274_layer2_closure_cases.csv"

with out_csv.open(
    "w",
    encoding="utf-8",
    newline="",
) as fh:
    writer = csv.DictWriter(
        fh,
        fieldnames=list(out_rows[0].keys()),
    )
    writer.writeheader()
    writer.writerows(out_rows)

distinct = {}

for row in out_rows:
    key = (
        row["closure_class"],
        row["expected_target_section"],
        row["expected_target_path"],
    )
    distinct.setdefault(key, 0)
    distinct[key] += 1

summary = {
    "layer2_cases": len(out_rows),
    "distinct_observed_patterns": len(expectations),
    "closure_classes": {},
    "status": "ADJUDICATED_NOT_APPLIED",
}

for row in out_rows:
    cls = row["closure_class"]
    summary["closure_classes"][cls] = (
        summary["closure_classes"].get(cls, 0) + 1
    )

out_json = outputs / "section_274_layer2_closure_summary.json"

out_json.write_text(
    json.dumps(
        summary,
        indent=2,
    ) + "\n",
    encoding="utf-8",
)

note = notes / "section_274_layer2_closure.md"

note.write_text(
    """# Section 274 Layer-2 structural closure

Five Layer-2 structural-resolution occurrences remain before the
bounded dependency trace can be treated as structurally closed.

The cases fall into four parser/resolution classes:

1. Consecutive nested structural markers.
2. Nested clause/subclause references.
3. A `thereof` reference whose antecedent is an explicit earlier section.
4. A heading boundary where the following paragraph marker was absorbed
   into the preceding reference.

The expected mappings in the regression fixture are adjudicated targets.
They do not modify the resolver. Resolver changes should be tested against
this fixture before the Section 274 trace is recomputed.
""",
    encoding="utf-8",
)

print("LAYER2_CASES=5")
print("STATUS=ADJUDICATED_NOT_APPLIED")
print()

for i, row in enumerate(out_rows, 1):
    print(
        f"{i}. "
        f"{row['observed_source_section']}:{row['observed_source_path']} | "
        f"{row['surface']} | "
        f"{row['closure_class']} | "
        f"observed={row['observed_target_section']}:{row['observed_target_path']} | "
        f"expected={row['expected_target_section']}:{row['expected_target_path']}"
    )

print()
print(f"CSV={out_csv}")
print(f"SUMMARY={out_json}")
print(f"NOTE={note}")
