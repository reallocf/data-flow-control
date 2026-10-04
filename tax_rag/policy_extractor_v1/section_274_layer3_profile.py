import csv
import json
import re
from collections import Counter, defaultdict
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

layer3 = [
    row
    for row in rows
    if row.get("layer") == "3"
    and row.get("status") == "needs_structural_resolution"
]

if len(layer3) != 153:
    raise RuntimeError(
        f"Expected 153 Layer-3 structural cases, found {len(layer3)}"
    )

known_keywords = (
    "subsection",
    "paragraph",
    "subparagraph",
    "clause",
    "subclause",
)

def marker_count(surface):
    return len(
        re.findall(
            r"\([A-Za-z0-9ivxlcdmIVXLCDM]+\)",
            surface,
        )
    )

def classify(row):
    surface = row.get("surface", "").strip()
    context = row.get("context_excerpt", "")
    low_surface = surface.lower()
    low_context = context.lower()

    markers = marker_count(surface)

    # Same shape as the Layer-2 heading-boundary failure:
    # a structurally complete reference followed by a space
    # and another parenthetical marker.
    if re.search(
        r"\)\s+\([A-Za-z0-9ivxlcdmIVXLCDM]+\)\s*$",
        surface,
    ):
        return "heading_boundary_candidate"

    # Same broad antecedent phenomenon as the validated
    # "subparagraph (A)(ii) thereof" Layer-2 case.
    if "thereof" in low_context:
        return "thereof_context_candidate"

    if markers >= 2:
        if low_surface.startswith("clause "):
            return "nested_clause_candidate"

        if low_surface.startswith("subclause "):
            return "nested_subclause_candidate"

        if low_surface.startswith("subparagraph "):
            return "nested_subparagraph_candidate"

        if low_surface.startswith("paragraph "):
            return "nested_paragraph_candidate"

        if low_surface.startswith("subsection "):
            return "nested_subsection_candidate"

        return "multiple_marker_other"

    if markers == 1:
        if any(
            low_surface.startswith(k + " ")
            for k in known_keywords
        ):
            return "single_relative_marker"

        return "single_marker_other"

    return "other"

profiled = []

for row in layer3:
    item = dict(row)

    item["profile_class"] = classify(row)

    item["surface_marker_count"] = str(
        marker_count(
            row.get("surface", "")
        )
    )

    item["known_layer2_shape"] = (
        "yes"
        if item["profile_class"] in {
            "heading_boundary_candidate",
            "thereof_context_candidate",
            "nested_clause_candidate",
            "nested_subclause_candidate",
        }
        else "no"
    )

    item["closure_status"] = "REVIEW_REQUIRED"

    profiled.append(item)

profile_counts = Counter(
    row["profile_class"]
    for row in profiled
)

candidate_state_counts = Counter(
    row.get("candidate_state", "")
    for row in profiled
)

signature_counts = Counter(
    (
        row["profile_class"],
        row.get("candidate_state", ""),
        row.get("surface", ""),
    )
    for row in profiled
)

distinct_case_keys = {
    (
        row.get("source_section", ""),
        row.get("source_path", ""),
        row.get("surface", ""),
        row.get("target_section", ""),
        row.get("target_path", ""),
    )
    for row in profiled
}

out_csv = outputs / "section_274_layer3_profile.csv"

fieldnames = list(profiled[0].keys())

with out_csv.open(
    "w",
    encoding="utf-8",
    newline="",
) as fh:
    writer = csv.DictWriter(
        fh,
        fieldnames=fieldnames,
    )
    writer.writeheader()
    writer.writerows(profiled)

examples = defaultdict(list)

for row in profiled:
    cls = row["profile_class"]

    if len(examples[cls]) >= 5:
        continue

    examples[cls].append({
        "source_section":
            row.get("source_section", ""),
        "source_path":
            row.get("source_path", ""),
        "surface":
            row.get("surface", ""),
        "candidate_state":
            row.get("candidate_state", ""),
        "candidate_path":
            row.get("candidate_path", ""),
        "target_section":
            row.get("target_section", ""),
        "target_path":
            row.get("target_path", ""),
    })

top_signatures = []

for (
    profile_class,
    candidate_state,
    surface,
), count in signature_counts.most_common(30):

    top_signatures.append({
        "count": count,
        "profile_class": profile_class,
        "candidate_state": candidate_state,
        "surface": surface,
    })

summary = {
    "layer": 3,
    "structural_occurrences": len(profiled),
    "distinct_case_keys": len(distinct_case_keys),
    "profile_counts": dict(
        sorted(profile_counts.items())
    ),
    "candidate_state_counts": dict(
        sorted(candidate_state_counts.items())
    ),
    "known_layer2_shape_occurrences": sum(
        row["known_layer2_shape"] == "yes"
        for row in profiled
    ),
    "automatically_applied": 0,
    "status": "PROFILE_ONLY_REVIEW_REQUIRED",
    "top_signatures": top_signatures,
    "examples": dict(examples),
}

out_json = outputs / "section_274_layer3_profile_summary.json"

out_json.write_text(
    json.dumps(
        summary,
        indent=2,
    ) + "\n",
    encoding="utf-8",
)

note = notes / "section_274_layer3_profile.md"

with note.open(
    "w",
    encoding="utf-8",
) as fh:

    fh.write(
        "# Section 274 Layer-3 structural profile\n\n"
    )

    fh.write(
        "This pass classifies unresolved structural "
        "references only. No new resolution is applied.\n\n"
    )

    fh.write(
        f"Structural occurrences: {len(profiled)}\n\n"
    )

    fh.write("## Profile counts\n\n")

    for cls, count in sorted(
        profile_counts.items()
    ):
        fh.write(
            f"- {cls}: {count}\n"
        )

    fh.write(
        "\n## Interpretation\n\n"
        "Cases sharing a shape with a validated Layer-2 "
        "failure are candidates for further validation, "
        "not automatic corrections. Each proposed rule "
        "must first be checked against statutory structure "
        "and source context.\n"
    )

print(f"LAYER3_STRUCTURAL={len(profiled)}")
print(
    "DISTINCT_CASE_KEYS="
    f"{len(distinct_case_keys)}"
)
print(
    "KNOWN_LAYER2_SHAPE="
    f"{summary['known_layer2_shape_occurrences']}"
)
print("AUTOMATICALLY_APPLIED=0")
print("STATUS=PROFILE_ONLY_REVIEW_REQUIRED")
print()

print("=== PROFILE COUNTS ===")

for cls, count in sorted(
    profile_counts.items(),
    key=lambda x: (-x[1], x[0]),
):
    print(f"{cls}={count}")

print()
print("=== CANDIDATE STATES ===")

for state, count in sorted(
    candidate_state_counts.items(),
    key=lambda x: (-x[1], x[0]),
):
    print(f"{state}={count}")

print()
print("=== TOP 20 SIGNATURES ===")

for item in top_signatures[:20]:
    print(
        f"{item['count']:>3} | "
        f"{item['profile_class']} | "
        f"{item['candidate_state']} | "
        f"{item['surface']}"
    )

print()
print(f"CSV={out_csv}")
print(f"SUMMARY={out_json}")
print(f"NOTE={note}")
