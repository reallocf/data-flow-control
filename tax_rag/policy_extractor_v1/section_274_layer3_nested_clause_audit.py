import csv
import json
import re
from collections import Counter
from pathlib import Path
from xml.etree import ElementTree as ET

base = Path(__file__).resolve().parent
inputs = base / "inputs"
outputs = base / "outputs"

profile_path = outputs / "section_274_layer3_profile.csv"
xml_path = inputs / "usc26.xml"

rows = list(
    csv.DictReader(
        profile_path.open(
            encoding="utf-8-sig",
            newline="",
        )
    )
)

cases = [
    row
    for row in rows
    if row.get("profile_class")
    == "nested_clause_candidate"
]

if len(cases) != 29:
    raise RuntimeError(
        f"Expected 29 nested-clause cases, found {len(cases)}"
    )

def norm_token(value):
    return value.strip().lower()

def marker_tokens(surface):
    return tuple(
        norm_token(value)
        for value in re.findall(
            r"\(([A-Za-z0-9ivxlcdmIVXLCDM]+)\)",
            surface,
        )
    )

def path_tuple(value):
    return tuple(
        norm_token(part)
        for part in value.split(".")
        if part.strip()
    )

def common_prefix_len(a, b):
    count = 0

    for left, right in zip(a, b):
        if left != right:
            break
        count += 1

    return count

# Build authoritative Title 26 path index.
root = ET.parse(xml_path).getroot()

paths_by_section = {}

for node in root.iter():
    identifier = node.attrib.get("identifier", "")

    found = re.fullmatch(
        r"/us/usc/t26/s([^/]+)(/.*)?",
        identifier,
    )

    if not found:
        continue

    section = norm_token(found.group(1))

    path = tuple(
        norm_token(part)
        for part in (
            found.group(2) or ""
        ).strip("/").split("/")
        if part
    )

    paths_by_section.setdefault(
        section,
        set(),
    ).add(path)

audited = []

for row in cases:
    source_section = norm_token(
        row.get("source_section", "")
    )

    target_section = norm_token(
        row.get("target_section", "")
        or source_section
    )

    source_path = path_tuple(
        row.get("source_path", "")
    )

    suffix = marker_tokens(
        row.get("surface", "")
    )

    section_paths = paths_by_section.get(
        target_section,
        set(),
    )

    suffix_matches = [
        path
        for path in section_paths
        if suffix
        and len(path) >= len(suffix)
        and path[-len(suffix):] == suffix
    ]

    ranked = []

    for path in suffix_matches:
        lcp = common_prefix_len(
            source_path,
            path,
        )

        ranked.append(
            (
                lcp,
                abs(len(path) - len(source_path)),
                path,
            )
        )

    ranked.sort(
        key=lambda item: (
            -item[0],
            item[1],
            item[2],
        )
    )

    best_lcp = (
        ranked[0][0]
        if ranked
        else -1
    )

    best = [
        item
        for item in ranked
        if item[0] == best_lcp
    ]

    unique_nearest = (
        len(best) == 1
        and best_lcp >= 0
    )

    proposed = (
        ".".join(best[0][2])
        if unique_nearest
        else ""
    )

    if (
        unique_nearest
        and best_lcp >= 2
    ):
        review_bucket = (
            "HIGH_CONFIDENCE_STRUCTURAL_CANDIDATE"
        )
    elif (
        unique_nearest
        and best_lcp >= 1
    ):
        review_bucket = (
            "MEDIUM_CONFIDENCE_STRUCTURAL_CANDIDATE"
        )
    elif unique_nearest:
        review_bucket = (
            "LOW_CONFIDENCE_STRUCTURAL_CANDIDATE"
        )
    else:
        review_bucket = "AMBIGUOUS"

    item = dict(row)

    item.update({
        "marker_suffix":
            ".".join(suffix),
        "xml_suffix_match_count":
            str(len(suffix_matches)),
        "nearest_common_prefix":
            str(best_lcp),
        "nearest_tie_count":
            str(len(best)),
        "proposed_target_section":
            target_section
            if unique_nearest else "",
        "proposed_target_path":
            proposed,
        "review_bucket":
            review_bucket,
        "applied":
            "no",
    })

    audited.append(item)

out_csv = (
    outputs
    / "section_274_layer3_nested_clause_audit.csv"
)

with out_csv.open(
    "w",
    encoding="utf-8",
    newline="",
) as fh:
    writer = csv.DictWriter(
        fh,
        fieldnames=list(audited[0].keys()),
    )
    writer.writeheader()
    writer.writerows(audited)

bucket_counts = Counter(
    row["review_bucket"]
    for row in audited
)

proposals = Counter(
    (
        row["surface"],
        row["proposed_target_path"],
        row["review_bucket"],
    )
    for row in audited
)

summary = {
    "cases": len(audited),
    "automatically_applied": 0,
    "bucket_counts": dict(
        sorted(bucket_counts.items())
    ),
    "unique_structural_candidate": sum(
        row["proposed_target_path"] != ""
        for row in audited
    ),
    "status": "AUDIT_ONLY_NOT_APPLIED",
}

summary_path = (
    outputs
    / "section_274_layer3_nested_clause_summary.json"
)

summary_path.write_text(
    json.dumps(
        summary,
        indent=2,
    ) + "\n",
    encoding="utf-8",
)

print(f"NESTED_CLAUSE_CASES={len(audited)}")
print(
    "UNIQUE_STRUCTURAL_CANDIDATE="
    f"{summary['unique_structural_candidate']}"
)
print("AUTOMATICALLY_APPLIED=0")
print("STATUS=AUDIT_ONLY_NOT_APPLIED")
print()

print("=== BUCKET COUNTS ===")

for key, value in sorted(
    bucket_counts.items(),
    key=lambda item: (-item[1], item[0]),
):
    print(f"{key}={value}")

print()
print("=== CANDIDATES ===")

for row in audited:
    print(
        f"{row['source_section']}:"
        f"{row['source_path']} | "
        f"{row['surface']} | "
        f"old={row['target_section']}:"
        f"{row['target_path']} | "
        f"proposal="
        f"{row['proposed_target_section']}:"
        f"{row['proposed_target_path']} | "
        f"LCP={row['nearest_common_prefix']} | "
        f"matches={row['xml_suffix_match_count']} | "
        f"{row['review_bucket']}"
    )

print()
print(f"CSV={out_csv}")
print(f"SUMMARY={summary_path}")
