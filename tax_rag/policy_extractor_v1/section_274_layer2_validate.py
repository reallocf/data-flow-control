import csv
import json
import re
from collections import defaultdict
from pathlib import Path
from xml.etree import ElementTree as ET

base = Path(__file__).resolve().parent
inputs = base / "inputs"
outputs = base / "outputs"

cases_path = outputs / "section_274_layer2_closure_cases.csv"
resolution_path = outputs / "section_274_resolution_items.csv"
xml_path = inputs / "usc26.xml"
sections_path = inputs / "title26_sections.jsonl"

cases = list(
    csv.DictReader(
        cases_path.open(
            encoding="utf-8-sig",
            newline="",
        )
    )
)

resolution_rows = list(
    csv.DictReader(
        resolution_path.open(
            encoding="utf-8-sig",
            newline="",
        )
    )
)

if len(cases) != 5:
    raise RuntimeError(
        f"Expected 5 closure cases, found {len(cases)}"
    )

def observed_key(row):
    return (
        row["observed_source_section"],
        row["observed_source_path"],
        row["surface"],
        row["observed_target_section"],
        row["observed_target_path"],
    )

def resolution_key(row):
    return (
        row["source_section"],
        row["source_path"],
        row["surface"],
        row["target_section"],
        row["target_path"],
    )

matches = defaultdict(list)

for row in resolution_rows:
    if (
        row["layer"] == "2"
        and row["status"] == "needs_structural_resolution"
    ):
        matches[resolution_key(row)].append(row)

for values in matches.values():
    values.sort(
        key=lambda row: int(row["span_start"])
    )

for case in cases:
    key = observed_key(case)

    if not matches[key]:
        raise RuntimeError(
            "Could not recover span for case: "
            + repr(key)
        )

    source = matches[key].pop(0)

    case["span_start"] = source["span_start"]
    case["span_end"] = source["span_end"]

# Build authoritative XML index.
root = ET.parse(xml_path).getroot()

xml_index = {}

for node in root.iter():
    identifier = node.attrib.get(
        "identifier",
        "",
    )

    found = re.fullmatch(
        r"/us/usc/t26/s([^/]+)(/.*)?",
        identifier,
    )

    if not found:
        continue

    section = found.group(1).lower()

    path = tuple(
        value.lower()
        for value in (
            found.group(2) or ""
        ).strip("/").split("/")
        if value
    )

    xml_index[(section, path)] = identifier

# Load statutory plain text for span checks.
sections = {}

for line in sections_path.read_text(
    encoding="utf-8-sig"
).splitlines():

    if not line.strip():
        continue

    row = json.loads(line)

    citation = str(
        row.get("citation", "")
    )

    found = re.search(
        r"§\s*([0-9][0-9A-Za-z-]*)",
        citation,
    )

    if found:
        sections[found.group(1)] = str(
            row.get("text", "")
        )

validated = []

for case in cases:
    section = case[
        "expected_target_section"
    ].lower()

    path = tuple(
        value.lower()
        for value in case[
            "expected_target_path"
        ].split(".")
        if value
    )

    identifier = xml_index.get(
        (section, path)
    )

    if not identifier:
        raise RuntimeError(
            "Expected authoritative target "
            "does not exist: "
            f"{section}:{'.'.join(path)}"
        )

    source_section = case[
        "observed_source_section"
    ]

    text = sections[source_section]

    start = int(case["span_start"])
    end = int(case["span_end"])

    raw_surface = text[start:end]

    if raw_surface.strip() != case[
        "surface"
    ].strip():
        raise RuntimeError(
            "Source span mismatch: "
            f"{source_section}:{start}-{end} "
            f"{raw_surface!r} != "
            f"{case['surface']!r}"
        )

    evidence = "target_exists_in_usc26_xml"

    if (
        case["closure_class"]
        == "thereof_antecedent"
    ):
        before = text[
            max(0, start - 240):start
        ]
        after = text[
            end:min(len(text), end + 32)
        ]

        if not re.search(
            r"section\s+1\(f\)\(3\)",
            before,
            flags=re.I,
        ):
            raise RuntimeError(
                "Expected antecedent "
                "section 1(f)(3) not found."
            )

        if not re.match(
            r"\s*thereof\b",
            after,
            flags=re.I,
        ):
            raise RuntimeError(
                "Expected 'thereof' not found."
            )

        evidence = (
            "explicit_section_1_f_3_"
            "antecedent_plus_thereof"
        )

    elif (
        case["closure_class"]
        == "heading_boundary"
    ):
        bad_path = (
            "217",
            ("c", "2", "1"),
        )

        next_heading = (
            "217",
            ("d", "1"),
        )

        if bad_path in xml_index:
            raise RuntimeError(
                "Unexpected XML node "
                "217:c.2.1 exists."
            )

        if next_heading not in xml_index:
            raise RuntimeError(
                "Expected XML node "
                "217:d.1 does not exist."
            )

        evidence = (
            "217_c_2_exists_"
            "217_c_2_1_absent_"
            "217_d_1_exists"
        )

    case["authority_identifier"] = (
        identifier
    )
    case["authority_validation"] = (
        "PASS"
    )
    case["authority_evidence"] = evidence

    validated.append(case)

fieldnames = list(validated[0].keys())

with cases_path.open(
    "w",
    encoding="utf-8",
    newline="",
) as fh:
    writer = csv.DictWriter(
        fh,
        fieldnames=fieldnames,
    )
    writer.writeheader()
    writer.writerows(validated)

summary = {
    "cases": len(validated),
    "authority_validated": sum(
        row["authority_validation"] == "PASS"
        for row in validated
    ),
    "status": "AUTHORITY_VALIDATED_NOT_APPLIED",
    "targets": [
        {
            "source_section":
                row["observed_source_section"],
            "span_start":
                int(row["span_start"]),
            "span_end":
                int(row["span_end"]),
            "surface":
                row["surface"],
            "expected_target":
                (
                    row["expected_target_section"]
                    + ":"
                    + row["expected_target_path"]
                ),
            "authority_identifier":
                row["authority_identifier"],
            "closure_class":
                row["closure_class"],
        }
        for row in validated
    ],
}

summary_path = (
    outputs
    / "section_274_layer2_validation.json"
)

summary_path.write_text(
    json.dumps(
        summary,
        indent=2,
    ) + "\n",
    encoding="utf-8",
)

print(
    "AUTHORITY_VALIDATED="
    f"{summary['authority_validated']}/"
    f"{summary['cases']}"
)

print(
    "STATUS="
    + summary["status"]
)

for index, row in enumerate(
    validated,
    1,
):
    print(
        f"{index}. "
        f"{row['observed_source_section']}:"
        f"{row['span_start']}-"
        f"{row['span_end']} | "
        f"{row['closure_class']} | "
        f"{row['expected_target_section']}:"
        f"{row['expected_target_path']} | "
        f"{row['authority_validation']}"
    )

print()
print(f"CASES={cases_path}")
print(f"SUMMARY={summary_path}")
