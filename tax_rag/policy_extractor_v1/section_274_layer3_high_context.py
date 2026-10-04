import csv
import json
import re
from collections import defaultdict
from pathlib import Path
from xml.etree import ElementTree as ET

base = Path(__file__).resolve().parent
inputs = base / "inputs"
outputs = base / "outputs"

audit_path = (
    outputs
    / "section_274_layer3_nested_clause_audit.csv"
)

resolution_path = (
    outputs
    / "section_274_resolution_items.csv"
)

sections_path = inputs / "title26_sections.jsonl"
xml_path = inputs / "usc26.xml"

for path in (
    audit_path,
    resolution_path,
    sections_path,
    xml_path,
):
    if not path.exists():
        raise FileNotFoundError(path)

audit_rows = list(
    csv.DictReader(
        audit_path.open(
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

high = [
    row
    for row in audit_rows
    if row.get("review_bucket")
    == "HIGH_CONFIDENCE_STRUCTURAL_CANDIDATE"
]

if len(high) != 20:
    raise RuntimeError(
        f"Expected 20 HIGH cases, found {len(high)}"
    )

key_fields = (
    "layer",
    "status",
    "source_section",
    "source_path",
    "surface",
    "target_section",
    "target_path",
)

def make_key(row):
    return tuple(
        str(row.get(field, ""))
        for field in key_fields
    )

# Recover original occurrence spans.
span_index = defaultdict(list)

for row in resolution_rows:
    if (
        row.get("layer") == "3"
        and row.get("status")
        == "needs_structural_resolution"
    ):
        span_index[make_key(row)].append(
            (
                int(row["span_start"]),
                int(row["span_end"]),
            )
        )

for values in span_index.values():
    values.sort()

span_used = defaultdict(int)

for row in high:
    key = make_key(row)
    values = span_index.get(key, [])
    index = span_used[key]

    if index >= len(values):
        raise RuntimeError(
            "No unused source span for: "
            + repr(key)
        )

    start, end = values[index]
    span_used[key] += 1

    row["span_start"] = str(start)
    row["span_end"] = str(end)

# Load the statutory section text.
sections = {}

for line in sections_path.read_text(
    encoding="utf-8-sig"
).splitlines():

    if not line.strip():
        continue

    item = json.loads(line)

    citation = str(
        item.get("citation", "")
    )

    match = re.search(
        r"§\s*([0-9][0-9A-Za-z-]*)",
        citation,
    )

    if match:
        sections[
            match.group(1).lower()
        ] = str(
            item.get("text", "")
        )

# Build authoritative XML path index.
root = ET.parse(xml_path).getroot()

xml_index = {}

for node in root.iter():
    identifier = node.attrib.get(
        "identifier",
        "",
    )

    match = re.fullmatch(
        r"/us/usc/t26/s([^/]+)(/.*)?",
        identifier,
    )

    if not match:
        continue

    section = match.group(1).lower()

    path = tuple(
        value.lower()
        for value in (
            match.group(2) or ""
        ).strip("/").split("/")
        if value
    )

    xml_index[(section, path)] = (
        identifier,
        node,
    )

def clean(value):
    return re.sub(
        r"\s+",
        " ",
        value,
    ).strip()

packet = []
surface_pass = 0
target_pass = 0

for row in high:
    source_section = (
        row["source_section"].lower()
    )

    if source_section not in sections:
        raise RuntimeError(
            "Source section text missing: "
            + row["source_section"]
        )

    source_text = sections[source_section]

    start = int(row["span_start"])
    end = int(row["span_end"])

    hit = source_text[start:end]

    if hit != row["surface"]:
        raise RuntimeError(
            "Surface/span mismatch for "
            f"{row['source_section']}:"
            f"{row['source_path']} "
            f"{start}-{end}: "
            f"{hit!r} != {row['surface']!r}"
        )

    surface_pass += 1

    target_section = (
        row["proposed_target_section"]
        .lower()
    )

    target_path = tuple(
        value.lower()
        for value in
        row["proposed_target_path"].split(".")
        if value
    )

    authority = xml_index.get(
        (
            target_section,
            target_path,
        )
    )

    if authority is None:
        raise RuntimeError(
            "Proposed XML target missing: "
            f"{target_section}:"
            f"{'.'.join(target_path)}"
        )

    target_pass += 1

    identifier, node = authority

    before = source_text[
        max(0, start - 500):start
    ]

    after = source_text[
        end:min(
            len(source_text),
            end + 500,
        )
    ]

    target_text = clean(
        " ".join(node.itertext())
    )

    packet.append({
        "source_section":
            row["source_section"],
        "source_path":
            row["source_path"],
        "span_start":
            start,
        "span_end":
            end,
        "surface":
            row["surface"],
        "old_target":
            (
                row["target_section"]
                + ":"
                + row["target_path"]
            ),
        "proposed_target":
            (
                row["proposed_target_section"]
                + ":"
                + row["proposed_target_path"]
            ),
        "nearest_common_prefix":
            int(
                row[
                    "nearest_common_prefix"
                ]
            ),
        "xml_suffix_match_count":
            int(
                row[
                    "xml_suffix_match_count"
                ]
            ),
        "source_context_before":
            clean(before),
        "source_reference_text":
            hit,
        "source_context_after":
            clean(after),
        "target_identifier":
            identifier,
        "target_text":
            target_text[:900],
        "decision":
            "UNREVIEWED",
    })

txt_path = (
    outputs
    / "section_274_layer3_high_context.txt"
)

json_path = (
    outputs
    / "section_274_layer3_high_context.json"
)

json_path.write_text(
    json.dumps(
        packet,
        indent=2,
        ensure_ascii=False,
    ) + "\n",
    encoding="utf-8",
)

with txt_path.open(
    "w",
    encoding="utf-8",
) as fh:

    fh.write(
        "SECTION 274 - LAYER 3 "
        "NESTED-CLAUSE CONTEXT AUDIT\n"
    )

    fh.write(
        "No proposed correction in this "
        "report has been applied.\n\n"
    )

    for index, item in enumerate(
        packet,
        1,
    ):
        fh.write("=" * 78 + "\n")
        fh.write(
            f"CASE {index}\n"
        )
        fh.write(
            "SOURCE: "
            f"{item['source_section']}:"
            f"{item['source_path']}\n"
        )
        fh.write(
            "SPAN: "
            f"{item['span_start']}-"
            f"{item['span_end']}\n"
        )
        fh.write(
            f"SURFACE: "
            f"{item['surface']}\n"
        )
        fh.write(
            f"OLD TARGET: "
            f"{item['old_target']}\n"
        )
        fh.write(
            f"PROPOSED TARGET: "
            f"{item['proposed_target']}\n"
        )
        fh.write(
            "COMMON PREFIX: "
            f"{item['nearest_common_prefix']}\n"
        )
        fh.write(
            "XML SUFFIX MATCHES: "
            f"{item['xml_suffix_match_count']}\n\n"
        )

        fh.write("SOURCE CONTEXT:\n")
        fh.write(
            item["source_context_before"]
            + " >>> "
            + item["source_reference_text"]
            + " <<< "
            + item["source_context_after"]
            + "\n\n"
        )

        fh.write("PROPOSED TARGET:\n")
        fh.write(
            item["target_identifier"]
            + "\n"
        )
        fh.write(
            item["target_text"]
            + "\n\n"
        )

print(
    f"HIGH_OCCURRENCES={len(high)}"
)
print(
    f"MATCHED_SPANS={len(packet)}"
)
print(
    f"SURFACE_CHECK={surface_pass}/20"
)
print(
    f"TARGET_XML_CHECK={target_pass}/20"
)
print("APPLIED=0")
print(f"TXT={txt_path}")
print(f"JSON={json_path}")
