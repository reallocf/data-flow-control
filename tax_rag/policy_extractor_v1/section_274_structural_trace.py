import csv, json, re, sys
from collections import Counter
from pathlib import Path
from xml.etree import ElementTree as ET
import tiktoken

base = Path(__file__).resolve().parent
inputs, outputs, notes = base / "inputs", base / "outputs", base / "notes"
sys.path.insert(0, str(base))
import rej16_reference_audit as rej

budget = json.loads((outputs / "section_274_budget_exact.json").read_text(encoding="utf-8"))
capacity = int(budget["legal_context_capacity"])
enc = tiktoken.get_encoding(budget["tokenizer"])

rows = rej.read_jsonl(inputs / "title26_sections.jsonl")
sections = {rej.section_number(r): r for r in rows if rej.section_number(r)}
known = set(sections)
texts = {
    k: rej.section_text(v)[:rej.main_cut(rej.section_text(v))]
    for k, v in sections.items()
}

def norm(v):
    return (
        str(v)
        .replace("\u2013", "-")
        .replace("\u2014", "-")
        .replace("\u2212", "-")
        .lower()
    )

def local(tag):
    return tag.rsplit("}", 1)[-1]

def node_text(node):
    parts = []

    def walk(x):
        if local(x.tag) in {"notes", "sourceCredit"}:
            return
        if x.text:
            parts.append(x.text)
        for child in x:
            walk(child)
            if child.tail:
                parts.append(child.tail)

    walk(node)
    return re.sub(r"\s+", " ", "".join(parts)).strip()

section_by_norm = {norm(k): v for k, v in sections.items()}
xml_root = ET.parse(inputs / "usc26.xml").getroot()
xml_index = {}

for node in xml_root.iter():
    ident = node.attrib.get("identifier", "")
    m = re.fullmatch(r"/us/usc/t26/s([^/]+)(/.*)?", ident)

    if m:
        path = tuple(
            norm(x)
            for x in (m.group(2) or "").strip("/").split("/")
            if x
        )
        xml_index[(norm(m.group(1)), path)] = (ident, node)

def map_unit(section, path):
    section = norm(section)
    requested = tuple(norm(x) for x in path)
    current = requested

    while True:
        hit = xml_index.get((section, current))

        if hit:
            return {
                "section": section,
                "path": current,
                "identifier": hit[0],
                "node": hit[1],
                "mapping": "exact" if current == requested else "ancestor",
            }

        if not current:
            return None

        current = current[:-1]

def contains(a, b):
    return (
        a["section"] == b["section"]
        and len(a["path"]) <= len(b["path"])
        and b["path"][:len(a["path"])] == a["path"]
    )

def merge_units(items):
    values = list({
        (x["section"], x["path"]): x
        for x in items
    }.values())

    return sorted(
        [
            x for x in values
            if not any(
                y is not x and contains(y, x)
                for y in values
            )
        ],
        key=lambda x: (x["section"], x["path"]),
    )

scan_cache = {}

def scan_section(section_norm):
    exact = next(
        (k for k in sections if norm(k) == section_norm),
        None,
    )

    if exact is None:
        return []

    if exact not in scan_cache:
        tree = rej.build_tree(texts[exact], exact)
        found = []

        for row in rej.scan(texts[exact], exact, known):
            item = dict(row)
            item["source_path"] = tuple(
                norm(x)
                for x in rej.deepest(
                    tree,
                    int(row["span_start"]),
                )["path"]
            )
            found.append(item)

        scan_cache[exact] = found

    return scan_cache[exact]

def target_paths(row):
    raw = str(row.get("target_path", "")).strip()

    if not raw:
        return [()]

    if "|" in raw:
        return []

    return [
        tuple(
            norm(x)
            for x in part.strip().split(".")
            if x
        )
        for part in raw.split(",")
        if part.strip()
    ]

def expand(frontier, layer):
    candidates = []
    diagnostics = []
    seen = set()

    for unit in frontier:
        for row in scan_section(unit["section"]):
            path = row["source_path"]

            if (
                len(unit["path"]) > len(path)
                or path[:len(unit["path"])] != unit["path"]
            ):
                continue

            key = (
                row["source_section"],
                row["span_start"],
                row["span_end"],
                row["surface"],
            )

            if key in seen:
                continue

            seen.add(key)

            if row["status"] in {
                "resolved_section",
                "resolved_local",
            }:
                for path in target_paths(row):
                    mapped = map_unit(
                        row["target_section"],
                        path,
                    )

                    if mapped:
                        candidates.append(mapped)
                    else:
                        diagnostics.append({
                            "layer": layer,
                            "status": "mapping_missing",
                            **row,
                        })

            elif row["status"] in {
                "needs_structural_resolution",
                "unresolved_missing_section",
                "resolved_external",
            }:
                diagnostics.append({
                    "layer": layer,
                    **row,
                })

    return merge_units(candidates), diagnostics

def context_tokens(items):
    chunks = []

    for item in items:
        row = section_by_norm[item["section"]]

        chunks.append(
            row.get("citation", "")
            + " "
            + item["identifier"]
            + "\n"
            + node_text(item["node"])
        )

    return len(
        enc.encode("\n\n".join(chunks))
    )

root = map_unit("274", ())
coverage = [root]
admitted = []
frontier = [root]

layer_rows = []
diagnostic_rows = []

last_fit = 0
first_over = None

for layer in range(1, 20):
    candidates, diagnostics = expand(
        frontier,
        layer,
    )

    diagnostic_rows.extend(diagnostics)

    candidates = [
        x for x in candidates
        if not any(
            contains(old, x)
            for old in coverage
        )
    ]

    before = {
        (x["section"], x["path"])
        for x in admitted
    }

    merged = merge_units(
        admitted + candidates
    )

    next_frontier = [
        x for x in merged
        if (x["section"], x["path"])
        not in before
    ]

    tok = context_tokens(merged)
    fits = tok <= capacity

    maps = Counter(
        x["mapping"]
        for x in candidates
    )

    diags = Counter(
        x["status"]
        for x in diagnostics
    )

    layer_rows.append({
        "layer": layer,
        "new_units": len(next_frontier),
        "new_sections": len({
            x["section"]
            for x in next_frontier
        }),
        "cumulative_units": len(merged),
        "exact_mappings":
            maps.get("exact", 0),
        "ancestor_mappings":
            maps.get("ancestor", 0),
        "context_tokens": tok,
        "legal_context_capacity":
            capacity,
        "fits": str(fits).lower(),
        "resolved_external":
            diags.get(
                "resolved_external",
                0,
            ),
        "needs_structural_resolution":
            diags.get(
                "needs_structural_resolution",
                0,
            ),
        "unresolved_missing_section":
            diags.get(
                "unresolved_missing_section",
                0,
            ),
        "mapping_missing":
            diags.get(
                "mapping_missing",
                0,
            ),
    })

    if fits:
        last_fit = layer
    elif first_over is None:
        first_over = layer

    admitted = merged
    coverage = merge_units(
        [root] + merged
    )
    frontier = next_frontier

    if not frontier or first_over is not None:
        break

target_diagnostics = [
    x for x in diagnostic_rows
    if int(x["layer"]) == 1
]

status = (
    "ADDITIONAL_RESOLUTION_REQUIRED"
    if target_diagnostics
    else "READY_FOR_BOUNDED_EXPANSION"
)

summary = {
    "baseline_status": status,
    "target_diagnostic_count":
        len(target_diagnostics),
    "target_external_count":
        sum(
            x["status"] == "resolved_external"
            for x in target_diagnostics
        ),
    "legal_context_capacity":
        capacity,
    "conditional_last_complete_structural_layer":
        last_fit,
    "conditional_first_structural_layer_beyond_capacity":
        first_over,
    "layers":
        layer_rows,
}

layer_csv = (
    outputs
    / "section_274_structural_layers.csv"
)

with layer_csv.open(
    "w",
    encoding="utf-8",
    newline="",
) as h:
    w = csv.DictWriter(
        h,
        fieldnames=list(layer_rows[0]),
    )
    w.writeheader()
    w.writerows(layer_rows)

diag_csv = (
    outputs
    / "section_274_resolution_items.csv"
)

fields = [
    "layer",
    "status",
    "source_section",
    "source_path",
    "span_start",
    "span_end",
    "surface",
    "target_section",
    "target_path",
]

with diag_csv.open(
    "w",
    encoding="utf-8",
    newline="",
) as h:
    w = csv.DictWriter(
        h,
        fieldnames=fields,
    )
    w.writeheader()

    for row in diagnostic_rows:
        w.writerow({
            "layer":
                row.get("layer", ""),
            "status":
                row.get("status", ""),
            "source_section":
                row.get(
                    "source_section",
                    "",
                ),
            "source_path":
                ".".join(
                    row.get(
                        "source_path",
                        (),
                    )
                ),
            "span_start":
                row.get(
                    "span_start",
                    "",
                ),
            "span_end":
                row.get(
                    "span_end",
                    "",
                ),
            "surface":
                row.get(
                    "surface",
                    "",
                ),
            "target_section":
                row.get(
                    "target_section",
                    "",
                ),
            "target_path":
                row.get(
                    "target_path",
                    "",
                ),
        })

summary_path = (
    outputs
    / "section_274_structural_trace.json"
)

summary_path.write_text(
    json.dumps(
        summary,
        indent=2,
    ),
    encoding="utf-8",
)

note_path = (
    notes
    / "section_274_structural_trace.md"
)

text = [
    "# Section 274 structural context trace",
    "",
    "## Baseline status",
    "",
    f"Status: {status}.",
    f"Target diagnostic items: {len(target_diagnostics)}.",
    f"Target external references: {summary['target_external_count']}.",
    "",
    "The capacity table below is a conditional tax-code measurement. Diagnostic items remain on the additional-resolution branch.",
    "",
    "## Structural layers",
    "",
    "| Layer | New units | Cumulative units | Context tokens | Fits | External | Structural resolution | Missing section |",
    "| ---: | ---: | ---: | ---: | :--- | ---: | ---: | ---: |",
]

for row in layer_rows:
    text.append(
        f"| {row['layer']} "
        f"| {row['new_units']} "
        f"| {row['cumulative_units']} "
        f"| {row['context_tokens']:,} "
        f"| {row['fits']} "
        f"| {row['resolved_external']} "
        f"| {row['needs_structural_resolution']} "
        f"| {row['unresolved_missing_section']} |"
    )

text += [
    "",
    "## Capacity result",
    "",
    f"Conditional last complete structural layer within capacity: {last_fit}.",
    f"Conditional first structural layer beyond capacity: {first_over}.",
    "",
    "The earlier whole-section comparator is conservative. The stopping point should come from the structural trace after mapping and containment removal.",
]

note_path.write_text(
    "\n".join(text) + "\n",
    encoding="utf-8",
)

print(
    json.dumps(
        summary,
        indent=2,
    )
)

print(layer_csv)
print(diag_csv)
print(summary_path)
print(note_path)