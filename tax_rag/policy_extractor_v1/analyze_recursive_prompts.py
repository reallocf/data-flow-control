import ast
import csv
import json
import math
import re
import statistics
from collections import deque
from pathlib import Path
from xml.etree import ElementTree as ET

import cornell_reference_audit as cx
import rej16_reference_audit as rej

base = Path(__file__).resolve().parent
input_path = base / "inputs" / "title26_sections.jsonl"
xml_path = base / "inputs" / "usc26.xml"
extractor_path = base / "extract_v1.py"
output_dir = base / "outputs"

def norm(value):
    return (
        re.sub(r"\s+", "", str(value))
        .replace("\u2013", "-")
        .replace("\u2014", "-")
        .replace("\u2212", "-")
        .replace("§", "")
    )

def prompt_parts():
    tree = ast.parse(
        extractor_path.read_text(encoding="utf-8-sig")
    )
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "generate_policy":
            for stmt in node.body:
                if (
                    isinstance(stmt, ast.Assign)
                    and any(
                        isinstance(target, ast.Name)
                        and target.id == "prompt"
                        for target in stmt.targets
                    )
                ):
                    if not isinstance(stmt.value, ast.JoinedStr):
                        raise RuntimeError("Prompt is not an f-string.")
                    return stmt.value.values
    raise RuntimeError("Prompt definition not found.")

def section_key(node):
    if (
        isinstance(node, ast.Subscript)
        and isinstance(node.value, ast.Name)
        and node.value.id == "section"
        and isinstance(node.slice, ast.Constant)
    ):
        return node.slice.value
    return None

def render_prompt(parts, section, body, context):
    out = []

    for part in parts:
        if isinstance(part, ast.Constant):
            out.append(str(part.value))
            continue

        if not isinstance(part, ast.FormattedValue):
            raise RuntimeError("Unexpected prompt element.")

        value = part.value

        if isinstance(value, ast.Name) and value.id == "context":
            out.append(context)
        elif section_key(value) == "citation":
            out.append(section["citation"])
        elif (
            isinstance(value, ast.Subscript)
            and section_key(value.value) == "text"
        ):
            out.append(body)
        else:
            raise RuntimeError(
                "Unexpected prompt expression: "
                + ast.dump(value, include_attributes=False)
            )

    return "".join(out)

def closure(graph, source):
    seen = {source}
    order = []
    queue = deque(sorted(graph[source]))

    while queue:
        target = queue.popleft()

        if target in seen:
            continue

        seen.add(target)
        order.append(target)

        queue.extend(
            item
            for item in sorted(graph[target])
            if item not in seen
        )

    return order

def percentile(values, fraction):
    values = sorted(values)
    position = (len(values) - 1) * fraction
    low = int(position)
    high = min(low + 1, len(values) - 1)
    return (
        values[low]
        + (values[high] - values[low])
        * (position - low)
    )

def main():
    rows = rej.read_jsonl(input_path)

    sections = {
        norm(rej.section_number(row)): row
        for row in rows
        if rej.section_number(row)
    }

    known = set(sections)

    bodies = {
        number: rej.section_text(row)[
            :rej.main_cut(rej.section_text(row))
        ]
        for number, row in sections.items()
    }

    predicted = set()

    for source, row in sections.items():
        for item in rej.scan(
            rej.section_text(row),
            source,
            known,
        ):
            target = norm(
                item.get("target_section", "")
            )

            if (
                item.get("status") == "resolved_section"
                and target in known
                and target != source
            ):
                predicted.add((source, target))

    old_xml_path = cx.XML_PATH
    cx.XML_PATH = xml_path
    xml_sections, raw_xml_pairs = cx.load_xml()
    cx.XML_PATH = old_xml_path

    xml_known = {
        norm(value)
        for value in xml_sections
    }

    eligible = known & xml_known

    xml_pairs = {
        (norm(source), norm(target))
        for source, target in raw_xml_pairs
        if norm(source) in eligible
        and norm(target) in eligible
        and norm(source) != norm(target)
    }

    raw_text = {}
    root = ET.parse(xml_path).getroot()

    for node in root.iter():
        if cx.local(node.tag) != "section":
            continue

        match = re.fullmatch(
            r"/us/usc/t26/s([^/]+)",
            node.attrib.get("identifier", ""),
        )

        if match:
            raw_text[
                norm(match.group(1))
            ] = cx.statute_text(node)

    typographic = set()

    for source, target in predicted - xml_pairs:
        if (
            "-" in target
            and any(
                target.replace("-", dash)
                in raw_text.get(source, "")
                for dash in (
                    "\u2013",
                    "\u2014",
                    "\u2212",
                )
            )
        ):
            typographic.add((source, target))

    accepted = (
        predicted & xml_pairs
    ) | typographic

    graph = {
        section: set()
        for section in known
    }

    for source, target in accepted:
        graph[source].add(target)

    parts = prompt_parts()
    metrics = []

    output_dir.mkdir(exist_ok=True)

    manifest_path = (
        output_dir
        / "title26_recursive_prompt_manifest.jsonl"
    )

    with manifest_path.open(
        "w",
        encoding="utf-8",
    ) as handle:
        for source in sorted(known):
            context_sections = closure(
                graph,
                source,
            )

            context = "\n\n".join(
                sections[target]["citation"]
                + "\n"
                + bodies[target]
                for target in context_sections
            )

            prompt = render_prompt(
                parts,
                sections[source],
                bodies[source],
                context,
            )

            row = {
                "source_section": source,
                "direct_refs": len(graph[source]),
                "reachable_refs": len(
                    context_sections
                ),
                "prompt_chars": len(prompt),
                "approx_tokens_4chars":
                    math.ceil(len(prompt) / 4),
            }

            metrics.append(row)

            handle.write(
                json.dumps(
                    {
                        **row,
                        "context_sections":
                            context_sections,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )

    size_path = (
        output_dir
        / "title26_recursive_prompt_sizes.csv"
    )

    with size_path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(metrics[0]),
        )
        writer.writeheader()
        writer.writerows(metrics)

    chars = [
        row["prompt_chars"]
        for row in metrics
    ]

    tokens = [
        row["approx_tokens_4chars"]
        for row in metrics
    ]

    largest = max(
        metrics,
        key=lambda row: row["prompt_chars"],
    )

    summary = {
        "input_sections": len(known),
        "predicted_internal_pairs":
            len(predicted),
        "xml_internal_pairs":
            len(xml_pairs),
        "accepted_graph_edges":
            len(accepted),
        "typographic_dash_pairs_added":
            len(typographic),
        "prompt_count":
            len(metrics),
        "average_prompt_chars":
            statistics.mean(chars),
        "maximum_prompt_chars":
            largest["prompt_chars"],
        "maximum_prompt_section":
            largest["source_section"],
        "average_approx_tokens_4chars":
            statistics.mean(tokens),
        "maximum_approx_tokens_4chars":
            largest["approx_tokens_4chars"],
        "p50_approx_tokens_4chars":
            percentile(tokens, 0.50),
        "p90_approx_tokens_4chars":
            percentile(tokens, 0.90),
        "p95_approx_tokens_4chars":
            percentile(tokens, 0.95),
        "p99_approx_tokens_4chars":
            percentile(tokens, 0.99),
        "lt_32000":
            sum(value < 32000 for value in tokens),
        "gte_1000000":
            sum(
                value >= 1000000
                for value in tokens
            ),
    }

    (
        output_dir
        / "title26_recursive_prompt_summary.json"
    ).write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )

    print(
        json.dumps(
            summary,
            indent=2,
        )
    )

if __name__ == "__main__":
    main()
