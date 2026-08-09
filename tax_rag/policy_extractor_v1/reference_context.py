import argparse
import json
import re
from collections import deque
from pathlib import Path
from xml.etree import ElementTree as ET

BASE = Path(__file__).resolve().parent
SECTIONS_PATH = BASE / "inputs" / "title26_sections.jsonl"
REFS_PATH = BASE / "inputs" / "cornell_title26_refs.jsonl"
XML_PATH = BASE / "inputs" / "usc26.xml"

BLOCKED_TAGS = {"notes", "sourceCredit"}

CONTEXT_PATHS = {
    "274": [
        "/us/usc/t26/s74",
        "/us/usc/t26/s143/k/2/B",
        "/us/usc/t26/s162/a",
        "/us/usc/t26/s212",
        "/us/usc/t26/s267/b",
        "/us/usc/t26/s267/c/4",
        "/us/usc/t26/s280F/d/4",
        "/us/usc/t26/s414/q",
    ],
}


def load_sections():
    sections = {}

    for line in SECTIONS_PATH.read_text(
        encoding="utf-8-sig"
    ).splitlines():
        if not line.strip():
            continue

        row = json.loads(line)
        ident = str(row.get("id", ""))

        if ident.startswith("26usc_"):
            sections[ident.removeprefix("26usc_")] = row

    return sections


def load_graph():
    latest = {}

    for line in REFS_PATH.read_text(
        encoding="utf-8-sig"
    ).splitlines():
        if not line.strip():
            continue

        row = json.loads(line)
        source = str(row.get("source_section", ""))

        if source:
            latest[source] = row

    graph = {}

    for source, row in latest.items():
        if row.get("status") != "ok":
            continue

        graph[source] = sorted({
            str(target)
            for target in row.get("targets", [])
            if str(target) != source
        })

    return graph


def expand(graph, source, max_hops=1):
    seen = {source}
    queue = deque([(source, 0)])
    result = []

    while queue:
        current, depth = queue.popleft()

        if depth >= max_hops:
            continue

        for target in graph.get(current, []):
            if target in seen:
                continue

            seen.add(target)
            result.append({
                "source": current,
                "target": target,
                "hop": depth + 1,
            })
            queue.append((target, depth + 1))

    return result


def build_context(sections, edges):
    parts = []

    for edge in edges:
        target = edge["target"]
        section = sections.get(target)

        if not section:
            continue

        parts.append({
            "section": target,
            "hop": edge["hop"],
            "citation": section.get("citation", ""),
            "text": section.get("text", ""),
        })

    return parts


def local(tag):
    return tag.rsplit("}", 1)[-1]


def node_text(node):
    pieces = []

    def walk(item):
        if local(item.tag) in BLOCKED_TAGS:
            return

        if item.text:
            pieces.append(item.text)

        for child in item:
            walk(child)

            if child.tail:
                pieces.append(child.tail)

    walk(node)

    return re.sub(
        r"\s+",
        " ",
        "".join(pieces),
    ).strip()


def identifier_target(identifier):
    match = re.match(
        r"/us/usc/t26/s([^/]+)",
        identifier,
    )
    return match.group(1) if match else ""


def scoped_context(source, sections, graph):
    wanted = CONTEXT_PATHS.get(source, [])

    if not wanted:
        return []

    allowed = set(graph.get(source, []))

    invalid = [
        ident
        for ident in wanted
        if identifier_target(ident) not in allowed
    ]

    if invalid:
        raise RuntimeError(
            "context path outside graph: "
            + ", ".join(invalid)
        )

    root = ET.parse(XML_PATH).getroot()

    by_id = {
        node.attrib["identifier"]: node
        for node in root.iter()
        if node.attrib.get("identifier") in wanted
    }

    missing = [
        ident
        for ident in wanted
        if ident not in by_id
    ]

    if missing:
        raise RuntimeError(
            "context path not found: "
            + ", ".join(missing)
        )

    result = []

    for ident in wanted:
        target = identifier_target(ident)
        node = by_id[ident]
        text = node_text(node)
        section = sections.get(target, {})

        result.append({
            "section": target,
            "citation": section.get(
                "citation",
                target,
            ),
            "identifier": ident,
            "tag": local(node.tag),
            "chars": len(text),
            "text": text,
        })

    return result


def render_context(blocks):
    return "\n\n".join(
        block["citation"]
        + " "
        + block["identifier"]
        + "\n"
        + block["text"]
        for block in blocks
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("section")
    parser.add_argument("--hops", type=int, default=1)
    parser.add_argument(
        "--scoped",
        action="store_true",
    )
    parser.add_argument(
        "--show-text",
        action="store_true",
    )
    args = parser.parse_args()

    sections = load_sections()
    graph = load_graph()

    if args.scoped:
        blocks = scoped_context(
            args.section,
            sections,
            graph,
        )

        result = {
            "source_section": args.section,
            "context_chars": sum(
                block["chars"]
                for block in blocks
            ),
            "context_blocks": [
                block
                if args.show_text
                else {
                    key: value
                    for key, value in block.items()
                    if key != "text"
                }
                for block in blocks
            ],
        }

        print(json.dumps(result, indent=2))
        return

    edges = expand(
        graph,
        args.section,
        args.hops,
    )
    context = build_context(
        sections,
        edges,
    )

    print(json.dumps({
        "source_section": args.section,
        "hops": args.hops,
        "edges": edges,
        "context_sections": [
            {
                "section": item["section"],
                "hop": item["hop"],
                "citation": item["citation"],
            }
            for item in context
        ],
    }, indent=2))


if __name__ == "__main__":
    main()
