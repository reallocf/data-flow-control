import csv
import json
import os
import re
from pathlib import Path
base = Path(__file__).resolve().parent
input_path = Path(os.getenv("INPUT", base / "inputs" / "title26_sections.jsonl"))
output_path = Path(os.getenv("OUTPUT", base / "outputs" / "rej16_reference_audit.csv"))
targets = {x.strip() for x in os.getenv("TARGETS", "274").split(",") if x.strip()}
section_re = re.compile(r"\u00a7\s*([0-9][0-9A-Za-z-]*)")
id_re = re.compile(r"(?:section|usc)[:_\s-]*([0-9][0-9A-Za-z-]*)", re.I)
explicit_re = re.compile(r"\b(?:sections?|\u00a7{1,2})\s+([0-9][0-9A-Za-z-]*)(?P<trail>(?:\s*\([A-Za-z0-9-]+\))*)", re.I)
local_re = re.compile(r"\b(?:this\s+|such\s+)?(subsection|paragraph|subparagraph|clause)s?\s+(?P<trail>(?:\([A-Za-z0-9-]+\)\s*)+)", re.I)
part_re = re.compile(r"\(([A-Za-z0-9-]+)\)")
mark_re = re.compile(r"(?<![A-Za-z0-9)])\(([A-Za-z0-9]+)\)")
complex_local_re = re.compile(r"\b(?P<kind>subsections?|paragraphs?|subparagraphs?|clauses?)\s+(?P<items>\([A-Za-z0-9-]+\)(?:\s*,\s*\([A-Za-z0-9-]+\))*(?:\s*,?\s*(?:and|or)\s*\([A-Za-z0-9-]+\))?)\s+of\s+(?P<parent_kind>subsections?|paragraphs?|subparagraphs?|clauses?)\s+(?P<parent>(?:\([A-Za-z0-9-]+\))+)", re.I)
self_re = re.compile(r"\bthis\s+(chapter|section|subsection|paragraph)\b", re.I)
part_structure_re = re.compile(r"\bpart\s+([IVXLCDM]+)\s+of\s+subchapter\s+([A-Z])\s+of\s+chapter\s+([0-9][0-9A-Za-z-]*)\b", re.I)
subchapter_re = re.compile(r"\bsubchapter\s+([A-Z])\s+of\s+chapter\s+([0-9][0-9A-Za-z-]*)\b", re.I)
chapter_re = re.compile(r"\bchapter\s+([0-9][0-9A-Za-z-]*)\b", re.I)
title_re = re.compile(r"\bsection\s+(?P<section>[0-9][0-9A-Za-z-]*)(?P<trail>(?:\([A-Za-z0-9-]+\))*)\s+of\s+title\s+(?P<title>\d+)\b", re.I)
external_act_re = re.compile(
    r"\bsection\s+(?P<section>[0-9][0-9A-Za-z-]*)(?P<trail>(?:\([A-Za-z0-9-]+\))*)\s+"
    r"of\s+the\s+(?P<act>[A-Z][A-Za-z0-9 ,'-]+?Act(?:\s+of\s+\d{4})?)\b"
)
coordinated_local_re = re.compile(
    r"\b(?:subsection|paragraph|subparagraph|clause)\s+"
    r"(?:\([A-Za-z0-9-]+\))+\s+(?:or|and)\s+"
    r"(?P<trail>(?:\([A-Za-z0-9-]+\))+)",
    re.I,
)
bare_section_re = re.compile(r"\b(?P<section>[0-9][0-9A-Za-z-]*)(?P<trail>(?:\([A-Za-z0-9-]+\))+)")
continuation_re = re.compile(r"\b(?:section|sections|\u00a7{1,2})\s+[0-9][0-9A-Za-z-]*(?:\([A-Za-z0-9-]+\))*\s+(?:or|and)\s*$", re.I)
roman = ["i","ii","iii","iv","v","vi","vii","viii","ix","x","xi","xii","xiii","xiv","xv","xvi","xvii","xviii","xix","xx"]
roman_rank = {value: index for index, value in enumerate(roman)}
level_by_word = {"subsection": 1, "paragraph": 2, "subparagraph": 3, "clause": 4}
def read_jsonl(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]
def pick(row, keys):
    for key in keys:
        value = row.get(key)
        if value:
            return str(value)
    return ""
def section_number(row):
    direct = pick(row, ["section", "section_number", "num", "number"])
    if direct:
        return direct
    text = pick(row, ["id", "ci" + "tation", "cite", "source_" + "ci" + "tation"])
    found = section_re.search(text)
    if found:
        return found.group(1)
    found = id_re.search(text)
    return found.group(1) if found else ""
def section_text(row):
    return pick(row, ["text", "body", "content", "section_text"])
def main_cut(text):
    markers = ["Editorial Notes", "Source Credit", "Statutory Notes and Related Subsidiaries", "Executive Documents"]
    positions = [text.find(marker) for marker in markers if text.find(marker) > 0]
    return min(positions) if positions else len(text)
def path_parts(text):
    return [part.lower() for part in part_re.findall(text or "")]
def norm_name(text):
    return re.sub(r"[^a-z0-9]+", ".", text.lower()).strip(".")
def next_mark(level, value):
    if level == 1 and len(value) == 1:
        return chr(ord(value) + 1)
    if level == 2 and value.isdigit():
        return str(int(value) + 1)
    if level == 3 and len(value) == 1:
        return chr(ord(value) + 1)
    if level == 4:
        index = roman_rank.get(value.lower())
        if index is not None and index + 1 < len(roman):
            return roman[index + 1]
    return None
def first_mark(level):
    return {1: "a", 2: "1", 3: "A", 4: "i"}[level]
def level_options(token):
    if token.isdigit():
        return [2]
    if token.isalpha() and token.upper() == token:
        return [3]
    result = []
    if token.lower() in roman_rank:
        result.append(4)
    if token.isalpha() and token.lower() == token:
        result.append(1)
    return result
def make_id(section, path):
    return section + "".join(f"({part})" for part in path)
def node_score(level, token, stack, seen, source):
    if level == 1:
        parent = source
    elif len(stack) >= level - 1:
        parent = stack[level - 2]["id"]
    else:
        return None
    previous = seen.get((parent, level))
    expected = first_mark(level) if previous is None else next_mark(level, previous)
    check = token.lower() if level == 4 else token
    exp = expected.lower() if level == 4 and expected else expected
    if check == exp:
        return 100 + level
    if previous is None and check == (first_mark(level).lower() if level == 4 else first_mark(level)):
        return 80 + level
    return None
def build_tree(text, source):
    root = {"id": source, "level": 0, "token": source, "path": [], "start": 0, "end": len(text)}
    nodes = [root]
    stack = []
    seen = {}
    used = set()
    for match in mark_re.finditer(text):
        token = match.group(1)
        if text[match.end():match.end() + 1] in {",", ";"}:
            continue
        choices = []
        for level in level_options(token):
            value = node_score(level, token, stack, seen, source)
            if value is not None:
                choices.append((value, level))
        if not choices:
            continue
        _, level = max(choices)
        stack = stack[:level - 1]
        parent = source if level == 1 else stack[-1]["id"]
        path = [item["token"] for item in stack] + [token]
        ident = make_id(source, path)
        if ident in used:
            continue
        used.add(ident)
        item = {"id": ident, "level": level, "token": token, "path": path, "start": match.start(), "end": len(text), "parent": parent}
        for old in nodes:
            if old["level"] >= level and old["end"] == len(text):
                old["end"] = match.start()
        nodes.append(item)
        stack.append(item)
        seen[(parent, level)] = token
    return nodes
def deepest(nodes, offset):
    hits = [node for node in nodes if node["start"] <= offset < node["end"]]
    return max(hits, key=lambda node: node["level"])
def resolve_path(nodes, source_node, target_level, parts, section):
    if target_level == 1:
        path = list(parts)
    else:
        current = list(source_node["path"])
        if len(current) < target_level - 1:
            return []
        path = current[:target_level - 1] + list(parts)
    ident = make_id(section, path)
    return [str(x).lower() for x in path] if any(node["id"].lower() == ident.lower() for node in nodes) else []
def scan(text, source, known):
    text = text[:main_cut(text)]
    nodes = build_tree(text, source)
    rows = []
    occupied = []
    def overlaps(start, end):
        return any(max(start, a) < min(end, b) for a, b in occupied)
    def add(start, end, surface, ref_class, target_section, target_path, status):
        if overlaps(start, end):
            return
        rows.append({"source_section": source, "span_start": start, "span_end": end, "surface": surface, "ref_class": ref_class, "target_section": target_section, "target_path": target_path, "status": status, "method": "rej16"})
        occupied.append((start, end))
    for match in complex_local_re.finditer(text):
        source_node = deepest(nodes, match.start())
        parent_word = match.group("parent_kind").lower().rstrip("s")
        parent_level = level_by_word.get(parent_word)
        parent_parts = path_parts(match.group("parent"))
        base_path = resolve_path(nodes, source_node, parent_level, parent_parts, source) if parent_level else []
        items = path_parts(match.group("items"))
        full = [base_path + [item] for item in items] if base_path else []
        target_path = ", ".join(".".join(path) for path in full) if full else ", ".join(".".join(parent_parts + [item]) for item in items)
        add(match.start(), match.end(), match.group(0), "local_structural", source, target_path, "resolved_local" if full else "needs_structural_resolution")
    for match in external_act_re.finditer(text):
        trail = ".".join(path_parts(match.group("trail")))
        name = norm_name(match.group("act"))
        target_path = f"{trail}|{name}" if trail else name
        add(match.start(), match.end(), match.group(0), "external", match.group("section"), target_path, "resolved_external")
    for match in title_re.finditer(text):
        trail = path_parts(match.group("trail"))
        add(match.start(), match.end(), match.group(0), "external", match.group("section"), ".".join(trail + ["title", match.group("title")]), "resolved_external")
    for match in explicit_re.finditer(text):
        target = match.group(1)
        add(match.start(), match.end(), match.group(0), "explicit", target, ".".join(path_parts(match.group("trail"))), "resolved_section" if target in known else "unresolved_missing_section")
    for match in coordinated_local_re.finditer(text):
        start, end = match.span("trail")
        parts = path_parts(match.group("trail"))
        add(start, end, match.group("trail"), "local_structural", source, ".".join(parts), "resolved_local")
    for match in local_re.finditer(text):
        word = match.group(1).lower()
        source_node = deepest(nodes, match.start())
        parts = path_parts(match.group("trail"))
        full = resolve_path(nodes, source_node, level_by_word[word], parts, source)
        start = match.start()
        end = match.end()
        while end > start and text[end - 1].isspace():
            end -= 1
        add(start, end, text[start:end], "local_structural", source, ".".join(full if full else parts), "resolved_local" if full else "needs_structural_resolution")
    for match in self_re.finditer(text):
        word = match.group(1).lower()
        source_node = deepest(nodes, match.start())
        if word == "chapter":
            add(match.start(), match.end(), match.group(0), "local_structural", "1", "", "resolved_structure")
        elif word == "section":
            add(match.start(), match.end(), match.group(0), "local_structural", source, "", "resolved_local")
        else:
            level = level_by_word[word]
            path = [str(x).lower() for x in source_node["path"][:level]]
            add(match.start(), match.end(), match.group(0), "local_structural", source, ".".join(path), "resolved_local" if len(path) == level else "needs_structural_resolution")
    for match in part_structure_re.finditer(text):
        add(match.start(), match.end(), match.group(0), "explicit", match.group(3), f"subchapter.{match.group(2).lower()}.part.{match.group(1).lower()}", "resolved_structure")
    for match in subchapter_re.finditer(text):
        add(match.start(), match.end(), match.group(0), "explicit", match.group(2), f"subchapter.{match.group(1).lower()}", "resolved_structure")
    for match in chapter_re.finditer(text):
        add(match.start(), match.end(), match.group(0), "explicit", match.group(1), "", "resolved_structure")
    for match in bare_section_re.finditer(text):
        start, end = match.span()
        if overlaps(start, end):
            continue
        prefix = text[max(0, start - 80):start]
        if not continuation_re.search(prefix):
            continue
        target = match.group("section")
        add(start, end, match.group(0), "explicit", target, ".".join(path_parts(match.group("trail"))), "resolved_section" if target in known else "unresolved_missing_section")
    return rows
def main():
    sections = read_jsonl(input_path)
    known = {section_number(row): row for row in sections if section_number(row)}
    rows = []
    for number in sorted(known):
        if number in targets:
            rows.extend(scan(section_text(known[number]), number, known))
    output_path.parent.mkdir(exist_ok=True)
    fields = ["source_section", "span_start", "span_end", "surface", "ref_class", "target_section", "target_path", "status", "method"]
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda r: (r["source_section"], int(r["span_start"]), int(r["span_end"]))))
    print(f"wrote {output_path} with {len(rows)} rows")
if __name__ == "__main__":
    main()
