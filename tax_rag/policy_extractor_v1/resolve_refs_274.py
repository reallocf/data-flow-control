import csv
import json
import re
from pathlib import Path

base = Path(__file__).resolve().parent
src_path = base / "inputs" / "title26_sections.jsonl"
audit_path = base / "outputs" / "reference_audit_274_main.csv"
out_path = base / "outputs" / "struct_ref_resolution_274.csv"

sec_re = re.compile(r"§\s*([0-9][0-9A-Za-z-]*)")
mark_re = re.compile(r"(?<![A-Za-z0-9)])\(([A-Za-z0-9]+)\)")
roman = [
    "i", "ii", "iii", "iv", "v", "vi", "vii", "viii", "ix", "x",
    "xi", "xii", "xiii", "xiv", "xv", "xvi", "xvii", "xviii", "xix", "xx",
]
roman_rank = {value: index for index, value in enumerate(roman)}

def read_rows(path):
    rows = []
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows

def pick(row, keys):
    for key in keys:
        value = row.get(key)
        if value:
            return str(value)
    return ""

def sec_no(row):
    value = pick(row, ["section", "section_number", "num", "number"])
    if value:
        return value
    text = pick(row, ["id", "source", "label", "name"])
    found = sec_re.search(text)
    if found:
        return found.group(1)
    found = re.search(r"(?:section|usc)[:_\s-]*([0-9][0-9A-Za-z-]*)", text, re.I)
    return found.group(1) if found else ""

def sec_body(row):
    return pick(row, ["text", "body", "content", "section_text"])

def next_mark(level, value):
    if level == 1 and len(value) == 1:
        return chr(ord(value) + 1)
    if level == 2 and value.isdigit():
        return str(int(value) + 1)
    if level == 3 and len(value) == 1:
        return chr(ord(value) + 1)
    if level == 4:
        index = roman_rank.get(value)
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
    if token in roman_rank:
        result.append(4)
    if token.isalpha() and token.lower() == token:
        result.append(1)
    return result

def make_id(section, path):
    return section + "".join(f"({part})" for part in path)

def score(level, token, stack, seen):
    if level == 1:
        parent = "274"
    elif len(stack) >= level - 1:
        parent = stack[level - 2]["id"]
    else:
        return None
    previous = seen.get((parent, level))
    expected = first_mark(level) if previous is None else next_mark(level, previous)
    if token == expected:
        return 100 + level
    if previous is None and token == first_mark(level):
        return 80 + level
    return None

def build_tree(text):
    root = {"id": "274", "level": 0, "token": "274", "path": [], "start": 0, "end": len(text), "parent": ""}
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
            value = score(level, token, stack, seen)
            if value is not None:
                choices.append((value, level))
        if not choices:
            continue
        _, level = max(choices)
        stack = stack[:level - 1]
        parent = "274" if level == 1 else stack[-1]["id"]
        path = [item["token"] for item in stack] + [token]
        ident = make_id("274", path)
        if ident in used:
            continue
        used.add(ident)
        item = {
            "id": ident,
            "level": level,
            "token": token,
            "path": path,
            "start": match.start(),
            "end": len(text),
            "parent": parent,
        }
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

def resolve(nodes, source, target_level, parts):
    if target_level == 1:
        ident = make_id("274", parts)
        return ident if any(node["id"] == ident for node in nodes) else ""
    path = list(source["path"])
    if len(path) < target_level - 1:
        return ""
    path = path[:target_level - 1] + parts
    ident = make_id("274", path)
    return ident if any(node["id"] == ident for node in nodes) else ""

def snippet(text, nodes_by_id, ident):
    node = nodes_by_id.get(ident)
    if not node:
        return ""
    value = re.sub(r"\s+", " ", text[node["start"]:node["end"]]).strip()
    return value[:180]

def main():
    text = ""
    for row in read_rows(src_path):
        if sec_no(row) == "274":
            text = sec_body(row)
            break
    if not text:
        raise RuntimeError("section 274 text not found")

    nodes = build_tree(text)
    nodes_by_id = {node["id"]: node for node in nodes}
    audit_rows = list(csv.DictReader(audit_path.open(newline="", encoding="utf-8-sig")))

    fields = [
        "source_section", "span_start", "span_end", "surface", "ref_class",
        "source_node_id", "target_path", "resolved_target_id",
        "resolution_status", "reason_code", "target_snippet",
    ]

    with out_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in audit_rows:
            if row.get("status") != "needs_structural_resolution":
                continue
            source = deepest(nodes, int(row["span_start"]))
            word = row["surface"].lower().split()[0].rstrip("s")
            target_level = {
                "subsection": 1,
                "paragraph": 2,
                "subparagraph": 3,
                "clause": 4,
            }.get(word)
            parts = [part for part in row["target_path"].split(".") if part]
            target = resolve(nodes, source, target_level, parts) if target_level and parts else ""
            writer.writerow({
                "source_section": row["source_section"],
                "span_start": row["span_start"],
                "span_end": row["span_end"],
                "surface": row["surface"],
                "ref_class": row["ref_class"],
                "source_node_id": source["id"],
                "target_path": row["target_path"],
                "resolved_target_id": target,
                "resolution_status": "resolved" if target else "open",
                "reason_code": "" if target else "no_unique_target",
                "target_snippet": snippet(text, nodes_by_id, target),
            })

    print(out_path)

if __name__ == "__main__":
    main()
