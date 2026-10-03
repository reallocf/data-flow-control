import importlib.util
import json
import re
import shutil
from datetime import datetime
from pathlib import Path
from xml.etree import ElementTree as ET

base = Path(__file__).resolve().parent
main = base / "rej16_reference_audit.py"
trial = base / "rej16_reference_audit.heading_trial.py"
inputs = base / "inputs"
outputs = base / "outputs"

text = main.read_text(encoding="utf-8")

if "structure_ref_first_re = re.compile" not in text:
    old = 'mark_re = re.compile(r"(?<![A-Za-z0-9)])\\(([A-Za-z0-9]+)\\)")\n'

    new = '''mark_re = re.compile(r"(?<![A-Za-z0-9)])\\(([A-Za-z0-9]+)\\)")
structure_ref_first_re = re.compile(r"\\b(?P<kind>subsection|paragraph|subparagraph|clause)s?\\s*$", re.I)
structure_ref_join_re = re.compile(
    r"\\b(?P<kind>subsection|paragraph|subparagraph|clause)s?\\s+"
    r"(?:\\([A-Za-z0-9-]+\\)\\s*,\\s*)*\\([A-Za-z0-9-]+\\)\\s*,?\\s*(?:and|or)\\s*$",
    re.I,
)
structure_ref_plural_re = re.compile(
    r"\\b(?P<kind>subsections|paragraphs|subparagraphs|clauses)\\s+"
    r"(?:\\([A-Za-z0-9-]+\\)\\s*,\\s*)+$",
    re.I,
)
'''

    if old not in text:
        raise RuntimeError("Marker declaration not found.")

    text = text.replace(old, new, 1)

    old = '''def build_tree(text, source):
    root = {"id": source, "level": 0, "token": source, "path": [], "start": 0, "end": len(text)}
'''

    new = '''def structure_word_level(kind):
    return level_by_word.get(kind.lower().rstrip("s"))

def mark_is_heading(text, match, level, stack, seen, source):
    tail = text[match.end():match.end() + 40]
    first = next((ch for ch in tail if not ch.isspace()), "")
    if first in {",", ";", ".", ")", "("}:
        return False

    prefix = text[max(0, match.start() - 160):match.start()]

    for pattern in (
        structure_ref_first_re,
        structure_ref_join_re,
        structure_ref_plural_re,
    ):
        hit = pattern.search(prefix)
        if hit and structure_word_level(hit.group("kind")) == level:
            return False

    if level >= 3:
        parent = (
            source
            if level == 1
            else (
                stack[level - 2]["id"]
                if len(stack) >= level - 1
                else None
            )
        )

        previous = seen.get((parent, level)) if parent else None

        if previous is None:
            before = text[:match.start()].rstrip()[-1:]
            next_word = re.search(r"[A-Za-z]", tail)
            capital = bool(
                next_word
                and next_word.group(0).isupper()
            )

            if (
                before not in {"-", "\\u2013", "\\u2014", ":"}
                and not capital
            ):
                return False

    return True

def build_tree(text, source):
    root = {"id": source, "level": 0, "token": source, "path": [], "start": 0, "end": len(text)}
'''

    if old not in text:
        raise RuntimeError("Tree function boundary not found.")

    text = text.replace(old, new, 1)

    old = '''    for match in mark_re.finditer(text):
        token = match.group(1)
        if text[match.end():match.end() + 1] in {",", ";"}:
            continue
        choices = []
        for level in level_options(token):
            value = node_score(level, token, stack, seen, source)
            if value is not None:
                choices.append((value, level))
'''

    new = '''    for match in mark_re.finditer(text):
        token = match.group(1)
        choices = []

        for level in level_options(token):
            value = node_score(
                level,
                token,
                stack,
                seen,
                source,
            )

            if (
                value is not None
                and mark_is_heading(
                    text,
                    match,
                    level,
                    stack,
                    seen,
                    source,
                )
            ):
                choices.append((value, level))
'''

    if old not in text:
        raise RuntimeError("Tree loop boundary not found.")

    text = text.replace(old, new, 1)

trial.write_text(text, encoding="utf-8")

def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

mod = load_module(trial, "heading_trial")

rows = mod.read_jsonl(
    inputs / "title26_sections.jsonl"
)

sections = {
    mod.section_number(row): row
    for row in rows
    if mod.section_number(row)
}

xml_root = ET.parse(
    inputs / "usc26.xml"
).getroot()

def expected_ids(section):
    result = set()

    for node in xml_root.iter():
        ident = node.attrib.get(
            "identifier",
            "",
        )

        hit = re.fullmatch(
            rf"/us/usc/t26/s{re.escape(section)}(/.*)?",
            ident,
        )

        if not hit:
            continue

        path = [
            x
            for x in (hit.group(1) or "").split("/")
            if x
        ]

        if len(path) <= 4:
            result.add(
                section
                + "".join(
                    f"({part})"
                    for part in path
                )
            )

    return result

checks = {}

for section in ("267", "274"):
    body = mod.section_text(
        sections[section]
    )

    body = body[
        :mod.main_cut(body)
    ]

    nodes = mod.build_tree(
        body,
        section,
    )

    found = {
        node["id"]
        for node in nodes
    }

    expected = expected_ids(section)

    checks[section] = {
        "found": len(found),
        "expected": len(expected),
        "extra": sorted(found - expected),
        "missing": sorted(expected - found),
    }

    if found != expected:
        raise RuntimeError(
            f"Hierarchy check failed for section {section}."
        )

body267 = mod.section_text(
    sections["267"]
)
body267 = body267[
    :mod.main_cut(body267)
]

nodes267 = {
    node["id"]: node
    for node in mod.build_tree(
        body267,
        "267",
    )
}

if nodes267["267(b)"]["start"] != body267.find("(b) Relationships"):
    raise RuntimeError("Section 267(b) location check failed.")

if nodes267["267(c)"]["start"] != body267.find("(c) Constructive ownership"):
    raise RuntimeError("Section 267(c) location check failed.")

body274 = mod.section_text(
    sections["274"]
)
body274 = body274[
    :mod.main_cut(body274)
]

nodes274 = {
    node["id"]: node
    for node in mod.build_tree(
        body274,
        "274",
    )
}

literal_checks = {
    "274(e)(3)(B)":
        "(B) where the services are performed for a person other than an employer",
    "274(k)(2)(B)":
        "(B) any other expense to the extent provided in regulations",
    "274(n)(2)(C)":
        "(C) such expense is for food or beverages-",
    "274(n)(2)(D)":
        "(D) such expense is-",
}

for ident, literal in literal_checks.items():
    expected = body274.find(literal)

    if (
        expected < 0
        or nodes274.get(ident, {}).get("start") != expected
    ):
        raise RuntimeError(
            f"Location check failed for {ident}."
        )

stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
saved = base / f"rej16_reference_audit.before_heading_fix_{stamp}.py"

shutil.copy2(main, saved)
shutil.copy2(trial, main)

report = {
    "status": "passed",
    "saved_copy": saved.name,
    "checks": checks,
}

report_path = outputs / "heading_filter_check.json"

report_path.write_text(
    json.dumps(
        report,
        indent=2,
    ),
    encoding="utf-8",
)

print(
    json.dumps(
        report,
        indent=2,
    )
)