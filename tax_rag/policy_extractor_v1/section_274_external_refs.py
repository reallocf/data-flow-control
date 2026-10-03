import hashlib
import json
import re
from pathlib import Path

from bs4 import BeautifulSoup

base = Path(__file__).resolve().parent
folder = base / "inputs" / "external_refs"
outputs = base / "outputs"
notes = base / "notes"

specs = [
    {
        "file": "15usc_78p.html",
        "surface": "section 16(a) of the Securities Exchange Act of 1934",
        "cite": "15 U.S.C. § 78p(a)",
        "section_mark": "§78p.",
        "start": "(a) Disclosures required",
        "end": "(b)",
    },
    {
        "file": "19usc_2702.html",
        "surface": "section 212(a)(1)(A) of the Caribbean Basin Economic Recovery Act",
        "cite": "19 U.S.C. § 2702(a)(1)(A)",
        "section_mark": "§2702.",
        "start": "(A) The term",
        "end": "(B) The term",
    },
    {
        "file": "46usc_2101.html",
        "surface": "section 2101 of title 46",
        "cite": "46 U.S.C. § 2101",
        "section_mark": "§2101.",
        "start": None,
        "end": None,
    },
]

def clean_page(path):
    html = path.read_text(encoding="utf-8", errors="replace")
    soup = BeautifulSoup(html, "html.parser")

    lines = [
        re.sub(r"\s+", " ", line).strip()
        for line in soup.get_text("\n").splitlines()
    ]

    return "\n".join(
        line for line in lines if line
    )

def section_body(text, mark):
    start = text.find(mark)

    if start < 0:
        start = text.find(mark.replace("§", "§ "))

    if start < 0:
        raise RuntimeError(
            f"Section marker not found: {mark}"
        )

    end = text.find(
        "Editorial Notes",
        start,
    )

    if end < 0:
        raise RuntimeError(
            f"Editorial boundary not found: {mark}"
        )

    return text[start:end].strip()

def fragment(body, start_mark, end_mark):
    if start_mark is None:
        return body

    start = body.find(start_mark)

    if start < 0:
        raise RuntimeError(
            f"Fragment start not found: {start_mark}"
        )

    end = body.find(
        end_mark,
        start + len(start_mark),
    )

    if end < 0:
        raise RuntimeError(
            f"Fragment end not found: {end_mark}"
        )

    return body[start:end].strip()

rows = []

for spec in specs:
    page_path = folder / spec["file"]
    page = clean_page(page_path)

    if "Under Maintenance" in page:
        raise RuntimeError(
            f"Source page unavailable: {spec['file']}"
        )

    date_match = re.search(
        r"Text contains those laws in effect on ([A-Za-z]+ \d{1,2}, \d{4})",
        page,
    )

    body = section_body(
        page,
        spec["section_mark"],
    )

    text = fragment(
        body,
        spec["start"],
        spec["end"],
    )

    txt_path = page_path.with_suffix(".txt")
    txt_path.write_text(
        text + "\n",
        encoding="utf-8",
    )

    digest = hashlib.sha256(
        text.encode("utf-8")
    ).hexdigest()

    rows.append({
        "surface": spec["surface"],
        "canonical_cite": spec["cite"],
        "source_date":
            date_match.group(1)
            if date_match
            else None,
        "text_file": txt_path.name,
        "characters": len(text),
        "sha256": digest,
    })

map_path = outputs / "section_274_external_map.json"

map_path.write_text(
    json.dumps(
        rows,
        indent=2,
        ensure_ascii=False,
    ),
    encoding="utf-8",
)

note = [
    "# Section 274 external references",
    "",
    "| Reference | Canonical cite | Source date | Characters |",
    "| --- | --- | --- | ---: |",
]

for row in rows:
    note.append(
        f"| {row['surface']} "
        f"| {row['canonical_cite']} "
        f"| {row['source_date']} "
        f"| {row['characters']:,} |"
    )

note += [
    "",
    "These three provisions form the external branch from section 274 before recursive tax-code expansion.",
]

note_path = notes / "section_274_external_refs.md"

note_path.write_text(
    "\n".join(note) + "\n",
    encoding="utf-8",
)

print(
    json.dumps(
        rows,
        indent=2,
        ensure_ascii=False,
    )
)