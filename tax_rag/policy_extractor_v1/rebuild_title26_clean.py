import json
import re
from pathlib import Path
from xml.etree import ElementTree as ET

base = Path(__file__).resolve().parent
xml_path = base / "inputs" / "usc26.xml"
out_path = base / "inputs" / "title26_sections.jsonl"

def local(tag):
    return tag.rsplit("}", 1)[-1]

def clean(text):
    return re.sub(r"\s+", " ", text).strip().replace("\u2013", "-").replace("\u2014", "-").replace("\u2212", "-")

def statute_text(node):
    parts = []

    def walk(cur):
        if local(cur.tag) in {"notes", "sourceCredit"}:
            if cur.tail:
                parts.append(cur.tail)
            return

        if cur.text:
            parts.append(cur.text)

        for child in cur:
            walk(child)

        if cur.tail:
            parts.append(cur.tail)

    walk(node)
    return clean(" ".join(parts))

root = ET.parse(xml_path).getroot()
rows = []

for node in root.iter():
    if local(node.tag) != "section":
        continue

    match = re.fullmatch(r"/us/usc/t26/s([^/]+)", node.attrib.get("identifier", ""))
    if not match:
        continue

    raw = match.group(1)

    if "..." in raw:
        continue

    number = clean(raw)
    body = statute_text(node)

    if not body:
        continue

    rows.append({
        "id": "26usc_" + number,
        "citation": f"26 U.S.C. § {number}",
        "text": body,
    })

with out_path.open("w", encoding="utf-8") as handle:
    for row in rows:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")

numbers = [row["citation"].split("§", 1)[1].strip() for row in rows]

print("rows =", len(rows))
print("unique =", len(set(numbers)))
print("duplicates =", len(rows) - len(set(numbers)))
print("1400Z-1 =", "1400Z-1" in numbers)
print("1400Z-2 =", "1400Z-2" in numbers)
