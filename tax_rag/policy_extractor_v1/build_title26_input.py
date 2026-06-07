import io
import json
import re
import urllib.request
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

URL = "https://uscode.house.gov/download/releasepoints/us/pl/119/95/xml_usc26@119-95.zip"
OUT = Path("inputs/title26_sections.jsonl")

def local_name(tag):
    return tag.rsplit("}", 1)[-1]

def text_of(node):
    return re.sub(r"\s+", " ", " ".join(s.strip() for s in node.itertext() if s.strip())).strip()

def section_number(node):
    for child in node:
        if local_name(child.tag) == "num":
            match = re.search(r"\d+[A-Za-z0-9-]*", text_of(child))
            return match.group(0) if match else None
    return None

def main():
    OUT.parent.mkdir(exist_ok=True)

    with urllib.request.urlopen(URL) as response:
        data = response.read()

    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        name = next(n for n in archive.namelist() if n.endswith(".xml"))
        root = ET.fromstring(archive.read(name))

    with OUT.open("w", encoding="utf-8") as f:
        for node in root.iter():
            if local_name(node.tag) != "section":
                continue
            number = section_number(node)
            if not number:
                continue
            body = text_of(node)
            if not body:
                continue
            row = {
                "id": "26usc_" + number.replace("-", "_"),
                "citation": f"26 U.S.C. § {number}",
                "text": body,
            }
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

if __name__ == "__main__":
    main()