import json
import math
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup

CHUNK = 1200
OVERLAP = 200
MAX_CHUNKS = 500

def xml_to_text(path):
    parts = []
    for _, elem in ET.iterparse(path, events=("end",)):
        if elem.text:
            parts.append(elem.text)
        if elem.tail:
            parts.append(elem.tail)
        elem.clear()
    return " ".join(p.strip() for p in parts if p and p.strip())

def html_to_text(path):
    with open(path, "rb") as f:
        soup = BeautifulSoup(f.read(), "lxml")
    return soup.get_text(separator=" ", strip=True)

def chunk_text(text):
    out = []
    i = 0
    n = len(text)
    while i < n and len(out) < MAX_CHUNKS:
        j = min(n, i + CHUNK)
        out.append(text[i:j])
        i = max(0, j - OVERLAP)
        if j == n:
            break
    return out

def main():
    regs = xml_to_text("ECFR-title26.xml")
    irc = html_to_text("USCODE-2023-title26.htm")

    chunks = []
    for k, t in enumerate(chunk_text(regs)):
        chunks.append({"id": f"cfr_{k}", "source": "26CFR", "text": t})
    for k, t in enumerate(chunk_text(irc)):
        chunks.append({"id": f"usc_{k}", "source": "26USC", "text": t})

    with open("chunks.jsonl", "w", encoding="utf-8") as f:
        for r in chunks:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"Wrote {len(chunks)} chunks to chunks.jsonl")

if __name__ == "__main__":
    main()
