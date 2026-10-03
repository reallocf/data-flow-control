import csv
import hashlib
import json
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests
import tiktoken
from bs4 import BeautifulSoup

base = Path(__file__).resolve().parent
outputs = base / "outputs"
notes = base / "notes"
pack = base / "section_274_cross_title_check2.zip"

if pack.exists():
    pack.unlink()

budget = json.loads(
    (outputs / "section_274_budget_exact.json").read_text(encoding="utf-8")
)
capacity = int(budget["legal_context_capacity"])
enc = tiktoken.get_encoding(budget["tokenizer"])

specs = [
    (
        "15 U.S.C. § 78p(a)",
        "https://www.law.cornell.edu/uscode/text/15/78p",
        r"\(a\)\s*Disclosures required\b",
        r"\(b\)\s*Profits from purchase and sale of security within six months\b",
        ("beneficial owner", "shall file"),
    ),
    (
        "19 U.S.C. § 2702(a)(1)(A)",
        "https://www.law.cornell.edu/uscode/text/19/2702",
        r"\(a\)\s*Definitions; termination of designation\s*"
        r"\(1\)\s*For purposes of this chapter\s*[—-]\s*"
        r"\(A\)",
        r"\(B\)\s*The term",
        ("beneficiary country", "proclamation"),
    ),
    (
        "46 U.S.C. § 2101",
        "https://www.law.cornell.edu/uscode/text/46/2101",
        r"In this subtitle\s*[—-]",
        r"Editorial Notes\b",
        ("associated equipment", "vessel"),
    ),
]

def page_text(url):
    r = requests.get(url, timeout=45)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    for tag in soup(["script", "style", "nav", "header", "footer", "aside"]):
        tag.decompose()

    h1 = soup.find("h1")
    if h1 is None:
        raise RuntimeError(f"Heading absent at {url}")

    heading = re.sub(r"\s+", " ", h1.get_text(" ", strip=True))
    flat = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))

    pos = flat.find(heading)
    if pos < 0:
        raise RuntimeError(f"Heading location absent at {url}")

    return flat[pos + len(heading):].strip()

def extract_between(text, start_pattern, end_pattern):
    start = re.search(start_pattern, text, flags=re.I | re.S)

    if start is None:
        raise RuntimeError("Start boundary absent")

    end = re.search(
        end_pattern,
        text[start.end():],
        flags=re.I | re.S,
    )

    if end is None:
        raise RuntimeError("End boundary absent")

    return text[start.end():start.end() + end.start()].strip()

rows = []
chunks = []

for cite, url, start_pattern, end_pattern, checks in specs:
    page = page_text(url)
    body = extract_between(page, start_pattern, end_pattern)

    lower = body.lower()

    if not all(term.lower() in lower for term in checks):
        raise RuntimeError(f"Text validation failed for {cite}")

    rendered = cite + "\n" + body
    token_count = len(enc.encode(rendered))

    rows.append({
        "citation": cite,
        "retrieval_site": "Cornell LII",
        "url": url,
        "retrieved_utc": datetime.now(timezone.utc).isoformat(),
        "characters": len(body),
        "tokens": token_count,
        "sha256": hashlib.sha256(body.encode("utf-8")).hexdigest(),
    })

    chunks.append(rendered)

cross_title_tokens = len(enc.encode("\n\n".join(chunks)))

structural = list(
    csv.DictReader(
        (outputs / "section_274_structural_layers.csv").open(
            encoding="utf-8-sig",
            newline="",
        )
    )
)

capacity_rows = []
last_fit = 0
first_over = None

for row in structural:
    layer = int(row["layer"])
    tax_tokens = int(row["context_tokens"])
    combined = tax_tokens + cross_title_tokens
    fits = combined <= capacity

    capacity_rows.append({
        "layer": layer,
        "tax_code_context_tokens": tax_tokens,
        "cross_title_tokens": cross_title_tokens,
        "combined_accounting_tokens": combined,
        "legal_context_capacity": capacity,
        "fits": str(fits).lower(),
    })

    if fits:
        last_fit = layer
    elif first_over is None:
        first_over = layer

map_path = outputs / "section_274_cross_title_map.json"
capacity_path = outputs / "section_274_capacity_cross_title.csv"
summary_path = outputs / "section_274_cross_title_summary.json"
note_path = notes / "section_274_cross_title_note.md"

map_path.write_text(
    json.dumps(
        {
            "retrieval_site": "Cornell LII",
            "tokenizer": enc.name,
            "cross_title_tokens": cross_title_tokens,
            "provisions": rows,
        },
        indent=2,
        ensure_ascii=False,
    ),
    encoding="utf-8",
)

with capacity_path.open("w", encoding="utf-8", newline="") as fh:
    writer = csv.DictWriter(fh, fieldnames=list(capacity_rows[0]))
    writer.writeheader()
    writer.writerows(capacity_rows)

summary = {
    "cross_title_tokens": cross_title_tokens,
    "last_complete_layer": last_fit,
    "first_layer_beyond_capacity": first_over,
    "provision_count": len(rows),
}

summary_path.write_text(
    json.dumps(summary, indent=2),
    encoding="utf-8",
)

lines = [
    "# Section 274 cross-title branch",
    "",
    "Retrieval site: Cornell LII.",
    "",
    "| Provision | Tokens |",
    "| --- | ---: |",
]

for row in rows:
    lines.append(
        f"| {row['citation']} | {row['tokens']:,} |"
    )

lines += [
    "",
    f"Cross-title context: {cross_title_tokens:,} tokens.",
    "",
    "## Capacity accounting",
    "",
    "| Layer | Tax-code context | Cross-title context | Combined | Fits |",
    "| ---: | ---: | ---: | ---: | :--- |",
]

for row in capacity_rows:
    lines.append(
        f"| {row['layer']} "
        f"| {row['tax_code_context_tokens']:,} "
        f"| {row['cross_title_tokens']:,} "
        f"| {row['combined_accounting_tokens']:,} "
        f"| {row['fits']} |"
    )

lines += [
    "",
    f"Last complete layer within capacity: {last_fit}.",
    f"First complete layer beyond capacity: {first_over}.",
    "",
    "Combined figures add the measured cross-title branch to the existing structural trace. The fit decision is unaffected unless a layer is close to the capacity boundary.",
]

note_path.write_text(
    "\n".join(lines) + "\n",
    encoding="utf-8",
)

with zipfile.ZipFile(pack, "w", zipfile.ZIP_DEFLATED) as z:
    for path in (map_path, capacity_path, summary_path, note_path):
        z.write(path, arcname=path.name)

print(json.dumps(summary, indent=2))
print(pack)