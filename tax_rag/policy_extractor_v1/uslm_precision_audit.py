import csv
import json
import math
import random
import re
from pathlib import Path
from xml.etree import ElementTree as ET

import cornell_reference_audit as cra

SAMPLE_SIZE = 150
SEED = 11984
AUDIT_PATH = Path("outputs/uslm_only_precision_sample.csv")
SUMMARY_PATH = Path("outputs/uslm_only_precision_summary.json")


def context_text(node):
    parts = []

    def walk(cur):
        if cra.local(cur.tag) in {"notes", "sourceCredit"}:
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
    return re.sub(r"\s+", " ", " ".join(parts))


with open(
    "outputs/cornell_reference_compare.csv",
    newline="",
    encoding="utf-8",
) as f:
    compare = list(csv.DictReader(f))

population = sorted(
    {
        (r["source_section"], r["target_section"])
        for r in compare
        if r["status"] == "uslm_only"
    },
    key=lambda p: (cra.section_key(p[0]), cra.section_key(p[1])),
)

rng = random.Random(SEED)
sample = rng.sample(population, min(SAMPLE_SIZE, len(population)))

root = ET.parse(cra.XML_PATH).getroot()
sections = {}

for node in root.iter():
    if cra.local(node.tag) != "section":
        continue
    m = re.fullmatch(
        r"/us/usc/t26/s([^/]+)",
        node.attrib.get("identifier", ""),
    )
    if m:
        sections[m.group(1)] = node

wanted = set(sample)
contexts = {pair: [] for pair in sample}

for source in {p[0] for p in sample}:
    text = context_text(sections[source])

    for match in cra.SECTION_EXPR_RE.finditer(text):
        suffix = text[match.end():match.end() + 160]

        if cra.EXTERNAL_SUFFIX_RE.match(suffix):
            continue

        targets = []

        for item in re.finditer(
            cra.ATOM,
            match.group("items"),
            re.I,
        ):
            h = cra.SECTION_HEAD_RE.match(item.group(0))

            if h:
                target = h.group(0)
                pair = (source, target)

                if pair in wanted:
                    targets.append(pair)

        if not targets:
            continue

        context = text[
            max(0, match.start() - 120):
            min(len(text), match.end() + 220)
        ]

        for pair in targets:
            if context not in contexts[pair]:
                contexts[pair].append(context)

existing = {}

if AUDIT_PATH.exists():
    with AUDIT_PATH.open(
        newline="",
        encoding="utf-8",
    ) as f:
        for row in csv.DictReader(f):
            existing[
                (row["source_section"], row["target_section"])
            ] = {
                "label": row["label"],
                "note": row["note"],
            }


def save():
    AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with AUDIT_PATH.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as f:
        w = csv.writer(f)
        w.writerow([
            "sample_index",
            "source_section",
            "target_section",
            "label",
            "note",
            "occurrence_count",
            "contexts",
        ])

        for i, pair in enumerate(sample, 1):
            old = existing.get(pair, {})

            w.writerow([
                i,
                pair[0],
                pair[1],
                old.get("label", ""),
                old.get("note", ""),
                len(contexts[pair]),
                "\n---\n".join(contexts[pair]),
            ])


def wilson(valid, total):
    if total == 0:
        return None, None

    z = 1.959963984540054
    p = valid / total
    d = 1 + z * z / total

    center = (
        p + z * z / (2 * total)
    ) / d

    half = (
        z
        * math.sqrt(
            p * (1 - p) / total
            + z * z / (4 * total * total)
        )
        / d
    )

    return center - half, center + half


save()

for i, pair in enumerate(sample, 1):
    old = existing.get(pair, {})

    if old.get("label") in {"valid", "invalid", "unsure"}:
        continue

    print()
    print("=" * 76)
    print(
        f"{i}/{len(sample)}   "
        f"Title 26 §{pair[0]} -> §{pair[1]}"
    )
    print("=" * 76)

    shown = contexts[pair]

    for j, ctx in enumerate(shown, 1):
        print()
        print(f"[occurrence {j}/{len(shown)}]")
        print(ctx)

    print()
    print(
        "y = genuine Title 26 cross-reference; "
        "n = false/external reference; "
        "u = unsure; q = save and quit"
    )

    while True:
        answer = input("> ").strip().lower()

        if answer in {"y", "n", "u", "q"}:
            break

    if answer == "q":
        save()
        print("saved")
        raise SystemExit

    label = {
        "y": "valid",
        "n": "invalid",
        "u": "unsure",
    }[answer]

    existing[pair] = {
        "label": label,
        "note": "",
    }

    save()

labels = [
    existing[p]["label"]
    for p in sample
    if p in existing
]

valid = labels.count("valid")
invalid = labels.count("invalid")
unsure = labels.count("unsure")
decided = valid + invalid

low, high = wilson(valid, decided)

summary = {
    "population_uslm_only_pairs": len(population),
    "sample_size": len(sample),
    "seed": SEED,
    "valid": valid,
    "invalid": invalid,
    "unsure": unsure,
    "decided": decided,
    "estimated_precision_pct": (
        round(100 * valid / decided, 2)
        if decided else None
    ),
    "wilson_95_low_pct": (
        round(100 * low, 2)
        if low is not None else None
    ),
    "wilson_95_high_pct": (
        round(100 * high, 2)
        if high is not None else None
    ),
}

SUMMARY_PATH.write_text(
    json.dumps(summary, indent=2) + "\n",
    encoding="utf-8",
)

print()
print(json.dumps(summary, indent=2))
