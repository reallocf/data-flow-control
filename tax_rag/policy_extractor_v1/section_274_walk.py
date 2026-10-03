import csv
import json
import re
from pathlib import Path

base = Path(__file__).resolve().parent
out = base / "outputs"
notes = base / "notes"
manifest = next(out.glob("*recursive_prompt_manifest.jsonl"))
sizes = next(out.glob("*recursive_prompt_sizes.csv"))
proposal = notes / "prompt_construction_algorithm_proposal.md"

adj = {}
row274 = None

with manifest.open(encoding="utf-8-sig") as fh:
    for line in fh:
        row = json.loads(line)
        section = str(row["source_section"])
        n = int(row["direct_refs"])
        seq = [str(x) for x in row["context_sections"]]
        adj[section] = seq[:n]
        if section == "274":
            row274 = row

if row274 is None:
    raise SystemExit("Section 274 was not found in the manifest.")

size274 = None

with sizes.open(encoding="utf-8-sig", newline="") as fh:
    for row in csv.DictReader(fh):
        if str(row["source_section"]) == "274":
            size274 = row
            break

if size274 is None:
    raise SystemExit("Section 274 was not found in the size table.")

note = proposal.read_text(encoding="utf-8-sig")
m = re.search(
    r"For section 274, direct mapped context is approximately ([\d,]+) tokens across (\d+) mapped blocks\.",
    note,
)

if not m:
    raise SystemExit("Section 274 direct-context figures were not found in the proposal note.")

direct_tokens = int(m.group(1).replace(",", ""))
direct_blocks = int(m.group(2))

seen = {"274"}
frontier = ["274"]
layers = []
level = 0

while frontier:
    next_nodes = []

    for section in frontier:
        for target in adj.get(section, []):
            if target not in seen:
                seen.add(target)
                next_nodes.append(target)

    if not next_nodes:
        break

    level += 1
    next_nodes = sorted(set(next_nodes), key=lambda x: (len(x), x))
    layers.append((level, next_nodes))
    frontier = next_nodes

csv_path = out / "section_274_layers.csv"

with csv_path.open("w", encoding="utf-8", newline="") as fh:
    w = csv.writer(fh)
    w.writerow(["layer", "section_count", "sections"])

    for level, sections in layers:
        w.writerow([level, len(sections), " ".join(sections)])

map_path = out / "section_274_map.mmd"
map_lines = ["flowchart LR", '  s274["§274"]']

for section in layers[0][1]:
    key = "s" + re.sub(r"[^A-Za-z0-9]", "_", section)
    map_lines.append(f'  {key}["§{section}"]')
    map_lines.append(f"  s274 --> {key}")

map_path.write_text("\n".join(map_lines) + "\n", encoding="utf-8")

brief_path = notes / "section_274_brief.md"

lines = [
    "# Section 274 bounded dependency walkthrough",
    "",
    "## Starting point",
    "",
    "Target provision: section 274.",
    f"Direct mapped context: {direct_blocks} blocks, approximately {direct_tokens:,} diagnostic tokens.",
    f"Direct section layer: {len(layers[0][1])} sections.",
    f'Complete section recursion: {int(size274["reachable_refs"]):,} sections, approximately {int(size274["approx_tokens_4chars"]):,} diagnostic tokens.',
    "",
    "## Direct section layer",
    "",
    ", ".join("§" + x for x in layers[0][1]),
    "",
    "## Recursive layer counts",
    "",
]

for level, sections in layers:
    lines.append(f"- Layer {level}: {len(sections):,} sections")

lines += [
    "",
    "## Bounded retrieval rule",
    "",
    "1. Start with the target provision.",
    "2. Admit the complete direct mapped context.",
    "3. Resolve flagged statutory references before further expansion.",
    "4. Add a complete next layer only when the full layer fits the remaining legal-context budget.",
    "5. Stop before the first complete layer that exceeds the remaining budget.",
    "6. Keep citation and structural identifiers with every admitted block.",
    "",
    "## Meeting point",
    "",
    "The direct context is modest, while unrestricted recursion is far larger. The bounded rule therefore governs recursive expansion rather than direct-reference inclusion.",
]

brief_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

print(brief_path)
print(csv_path)
print(map_path)
print("layers=" + str(len(layers)))
print("reachable=" + str(size274["reachable_refs"]))
print("approx_tokens=" + str(size274["approx_tokens_4chars"]))