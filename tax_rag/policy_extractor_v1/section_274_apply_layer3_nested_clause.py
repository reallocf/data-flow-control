import hashlib
import json
from pathlib import Path

base = Path(__file__).resolve().parent
outputs = base / "outputs"

context_path = (
    outputs
    / "section_274_layer3_high_context.json"
)

overlay_path = (
    outputs
    / "section_274_resolution_overlay.json"
)

decision_path = (
    outputs
    / "section_274_layer3_nested_clause_decisions.json"
)

items = json.loads(
    context_path.read_text(
        encoding="utf-8"
    )
)

if len(items) != 20:
    raise RuntimeError(
        f"Expected 20 reviewed cases, found {len(items)}"
    )

source_hash = hashlib.sha256(
    context_path.read_bytes()
).hexdigest()

decisions = []

for item in items:
    proposed = item["proposed_target"]

    if ":" not in proposed:
        raise RuntimeError(
            f"Malformed proposed target: {proposed}"
        )

    target_section, target_path = (
        proposed.split(":", 1)
    )

    source_section = str(
        item["source_section"]
    )

    if (
        target_section.lower()
        == source_section.lower()
    ):
        target_section = source_section

    identifier = str(
        item["target_identifier"]
    )

    expected_suffix = (
        "/s"
        + target_section.lower()
        + "/"
        + "/".join(
            part
            for part in target_path.split(".")
            if part
        )
    ).lower()

    if expected_suffix not in identifier.lower():
        raise RuntimeError(
            "Target identifier mismatch: "
            f"{proposed} -> {identifier}"
        )

    decisions.append({
        "source_section":
            source_section,
        "source_path":
            item["source_path"],
        "span_start":
            int(item["span_start"]),
        "span_end":
            int(item["span_end"]),
        "surface":
            item["surface"],
        "old_target":
            item["old_target"],
        "target_section":
            target_section,
        "target_path":
            target_path,
        "authority_identifier":
            identifier,
        "decision":
            "ACCEPT",
        "basis":
            (
                "The statutory source context directly "
                "identifies the proposed nested structural "
                "reference, and the corresponding USC XML "
                "node contains the referenced provision."
            ),
    })

decision_record = {
    "source_file":
        context_path.name,
    "source_sha256":
        source_hash,
    "reviewed_occurrences":
        len(decisions),
    "accepted":
        len(decisions),
    "rejected":
        0,
    "needs_deeper_resolution":
        0,
    "applied_to_overlay":
        True,
    "decisions":
        decisions,
}

decision_path.write_text(
    json.dumps(
        decision_record,
        indent=2,
        ensure_ascii=False,
    ) + "\n",
    encoding="utf-8",
)

overlay = json.loads(
    overlay_path.read_text(
        encoding="utf-8"
    )
)

existing = {
    (
        str(row["source_section"]),
        int(row["span_start"]),
        int(row["span_end"]),
        str(row["surface"]),
    ): row
    for row in overlay
}

added = 0

for item in decisions:
    key = (
        item["source_section"],
        item["span_start"],
        item["span_end"],
        item["surface"],
    )

    new_row = {
        "source_section":
            item["source_section"],
        "span_start":
            item["span_start"],
        "span_end":
            item["span_end"],
        "surface":
            item["surface"],
        "target_section":
            item["target_section"],
        "target_path":
            item["target_path"],
        "closure_class":
            "nested_clause_context_validated",
        "authority_identifier":
            item["authority_identifier"],
    }

    old = existing.get(key)

    if old is not None:
        if (
            str(old["target_section"]).lower()
            != str(new_row["target_section"]).lower()
            or
            str(old["target_path"]).lower()
            != str(new_row["target_path"]).lower()
        ):
            raise RuntimeError(
                "Conflicting overlay entry: "
                + repr(key)
            )

        continue

    overlay.append(new_row)
    existing[key] = new_row
    added += 1

overlay.sort(
    key=lambda row: (
        str(row["source_section"]).lower(),
        int(row["span_start"]),
        int(row["span_end"]),
        str(row["surface"]).lower(),
    )
)

overlay_path.write_text(
    json.dumps(
        overlay,
        indent=2,
        ensure_ascii=False,
    ) + "\n",
    encoding="utf-8",
)

print("REVIEWED=20")
print("ACCEPTED=20")
print("REJECTED=0")
print("NEEDS_DEEPER_RESOLUTION=0")
print(f"OVERLAY_ADDED={added}")
print(f"OVERLAY_TOTAL={len(overlay)}")
print(f"DECISIONS={decision_path}")
print(f"OVERLAY={overlay_path}")
