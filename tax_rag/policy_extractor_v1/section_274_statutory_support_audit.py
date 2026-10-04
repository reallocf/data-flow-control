import csv
import json
from collections import defaultdict
from pathlib import Path

base = Path(__file__).resolve().parent
outputs = base / "outputs"

run_dir = (
    outputs
    / "experiments"
    / "section_274_three_mode_pilot_20261004-015917"
)

summary_path = run_dir / "experiment_summary.json"

if not summary_path.exists():
    raise FileNotFoundError(summary_path)

experiment = json.loads(
    summary_path.read_text(
        encoding="utf-8"
    )
)

if experiment.get("status") != "COMPLETE":
    raise RuntimeError(
        "Pilot experiment is not COMPLETE."
    )


# Strict statutory-support adjudication.
#
# SUPPORTED:
#   The constraint follows from the cited statutory rule.
#
# SUPPORTED_WITH_ABSTRACTION:
#   The rule is supported, but a receipt field represents
#   a bundle of statutory facts.
#
# OVERBROAD:
#   The constraint states a real statutory rule but omits
#   conditions/exceptions in a way that rejects cases not
#   actually prohibited by the statute.
#
# INCORRECT:
#   The constraint materially changes the statutory rule
#   or contains a logic error.
#
# Citation status is evaluated separately from semantics.

audit_spec = {
    "target_only": [
        {
            "verdict": "SUPPORTED",
            "citation_status": "ALIGNED",
            "family": "entertainment_disallowance",
            "basis": "26 U.S.C. § 274(a)(1)(A)",
            "issue": "",
        },
        {
            "verdict": "SUPPORTED",
            "citation_status": "ALIGNED",
            "family": "club_dues_disallowance",
            "basis": "26 U.S.C. § 274(a)(3)",
            "issue": "",
        },
        {
            "verdict": "INCORRECT",
            "citation_status": "ALIGNED",
            "family": "gift_limit",
            "basis": "26 U.S.C. § 274(b)(1)",
            "issue": (
                "The statute disallows the expense only "
                "to the extent the annual per-recipient "
                "gift total exceeds $25. The constraint "
                "sets the entire deduction to zero whenever "
                "a single gift costs more than $25 and does "
                "not represent prior gifts."
            ),
        },
        {
            "verdict": "SUPPORTED_WITH_ABSTRACTION",
            "citation_status": "ALIGNED",
            "family": "substantiation",
            "basis": "26 U.S.C. § 274(d)",
            "issue": (
                "R.substantiated is treated as an abstract "
                "boolean representing the statutory "
                "substantiation requirements."
            ),
        },
        {
            "verdict": "OVERBROAD",
            "citation_status": "ALIGNED",
            "family": "employee_food_exception",
            "basis": "26 U.S.C. § 274(e)(1)",
            "issue": (
                "Section 274(e)(1) is an exception to "
                "subsection (a); it does not itself establish "
                "a full 100-percent deduction."
            ),
        },
        {
            "verdict": "OVERBROAD",
            "citation_status": "ALIGNED",
            "family": "meal_50_percent_limit",
            "basis": "26 U.S.C. § 274(n)(1)-(2)",
            "issue": (
                "The 50-percent rule is real, but the "
                "constraint omits the statutory exceptions "
                "in § 274(n)(2), so it can reject excepted "
                "meal expenses."
            ),
        },
    ],

    "direct": [
        {
            "verdict": "SUPPORTED",
            "citation_status": "PARTIAL",
            "family": "entertainment_disallowance",
            "basis": "26 U.S.C. § 274(a)(1)",
            "issue": (
                "The constraint is supported by § 274(a)(1), "
                "but one listed supporting citation is not "
                "needed for this rule."
            ),
        },
        {
            "verdict": "INCORRECT",
            "citation_status": "ALIGNED",
            "family": "gift_limit",
            "basis": "26 U.S.C. § 274(b)(1)",
            "issue": (
                "The rule converts an excess-over-$25 "
                "limitation into total disallowance."
            ),
        },
        {
            "verdict": "SUPPORTED_WITH_ABSTRACTION",
            "citation_status": "ALIGNED",
            "family": "substantiation",
            "basis": "26 U.S.C. § 274(d)",
            "issue": (
                "R.substantiated abstracts the required "
                "records/evidence elements."
            ),
        },
        {
            "verdict": "OVERBROAD",
            "citation_status": "MISALIGNED",
            "family": "meal_50_percent_limit",
            "basis": "26 U.S.C. § 274(n)(1)-(2)",
            "issue": (
                "The 50-percent limitation arises from "
                "§ 274(n), not § 274(e)(1), and the "
                "constraint omits § 274(n)(2) exceptions."
            ),
        },
        {
            "verdict": "INCORRECT",
            "citation_status": "MISALIGNED",
            "family": "employee_food_exception",
            "basis": "26 U.S.C. § 274(e)(1)",
            "issue": (
                "The citation structure is malformed and "
                "the constraint turns an exception to the "
                "entertainment disallowance into a broad "
                "food-expense disallowance."
            ),
        },
        {
            "verdict": "OVERBROAD",
            "citation_status": "ALIGNED",
            "family": "employee_recreation_exception",
            "basis": "26 U.S.C. § 274(e)(4)",
            "issue": (
                "Section 274(e)(4) removes the subsection "
                "(a) disallowance in qualifying cases; it "
                "does not independently establish a full "
                "100-percent deduction under all Code rules."
            ),
        },
    ],

    "bounded": [
        {
            "verdict": "SUPPORTED",
            "citation_status": "PARTIAL",
            "family": "entertainment_disallowance",
            "basis": "26 U.S.C. § 274(a)(1)(A)-(B)",
            "issue": (
                "The source provision supports the rule, "
                "although the listed supporting citations "
                "include provisions not needed to establish it."
            ),
        },
        {
            "verdict": "INCORRECT",
            "citation_status": "ALIGNED",
            "family": "gift_limit",
            "basis": "26 U.S.C. § 274(b)(1)",
            "issue": (
                "The statutory $25 dollar limitation is "
                "incorrectly converted into a 0.25 deduction "
                "fraction, and the implication predicate is "
                "reversed relative to the statutory excess rule."
            ),
        },
        {
            "verdict": "OVERBROAD",
            "citation_status": "PARTIAL",
            "family": "substantiation",
            "basis": "26 U.S.C. § 274(d)",
            "issue": (
                "The constraint applies to every receipt "
                "with a missing purpose instead of only the "
                "travel, gift, and listed-property categories "
                "covered by § 274(d)."
            ),
        },
        {
            "verdict": "OVERBROAD",
            "citation_status": "MISALIGNED",
            "family": "meal_50_percent_limit",
            "basis": "26 U.S.C. § 274(n)(1)-(2)",
            "issue": (
                "The rule is attributed to § 274(e)(1), "
                "while the 50-percent limit is in § 274(n); "
                "the statutory exceptions are also omitted."
            ),
        },
        {
            "verdict": "OVERBROAD",
            "citation_status": "MISALIGNED",
            "family": "employee_food_exception",
            "basis": "26 U.S.C. § 274(e)(1)",
            "issue": (
                "The cited § 274(e)(1)(A) structure does not "
                "exist here, and an exception to subsection "
                "(a) is incorrectly represented as an "
                "affirmative full deduction."
            ),
        },
        {
            "verdict": "OVERBROAD",
            "citation_status": "MISALIGNED",
            "family": "commuting_transportation",
            "basis": "26 U.S.C. § 274(l)",
            "issue": (
                "The commuting rule is in § 274(l), not "
                "§ 274(n), and the constraint omits the "
                "statutory employee-safety exception."
            ),
        },
        {
            "verdict": "INCORRECT",
            "citation_status": "PARTIAL",
            "family": "accompanying_person_travel",
            "basis": "26 U.S.C. § 274(m)(3)",
            "issue": (
                "The predicate makes spouse/dependent/other "
                "status incompatible with the inner employee "
                "test, effectively collapsing the exception. "
                "It also omits § 274(m)(3)(C), which requires "
                "the expense otherwise to be deductible by "
                "the accompanying individual."
            ),
        },
        {
            "verdict": "OVERBROAD",
            "citation_status": "PARTIAL",
            "family": "meal_50_percent_limit",
            "basis": "26 U.S.C. § 274(n)(1)-(2)",
            "issue": (
                "The basic 50-percent limit is real, but the "
                "constraint omits the exceptions in "
                "§ 274(n)(2). It also duplicates another "
                "bounded-output constraint."
            ),
            "duplicate_of": 4,
        },
    ],
}


files = {
    "target_only":
        run_dir / "target_only_policies.json",
    "direct":
        run_dir / "direct_policies.json",
    "bounded":
        run_dir / "bounded_policies.json",
}

rows = []

for mode, path in files.items():
    policies = json.loads(
        path.read_text(
            encoding="utf-8-sig"
        )
    )

    spec = audit_spec[mode]

    if len(policies) != len(spec):
        raise RuntimeError(
            f"{mode}: expected {len(spec)} "
            f"policies, found {len(policies)}"
        )

    for index, (policy, audit) in enumerate(
        zip(policies, spec),
        start=1,
    ):
        verdict = audit["verdict"]

        counts_as_correct = verdict in {
            "SUPPORTED",
            "SUPPORTED_WITH_ABSTRACTION",
        }

        rows.append({
            "condition":
                mode,
            "policy_index":
                index,
            "source_citation":
                policy.get(
                    "source_citation",
                    "",
                ),
            "supporting_citations":
                " | ".join(
                    policy.get(
                        "supporting_citations",
                        [],
                    )
                ),
            "constraint":
                policy.get(
                    "constraint",
                    "",
                ),
            "rule_family":
                audit["family"],
            "verdict":
                verdict,
            "counts_as_correct":
                counts_as_correct,
            "citation_status":
                audit[
                    "citation_status"
                ],
            "statutory_basis":
                audit["basis"],
            "issue":
                audit["issue"],
            "duplicate_of":
                audit.get(
                    "duplicate_of",
                    "",
                ),
        })


# ------------------------------------------------------------
# Compute strict support statistics.
# ------------------------------------------------------------

summary = {}

for mode in (
    "target_only",
    "direct",
    "bounded",
):
    current = [
        row
        for row in rows
        if row["condition"] == mode
    ]

    supported = [
        row
        for row in current
        if row["counts_as_correct"]
    ]

    unique_constraints = {}

    for row in current:
        unique_constraints.setdefault(
            row["constraint"],
            row,
        )

    unique_supported = [
        row
        for row
        in unique_constraints.values()
        if row["counts_as_correct"]
    ]

    citation_aligned = sum(
        row["citation_status"]
        == "ALIGNED"
        for row in current
    )

    summary[mode] = {
        "raw_policies":
            len(current),
        "unique_constraints":
            len(
                unique_constraints
            ),
        "strict_supported_raw":
            len(supported),
        "strict_support_rate_raw":
            round(
                len(supported)
                / len(current),
                4,
            ),
        "strict_supported_unique":
            len(
                unique_supported
            ),
        "strict_support_rate_unique":
            round(
                len(unique_supported)
                / len(
                    unique_constraints
                ),
                4,
            ),
        "citation_aligned_raw":
            citation_aligned,
        "supported_rule_families":
            sorted({
                row["rule_family"]
                for row in supported
            }),
    }


report = {
    "experiment":
        run_dir.name,
    "evaluation":
        "strict_statutory_support",
    "scope":
        (
            "Candidate-level correctness against "
            "the operative Section 274 text and "
            "the candidate's cited statutory basis."
        ),
    "summary":
        summary,
    "policies":
        rows,
    "interpretation": {
        "raw_policy_count_is_not_completeness":
            True,
        "result":
            (
                "The pilot does not support the claim "
                "that more retrieved context improved "
                "candidate correctness. The bounded "
                "condition generated more candidates, "
                "but also introduced material semantic, "
                "scope, citation, and duplication errors."
            ),
        "next_evaluation":
            (
                "Construct an adjudicated representable "
                "Section 274 rule-family inventory and "
                "measure family recall/completeness "
                "separately from candidate correctness."
            ),
    },
}


json_path = (
    run_dir
    / "statutory_support_audit.json"
)

csv_path = (
    run_dir
    / "statutory_support_audit.csv"
)

md_path = (
    run_dir
    / "statutory_support_audit.md"
)


json_path.write_text(
    json.dumps(
        report,
        indent=2,
        ensure_ascii=False,
    ) + "\n",
    encoding="utf-8",
)


with csv_path.open(
    "w",
    encoding="utf-8",
    newline="",
) as handle:
    writer = csv.DictWriter(
        handle,
        fieldnames=list(
            rows[0].keys()
        ),
    )

    writer.writeheader()
    writer.writerows(rows)


with md_path.open(
    "w",
    encoding="utf-8",
) as handle:
    handle.write(
        "# Section 274 three-mode statutory support audit\n\n"
    )

    handle.write(
        "This audit evaluates candidate-level statutory "
        "support separately from policy count.\n\n"
    )

    handle.write(
        "| Condition | Raw | Unique | Strict supported | "
        "Raw support rate | Unique support rate |\n"
    )

    handle.write(
        "|---|---:|---:|---:|---:|---:|\n"
    )

    for mode in (
        "target_only",
        "direct",
        "bounded",
    ):
        item = summary[mode]

        handle.write(
            f"| {mode} "
            f"| {item['raw_policies']} "
            f"| {item['unique_constraints']} "
            f"| {item['strict_supported_raw']} "
            f"| {item['strict_support_rate_raw']:.4f} "
            f"| {item['strict_support_rate_unique']:.4f} |\n"
        )

    handle.write(
        "\n## Interpretation\n\n"
        "The pilot does not establish that larger "
        "retrieved context improves candidate correctness. "
        "The bounded condition produced more policies, but "
        "the additional output includes material statutory "
        "scope, citation, exception-handling, and duplicate "
        "errors. Completeness must therefore be measured "
        "against a separate adjudicated rule-family "
        "inventory rather than raw policy count.\n"
    )


print("AUDIT_STATUS=PASS")

for mode in (
    "target_only",
    "direct",
    "bounded",
):
    item = summary[mode]

    print(
        f"{mode.upper()}_RAW="
        f"{item['raw_policies']}"
    )

    print(
        f"{mode.upper()}_UNIQUE="
        f"{item['unique_constraints']}"
    )

    print(
        f"{mode.upper()}_STRICT_SUPPORTED="
        f"{item['strict_supported_raw']}"
    )

    print(
        f"{mode.upper()}_STRICT_SUPPORT_RATE="
        f"{item['strict_support_rate_raw']:.4f}"
    )

print(
    "NEXT=BUILD_REPRESENTABLE_RULE_FAMILY_INVENTORY"
)

print(f"JSON={json_path}")
print(f"CSV={csv_path}")
print(f"MD={md_path}")
