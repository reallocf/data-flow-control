import csv
import hashlib
import json
import re
import zipfile
from collections import Counter, defaultdict
from pathlib import Path

base = Path(__file__).resolve().parent
inputs = base / "inputs"
outputs = base / "outputs"
notes = base / "notes"

run_dir = (
    outputs
    / "experiments"
    / "section_274_three_mode_pilot_20261004-015917"
)

required_run_files = [
    run_dir / "target_only_policies.json",
    run_dir / "direct_policies.json",
    run_dir / "bounded_policies.json",
    run_dir / "experiment_summary.json",
]

for path in required_run_files:
    if not path.exists():
        raise FileNotFoundError(path)


# ============================================================
# PART 1
# Recover authoritative Section 274 source used by extractor.
# ============================================================

sections_path = (
    inputs
    / "title26_sections.jsonl"
)

if not sections_path.exists():
    raise FileNotFoundError(
        sections_path
    )

section_274 = None

for line in sections_path.read_text(
    encoding="utf-8-sig"
).splitlines():

    if not line.strip():
        continue

    row = json.loads(line)

    citation = str(
        row.get(
            "citation",
            "",
        )
    )

    if re.search(
        r"§\s*274\b",
        citation,
    ):
        section_274 = row
        break


if section_274 is None:
    raise RuntimeError(
        "Section 274 source not found."
    )


source_text = str(
    section_274.get(
        "text",
        "",
    )
)

cut_markers = [
    "Editorial Notes",
    "Source Credit",
    "Statutory Notes and Related Subsidiaries",
    "Executive Documents",
]

positions = [
    source_text.find(marker)
    for marker in cut_markers
    if source_text.find(marker) > 0
]

if positions:
    source_text = source_text[
        :min(positions)
    ]


def compact(value):
    return re.sub(
        r"\s+",
        " ",
        value,
    ).strip()


source_compact = compact(
    source_text
)


# ============================================================
# PART 2
# Build the adjudicated rule-family inventory.
#
# Representability classes:
#
# PER_RECEIPT_DIRECT
#   Can be stated from ordinary attributes of one Receipt /
#   Expense pair.
#
# PER_RECEIPT_ABSTRACTABLE
#   Can be stated only if preprocessing supplies a derived
#   receipt attribute such as substantiated, safety_required,
#   business_purpose, or exception eligibility.
#
# REQUIRES_CROSS_RECEIPT_STATE
#   Exact rule requires annual / per-recipient aggregation or
#   other state outside a single Receipt -> Expense pair.
#
# Definitions and pure regulatory-authority provisions are
# not separate policy families.
# ============================================================

inventory = [
    {
        "family_id":
            "entertainment_disallowance",
        "title":
            "Entertainment, amusement, and recreation disallowance",
        "statutory_basis":
            "26 U.S.C. § 274(a)(1), subject to § 274(e)",
        "representability":
            "PER_RECEIPT_ABSTRACTABLE",
        "evaluation_eligible":
            True,
        "anchor":
            "Entertainment, amusement, recreation, or qualified transportation fringes",
        "mandatory_modifiers": [
            "§274(e)(1)-(9) exceptions",
            "§274(f) where applicable",
        ],
        "reason":
            (
                "The basic category disallowance is per-receipt, "
                "but executable correctness requires exception "
                "eligibility facts."
            ),
    },
    {
        "family_id":
            "club_dues_disallowance",
        "title":
            "Club dues disallowance",
        "statutory_basis":
            "26 U.S.C. § 274(a)(3)",
        "representability":
            "PER_RECEIPT_DIRECT",
        "evaluation_eligible":
            True,
        "anchor":
            "Denial of deduction for club dues",
        "mandatory_modifiers": [],
        "reason":
            "A receipt type and zero deduction are sufficient.",
    },
    {
        "family_id":
            "qualified_transportation_fringe",
        "title":
            "Qualified transportation fringe disallowance",
        "statutory_basis":
            "26 U.S.C. § 274(a)(4)",
        "representability":
            "PER_RECEIPT_DIRECT",
        "evaluation_eligible":
            True,
        "anchor":
            "Qualified transportation fringes",
        "mandatory_modifiers": [],
        "reason":
            "A qualified-fringe receipt classification is sufficient.",
    },
    {
        "family_id":
            "gift_annual_limit",
        "title":
            "Annual per-recipient gift limitation",
        "statutory_basis":
            "26 U.S.C. § 274(b)",
        "representability":
            "REQUIRES_CROSS_RECEIPT_STATE",
        "evaluation_eligible":
            False,
        "anchor":
            "exceeds $25",
        "mandatory_modifiers": [
            "prior gifts to the same individual in the taxable year",
            "§274(b)(1)(A)-(B) exclusions",
            "§274(b)(2) aggregation rules",
        ],
        "reason":
            (
                "Exact enforcement requires cumulative annual "
                "per-recipient state, not only the current receipt."
            ),
    },
    {
        "family_id":
            "foreign_travel_allocation",
        "title":
            "Foreign travel business-allocation limitation",
        "statutory_basis":
            "26 U.S.C. § 274(c)",
        "representability":
            "PER_RECEIPT_ABSTRACTABLE",
        "evaluation_eligible":
            True,
        "anchor":
            "Certain foreign travel",
        "mandatory_modifiers": [
            "§274(c)(2) one-week / 25-percent exceptions",
            "§274(c)(3) domestic-travel exclusion",
        ],
        "reason":
            (
                "Requires derived trip-duration and business-use "
                "allocation attributes."
            ),
    },
    {
        "family_id":
            "substantiation",
        "title":
            "Substantiation requirement",
        "statutory_basis":
            "26 U.S.C. § 274(d), (i)",
        "representability":
            "PER_RECEIPT_ABSTRACTABLE",
        "evaluation_eligible":
            True,
        "anchor":
            "Substantiation required",
        "mandatory_modifiers": [
            "amount",
            "time/place or date/description",
            "business purpose",
            "business relationship",
            "qualified nonpersonal use vehicle exception",
        ],
        "reason":
            (
                "A derived substantiation predicate can represent "
                "the evidentiary requirements."
            ),
    },
    {
        "family_id":
            "foreign_convention_limitation",
        "title":
            "Convention outside North American area limitation",
        "statutory_basis":
            "26 U.S.C. § 274(h)(1), (3), (6)",
        "representability":
            "PER_RECEIPT_ABSTRACTABLE",
        "evaluation_eligible":
            True,
        "anchor":
            "Attendance at conventions, etc.",
        "mandatory_modifiers": [
            "business relationship test",
            "North American area definition",
            "qualifying Caribbean-country treatment",
        ],
        "reason":
            "Requires derived location and business-purpose facts.",
    },
    {
        "family_id":
            "cruise_convention_limit",
        "title":
            "Cruise-ship convention limitation and annual cap",
        "statutory_basis":
            "26 U.S.C. § 274(h)(2), (5)",
        "representability":
            "REQUIRES_CROSS_RECEIPT_STATE",
        "evaluation_eligible":
            False,
        "anchor":
            "Conventions on cruise ships",
        "mandatory_modifiers": [
            "U.S.-registered vessel",
            "qualifying ports",
            "reporting requirements",
            "$2,000 annual limitation",
        ],
        "reason":
            (
                "The annual $2,000 cap requires state across "
                "multiple convention expenses."
            ),
    },
    {
        "family_id":
            "section212_convention_disallowance",
        "title":
            "Section 212 seminar and convention disallowance",
        "statutory_basis":
            "26 U.S.C. § 274(h)(7)",
        "representability":
            "PER_RECEIPT_DIRECT",
        "evaluation_eligible":
            True,
        "anchor":
            "Seminars, etc. for section 212 purposes",
        "mandatory_modifiers": [],
        "reason":
            "A receipt purpose/category predicate is sufficient.",
    },
    {
        "family_id":
            "employee_achievement_award",
        "title":
            "Employee achievement award deduction limits",
        "statutory_basis":
            "26 U.S.C. § 274(j)",
        "representability":
            "REQUIRES_CROSS_RECEIPT_STATE",
        "evaluation_eligible":
            False,
        "anchor":
            "Employee achievement awards",
        "mandatory_modifiers": [
            "$400 nonqualified-plan annual limit",
            "$1,600 qualified-plan annual limit",
            "award definitions and special rules",
        ],
        "reason":
            (
                "Exact annual award aggregation cannot be enforced "
                "from a single receipt alone."
            ),
    },
    {
        "family_id":
            "business_meal_eligibility",
        "title":
            "Business-meal eligibility requirements",
        "statutory_basis":
            "26 U.S.C. § 274(k)",
        "representability":
            "PER_RECEIPT_ABSTRACTABLE",
        "evaluation_eligible":
            True,
        "anchor":
            "Business meals",
        "mandatory_modifiers": [
            "not lavish or extravagant",
            "taxpayer/employee presence",
            "§274(k)(2) exceptions",
        ],
        "reason":
            (
                "Requires meal-quality, presence, and exception "
                "eligibility attributes."
            ),
    },
    {
        "family_id":
            "commuting_transportation",
        "title":
            "Employer-provided commuting transportation disallowance",
        "statutory_basis":
            "26 U.S.C. § 274(l)",
        "representability":
            "PER_RECEIPT_ABSTRACTABLE",
        "evaluation_eligible":
            True,
        "anchor":
            "Transportation and commuting benefits",
        "mandatory_modifiers": [
            "employee-safety exception",
        ],
        "reason":
            (
                "The base category is simple; exactness requires "
                "a safety-necessity predicate."
            ),
    },
    {
        "family_id":
            "luxury_water_transport",
        "title":
            "Luxury water transportation limitation",
        "statutory_basis":
            "26 U.S.C. § 274(m)(1)",
        "representability":
            "PER_RECEIPT_ABSTRACTABLE",
        "evaluation_eligible":
            True,
        "anchor":
            "Luxury water transportation",
        "mandatory_modifiers": [
            "twice aggregate per-diem amount",
            "§274(m)(1)(B) exceptions",
        ],
        "reason":
            (
                "Requires a derived trip per-diem ceiling and "
                "exception classification."
            ),
    },
    {
        "family_id":
            "travel_as_education",
        "title":
            "Travel as a form of education disallowance",
        "statutory_basis":
            "26 U.S.C. § 274(m)(2)",
        "representability":
            "PER_RECEIPT_DIRECT",
        "evaluation_eligible":
            True,
        "anchor":
            "Travel as form of education",
        "mandatory_modifiers": [],
        "reason":
            "Travel purpose is sufficient for the prohibition.",
    },
    {
        "family_id":
            "accompanying_person_travel",
        "title":
            "Spouse, dependent, or accompanying-person travel limitation",
        "statutory_basis":
            "26 U.S.C. § 274(m)(3)",
        "representability":
            "PER_RECEIPT_ABSTRACTABLE",
        "evaluation_eligible":
            True,
        "anchor":
            "Travel expenses of spouse, dependent, or others",
        "mandatory_modifiers": [
            "accompanying person is employee",
            "bona fide business purpose",
            "expense otherwise deductible by accompanying person",
        ],
        "reason":
            "Requires relationship, employment, and purpose predicates.",
    },
    {
        "family_id":
            "meal_percentage_limit",
        "title":
            "Meal deduction percentage limitation",
        "statutory_basis":
            "26 U.S.C. § 274(n)",
        "representability":
            "PER_RECEIPT_ABSTRACTABLE",
        "evaluation_eligible":
            True,
        "anchor":
            "Only 50 percent of meal expenses allowed as deduction",
        "mandatory_modifiers": [
            "§274(n)(2) exceptions",
            "§274(n)(3) 80-percent rule",
        ],
        "reason":
            (
                "E.deduct can encode the percentage, but exactness "
                "requires exception and special-rate predicates."
            ),
    },
    {
        "family_id":
            "employer_convenience_meals",
        "title":
            "Meals provided for employer convenience disallowance",
        "statutory_basis":
            "26 U.S.C. § 274(o)",
        "representability":
            "PER_RECEIPT_ABSTRACTABLE",
        "evaluation_eligible":
            True,
        "anchor":
            "Meals provided at convenience of employer",
        "mandatory_modifiers": [
            "§274(e)(8) exception",
            "§274(n)(2)(C) exception",
        ],
        "reason":
            "Requires meal-source and exception-eligibility attributes.",
    },
]


# Verify every inventory family is anchored in the actual
# Section 274 main text used in this experiment.

for item in inventory:
    if item["anchor"].lower() not in source_compact.lower():
        raise RuntimeError(
            "Statutory anchor missing for "
            f"{item['family_id']}: "
            f"{item['anchor']!r}"
        )


inventory_meta = {
    "section":
        "26 U.S.C. § 274",
    "source_citation":
        section_274.get(
            "citation",
            "",
        ),
    "source_sha256":
        hashlib.sha256(
            source_text.encode(
                "utf-8"
            )
        ).hexdigest(),
    "representation_model":
        (
            "Single Receipt -> Expense implication-style DFC "
            "constraint; no SELECT, EXISTS, or WHERE."
        ),
    "global_cross_cutting_modifier":
        "26 U.S.C. § 274(f)",
    "family_count":
        len(inventory),
    "evaluation_eligible_count":
        sum(
            item[
                "evaluation_eligible"
            ]
            for item in inventory
        ),
    "inventory":
        inventory,
}


gold_json = (
    outputs
    / "section_274_gold_family_inventory.json"
)

gold_csv = (
    outputs
    / "section_274_gold_family_inventory.csv"
)

gold_md = (
    notes
    / "section_274_gold_family_inventory.md"
)


gold_json.write_text(
    json.dumps(
        inventory_meta,
        indent=2,
        ensure_ascii=False,
    ) + "\n",
    encoding="utf-8",
)


with gold_csv.open(
    "w",
    encoding="utf-8",
    newline="",
) as handle:

    fields = [
        "family_id",
        "title",
        "statutory_basis",
        "representability",
        "evaluation_eligible",
        "mandatory_modifiers",
        "reason",
    ]

    writer = csv.DictWriter(
        handle,
        fieldnames=fields,
    )

    writer.writeheader()

    for item in inventory:
        writer.writerow({
            **{
                key: item[key]
                for key in fields
                if key
                not in {
                    "mandatory_modifiers"
                }
            },
            "mandatory_modifiers":
                " | ".join(
                    item[
                        "mandatory_modifiers"
                    ]
                ),
        })


with gold_md.open(
    "w",
    encoding="utf-8",
) as handle:

    handle.write(
        "# Section 274 DFC rule-family inventory\n\n"
    )

    handle.write(
        "The inventory separates statutory rule coverage "
        "from executable-policy correctness.\n\n"
    )

    handle.write(
        "| Family | Basis | Representation | Eligible |\n"
    )
    handle.write(
        "|---|---|---|---|\n"
    )

    for item in inventory:
        handle.write(
            f"| {item['family_id']} "
            f"| {item['statutory_basis']} "
            f"| {item['representability']} "
            f"| {item['evaluation_eligible']} |\n"
        )


# ============================================================
# PART 3
# Candidate audit v2.
#
# EXACT:
#   Executable constraint preserves the operative statutory
#   rule, including mandatory conditions/exceptions needed to
#   avoid rejecting legally excepted receipts.
#
# PARTIAL:
#   Recognizes a real rule family and captures a valid base
#   rule, but omits mandatory exceptions/qualifications.
#
# INCORRECT:
#   Materially changes the rule, reverses logic, applies it to
#   unrelated receipts, or treats an exception as an
#   affirmative deduction entitlement.
# ============================================================

audit_spec = {
    "target_only": [
        {
            "family_id":
                "entertainment_disallowance",
            "semantic_status":
                "PARTIAL",
            "citation_status":
                "ALIGNED",
            "issue":
                (
                    "Captures §274(a)(1) base disallowance but "
                    "does not encode the mandatory §274(e) "
                    "exceptions."
                ),
        },
        {
            "family_id":
                "club_dues_disallowance",
            "semantic_status":
                "EXACT",
            "citation_status":
                "ALIGNED",
            "issue":
                "",
        },
        {
            "family_id":
                "gift_annual_limit",
            "semantic_status":
                "INCORRECT",
            "citation_status":
                "ALIGNED",
            "issue":
                (
                    "Converts an annual per-recipient excess "
                    "limitation into total disallowance of a "
                    "single gift above $25."
                ),
        },
        {
            "family_id":
                "substantiation",
            "semantic_status":
                "PARTIAL",
            "citation_status":
                "ALIGNED",
            "issue":
                (
                    "Uses an abstract substantiated flag, but "
                    "does not encode the qualified nonpersonal "
                    "use vehicle exception."
                ),
        },
        {
            "family_id":
                "entertainment_disallowance",
            "semantic_status":
                "INCORRECT",
            "citation_status":
                "ALIGNED",
            "issue":
                (
                    "§274(e)(1) removes a particular §274(a) "
                    "disallowance; it does not independently "
                    "guarantee E.deduct = 1."
                ),
        },
        {
            "family_id":
                "meal_percentage_limit",
            "semantic_status":
                "PARTIAL",
            "citation_status":
                "ALIGNED",
            "issue":
                (
                    "Captures the 50-percent base limit but "
                    "omits §274(n)(2) exceptions and the "
                    "§274(n)(3) 80-percent rule."
                ),
        },
    ],

    "direct": [
        {
            "family_id":
                "entertainment_disallowance",
            "semantic_status":
                "PARTIAL",
            "citation_status":
                "PARTIAL",
            "issue":
                (
                    "Captures the base entertainment rule but "
                    "does not encode all §274(e) exceptions."
                ),
        },
        {
            "family_id":
                "gift_annual_limit",
            "semantic_status":
                "INCORRECT",
            "citation_status":
                "ALIGNED",
            "issue":
                (
                    "Converts the annual excess limitation "
                    "into total single-receipt disallowance."
                ),
        },
        {
            "family_id":
                "substantiation",
            "semantic_status":
                "PARTIAL",
            "citation_status":
                "ALIGNED",
            "issue":
                (
                    "Captures the substantiation condition but "
                    "omits the qualified nonpersonal use "
                    "vehicle exception."
                ),
        },
        {
            "family_id":
                "meal_percentage_limit",
            "semantic_status":
                "PARTIAL",
            "citation_status":
                "MISALIGNED",
            "issue":
                (
                    "The constraint resembles §274(n)(1), not "
                    "the cited §274(e)(1), and omits §274(n) "
                    "exceptions."
                ),
        },
        {
            "family_id":
                "entertainment_disallowance",
            "semantic_status":
                "INCORRECT",
            "citation_status":
                "MISALIGNED",
            "issue":
                (
                    "Creates a broad food disallowance from an "
                    "exception provision and uses a malformed "
                    "§274(e)(5)(1) citation."
                ),
        },
        {
            "family_id":
                "entertainment_disallowance",
            "semantic_status":
                "INCORRECT",
            "citation_status":
                "ALIGNED",
            "issue":
                (
                    "§274(e)(4) is an exception to subsection "
                    "(a), not an independent entitlement to a "
                    "100-percent deduction."
                ),
        },
    ],

    "bounded": [
        {
            "family_id":
                "entertainment_disallowance",
            "semantic_status":
                "PARTIAL",
            "citation_status":
                "PARTIAL",
            "issue":
                (
                    "Captures the base entertainment rule but "
                    "omits mandatory §274(e) exceptions."
                ),
        },
        {
            "family_id":
                "gift_annual_limit",
            "semantic_status":
                "INCORRECT",
            "citation_status":
                "ALIGNED",
            "issue":
                (
                    "Transforms the $25 dollar threshold into "
                    "a 0.25 deduction fraction and reverses the "
                    "threshold predicate."
                ),
        },
        {
            "family_id":
                "substantiation",
            "semantic_status":
                "INCORRECT",
            "citation_status":
                "ALIGNED",
            "issue":
                (
                    "A non-null purpose is not equivalent to "
                    "the four statutory substantiation "
                    "requirements, and the constraint applies "
                    "to unrelated receipts."
                ),
        },
        {
            "family_id":
                "meal_percentage_limit",
            "semantic_status":
                "PARTIAL",
            "citation_status":
                "MISALIGNED",
            "issue":
                (
                    "Captures a 50-percent meal limit but "
                    "attributes it to §274(e)(1) and omits "
                    "§274(n)(2)-(3)."
                ),
        },
        {
            "family_id":
                "entertainment_disallowance",
            "semantic_status":
                "INCORRECT",
            "citation_status":
                "MISALIGNED",
            "issue":
                (
                    "Treats an exception to subsection (a) as "
                    "an affirmative full-deduction rule."
                ),
        },
        {
            "family_id":
                "commuting_transportation",
            "semantic_status":
                "PARTIAL",
            "citation_status":
                "MISALIGNED",
            "issue":
                (
                    "Captures the commuting disallowance but "
                    "omits the employee-safety exception and "
                    "misidentifies the source as §274(n)."
                ),
        },
        {
            "family_id":
                "accompanying_person_travel",
            "semantic_status":
                "INCORRECT",
            "citation_status":
                "PARTIAL",
            "issue":
                (
                    "The boolean condition collapses the "
                    "employee exception and omits "
                    "§274(m)(3)(C)."
                ),
        },
        {
            "family_id":
                "meal_percentage_limit",
            "semantic_status":
                "PARTIAL",
            "citation_status":
                "PARTIAL",
            "issue":
                (
                    "Repeats the 50-percent base limitation "
                    "without mandatory exceptions."
                ),
        },
    ],
}


policy_files = {
    "target_only":
        run_dir
        / "target_only_policies.json",
    "direct":
        run_dir
        / "direct_policies.json",
    "bounded":
        run_dir
        / "bounded_policies.json",
}


audit_rows = []

for condition, path in policy_files.items():

    policies = json.loads(
        path.read_text(
            encoding="utf-8-sig"
        )
    )

    spec = audit_spec[
        condition
    ]

    if len(policies) != len(spec):
        raise RuntimeError(
            f"{condition}: expected "
            f"{len(spec)} policies, "
            f"found {len(policies)}"
        )

    seen_constraints = {}

    for index, (
        policy,
        adjudication,
    ) in enumerate(
        zip(
            policies,
            spec,
        ),
        start=1,
    ):
        constraint = str(
            policy.get(
                "constraint",
                "",
            )
        ).strip()

        constraint_hash = (
            hashlib.sha256(
                constraint.encode(
                    "utf-8"
                )
            ).hexdigest()
        )

        duplicate_of = (
            seen_constraints.get(
                constraint
            )
        )

        if duplicate_of is None:
            seen_constraints[
                constraint
            ] = index

        family = next(
            item
            for item in inventory
            if item[
                "family_id"
            ]
            == adjudication[
                "family_id"
            ]
        )

        semantic_status = (
            adjudication[
                "semantic_status"
            ]
        )

        audit_rows.append({
            "condition":
                condition,
            "policy_index":
                index,
            "family_id":
                adjudication[
                    "family_id"
                ],
            "family_representability":
                family[
                    "representability"
                ],
            "evaluation_eligible":
                family[
                    "evaluation_eligible"
                ],
            "semantic_status":
                semantic_status,
            "exact_executable":
                semantic_status
                == "EXACT",
            "usable_family_signal":
                semantic_status
                in {
                    "EXACT",
                    "PARTIAL",
                },
            "family_detected":
                True,
            "citation_status":
                adjudication[
                    "citation_status"
                ],
            "duplicate_of_policy":
                duplicate_of
                if duplicate_of
                is not None
                else "",
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
                constraint,
            "constraint_sha256":
                constraint_hash,
            "issue":
                adjudication[
                    "issue"
                ],
        })


audit_v2 = {
    "experiment":
        run_dir.name,
    "version":
        2,
    "supersedes":
        "statutory_support_audit.json",
    "supersession_reason":
        (
            "Version 1 treated some base-clause matches as "
            "supported even when mandatory statutory "
            "exceptions were absent. Version 2 evaluates "
            "executable correctness under the implication-style "
            "DFC semantics."
        ),
    "status_definitions": {
        "EXACT":
            (
                "Executable constraint preserves the operative "
                "rule and mandatory exceptions/conditions."
            ),
        "PARTIAL":
            (
                "Recognizes a valid rule family and base rule "
                "but omits a mandatory qualification or exception."
            ),
        "INCORRECT":
            (
                "Materially changes, reverses, overgeneralizes, "
                "or misuses the statutory rule."
            ),
    },
    "policies":
        audit_rows,
}


audit_json = (
    run_dir
    / "statutory_support_audit_v2.json"
)

audit_csv = (
    run_dir
    / "statutory_support_audit_v2.csv"
)

audit_md = (
    run_dir
    / "statutory_support_audit_v2.md"
)


audit_json.write_text(
    json.dumps(
        audit_v2,
        indent=2,
        ensure_ascii=False,
    ) + "\n",
    encoding="utf-8",
)


with audit_csv.open(
    "w",
    encoding="utf-8",
    newline="",
) as handle:

    writer = csv.DictWriter(
        handle,
        fieldnames=list(
            audit_rows[0].keys()
        ),
    )

    writer.writeheader()
    writer.writerows(
        audit_rows
    )


# ============================================================
# PART 4
# Completeness / correctness metrics.
# ============================================================

eligible_families = {
    item["family_id"]
    for item in inventory
    if item[
        "evaluation_eligible"
    ]
}

direct_families = {
    item["family_id"]
    for item in inventory
    if item[
        "representability"
    ]
    == "PER_RECEIPT_DIRECT"
}

abstractable_families = {
    item["family_id"]
    for item in inventory
    if item[
        "representability"
    ]
    == "PER_RECEIPT_ABSTRACTABLE"
}

stateful_families = {
    item["family_id"]
    for item in inventory
    if item[
        "representability"
    ]
    == "REQUIRES_CROSS_RECEIPT_STATE"
}


condition_metrics = {}

for condition in (
    "target_only",
    "direct",
    "bounded",
):
    rows = [
        row
        for row in audit_rows
        if row[
            "condition"
        ] == condition
    ]

    unique_rows = []

    seen = set()

    for row in rows:
        constraint = row[
            "constraint"
        ]

        if constraint in seen:
            continue

        seen.add(
            constraint
        )

        unique_rows.append(
            row
        )

    detected = {
        row["family_id"]
        for row in rows
        if (
            row[
                "evaluation_eligible"
            ]
            and row[
                "family_detected"
            ]
        )
    }

    usable = {
        row["family_id"]
        for row in rows
        if (
            row[
                "evaluation_eligible"
            ]
            and row[
                "usable_family_signal"
            ]
        )
    }

    exact = {
        row["family_id"]
        for row in rows
        if (
            row[
                "evaluation_eligible"
            ]
            and row[
                "exact_executable"
            ]
        )
    }

    exact_candidates = sum(
        row[
            "exact_executable"
        ]
        for row in rows
    )

    usable_candidates = sum(
        row[
            "usable_family_signal"
        ]
        for row in rows
    )

    unique_exact_candidates = sum(
        row[
            "exact_executable"
        ]
        for row in unique_rows
    )

    unique_usable_candidates = sum(
        row[
            "usable_family_signal"
        ]
        for row in unique_rows
    )

    condition_metrics[
        condition
    ] = {
        "raw_candidates":
            len(rows),
        "unique_constraints":
            len(unique_rows),

        "eligible_family_denominator":
            len(
                eligible_families
            ),

        "detected_eligible_families":
            sorted(
                detected
            ),
        "detected_family_count":
            len(
                detected
            ),
        "family_detection_recall":
            round(
                len(detected)
                / len(
                    eligible_families
                ),
                4,
            ),

        "usable_eligible_families":
            sorted(
                usable
            ),
        "usable_family_count":
            len(
                usable
            ),
        "usable_family_recall":
            round(
                len(usable)
                / len(
                    eligible_families
                ),
                4,
            ),

        "exact_eligible_families":
            sorted(
                exact
            ),
        "exact_family_count":
            len(
                exact
            ),
        "exact_family_recall":
            round(
                len(exact)
                / len(
                    eligible_families
                ),
                4,
            ),

        "exact_candidate_count":
            exact_candidates,
        "exact_candidate_precision":
            round(
                exact_candidates
                / len(rows),
                4,
            ),

        "usable_candidate_count":
            usable_candidates,
        "usable_candidate_rate":
            round(
                usable_candidates
                / len(rows),
                4,
            ),

        "unique_exact_candidate_count":
            unique_exact_candidates,
        "unique_exact_candidate_precision":
            round(
                unique_exact_candidates
                / len(
                    unique_rows
                ),
                4,
            ),

        "unique_usable_candidate_count":
            unique_usable_candidates,
        "unique_usable_candidate_rate":
            round(
                unique_usable_candidates
                / len(
                    unique_rows
                ),
                4,
            ),

        "duplicate_candidates":
            len(rows)
            - len(
                unique_rows
            ),

        "citation_status_counts":
            dict(
                Counter(
                    row[
                        "citation_status"
                    ]
                    for row in rows
                )
            ),

        "semantic_status_counts":
            dict(
                Counter(
                    row[
                        "semantic_status"
                    ]
                    for row in rows
                )
            ),
    }


evaluation = {
    "experiment":
        run_dir.name,
    "evaluation_version":
        2,
    "api_called":
        False,
    "gold_inventory": {
        "all_rule_families":
            len(
                inventory
            ),
        "evaluation_eligible":
            len(
                eligible_families
            ),
        "per_receipt_direct":
            len(
                direct_families
            ),
        "per_receipt_abstractable":
            len(
                abstractable_families
            ),
        "requires_cross_receipt_state":
            len(
                stateful_families
            ),
    },
    "metric_definitions": {
        "family_detection_recall":
            (
                "Eligible statutory families for which the "
                "condition produced any recognizable candidate, "
                "regardless of whether the constraint was correct."
            ),
        "usable_family_recall":
            (
                "Eligible families for which at least one "
                "candidate was EXACT or PARTIAL."
            ),
        "exact_family_recall":
            (
                "Eligible families for which at least one "
                "candidate was executable-correct."
            ),
        "exact_candidate_precision":
            (
                "Fraction of raw generated candidates judged "
                "EXACT."
            ),
    },
    "conditions":
        condition_metrics,
    "interpretation": {
        "primary_observation":
            (
                "The bounded condition broadens statutory-family "
                "discovery, but broader discovery does not "
                "translate into executable-correct policies in "
                "this pilot."
            ),
        "separation_required":
            (
                "Completeness and correctness must be reported "
                "separately. Raw policy count is not a valid "
                "completeness metric."
            ),
        "pilot_limitation":
            (
                "This is one generation per condition. "
                "Stability must be measured with independent "
                "replicate generations before a final comparative "
                "claim."
            ),
    },
}


eval_json = (
    outputs
    / "section_274_completeness_correctness_v2.json"
)

eval_csv = (
    outputs
    / "section_274_completeness_correctness_v2.csv"
)

eval_md = (
    notes
    / "section_274_completeness_correctness_v2.md"
)


eval_json.write_text(
    json.dumps(
        evaluation,
        indent=2,
        ensure_ascii=False,
    ) + "\n",
    encoding="utf-8",
)


with eval_csv.open(
    "w",
    encoding="utf-8",
    newline="",
) as handle:

    fields = [
        "condition",
        "raw_candidates",
        "unique_constraints",
        "family_detection_recall",
        "usable_family_recall",
        "exact_family_recall",
        "exact_candidate_precision",
        "unique_exact_candidate_precision",
        "duplicate_candidates",
    ]

    writer = csv.DictWriter(
        handle,
        fieldnames=fields,
    )

    writer.writeheader()

    for condition in (
        "target_only",
        "direct",
        "bounded",
    ):
        row = condition_metrics[
            condition
        ]

        writer.writerow({
            "condition":
                condition,
            **{
                key: row[key]
                for key in fields
                if key != "condition"
            },
        })


with eval_md.open(
    "w",
    encoding="utf-8",
) as handle:

    handle.write(
        "# Section 274 completeness/correctness evaluation v2\n\n"
    )

    handle.write(
        "Completeness is measured at the statutory rule-family "
        "level; correctness is measured at the executable "
        "candidate level.\n\n"
    )

    handle.write(
        "| Condition | Raw | Unique | Detection recall | "
        "Usable recall | Exact recall | Exact precision |\n"
    )

    handle.write(
        "|---|---:|---:|---:|---:|---:|---:|\n"
    )

    for condition in (
        "target_only",
        "direct",
        "bounded",
    ):
        row = condition_metrics[
            condition
        ]

        handle.write(
            f"| {condition} "
            f"| {row['raw_candidates']} "
            f"| {row['unique_constraints']} "
            f"| {row['family_detection_recall']:.4f} "
            f"| {row['usable_family_recall']:.4f} "
            f"| {row['exact_family_recall']:.4f} "
            f"| {row['exact_candidate_precision']:.4f} |\n"
        )

    handle.write(
        "\n## Current interpretation\n\n"
    )

    handle.write(
        "The bounded condition reaches more eligible statutory "
        "families than the smaller-context conditions, but the "
        "extra coverage is not converted into exact executable "
        "constraints in this pilot. Long-context retrieval should "
        "therefore be evaluated as two distinct stages: family "
        "discovery and faithful policy encoding.\n\n"
    )

    handle.write(
        "The result is provisional because each context condition "
        "has only one generation run.\n"
    )


with audit_md.open(
    "w",
    encoding="utf-8",
) as handle:

    handle.write(
        "# Section 274 statutory-support audit v2\n\n"
    )

    handle.write(
        "Version 2 supersedes the earlier audit for comparative "
        "metrics. The earlier file is retained as an audit record.\n\n"
    )

    handle.write(
        "| Condition | Index | Family | Semantic | Citation | Duplicate |\n"
    )

    handle.write(
        "|---|---:|---|---|---|---:|\n"
    )

    for row in audit_rows:
        handle.write(
            f"| {row['condition']} "
            f"| {row['policy_index']} "
            f"| {row['family_id']} "
            f"| {row['semantic_status']} "
            f"| {row['citation_status']} "
            f"| {row['duplicate_of_policy'] or ''} |\n"
        )


# ============================================================
# PART 5
# Produce a concise frozen research snapshot.
# ============================================================

snapshot = {
    "research_stage":
        "three_mode_single_run_evaluated",
    "context_conditions": {
        "target_only_prompt_tokens":
            5926,
        "direct_prompt_tokens":
            30260,
        "bounded_prompt_tokens":
            671210,
        "bounded_last_complete_layer":
            3,
        "bounded_first_layer_beyond_capacity":
            4,
    },
    "gold_family_counts":
        evaluation[
            "gold_inventory"
        ],
    "results":
        condition_metrics,
    "next_required_stage":
        (
            "Independent replicate generations for each "
            "condition, followed by the same family-level "
            "and executable-correctness audit."
        ),
}


snapshot_json = (
    outputs
    / "section_274_research_snapshot.json"
)

snapshot_md = (
    notes
    / "section_274_research_snapshot.md"
)


snapshot_json.write_text(
    json.dumps(
        snapshot,
        indent=2,
        ensure_ascii=False,
    ) + "\n",
    encoding="utf-8",
)


with snapshot_md.open(
    "w",
    encoding="utf-8",
) as handle:

    handle.write(
        "# Section 274 research snapshot\n\n"
    )

    handle.write(
        "Current stage: the bounded recursive-context algorithm "
        "is executable, the complete-layer cutoff is implemented, "
        "and one target-only/direct/bounded generation has been "
        "evaluated against an adjudicated statutory family "
        "inventory.\n\n"
    )

    handle.write(
        "A final comparative claim requires independent "
        "replicate generations.\n"
    )


# ============================================================
# PART 6
# Package only the evaluation artifacts needed for review.
# ============================================================

package = (
    base
    / "section_274_gold_evaluation_pack.zip"
)

package_files = [
    gold_json,
    gold_csv,
    gold_md,
    audit_json,
    audit_csv,
    audit_md,
    eval_json,
    eval_csv,
    eval_md,
    snapshot_json,
    snapshot_md,
    run_dir / "target_only_policies.json",
    run_dir / "direct_policies.json",
    run_dir / "bounded_policies.json",
    run_dir / "experiment_summary.json",
]


if package.exists():
    package.unlink()


with zipfile.ZipFile(
    package,
    "w",
    compression=zipfile.ZIP_DEFLATED,
) as archive:

    for path in package_files:
        if not path.exists():
            raise FileNotFoundError(
                path
            )

        archive.write(
            path,
            arcname=path.name,
        )


# ============================================================
# Compact output.
# ============================================================

print("GOLD_STATUS=PASS")
print(
    f"GOLD_TOTAL={len(inventory)}"
)
print(
    "EVALUATION_ELIGIBLE="
    f"{len(eligible_families)}"
)
print(
    "PER_RECEIPT_DIRECT="
    f"{len(direct_families)}"
)
print(
    "PER_RECEIPT_ABSTRACTABLE="
    f"{len(abstractable_families)}"
)
print(
    "REQUIRES_CROSS_RECEIPT_STATE="
    f"{len(stateful_families)}"
)

print()

for condition in (
    "target_only",
    "direct",
    "bounded",
):
    row = condition_metrics[
        condition
    ]

    prefix = condition.upper()

    print(
        f"{prefix}_RAW="
        f"{row['raw_candidates']}"
    )

    print(
        f"{prefix}_UNIQUE="
        f"{row['unique_constraints']}"
    )

    print(
        f"{prefix}_DETECTION_RECALL="
        f"{row['family_detection_recall']:.4f}"
    )

    print(
        f"{prefix}_USABLE_RECALL="
        f"{row['usable_family_recall']:.4f}"
    )

    print(
        f"{prefix}_EXACT_RECALL="
        f"{row['exact_family_recall']:.4f}"
    )

    print(
        f"{prefix}_EXACT_PRECISION="
        f"{row['exact_candidate_precision']:.4f}"
    )

    print()


print(
    "AUDIT_V2_SUPERSEDES="
    "statutory_support_audit.json"
)

print("API_CALLED=0")

print(
    "NEXT="
    "REPLICATE_THREE_MODE_AND_AUDIT_STABILITY"
)

print(
    f"PACKAGE={package}"
)
