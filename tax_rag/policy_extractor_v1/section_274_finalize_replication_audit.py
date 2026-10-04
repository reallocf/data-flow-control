import csv
import itertools
import json
import statistics
import zipfile
from collections import Counter
from pathlib import Path

base = Path(__file__).resolve().parent
outputs = base / "outputs"
notes = base / "notes"

run_dir = (
    outputs
    / "experiments"
    / "section_274_replication_20261004-023532"
)

queue_path = (
    run_dir
    / "replicate_audit_queue.json"
)

replication_summary_path = (
    run_dir
    / "replication_summary.json"
)

gold_path = (
    outputs
    / "section_274_gold_family_inventory.json"
)

for path in (
    queue_path,
    replication_summary_path,
    gold_path,
):
    if not path.exists():
        raise FileNotFoundError(path)


def load_json(path):
    return json.loads(
        path.read_text(
            encoding="utf-8-sig"
        )
    )


queue = load_json(
    queue_path
)

replication_summary = load_json(
    replication_summary_path
)

gold = load_json(
    gold_path
)

rows = queue["rows"]

unreviewed = [
    row
    for row in rows
    if row.get(
        "semantic_status"
    ) == "UNREVIEWED"
]

if len(unreviewed) != 37:
    raise RuntimeError(
        "Expected 37 unreviewed R2/R3 "
        f"candidates; found {len(unreviewed)}"
    )


# ============================================================
# Manual statutory adjudication for replicates 2 and 3.
#
# EXACT:
#   Executable constraint preserves the operative rule.
#
# PARTIAL:
#   Detects a real rule and useful base condition but omits
#   a mandatory qualification / exception.
#
# INCORRECT:
#   Materially changes, reverses, overgeneralizes, or
#   misuses the statutory rule.
# ============================================================

D = {}


def decision(
    rep,
    condition,
    index,
    family,
    semantic,
    citation,
    issue,
):
    D[
        (
            rep,
            condition,
            index,
        )
    ] = {
        "family_id":
            family,
        "semantic_status":
            semantic,
        "citation_status":
            citation,
        "issue":
            issue,
    }


# -------------------------
# Replicate 2: target-only
# -------------------------

decision(
    2,
    "target_only",
    1,
    "entertainment_disallowance",
    "PARTIAL",
    "ALIGNED",
    (
        "The entertainment component omits the mandatory "
        "Section 274(e) exceptions. The same constraint also "
        "contains an exact club-dues component."
    ),
)

decision(
    2,
    "target_only",
    2,
    "gift_annual_limit",
    "INCORRECT",
    "ALIGNED",
    (
        "The candidate uses prior-gift state but disallows "
        "the whole receipt after the threshold is crossed. "
        "The statute disallows only the excess, and the "
        "$4 promotional-item exclusion has additional "
        "conditions not represented here."
    ),
)

decision(
    2,
    "target_only",
    3,
    "foreign_travel_allocation",
    "INCORRECT",
    "ALIGNED",
    (
        "The rule correctly notices the one-week and "
        "25-percent thresholds but disallows the whole "
        "travel expense instead of the nonbusiness-allocable "
        "portion."
    ),
)

decision(
    2,
    "target_only",
    4,
    "substantiation",
    "PARTIAL",
    "ALIGNED",
    (
        "The covered categories and abstract substantiation "
        "predicate are useful, but the qualified nonpersonal "
        "use vehicle exception is omitted."
    ),
)

decision(
    2,
    "target_only",
    5,
    "employee_achievement_award",
    "INCORRECT",
    "ALIGNED",
    (
        "The candidate uses annual award state but converts "
        "the statutory deduction limits into total "
        "disallowance and omits required award definitions "
        "and special rules."
    ),
)

decision(
    2,
    "target_only",
    6,
    "business_meal_eligibility",
    "INCORRECT",
    "ALIGNED",
    (
        "Section 274(k)(1) requires both non-lavish expense "
        "and taxpayer/employee presence. The candidate "
        "triggers only when lavishness and absence occur "
        "simultaneously."
    ),
)

decision(
    2,
    "target_only",
    7,
    "meal_percentage_limit",
    "PARTIAL",
    "ALIGNED",
    (
        "The 50-percent base rule is captured, but "
        "Section 274(n)(2) exceptions and the "
        "Section 274(n)(3) 80-percent rule are omitted."
    ),
)


# -------------------
# Replicate 2: direct
# -------------------

decision(
    2,
    "direct",
    1,
    "entertainment_disallowance",
    "PARTIAL",
    "PARTIAL",
    (
        "The base entertainment disallowance is captured "
        "but the Section 274(e) exceptions are not encoded."
    ),
)

decision(
    2,
    "direct",
    2,
    "gift_annual_limit",
    "INCORRECT",
    "ALIGNED",
    (
        "The candidate treats a current gift over $25 as "
        "fully nondeductible and omits annual per-recipient "
        "aggregation."
    ),
)

decision(
    2,
    "direct",
    3,
    "substantiation",
    "PARTIAL",
    "ALIGNED",
    (
        "The substantiation predicate is useful, but the "
        "qualified nonpersonal use vehicle exception is "
        "omitted."
    ),
)

decision(
    2,
    "direct",
    4,
    "meal_percentage_limit",
    "PARTIAL",
    "MISALIGNED",
    (
        "The 50-percent rule arises from Section 274(n), "
        "not Section 274(e)(1), and its exceptions and "
        "special rate are omitted."
    ),
)

decision(
    2,
    "direct",
    5,
    "entertainment_disallowance",
    "INCORRECT",
    "PARTIAL",
    (
        "Section 274(e)(1) is an exception to the "
        "entertainment disallowance, not an affirmative "
        "100-percent deduction entitlement. Section "
        "274(e)(5) is unrelated to this predicate."
    ),
)

decision(
    2,
    "direct",
    6,
    "qualified_transportation_fringe",
    "EXACT",
    "ALIGNED",
    (
        "The constraint directly encodes the Section "
        "274(a)(4) disallowance; Section 132(f) supplies "
        "the qualified-fringe definition."
    ),
)


# -------------------------
# Replicate 3: target-only
# -------------------------

decision(
    3,
    "target_only",
    1,
    "gift_annual_limit",
    "INCORRECT",
    "ALIGNED",
    (
        "The constraint disallows every gift rather than "
        "only the annual per-recipient excess over $25."
    ),
)

decision(
    3,
    "target_only",
    2,
    "entertainment_disallowance",
    "PARTIAL",
    "ALIGNED",
    (
        "The base entertainment disallowance is captured "
        "but the Section 274(e) exceptions are omitted."
    ),
)

decision(
    3,
    "target_only",
    3,
    "meal_percentage_limit",
    "PARTIAL",
    "MISALIGNED",
    (
        "The constraint resembles the Section 274(n)(1) "
        "50-percent rule, while the citation points to "
        "Section 274(k)(1). Section 274(n)(2)-(3) is also "
        "omitted."
    ),
)


# -------------------
# Replicate 3: direct
# -------------------

decision(
    3,
    "direct",
    1,
    "entertainment_disallowance",
    "PARTIAL",
    "PARTIAL",
    (
        "The base entertainment disallowance is captured "
        "but the Section 274(e) exceptions are omitted."
    ),
)

decision(
    3,
    "direct",
    2,
    "gift_annual_limit",
    "INCORRECT",
    "ALIGNED",
    (
        "The candidate treats a current gift over $25 as "
        "fully nondeductible and omits annual per-recipient "
        "aggregation."
    ),
)

decision(
    3,
    "direct",
    3,
    "substantiation",
    "PARTIAL",
    "ALIGNED",
    (
        "The substantiation predicate is useful, but the "
        "qualified nonpersonal use vehicle exception is "
        "omitted."
    ),
)

decision(
    3,
    "direct",
    4,
    "meal_percentage_limit",
    "PARTIAL",
    "MISALIGNED",
    (
        "The 50-percent rule belongs to Section 274(n), "
        "not Section 274(e)(2), and its exceptions and "
        "special rate are omitted."
    ),
)

decision(
    3,
    "direct",
    5,
    "entertainment_disallowance",
    "INCORRECT",
    "ALIGNED",
    (
        "The candidate treats the Section 274(e)(1) "
        "exception as an affirmative full-deduction rule "
        "and rejects other food/beverage cases."
    ),
)


# ------------------------------------------------------------
# Bounded R2 and R3 are exact constraint repeats of bounded R1.
# Reuse the already completed R1 semantic adjudication.
# ------------------------------------------------------------

r1_bounded = {
    int(row["policy_index"]):
        row
    for row in rows
    if (
        int(row["replicate"]) == 1
        and row["condition"] == "bounded"
    )
}

if len(r1_bounded) != 8:
    raise RuntimeError(
        "Expected 8 replicate-1 bounded candidates."
    )

for rep in (2, 3):
    for index in range(1, 9):
        prior = r1_bounded[
            index
        ]

        decision(
            rep,
            "bounded",
            index,
            prior["family_id"],
            prior["semantic_status"],
            prior["citation_status"],
            prior["issue"],
        )


if len(D) != 37:
    raise RuntimeError(
        f"Expected 37 adjudications; found {len(D)}"
    )


# ============================================================
# Additional candidate -> family edge.
#
# R2 target-only candidate 1 combines two statutory families:
# entertainment and club dues.
# ============================================================

extra_edges = {
    (
        2,
        "target_only",
        1,
    ): [
        {
            "family_id":
                "club_dues_disallowance",
            "semantic_status":
                "EXACT",
            "citation_status":
                "ALIGNED",
            "basis":
                "26 U.S.C. § 274(a)(3)",
        }
    ],
}


# ============================================================
# Apply the adjudications.
# ============================================================

candidate_rows = []

for original in rows:
    row = dict(original)

    rep = int(
        row["replicate"]
    )

    condition = row[
        "condition"
    ]

    index = int(
        row["policy_index"]
    )

    if rep in (2, 3):
        key = (
            rep,
            condition,
            index,
        )

        if key not in D:
            raise RuntimeError(
                "Missing adjudication: "
                + repr(key)
            )

        item = D[key]

        row[
            "family_id"
        ] = item[
            "family_id"
        ]

        row[
            "semantic_status"
        ] = item[
            "semantic_status"
        ]

        row[
            "citation_status"
        ] = item[
            "citation_status"
        ]

        row[
            "issue"
        ] = item[
            "issue"
        ]

    if (
        row.get(
            "semantic_status"
        )
        == "UNREVIEWED"
    ):
        raise RuntimeError(
            "Unreviewed candidate remains: "
            f"R{rep} {condition} #{index}"
        )

    row[
        "exact_executable"
    ] = (
        row[
            "semantic_status"
        ]
        == "EXACT"
    )

    row[
        "usable_family_signal"
    ] = (
        row[
            "semantic_status"
        ]
        in {
            "EXACT",
            "PARTIAL",
        }
    )

    candidate_rows.append(
        row
    )


if len(candidate_rows) != 57:
    raise RuntimeError(
        "Expected 57 total candidates; "
        f"found {len(candidate_rows)}"
    )


# ============================================================
# Family-edge representation.
# ============================================================

gold_lookup = {
    item["family_id"]:
        item
    for item
    in gold["inventory"]
}

eligible_families = {
    item["family_id"]
    for item
    in gold["inventory"]
    if item[
        "evaluation_eligible"
    ]
}

if len(eligible_families) != 14:
    raise RuntimeError(
        "Expected 14 evaluation-eligible families."
    )


family_edges = []

for row in candidate_rows:

    key = (
        int(
            row["replicate"]
        ),
        row["condition"],
        int(
            row["policy_index"]
        ),
    )

    primary = row[
        "family_id"
    ]

    if primary not in gold_lookup:
        raise RuntimeError(
            "Unknown family: "
            + primary
        )

    family_edges.append({
        "replicate":
            key[0],
        "condition":
            key[1],
        "policy_index":
            key[2],
        "family_id":
            primary,
        "edge_type":
            "primary",
        "semantic_status":
            row[
                "semantic_status"
            ],
        "citation_status":
            row[
                "citation_status"
            ],
        "evaluation_eligible":
            gold_lookup[
                primary
            ][
                "evaluation_eligible"
            ],
    })

    for extra in extra_edges.get(
        key,
        [],
    ):
        family = extra[
            "family_id"
        ]

        family_edges.append({
            "replicate":
                key[0],
            "condition":
                key[1],
            "policy_index":
                key[2],
            "family_id":
                family,
            "edge_type":
                "additional",
            "semantic_status":
                extra[
                    "semantic_status"
                ],
            "citation_status":
                extra[
                    "citation_status"
                ],
            "evaluation_eligible":
                gold_lookup[
                    family
                ][
                    "evaluation_eligible"
                ],
        })


# ============================================================
# Per-run evaluation.
# ============================================================

conditions = (
    "target_only",
    "direct",
    "bounded",
)

replicates = (
    1,
    2,
    3,
)

per_run = {}


for rep in replicates:
    for condition in conditions:

        current_candidates = [
            row
            for row
            in candidate_rows
            if (
                int(
                    row["replicate"]
                ) == rep
                and row[
                    "condition"
                ] == condition
            )
        ]

        current_edges = [
            row
            for row
            in family_edges
            if (
                int(
                    row["replicate"]
                ) == rep
                and row[
                    "condition"
                ] == condition
                and row[
                    "evaluation_eligible"
                ]
            )
        ]

        detected = {
            row["family_id"]
            for row
            in current_edges
        }

        usable = {
            row["family_id"]
            for row
            in current_edges
            if row[
                "semantic_status"
            ]
            in {
                "EXACT",
                "PARTIAL",
            }
        }

        exact = {
            row["family_id"]
            for row
            in current_edges
            if row[
                "semantic_status"
            ]
            == "EXACT"
        }

        exact_candidates = sum(
            row[
                "semantic_status"
            ] == "EXACT"
            for row
            in current_candidates
        )

        usable_candidates = sum(
            row[
                "semantic_status"
            ]
            in {
                "EXACT",
                "PARTIAL",
            }
            for row
            in current_candidates
        )

        unique_constraints = {
            " ".join(
                str(
                    row[
                        "constraint"
                    ]
                ).split()
            )
            for row
            in current_candidates
        }

        per_run[
            (
                rep,
                condition,
            )
        ] = {
            "replicate":
                rep,
            "condition":
                condition,
            "raw_candidates":
                len(
                    current_candidates
                ),
            "unique_constraints":
                len(
                    unique_constraints
                ),
            "detected_families":
                sorted(
                    detected
                ),
            "detected_family_count":
                len(
                    detected
                ),
            "family_detection_recall":
                len(
                    detected
                )
                / len(
                    eligible_families
                ),
            "usable_families":
                sorted(
                    usable
                ),
            "usable_family_count":
                len(
                    usable
                ),
            "usable_family_recall":
                len(
                    usable
                )
                / len(
                    eligible_families
                ),
            "exact_families":
                sorted(
                    exact
                ),
            "exact_family_count":
                len(
                    exact
                ),
            "exact_family_recall":
                len(
                    exact
                )
                / len(
                    eligible_families
                ),
            "exact_candidate_count":
                exact_candidates,
            "exact_candidate_precision":
                (
                    exact_candidates
                    / len(
                        current_candidates
                    )
                ),
            "usable_candidate_count":
                usable_candidates,
            "usable_candidate_rate":
                (
                    usable_candidates
                    / len(
                        current_candidates
                    )
                ),
            "citation_status_counts":
                dict(
                    Counter(
                        row[
                            "citation_status"
                        ]
                        for row
                        in current_candidates
                    )
                ),
            "semantic_status_counts":
                dict(
                    Counter(
                        row[
                            "semantic_status"
                        ]
                        for row
                        in current_candidates
                    )
                ),
        }


# ============================================================
# Stability at semantic-family level.
# ============================================================

def jaccard(left, right):
    union = (
        left
        | right
    )

    if not union:
        return 1.0

    return (
        len(
            left
            & right
        )
        / len(
            union
        )
    )


def family_stability(
    condition,
    field,
):
    sets = [
        set(
            per_run[
                (
                    rep,
                    condition,
                )
            ][field]
        )
        for rep
        in replicates
    ]

    pairwise = [
        jaccard(
            left,
            right,
        )
        for left, right
        in itertools.combinations(
            sets,
            2,
        )
    ]

    return {
        "union_count":
            len(
                set.union(
                    *sets
                )
            ),
        "all_three_count":
            len(
                set.intersection(
                    *sets
                )
            ),
        "at_least_two_count":
            sum(
                sum(
                    family
                    in current
                    for current
                    in sets
                )
                >= 2
                for family
                in set.union(
                    *sets
                )
            ),
        "mean_pairwise_jaccard":
            sum(
                pairwise
            )
            / len(
                pairwise
            ),
        "pairwise_jaccard":
            pairwise,
    }


# ============================================================
# Aggregate over the three replicates.
# ============================================================

aggregate = {}


for condition in conditions:

    current = [
        per_run[
            (
                rep,
                condition,
            )
        ]
        for rep
        in replicates
    ]

    total_candidates = sum(
        row[
            "raw_candidates"
        ]
        for row
        in current
    )

    total_exact = sum(
        row[
            "exact_candidate_count"
        ]
        for row
        in current
    )

    total_usable = sum(
        row[
            "usable_candidate_count"
        ]
        for row
        in current
    )

    raw_constraint_stability = (
        replication_summary[
            "constraint_stability"
        ][condition][
            "mean_pairwise_jaccard"
        ]
    )

    aggregate[
        condition
    ] = {
        "replicate_candidate_counts":
            [
                row[
                    "raw_candidates"
                ]
                for row
                in current
            ],

        "mean_family_detection_recall":
            statistics.mean(
                row[
                    "family_detection_recall"
                ]
                for row
                in current
            ),

        "mean_usable_family_recall":
            statistics.mean(
                row[
                    "usable_family_recall"
                ]
                for row
                in current
            ),

        "mean_exact_family_recall":
            statistics.mean(
                row[
                    "exact_family_recall"
                ]
                for row
                in current
            ),

        "pooled_exact_candidate_precision":
            (
                total_exact
                / total_candidates
            ),

        "pooled_usable_candidate_rate":
            (
                total_usable
                / total_candidates
            ),

        "pooled_candidates":
            total_candidates,

        "pooled_exact_candidates":
            total_exact,

        "pooled_usable_candidates":
            total_usable,

        "exact_constraint_mean_jaccard":
            raw_constraint_stability,

        "family_detection_stability":
            family_stability(
                condition,
                "detected_families",
            ),

        "usable_family_stability":
            family_stability(
                condition,
                "usable_families",
            ),

        "exact_family_stability":
            family_stability(
                condition,
                "exact_families",
            ),
    }


# ============================================================
# Assertions for this frozen three-replicate experiment.
# ============================================================

expected = {
    "target_only": {
        "detection": 0.2857,
        "usable": 0.2381,
        "exact_recall": 0.0476,
        "exact_precision": 0.0625,
        "usable_rate": 0.5625,
        "constraint_jaccard": 0.0000,
        "family_jaccard": 0.5000,
    },
    "direct": {
        "detection": 0.2381,
        "usable": 0.2381,
        "exact_recall": 0.0238,
        "exact_precision": 0.0588,
        "usable_rate": 0.5882,
        "constraint_jaccard": 0.4266,
        "family_jaccard": 0.8333,
    },
    "bounded": {
        "detection": 0.3571,
        "usable": 0.2143,
        "exact_recall": 0.0000,
        "exact_precision": 0.0000,
        "usable_rate": 0.5000,
        "constraint_jaccard": 1.0000,
        "family_jaccard": 1.0000,
    },
}


for condition in conditions:

    row = aggregate[
        condition
    ]

    checks = {
        "detection":
            row[
                "mean_family_detection_recall"
            ],
        "usable":
            row[
                "mean_usable_family_recall"
            ],
        "exact_recall":
            row[
                "mean_exact_family_recall"
            ],
        "exact_precision":
            row[
                "pooled_exact_candidate_precision"
            ],
        "usable_rate":
            row[
                "pooled_usable_candidate_rate"
            ],
        "constraint_jaccard":
            row[
                "exact_constraint_mean_jaccard"
            ],
        "family_jaccard":
            row[
                "family_detection_stability"
            ][
                "mean_pairwise_jaccard"
            ],
    }

    for key, value in checks.items():

        if round(
            value,
            4,
        ) != expected[
            condition
        ][key]:

            raise RuntimeError(
                f"{condition} {key} "
                f"unexpected: "
                f"{value:.4f}"
            )


# ============================================================
# Persist candidate audit and family edges.
# ============================================================

audit_json = (
    outputs
    / "section_274_replication_audit_v3.json"
)

audit_csv = (
    outputs
    / "section_274_replication_audit_v3.csv"
)

edge_csv = (
    outputs
    / "section_274_replication_family_edges_v3.csv"
)


audit_json.write_text(
    json.dumps(
        {
            "status":
                "COMPLETE",
            "version":
                3,
            "replicates":
                3,
            "candidates":
                candidate_rows,
            "family_edges":
                family_edges,
            "notes": [
                (
                    "Replicate 1 adjudication was inherited "
                    "from statutory_support_audit_v2."
                ),
                (
                    "Replicates 2 and 3 were adjudicated "
                    "against the frozen Section 274 family "
                    "inventory."
                ),
                (
                    "A candidate may map to more than one "
                    "statutory family when its constraint "
                    "combines independently identifiable rules."
                ),
            ],
        },
        indent=2,
        ensure_ascii=False,
    ) + "\n",
    encoding="utf-8",
)


candidate_fields = [
    "replicate",
    "condition",
    "policy_index",
    "constraint",
    "source_citation",
    "supporting_citations",
    "valid_dfc_subset",
    "family_id",
    "semantic_status",
    "citation_status",
    "issue",
    "exact_executable",
    "usable_family_signal",
]


with audit_csv.open(
    "w",
    encoding="utf-8",
    newline="",
) as handle:

    writer = csv.DictWriter(
        handle,
        fieldnames=candidate_fields,
        extrasaction="ignore",
    )

    writer.writeheader()

    for row in candidate_rows:

        out = dict(row)

        if isinstance(
            out.get(
                "supporting_citations"
            ),
            list,
        ):
            out[
                "supporting_citations"
            ] = " | ".join(
                out[
                    "supporting_citations"
                ]
            )

        writer.writerow(
            out
        )


with edge_csv.open(
    "w",
    encoding="utf-8",
    newline="",
) as handle:

    writer = csv.DictWriter(
        handle,
        fieldnames=list(
            family_edges[0].keys()
        ),
    )

    writer.writeheader()
    writer.writerows(
        family_edges
    )


# ============================================================
# Save metrics.
# ============================================================

metrics_json = (
    outputs
    / "section_274_replication_metrics_v3.json"
)

metrics_csv = (
    outputs
    / "section_274_replication_metrics_v3.csv"
)


result = {
    "status":
        "THREE_REPLICATE_EVALUATION_COMPLETE",
    "section":
        "26 U.S.C. § 274",
    "model":
        "gpt-4.1-mini",
    "replicates":
        3,
    "gold_family_count":
        gold[
            "family_count"
        ],
    "evaluation_eligible_family_count":
        len(
            eligible_families
        ),
    "per_run":
        {
            (
                f"R{rep}_"
                f"{condition}"
            ):
                per_run[
                    (
                        rep,
                        condition,
                    )
                ]
            for rep
            in replicates
            for condition
            in conditions
        },
    "aggregate":
        aggregate,
    "interpretation": {
        "bounded_discovery":
            (
                "Bounded context has the highest mean "
                "eligible-family detection recall."
            ),
        "bounded_encoding":
            (
                "The broader bounded context does not "
                "increase usable-family recall or exact "
                "executable correctness in this experiment."
            ),
        "bounded_stability":
            (
                "Bounded output is perfectly stable at both "
                "exact normalized-constraint level and "
                "detected-family level across the three runs."
            ),
        "central_result":
            (
                "More recursive statutory context increases "
                "rule-family discovery breadth, but the "
                "additional breadth is not converted into "
                "faithful executable DFC constraints. The "
                "bounded condition is especially notable "
                "because its errors are stable rather than "
                "run-to-run noise."
            ),
        "scope_limit":
            (
                "This result is specific to Section 274, "
                "gpt-4.1-mini, the frozen prompt, and three "
                "replicates. It should be presented as a "
                "case-study result rather than a general "
                "model claim."
            ),
    },
    "api_called":
        False,
}


metrics_json.write_text(
    json.dumps(
        result,
        indent=2,
        ensure_ascii=False,
    ) + "\n",
    encoding="utf-8",
)


fields = [
    "condition",
    "mean_family_detection_recall",
    "mean_usable_family_recall",
    "mean_exact_family_recall",
    "pooled_exact_candidate_precision",
    "pooled_usable_candidate_rate",
    "exact_constraint_mean_jaccard",
    "family_detection_mean_jaccard",
    "usable_family_mean_jaccard",
    "family_union",
    "family_all_three",
]


with metrics_csv.open(
    "w",
    encoding="utf-8",
    newline="",
) as handle:

    writer = csv.DictWriter(
        handle,
        fieldnames=fields,
    )

    writer.writeheader()

    for condition in conditions:

        row = aggregate[
            condition
        ]

        writer.writerow({
            "condition":
                condition,

            "mean_family_detection_recall":
                round(
                    row[
                        "mean_family_detection_recall"
                    ],
                    4,
                ),

            "mean_usable_family_recall":
                round(
                    row[
                        "mean_usable_family_recall"
                    ],
                    4,
                ),

            "mean_exact_family_recall":
                round(
                    row[
                        "mean_exact_family_recall"
                    ],
                    4,
                ),

            "pooled_exact_candidate_precision":
                round(
                    row[
                        "pooled_exact_candidate_precision"
                    ],
                    4,
                ),

            "pooled_usable_candidate_rate":
                round(
                    row[
                        "pooled_usable_candidate_rate"
                    ],
                    4,
                ),

            "exact_constraint_mean_jaccard":
                round(
                    row[
                        "exact_constraint_mean_jaccard"
                    ],
                    4,
                ),

            "family_detection_mean_jaccard":
                round(
                    row[
                        "family_detection_stability"
                    ][
                        "mean_pairwise_jaccard"
                    ],
                    4,
                ),

            "usable_family_mean_jaccard":
                round(
                    row[
                        "usable_family_stability"
                    ][
                        "mean_pairwise_jaccard"
                    ],
                    4,
                ),

            "family_union":
                row[
                    "family_detection_stability"
                ][
                    "union_count"
                ],

            "family_all_three":
                row[
                    "family_detection_stability"
                ][
                    "all_three_count"
                ],
        })


# ============================================================
# Presentation-ready research note.
# ============================================================

note_path = (
    notes
    / "section_274_three_replicate_results_v3.md"
)


with note_path.open(
    "w",
    encoding="utf-8",
) as handle:

    handle.write(
        "# Section 274 three-replicate result\n\n"
    )

    handle.write(
        "The experiment compares three fixed prompt "
        "conditions while holding the model and policy "
        "instructions constant:\n\n"
    )

    handle.write(
        "- target-only: 5,926 prompt tokens\n"
        "- direct references: 30,260 prompt tokens\n"
        "- bounded recursive context: 671,210 prompt tokens\n\n"
    )

    handle.write(
        "The bounded context contains every complete "
        "dependency layer through Layer 3; the complete "
        "Layer 4 frontier exceeds the statutory-context "
        "budget.\n\n"
    )

    handle.write(
        "| Condition | Family discovery recall | "
        "Usable-family recall | Exact-family recall | "
        "Exact candidate precision | Exact-string stability | "
        "Family stability |\n"
    )

    handle.write(
        "|---|---:|---:|---:|---:|---:|---:|\n"
    )

    for condition in conditions:

        row = aggregate[
            condition
        ]

        handle.write(
            f"| {condition} "
            f"| {row['mean_family_detection_recall']:.4f} "
            f"| {row['mean_usable_family_recall']:.4f} "
            f"| {row['mean_exact_family_recall']:.4f} "
            f"| {row['pooled_exact_candidate_precision']:.4f} "
            f"| {row['exact_constraint_mean_jaccard']:.4f} "
            f"| "
            f"{row['family_detection_stability']['mean_pairwise_jaccard']:.4f} "
            f"|\n"
        )

    handle.write(
        "\n## Result\n\n"
    )

    handle.write(
        "Bounded recursive context produced the broadest "
        "statutory-family discovery: mean eligible-family "
        "detection recall was 0.3571, compared with 0.2857 "
        "for target-only and 0.2381 for direct context. "
        "However, bounded usable-family recall was 0.2143 "
        "and no bounded candidate was judged executable-exact.\n\n"
    )

    handle.write(
        "The stability result is particularly informative. "
        "The bounded condition reproduced the same seven "
        "unique normalized constraints in all three runs "
        "(exact-string mean Jaccard 1.0000), and its detected "
        "family set was also identical across all three runs. "
        "Thus the observed encoding errors are systematic "
        "under this frozen prompt rather than merely "
        "generation instability.\n\n"
    )

    handle.write(
        "This supports a two-stage interpretation for the "
        "Section 274 case study: recursive retrieval improves "
        "legal-rule discovery breadth, while faithful "
        "translation of the retrieved law into executable "
        "DFC constraints remains the limiting stage.\n\n"
    )

    handle.write(
        "The evidence is a Section 274 case study with "
        "three generations per condition and should not be "
        "generalized to other statutes or models without "
        "additional experiments.\n"
    )


# ============================================================
# Research snapshot.
# ============================================================

snapshot_path = (
    outputs
    / "section_274_research_snapshot_v3.json"
)


snapshot = {
    "stage":
        "three_replicate_evaluation_complete",

    "retrieval": {
        "bounded_last_complete_layer":
            3,
        "bounded_first_layer_beyond_capacity":
            4,
        "target_only_prompt_tokens":
            5926,
        "direct_prompt_tokens":
            30260,
        "bounded_prompt_tokens":
            671210,
    },

    "replication": {
        "replicates_per_condition":
            3,

        "target_only_policy_counts":
            [
                6,
                7,
                3,
            ],

        "direct_policy_counts":
            [
                6,
                6,
                5,
            ],

        "bounded_policy_counts":
            [
                8,
                8,
                8,
            ],
    },

    "aggregate_metrics":
        aggregate,

    "main_case_study_result":
        (
            "Bounded recursive retrieval increases "
            "statutory-family discovery breadth but does not "
            "improve faithful executable policy encoding. "
            "Bounded errors are highly reproducible across "
            "three generations."
        ),

    "next_stage":
        (
            "Freeze the Section 274 experiment and update "
            "the Charles presentation with the algorithm, "
            "concrete Section 274 example, token boundary, "
            "and three-replicate result."
        ),

    "api_called":
        False,
}


snapshot_path.write_text(
    json.dumps(
        snapshot,
        indent=2,
        ensure_ascii=False,
    ) + "\n",
    encoding="utf-8",
)


# ============================================================
# Package the final evaluation artifacts.
# ============================================================

package = (
    base
    / "section_274_replication_evaluation_v3.zip"
)

if package.exists():
    package.unlink()


package_files = [
    audit_json,
    audit_csv,
    edge_csv,
    metrics_json,
    metrics_csv,
    note_path,
    snapshot_path,
    gold_path,
    replication_summary_path,
]


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
# Compact terminal output.
# ============================================================

print(
    "AUDIT_STATUS=COMPLETE"
)

print(
    "TOTAL_CANDIDATES=57"
)

print(
    "R2_R3_ADJUDICATED=37/37"
)

print(
    "ELIGIBLE_GOLD_FAMILIES=14"
)

print()

for condition in conditions:

    row = aggregate[
        condition
    ]

    prefix = condition.upper()

    print(
        f"{prefix}_MEAN_DETECTION_RECALL="
        f"{row['mean_family_detection_recall']:.4f}"
    )

    print(
        f"{prefix}_MEAN_USABLE_RECALL="
        f"{row['mean_usable_family_recall']:.4f}"
    )

    print(
        f"{prefix}_MEAN_EXACT_RECALL="
        f"{row['mean_exact_family_recall']:.4f}"
    )

    print(
        f"{prefix}_POOLED_EXACT_PRECISION="
        f"{row['pooled_exact_candidate_precision']:.4f}"
    )

    print(
        f"{prefix}_POOLED_USABLE_RATE="
        f"{row['pooled_usable_candidate_rate']:.4f}"
    )

    print(
        f"{prefix}_EXACT_STRING_JACCARD="
        f"{row['exact_constraint_mean_jaccard']:.4f}"
    )

    print(
        f"{prefix}_FAMILY_JACCARD="
        f"{row['family_detection_stability']['mean_pairwise_jaccard']:.4f}"
    )

    print()


print(
    "BOUNDED_SYSTEMATIC_RESULT="
    "HIGHER_DISCOVERY_BUT_NO_EXACT_EXECUTABLE_GAIN"
)

print(
    "API_CALLED=0"
)

print(
    "NEXT="
    "FREEZE_SECTION274_AND_UPDATE_CHARLES_PRESENTATION"
)

print(
    f"METRICS={metrics_json}"
)

print(
    f"NOTE={note_path}"
)

print(
    f"PACKAGE={package}"
)
