import csv
import hashlib
import itertools
import json
import os
import shutil
import subprocess
import sys
import time
import zipfile
from collections import Counter
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
import tiktoken


base = Path(__file__).resolve().parent
outputs = base / "outputs"

pilot_dir = (
    outputs
    / "experiments"
    / "section_274_three_mode_pilot_20261004-015917"
)

extract_path = base / "extract_v1.py"

required = [
    extract_path,
    pilot_dir / "target_only_policies.json",
    pilot_dir / "direct_policies.json",
    pilot_dir / "bounded_policies.json",
    pilot_dir / "experiment_summary.json",
    outputs / "section_274_target_only_prompt.txt",
    outputs / "section_274_direct_prompt.txt",
    outputs / "section_274_bounded_prompt.txt",
    outputs / "section_274_api_prompt_preflight.json",
    outputs / "section_274_gold_family_inventory.json",
    outputs / "section_274_completeness_correctness_v2.json",
]

for path in required:
    if not path.exists():
        raise FileNotFoundError(path)


load_dotenv(base / ".env")

if not os.getenv("OPENAI_API_KEY"):
    raise RuntimeError(
        "OPENAI_API_KEY is unavailable."
    )


stamp = datetime.now().strftime(
    "%Y%m%d-%H%M%S"
)

experiment_dir = (
    outputs
    / "experiments"
    / f"section_274_replication_{stamp}"
)

experiment_dir.mkdir(
    parents=True,
    exist_ok=True,
)

checkpoint = (
    base
    / "checkpoints"
    / f"before_section274_replication_{stamp}"
)

checkpoint.mkdir(
    parents=True,
    exist_ok=True,
)

shutil.copy2(
    extract_path,
    checkpoint / "extract_v1.py",
)


# ============================================================
# 1. Freeze and verify the three prompt conditions.
# ============================================================

enc = tiktoken.get_encoding(
    "o200k_base"
)

prompt_paths = {
    "target_only":
        outputs
        / "section_274_target_only_prompt.txt",
    "direct":
        outputs
        / "section_274_direct_prompt.txt",
    "bounded":
        outputs
        / "section_274_bounded_prompt.txt",
}


def prompt_info(path):
    data = path.read_bytes()

    if b"\r\n" in data:
        raise RuntimeError(
            f"CRLF found in canonical prompt: {path}"
        )

    text = data.decode("utf-8")

    return {
        "bytes":
            len(data),
        "tokens":
            len(enc.encode(text)),
        "sha256":
            hashlib.sha256(data).hexdigest(),
    }


prompt_manifest = {
    key: prompt_info(path)
    for key, path
    in prompt_paths.items()
}


expected_tokens = {
    "target_only": 5926,
    "direct": 30260,
    "bounded": 671210,
}

for condition, expected in expected_tokens.items():
    actual = prompt_manifest[
        condition
    ]["tokens"]

    if actual != expected:
        raise RuntimeError(
            f"{condition} prompt changed: "
            f"{actual} != {expected}"
        )


print("PROMPT_FREEZE=PASS")

for condition in (
    "target_only",
    "direct",
    "bounded",
):
    item = prompt_manifest[
        condition
    ]

    print(
        f"{condition.upper()}_PROMPT_TOKENS="
        f"{item['tokens']}"
    )

    print(
        f"{condition.upper()}_PROMPT_SHA256="
        f"{item['sha256']}"
    )


# ============================================================
# 2. Import replicate 1 from completed pilot.
# ============================================================

replicate_dirs = {
    1: pilot_dir,
}

condition_files = {
    "target_only":
        "target_only_policies.json",
    "direct":
        "direct_policies.json",
    "bounded":
        "bounded_policies.json",
}

api_files = {
    "target_only":
        "target_only_api.json",
    "direct":
        "direct_api.json",
    "bounded":
        "bounded_api.json",
}


# ============================================================
# 3. Run replicates 2 and 3.
#
# Fixed Latin-square-like ordering:
#
# R1: target_only -> direct -> bounded
# R2: direct -> bounded -> target_only
# R3: bounded -> target_only -> direct
#
# This avoids using the same condition position in every run.
# ============================================================

orders = {
    2: [
        ("direct", "direct"),
        ("bounded", "bounded"),
        ("target_only", "none"),
    ],
    3: [
        ("bounded", "bounded"),
        ("target_only", "none"),
        ("direct", "direct"),
    ],
}


for replicate in (2, 3):

    rep_dir = (
        experiment_dir
        / f"replicate_{replicate:02d}"
    )

    rep_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    replicate_dirs[
        replicate
    ] = rep_dir

    print()
    print(
        f"=== REPLICATE {replicate} ==="
    )

    for position, (
        condition,
        context_mode,
    ) in enumerate(
        orders[replicate],
        start=1,
    ):
        output_path = (
            rep_dir
            / condition_files[
                condition
            ]
        )

        api_path = (
            rep_dir
            / api_files[
                condition
            ]
        )

        console_path = (
            rep_dir
            / f"{condition}_console.txt"
        )

        env = os.environ.copy()

        env.update({
            "MODEL":
                "gpt-4.1-mini",
            "TARGETS":
                "274",
            "CONTEXT_MODE":
                context_mode,
            "DRY_RUN":
                "0",
            "RUN_JUDGE":
                "0",
            "OUTPUT":
                str(output_path),
            "API_LOG_OUTPUT":
                str(api_path),
        })

        print(
            f"R{replicate}_POSITION="
            f"{position}"
        )

        print(
            f"R{replicate}_RUNNING="
            f"{condition}"
        )

        started = (
            time.perf_counter()
        )

        result = subprocess.run(
            [
                sys.executable,
                str(extract_path),
            ],
            cwd=base,
            env=env,
            text=True,
            capture_output=True,
        )

        elapsed = (
            time.perf_counter()
            - started
        )

        console_path.write_text(
            (
                "STDOUT\n"
                + result.stdout
                + "\n\nSTDERR\n"
                + result.stderr
            ),
            encoding="utf-8",
        )

        print(
            f"R{replicate}_{condition.upper()}_"
            f"RETURN_CODE={result.returncode}"
        )

        print(
            f"R{replicate}_{condition.upper()}_"
            f"WALL_SECONDS={elapsed:.3f}"
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"Replicate {replicate}, "
                f"{condition} failed. "
                f"See {console_path}"
            )

        if not output_path.exists():
            raise RuntimeError(
                "Expected output absent: "
                + str(output_path)
            )

        if not api_path.exists():
            raise RuntimeError(
                "API metadata absent: "
                + str(api_path)
            )


# ============================================================
# 4. Parse every replicate.
# ============================================================

def load_json(path):
    return json.loads(
        path.read_text(
            encoding="utf-8-sig"
        )
    )


def normalize_constraint(value):
    return " ".join(
        str(value).split()
    )


def policy_signature(row):
    return {
        "constraint":
            normalize_constraint(
                row.get(
                    "constraint",
                    "",
                )
            ),
        "source_citation":
            str(
                row.get(
                    "source_citation",
                    "",
                )
            ).strip(),
        "supporting_citations":
            tuple(
                str(value).strip()
                for value
                in row.get(
                    "supporting_citations",
                    []
                )
            ),
    }


run_rows = []
audit_queue = []


for replicate in (1, 2, 3):

    rep_dir = (
        replicate_dirs[
            replicate
        ]
    )

    for condition in (
        "target_only",
        "direct",
        "bounded",
    ):
        policy_path = (
            rep_dir
            / condition_files[
                condition
            ]
        )

        api_path = (
            rep_dir
            / api_files[
                condition
            ]
        )

        policies = load_json(
            policy_path
        )

        api_data = load_json(
            api_path
        )

        calls = api_data.get(
            "calls",
            [],
        )

        if len(calls) != 1:
            raise RuntimeError(
                f"Expected exactly one API call "
                f"for R{replicate} {condition}; "
                f"found {len(calls)}"
            )

        usage = calls[0].get(
            "usage"
        )

        if not isinstance(
            usage,
            dict,
        ):
            raise RuntimeError(
                "Structured API usage missing "
                f"for R{replicate} {condition}"
            )

        input_tokens = int(
            usage.get(
                "input_tokens",
                -1,
            )
        )

        output_tokens = int(
            usage.get(
                "output_tokens",
                -1,
            )
        )

        expected_api_input = (
            prompt_manifest[
                condition
            ]["tokens"]
            + 128
        )

        if (
            input_tokens
            != expected_api_input
        ):
            raise RuntimeError(
                "API input-token drift for "
                f"R{replicate} {condition}: "
                f"{input_tokens} != "
                f"{expected_api_input}"
            )

        constraints = [
            normalize_constraint(
                row.get(
                    "constraint",
                    "",
                )
            )
            for row in policies
            if normalize_constraint(
                row.get(
                    "constraint",
                    "",
                )
            )
        ]

        unique_constraints = sorted(
            set(constraints)
        )

        policy_data = (
            policy_path.read_bytes()
        )

        run_rows.append({
            "replicate":
                replicate,
            "condition":
                condition,
            "policy_count":
                len(policies),
            "unique_constraint_count":
                len(
                    unique_constraints
                ),
            "duplicate_count":
                len(policies)
                - len(
                    unique_constraints
                ),
            "valid_dfc_count":
                sum(
                    bool(
                        row.get(
                            "valid_dfc_subset"
                        )
                    )
                    for row in policies
                ),
            "input_tokens":
                input_tokens,
            "output_tokens":
                output_tokens,
            "api_elapsed_seconds":
                calls[0].get(
                    "elapsed_seconds"
                ),
            "response_id":
                calls[0].get(
                    "response_id"
                ),
            "policy_sha256":
                hashlib.sha256(
                    policy_data
                ).hexdigest(),
        })

        for index, policy in enumerate(
            policies,
            start=1,
        ):
            sig = policy_signature(
                policy
            )

            audit_queue.append({
                "replicate":
                    replicate,
                "condition":
                    condition,
                "policy_index":
                    index,
                "constraint":
                    sig[
                        "constraint"
                    ],
                "source_citation":
                    sig[
                        "source_citation"
                    ],
                "supporting_citations":
                    list(
                        sig[
                            "supporting_citations"
                        ]
                    ),
                "valid_dfc_subset":
                    bool(
                        policy.get(
                            "valid_dfc_subset"
                        )
                    ),
                "family_id":
                    "",
                "semantic_status":
                    "UNREVIEWED",
                "citation_status":
                    "UNREVIEWED",
                "issue":
                    "",
            })


# ============================================================
# 5. Exact-constraint stability.
# ============================================================

constraint_sets = {}

for replicate in (1, 2, 3):

    for condition in (
        "target_only",
        "direct",
        "bounded",
    ):
        path = (
            replicate_dirs[
                replicate
            ]
            / condition_files[
                condition
            ]
        )

        policies = load_json(
            path
        )

        constraint_sets[
            (
                replicate,
                condition,
            )
        ] = {
            normalize_constraint(
                row.get(
                    "constraint",
                    "",
                )
            )
            for row in policies
            if normalize_constraint(
                row.get(
                    "constraint",
                    "",
                )
            )
        }


def jaccard(left, right):

    union = left | right

    if not union:
        return 1.0

    return (
        len(left & right)
        / len(union)
    )


stability_rows = []


for condition in (
    "target_only",
    "direct",
    "bounded",
):

    pair_scores = []

    for left, right in (
        (1, 2),
        (1, 3),
        (2, 3),
    ):
        a = constraint_sets[
            (
                left,
                condition,
            )
        ]

        b = constraint_sets[
            (
                right,
                condition,
            )
        ]

        score = jaccard(
            a,
            b,
        )

        pair_scores.append(
            score
        )

        stability_rows.append({
            "condition":
                condition,
            "replicate_a":
                left,
            "replicate_b":
                right,
            "intersection":
                len(
                    a & b
                ),
            "union":
                len(
                    a | b
                ),
            "jaccard":
                round(
                    score,
                    4,
                ),
        })


# ============================================================
# 6. Persistent constraints and discovery across replicates.
# ============================================================

condition_stability = {}


for condition in (
    "target_only",
    "direct",
    "bounded",
):

    sets = [
        constraint_sets[
            (
                replicate,
                condition,
            )
        ]
        for replicate
        in (1, 2, 3)
    ]

    all_three = (
        sets[0]
        & sets[1]
        & sets[2]
    )

    union = (
        sets[0]
        | sets[1]
        | sets[2]
    )

    frequency = Counter()

    for constraint in union:
        frequency[
            constraint
        ] = sum(
            constraint
            in current
            for current in sets
        )

    condition_stability[
        condition
    ] = {
        "replicate_policy_counts":
            [
                next(
                    row[
                        "policy_count"
                    ]
                    for row
                    in run_rows
                    if (
                        row[
                            "replicate"
                        ]
                        == replicate
                        and row[
                            "condition"
                        ]
                        == condition
                    )
                )
                for replicate
                in (1, 2, 3)
            ],

        "replicate_unique_counts":
            [
                len(
                    constraint_sets[
                        (
                            replicate,
                            condition,
                        )
                    ]
                )
                for replicate
                in (1, 2, 3)
            ],

        "union_unique_constraints":
            len(
                union
            ),

        "present_in_all_three":
            len(
                all_three
            ),

        "present_in_two_or_more":
            sum(
                count >= 2
                for count
                in frequency.values()
            ),

        "mean_pairwise_jaccard":
            round(
                sum(
                    row[
                        "jaccard"
                    ]
                    for row
                    in stability_rows
                    if row[
                        "condition"
                    ]
                    == condition
                )
                / 3,
                4,
            ),

        "constraints_all_three":
            sorted(
                all_three
            ),
    }


# ============================================================
# 7. Create compact review queue.
#
# Replicate 1 was already manually adjudicated. Mark it as such
# and leave R2/R3 explicitly UNREVIEWED.
# ============================================================

pilot_audit_path = (
    pilot_dir
    / "statutory_support_audit_v2.json"
)

if pilot_audit_path.exists():

    pilot_audit = load_json(
        pilot_audit_path
    )

    pilot_lookup = {
        (
            row[
                "condition"
            ],
            int(
                row[
                    "policy_index"
                ]
            ),
        ):
            row
        for row
        in pilot_audit[
            "policies"
        ]
    }

    for row in audit_queue:

        if row[
            "replicate"
        ] != 1:
            continue

        prior = pilot_lookup.get(
            (
                row[
                    "condition"
                ],
                row[
                    "policy_index"
                ],
            )
        )

        if prior is None:
            continue

        row[
            "family_id"
        ] = prior.get(
            "family_id",
            "",
        )

        row[
            "semantic_status"
        ] = prior.get(
            "semantic_status",
            "UNREVIEWED",
        )

        row[
            "citation_status"
        ] = prior.get(
            "citation_status",
            "UNREVIEWED",
        )

        row[
            "issue"
        ] = prior.get(
            "issue",
            "",
        )


# ============================================================
# 8. Research summary.
# ============================================================

summary = {
    "status":
        "REPLICATION_GENERATION_COMPLETE",
    "model":
        "gpt-4.1-mini",
    "target":
        "274",
    "temperature":
        0,
    "run_judge":
        False,
    "total_replicates":
        3,
    "new_api_calls":
        6,
    "condition_orders": {
        "replicate_1": [
            "target_only",
            "direct",
            "bounded",
        ],
        "replicate_2": [
            "direct",
            "bounded",
            "target_only",
        ],
        "replicate_3": [
            "bounded",
            "target_only",
            "direct",
        ],
    },
    "prompt_manifest":
        prompt_manifest,
    "api_overhead_tokens":
        128,
    "runs":
        run_rows,
    "constraint_stability":
        condition_stability,
    "pairwise_stability":
        stability_rows,
    "interpretation_scope":
        (
            "These stability metrics compare exact normalized "
            "constraints only. Semantic family stability and "
            "statutory correctness require adjudication of "
            "replicates 2 and 3."
        ),
    "next_required_stage":
        (
            "Adjudicate replicate-2 and replicate-3 candidates "
            "against the frozen Section 274 gold-family inventory, "
            "then aggregate family recall and executable precision "
            "across all three replicates."
        ),
}


summary_path = (
    experiment_dir
    / "replication_summary.json"
)

summary_path.write_text(
    json.dumps(
        summary,
        indent=2,
        ensure_ascii=False,
    ) + "\n",
    encoding="utf-8",
)


queue_path = (
    experiment_dir
    / "replicate_audit_queue.json"
)

queue_path.write_text(
    json.dumps(
        {
            "gold_inventory":
                str(
                    outputs
                    / "section_274_gold_family_inventory.json"
                ),
            "rows":
                audit_queue,
        },
        indent=2,
        ensure_ascii=False,
    ) + "\n",
    encoding="utf-8",
)


runs_csv = (
    experiment_dir
    / "replication_runs.csv"
)

with runs_csv.open(
    "w",
    encoding="utf-8",
    newline="",
) as handle:

    writer = csv.DictWriter(
        handle,
        fieldnames=list(
            run_rows[0].keys()
        ),
    )

    writer.writeheader()
    writer.writerows(
        run_rows
    )


stability_csv = (
    experiment_dir
    / "pairwise_constraint_stability.csv"
)

with stability_csv.open(
    "w",
    encoding="utf-8",
    newline="",
) as handle:

    writer = csv.DictWriter(
        handle,
        fieldnames=list(
            stability_rows[0].keys()
        ),
    )

    writer.writeheader()
    writer.writerows(
        stability_rows
    )


# ============================================================
# 9. Freeze supporting research artifacts into replication dir.
# ============================================================

support_files = [
    outputs
    / "section_274_gold_family_inventory.json",

    outputs
    / "section_274_completeness_correctness_v2.json",

    outputs
    / "section_274_research_snapshot.json",

    outputs
    / "section_274_api_prompt_preflight.json",
]

for src in support_files:
    if src.exists():
        shutil.copy2(
            src,
            experiment_dir
            / src.name,
        )


if pilot_audit_path.exists():
    shutil.copy2(
        pilot_audit_path,
        experiment_dir
        / "replicate_01_statutory_support_audit_v2.json",
    )


# Copy replicate-1 policy outputs so the package is self-contained.

rep1_dir = (
    experiment_dir
    / "replicate_01"
)

rep1_dir.mkdir(
    parents=True,
    exist_ok=True,
)

for condition in (
    "target_only",
    "direct",
    "bounded",
):
    for filename in (
        condition_files[
            condition
        ],
        api_files[
            condition
        ],
    ):
        src = (
            pilot_dir
            / filename
        )

        if src.exists():
            shutil.copy2(
                src,
                rep1_dir
                / filename,
            )


# ============================================================
# 10. Package for manual semantic/statutory review.
# ============================================================

package = (
    base
    / f"section_274_replication_pack_{stamp}.zip"
)

with zipfile.ZipFile(
    package,
    "w",
    compression=zipfile.ZIP_DEFLATED,
) as archive:

    for path in sorted(
        experiment_dir.rglob("*")
    ):
        if path.is_file():
            archive.write(
                path,
                arcname=str(
                    path.relative_to(
                        experiment_dir
                    )
                ),
            )


# ============================================================
# 11. Compact terminal report.
# ============================================================

print()
print(
    "REPLICATION_STATUS="
    "GENERATION_COMPLETE"
)

print(
    "TOTAL_REPLICATES=3"
)

print(
    "NEW_API_CALLS=6"
)

print(
    "API_INPUT_TOKEN_CHECK=PASS"
)

for condition in (
    "target_only",
    "direct",
    "bounded",
):
    item = condition_stability[
        condition
    ]

    prefix = condition.upper()

    print()
    print(
        f"{prefix}_POLICY_COUNTS="
        + ",".join(
            str(value)
            for value
            in item[
                "replicate_policy_counts"
            ]
        )
    )

    print(
        f"{prefix}_UNIQUE_COUNTS="
        + ",".join(
            str(value)
            for value
            in item[
                "replicate_unique_counts"
            ]
        )
    )

    print(
        f"{prefix}_UNION_UNIQUE="
        f"{item['union_unique_constraints']}"
    )

    print(
        f"{prefix}_ALL3_EXACT="
        f"{item['present_in_all_three']}"
    )

    print(
        f"{prefix}_AT_LEAST2_EXACT="
        f"{item['present_in_two_or_more']}"
    )

    print(
        f"{prefix}_MEAN_JACCARD="
        f"{item['mean_pairwise_jaccard']:.4f}"
    )


unreviewed = sum(
    row[
        "semantic_status"
    ]
    == "UNREVIEWED"
    for row
    in audit_queue
)

print()
print(
    f"UNREVIEWED_CANDIDATES={unreviewed}"
)

print(
    "NEXT="
    "MANUAL_REPLICATE_STATUTORY_AUDIT"
)

print(
    f"SUMMARY={summary_path}"
)

print(
    f"AUDIT_QUEUE={queue_path}"
)

print(
    f"PACKAGE={package}"
)

print(
    f"CHECKPOINT={checkpoint}"
)
