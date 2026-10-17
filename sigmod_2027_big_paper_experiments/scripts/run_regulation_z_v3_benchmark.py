#!/usr/bin/env python3
"""Run the Regulation Z v3 disputed-balance allocation benchmark."""

from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from experiment_harness import ExperimentConfig, ExperimentRunner  # noqa: E402

from vldb_experiments.strategies.regulation_z_payment_allocation_benchmark import (  # noqa: E402
    AVAILABLE_V2_BEDROCK_MODEL_SPECS,
    DEFAULT_AWS_REGION,
    DEFAULT_MAX_CONCURRENCY,
    DEFAULT_MEASURED_RUNS,
    DEFAULT_NUM_ACCOUNTS,
    DEFAULT_NUM_UPDATES,
    DEFAULT_V3_BEDROCK_MODEL_SPECS,
    DEFAULT_WARMUP_RUNS,
    DEFAULT_WORKLOAD_SEED,
    RegulationZV3BenchmarkStrategy,
    audit_regulation_z_v3_policies,
    generate_payment_allocation_workload,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the Regulation Z v3 disputed-balance allocation benchmark."
    )
    parser.add_argument(
        "--audit-policies",
        action="store_true",
        help="Also execute every policy separately and save its rejection counts.",
    )
    parser.add_argument(
        "--dfc-only",
        action="store_true",
        help="Run only the 31-policy DFC path; no model calls or no-policy baseline.",
    )
    parser.add_argument(
        "--no-ties",
        action="store_true",
        help="Generate distinct cash-advance APRs so the selected contributor is unambiguous.",
    )
    parser.add_argument("--num-accounts", type=int, default=DEFAULT_NUM_ACCOUNTS)
    parser.add_argument(
        "--skip-first-updates",
        type=int,
        default=0,
        help="Skip this many initial cases for every model; retain the same workload seed.",
    )
    parser.add_argument(
        "--num-updates",
        "--num-inputs",
        "--num-allocations",
        dest="num_updates",
        type=int,
        default=DEFAULT_NUM_UPDATES,
        help="Completed allocations audited by every approach. Use 1 for a paid smoke test.",
    )
    parser.add_argument("--workload-seed", type=int, default=DEFAULT_WORKLOAD_SEED)
    parser.add_argument("--runs", type=int, default=DEFAULT_MEASURED_RUNS)
    parser.add_argument("--warmups", type=int, default=DEFAULT_WARMUP_RUNS)
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=DEFAULT_MAX_CONCURRENCY,
        help="Maximum simultaneous Bedrock requests shared across all models (1-50).",
    )
    parser.add_argument(
        "--aws-region",
        default=os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", DEFAULT_AWS_REGION)),
    )
    parser.add_argument("--aws-profile", default=os.getenv("AWS_PROFILE"))
    parser.add_argument(
        "--models",
        nargs="+",
        choices=[spec["key"] for spec in AVAILABLE_V2_BEDROCK_MODEL_SPECS],
        default=[spec["key"] for spec in DEFAULT_V3_BEDROCK_MODEL_SPECS],
    )
    for spec in AVAILABLE_V2_BEDROCK_MODEL_SPECS:
        parser.add_argument(
            f"--{spec['key'].replace('_', '-')}-model-id",
            default=spec["model_id"],
            help=f"Bedrock model ID or inference profile for {spec['display_name']}.",
        )
    parser.add_argument(
        "--output-filename",
        default="regulation_z_v3_benchmark_results.csv",
    )
    parser.add_argument(
        "--progress-filename",
        help="Incremental per-allocation CSV. Defaults to <output stem>.progress.csv.",
    )
    args = parser.parse_args()
    if not 0 <= args.skip_first_updates < args.num_updates:
        parser.error("--skip-first-updates must be between 0 and num-updates minus 1")

    progress_filename = args.progress_filename or (
        f"{Path(args.output_filename).stem}.progress.csv"
    )
    progress_path = Path("./results") / progress_filename
    progress_path.unlink(missing_ok=True)
    specs_by_key = {str(spec["key"]): spec for spec in AVAILABLE_V2_BEDROCK_MODEL_SPECS}
    models = []
    for key in [] if args.dfc_only else args.models:
        spec = dict(specs_by_key[key])
        spec["model_id"] = getattr(args, f"{key}_model_id")
        models.append(spec)

    print("Running Regulation Z v3 disputed-balance allocation benchmark:")
    print(f"  Accounts in database: {args.num_accounts}")
    print(f"  Completed allocations audited: {args.num_updates}")
    print(f"  Workload seed: {args.workload_seed}")
    print(f"  APR ties: {not args.no_ties}")
    print(f"  AWS region: {args.aws_region}")
    print(
        "  Approaches: Rust DFC (31 policies)"
        + ("" if args.dfc_only else ", No Policy, " + ", ".join(m["display_name"] for m in models))
    )
    total_requests = (
        (args.num_updates - args.skip_first_updates) * len(models) * (args.runs + args.warmups)
    )
    print(f"  Planned Bedrock requests: {total_requests}")
    print(f"  Maximum Bedrock concurrency: {args.max_concurrency}")
    print(f"  Incremental progress: {progress_path}")
    print("  Sampling: provider defaults")
    nonce_models = [
        str(model["display_name"]) for model in models if model.get("prompt_nonce", False)
    ]
    print(
        "  Cache control: unique prompt nonce for "
        + (", ".join(nonce_models) if nonce_models else "none")
    )

    config = ExperimentConfig(
        num_executions=args.runs,
        num_warmup_runs=args.warmups,
        database_config={"database": ":memory:"},
        strategy_config={
            "num_accounts": args.num_accounts,
            "num_updates": args.num_updates,
            "workload_seed": args.workload_seed,
            "no_ties": args.no_ties,
            "aws_region": args.aws_region,
            "aws_profile": args.aws_profile,
            "models": models,
            "dfc_only": args.dfc_only,
            "max_concurrency": args.max_concurrency,
            "skip_model_updates": {
                str(model["key"]): list(range(1, args.skip_first_updates + 1)) for model in models
            },
            "progress_path": str(progress_path),
        },
        output_dir="./results",
        output_filename=args.output_filename,
        verbose=True,
    )
    ExperimentRunner(RegulationZV3BenchmarkStrategy(), config).run()
    if args.audit_policies:
        audit = audit_regulation_z_v3_policies(
            generate_payment_allocation_workload(
                num_accounts=args.num_accounts,
                num_updates=args.num_updates,
                workload_seed=args.workload_seed,
                no_ties=args.no_ties,
            )
        )
        audit_path = Path("./results") / f"{Path(args.output_filename).stem}.policy-audit.csv"
        with audit_path.open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(audit[0]))
            writer.writeheader()
            writer.writerows(audit)
        print(f"  Per-policy audit: {audit_path}")
        if not all(row["matches_expected_ids"] for row in audit):
            raise RuntimeError("Policy-isolation audit did not match expected allocation IDs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
