#!/usr/bin/env python3
"""Run the Regulation Z v2 credit-card statement benchmark."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from experiment_harness import ExperimentConfig, ExperimentRunner  # noqa: E402

from vldb_experiments.strategies.regulation_z_statement_benchmark import (  # noqa: E402
    AVAILABLE_V2_BEDROCK_MODEL_SPECS,
    DEFAULT_AWS_REGION,
    DEFAULT_CATALOG_PATH,
    DEFAULT_MAX_CONCURRENCY,
    DEFAULT_MEASURED_RUNS,
    DEFAULT_NUM_ACCOUNTS,
    DEFAULT_NUM_UPDATES,
    DEFAULT_V2_BEDROCK_MODEL_SPECS,
    DEFAULT_WARMUP_RUNS,
    RegulationZV2BenchmarkStrategy,
)
from vldb_experiments.strategies.regulation_z_v2_data import (  # noqa: E402
    load_product_terms,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the Regulation Z v2 credit-card statement benchmark."
    )
    parser.add_argument("--num-accounts", type=int, default=DEFAULT_NUM_ACCOUNTS)
    parser.add_argument(
        "--num-updates",
        "--num-inputs",
        "--num-statements",
        dest="num_updates",
        type=int,
        default=DEFAULT_NUM_UPDATES,
        help="Draft statements evaluated by every approach. Use 1 for the smallest paid smoke test.",
    )
    parser.add_argument("--runs", type=int, default=DEFAULT_MEASURED_RUNS)
    parser.add_argument("--warmups", type=int, default=DEFAULT_WARMUP_RUNS)
    parser.add_argument(
        "--catalog-path",
        type=Path,
        default=DEFAULT_CATALOG_PATH,
        help="Normalized CSV or raw TCCP workbook used to seed public product terms.",
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=DEFAULT_MAX_CONCURRENCY,
        help="Maximum simultaneous Bedrock requests per model (1-50).",
    )
    parser.add_argument(
        "--prompt-scope",
        choices=("relevant_rows", "full_database"),
        default="relevant_rows",
        help="Supply either only query-relevant rows or every row in every input table.",
    )
    parser.add_argument(
        "--prompt-shuffle-seed",
        type=int,
        default=0,
        help="Base seed used to independently shuffle each full-database table.",
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
        default=[spec["key"] for spec in DEFAULT_V2_BEDROCK_MODEL_SPECS],
    )
    for spec in AVAILABLE_V2_BEDROCK_MODEL_SPECS:
        parser.add_argument(
            f"--{spec['key'].replace('_', '-')}-model-id",
            default=spec["model_id"],
            help=f"Bedrock model ID or inference profile for {spec['display_name']}.",
        )
    parser.add_argument(
        "--output-filename",
        default="regulation_z_v2_benchmark_results.csv",
    )
    parser.add_argument(
        "--progress-filename",
        help="Incremental per-update CSV. Defaults to <output stem>.progress.csv.",
    )
    args = parser.parse_args()

    product_terms = load_product_terms(args.catalog_path)
    progress_filename = args.progress_filename or (
        f"{Path(args.output_filename).stem}.progress.csv"
    )
    progress_path = Path("./results") / progress_filename
    progress_path.unlink(missing_ok=True)
    specs_by_key = {str(spec["key"]): spec for spec in AVAILABLE_V2_BEDROCK_MODEL_SPECS}
    models = []
    for key in args.models:
        spec = dict(specs_by_key[key])
        spec["model_id"] = getattr(args, f"{key}_model_id")
        models.append(spec)

    print("Running Regulation Z v2 credit-card statement benchmark:")
    print(f"  Accounts: {args.num_accounts}")
    print(f"  Draft statements: {args.num_updates}")
    print(f"  Public product terms: {len(product_terms)} from {args.catalog_path}")
    print(f"  AWS region: {args.aws_region}")
    print("  Approaches: DFC, No Policy, " + ", ".join(m["display_name"] for m in models))
    total_requests = args.num_updates * len(models) * (args.runs + args.warmups)
    print(f"  Planned Bedrock requests: {total_requests}")
    print(f"  Maximum Bedrock concurrency: {args.max_concurrency}")
    print(f"  Prompt scope: {args.prompt_scope}")
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
            "catalog_path": str(args.catalog_path),
            "aws_region": args.aws_region,
            "aws_profile": args.aws_profile,
            "models": models,
            "max_concurrency": args.max_concurrency,
            "prompt_scope": args.prompt_scope,
            "prompt_shuffle_seed": args.prompt_shuffle_seed,
            "progress_path": str(progress_path),
        },
        output_dir="./results",
        output_filename=args.output_filename,
        verbose=True,
    )
    ExperimentRunner(RegulationZV2BenchmarkStrategy(), config).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
