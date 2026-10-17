#!/usr/bin/env python3
"""Run the Regulation Z ability-to-repay application benchmark."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from experiment_harness import ExperimentConfig, ExperimentRunner  # noqa: E402

from vldb_experiments.strategies.llm_validation_common import (  # noqa: E402
    DEFAULT_BEDROCK_MODEL_SPECS,
)
from vldb_experiments.strategies.regulation_z_benchmark import (  # noqa: E402
    DEFAULT_AWS_REGION,
    DEFAULT_MAX_CONCURRENCY,
    DEFAULT_MEASURED_RUNS,
    DEFAULT_NUM_APPLICATIONS,
    DEFAULT_NUM_UPDATES,
    DEFAULT_WARMUP_RUNS,
    RegulationZBenchmarkStrategy,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the Regulation Z ability-to-repay application benchmark."
    )
    parser.add_argument("--num-applications", type=int, default=DEFAULT_NUM_APPLICATIONS)
    parser.add_argument(
        "--num-updates",
        "--num-inputs",
        dest="num_updates",
        type=int,
        default=DEFAULT_NUM_UPDATES,
        help=(
            "Number of candidate updates evaluated by every policy. Use --num-inputs 1 "
            "for the cheapest end-to-end smoke test."
        ),
    )
    parser.add_argument("--runs", type=int, default=DEFAULT_MEASURED_RUNS)
    parser.add_argument("--warmups", type=int, default=DEFAULT_WARMUP_RUNS)
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=DEFAULT_MAX_CONCURRENCY,
        help="Maximum simultaneous Bedrock requests per model (1-50).",
    )
    parser.add_argument(
        "--aws-region",
        default=os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", DEFAULT_AWS_REGION)),
    )
    parser.add_argument("--aws-profile", default=os.getenv("AWS_PROFILE"))
    parser.add_argument(
        "--models",
        nargs="+",
        choices=[spec["key"] for spec in DEFAULT_BEDROCK_MODEL_SPECS],
        default=[spec["key"] for spec in DEFAULT_BEDROCK_MODEL_SPECS],
    )
    for spec in DEFAULT_BEDROCK_MODEL_SPECS:
        parser.add_argument(
            f"--{spec['key'].replace('_', '-')}-model-id",
            default=spec["model_id"],
            help=f"Bedrock model ID or inference profile for {spec['display_name']}.",
        )
    parser.add_argument(
        "--output-filename",
        default="regulation_z_benchmark_results.csv",
    )
    parser.add_argument(
        "--progress-filename",
        help=(
            "Incremental per-update CSV filename. Defaults to the output filename "
            "with '.progress.csv' appended to its stem."
        ),
    )
    args = parser.parse_args()

    progress_filename = args.progress_filename or (
        f"{Path(args.output_filename).stem}.progress.csv"
    )
    progress_path = Path("./results") / progress_filename
    progress_path.unlink(missing_ok=True)

    specs_by_key = {str(spec["key"]): spec for spec in DEFAULT_BEDROCK_MODEL_SPECS}
    models = []
    for key in args.models:
        spec = dict(specs_by_key[key])
        spec["model_id"] = getattr(args, f"{key}_model_id")
        models.append(spec)

    print("Running Regulation Z ability-to-repay benchmark:")
    print(f"  Applications: {args.num_applications}")
    print(f"  Updates: {args.num_updates}")
    print(f"  AWS region: {args.aws_region}")
    print("  Approaches: DFC, No Policy, " + ", ".join(m["display_name"] for m in models))
    total_bedrock_requests = args.num_updates * len(models) * (args.runs + args.warmups)
    print(f"  Planned Bedrock requests: {total_bedrock_requests}")
    print(f"  Maximum Bedrock concurrency: {args.max_concurrency}")
    print(f"  Incremental progress: {progress_path}")
    print("  Sampling: provider defaults")
    print("  Cache control: fresh nonce at the beginning of every LLM prompt")

    config = ExperimentConfig(
        num_executions=args.runs,
        num_warmup_runs=args.warmups,
        database_config={"database": ":memory:"},
        strategy_config={
            "num_applications": args.num_applications,
            "num_updates": args.num_updates,
            "aws_region": args.aws_region,
            "aws_profile": args.aws_profile,
            "models": models,
            "max_concurrency": args.max_concurrency,
            "progress_path": str(progress_path),
        },
        output_dir="./results",
        output_filename=args.output_filename,
        verbose=True,
    )
    ExperimentRunner(RegulationZBenchmarkStrategy(), config).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
