#!/usr/bin/env python3
"""Run tax-agent experiment across provider/approach/policy-count settings."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from experiment_harness import ExperimentConfig, ExperimentRunner  # noqa: E402

from vldb_experiments.strategies.tax_agent_strategy import (  # noqa: E402
    DEFAULT_POLICY_COUNTS,
    TaxAgentStrategy,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run tax-agent strategy experiment.")
    parser.add_argument(
        "--policy-counts",
        type=int,
        nargs="+",
        default=DEFAULT_POLICY_COUNTS,
        help="Policy counts to run (default: 0 1 2 4 8 16 32).",
    )
    parser.add_argument(
        "--runs-per-setting",
        type=int,
        default=5,
        help="Measured runs per setting (default: 5).",
    )
    parser.add_argument(
        "--warmup-per-setting",
        type=int,
        default=0,
        help="Warmup runs per setting (default: 0).",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=40,
        help="LangChain recursion limit per run (default: 40).",
    )
    parser.add_argument(
        "--claude-model",
        default="claude-4.6-opus",
        help="Bedrock model id for Claude setting.",
    )
    parser.add_argument(
        "--gpt-model",
        default="gpt-5.2",
        help="OpenAI model id for GPT setting.",
    )
    parser.add_argument(
        "--include-bedrock",
        action="store_true",
        help="Include Bedrock/Claude settings (default: disabled).",
    )
    parser.add_argument(
        "--output-filename",
        default="tax_agent_results.csv",
        help="CSV output filename.",
    )
    args = parser.parse_args()

    # Temporarily default to OpenAI-only runs; re-enable Bedrock by default once that path is stable again.
    settings_per_policy_count = 4 if args.include_bedrock else 2
    settings_count = len(args.policy_counts) * settings_per_policy_count
    total_executions = settings_count * args.runs_per_setting
    print("Running tax-agent experiment:")
    print(f"  Policy counts: {args.policy_counts}")
    print(f"  Settings per policy count: {settings_per_policy_count}")
    print(f"  Include Bedrock: {args.include_bedrock}")
    print(f"  Total settings: {settings_count}")
    print(f"  Runs per setting: {args.runs_per_setting}")
    print(f"  Warmups per setting: {args.warmup_per_setting}")
    print(f"  Total measured executions: {total_executions}")

    config = ExperimentConfig(
        num_executions=total_executions,
        num_warmup_runs=0,
        warmup_mode="per_setting",
        warmup_runs_per_setting=args.warmup_per_setting,
        strategy_config={
            "policy_counts": args.policy_counts,
            "runs_per_setting": args.runs_per_setting,
            "max_iterations": args.max_iterations,
            "claude_model": args.claude_model,
            "gpt_model": args.gpt_model,
            "include_bedrock": args.include_bedrock,
            "results_dir": "./results",
        },
        output_dir="./results",
        output_filename=args.output_filename,
        verbose=True,
    )

    runner = ExperimentRunner(TaxAgentStrategy(), config)
    runner.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
