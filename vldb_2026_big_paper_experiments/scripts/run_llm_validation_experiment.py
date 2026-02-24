#!/usr/bin/env python3
"""Run LLM validation experiment across TPC-H queries and policy counts."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from experiment_harness import ExperimentConfig, ExperimentRunner  # noqa: E402

from vldb_experiments.strategies.llm_validation_strategy import (  # noqa: E402
    DEFAULT_POLICY_COUNTS,
    DEFAULT_RUNS_PER_SETTING,
    DEFAULT_TPCH_SF,
    TPCH_QUERIES_ALL,
    LLMValidationStrategy,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run LLM policy-validation experiment.")
    parser.add_argument(
        "--tpch-sf",
        type=float,
        default=DEFAULT_TPCH_SF,
        help=f"TPC-H scale factor (default: {DEFAULT_TPCH_SF}).",
    )
    parser.add_argument(
        "--policy-counts",
        type=int,
        nargs="+",
        default=DEFAULT_POLICY_COUNTS,
        help="Policy counts to run (default: 1 2 4 8 16 32).",
    )
    parser.add_argument(
        "--queries",
        type=int,
        nargs="+",
        default=TPCH_QUERIES_ALL,
        help=f"TPC-H query numbers to run (default supported subset: {TPCH_QUERIES_ALL}).",
    )
    parser.add_argument(
        "--runs-per-setting",
        type=int,
        default=DEFAULT_RUNS_PER_SETTING,
        help=f"Measured runs per setting (default: {DEFAULT_RUNS_PER_SETTING}).",
    )
    parser.add_argument(
        "--warmup-per-setting",
        type=int,
        default=0,
        help="Warmup runs per setting (default: 0).",
    )
    parser.add_argument(
        "--gpt-model",
        default="gpt-5.2",
        help="OpenAI model id for GPT baselines.",
    )
    parser.add_argument(
        "--claude-model",
        default="claude-4.6-opus",
        help="Bedrock model id for Opus baselines.",
    )
    parser.add_argument(
        "--disable-openai",
        action="store_true",
        help="Disable GPT baselines.",
    )
    parser.add_argument(
        "--disable-bedrock",
        action="store_true",
        help="Disable Opus baselines.",
    )
    parser.add_argument(
        "--output-filename",
        default="llm_validation_results.csv",
        help="CSV output filename.",
    )
    args = parser.parse_args()

    include_openai = not args.disable_openai
    include_bedrock = not args.disable_bedrock
    if not include_openai and not include_bedrock:
        raise ValueError("At least one of OpenAI or Bedrock must be enabled.")

    settings_per_query_policy = 1 + (2 if include_openai else 0) + (2 if include_bedrock else 0)
    total_settings = len(args.queries) * len(args.policy_counts) * settings_per_query_policy
    total_executions = total_settings * args.runs_per_setting

    print("Running LLM validation experiment:")
    print(f"  TPC-H sf: {args.tpch_sf}")
    print(f"  Queries: {args.queries}")
    print(f"  Policy counts: {args.policy_counts}")
    print(f"  Settings/query-policy: {settings_per_query_policy}")
    print(f"  Include OpenAI: {include_openai}")
    print(f"  Include Bedrock: {include_bedrock}")
    print(f"  Runs per setting: {args.runs_per_setting}")
    print(f"  Warmups per setting: {args.warmup_per_setting}")
    print(f"  Total measured executions: {total_executions}")

    config = ExperimentConfig(
        num_executions=total_executions,
        num_warmup_runs=0,
        warmup_mode="per_setting",
        warmup_runs_per_setting=args.warmup_per_setting,
        strategy_config={
            "tpch_sf": args.tpch_sf,
            "policy_counts": args.policy_counts,
            "queries": args.queries,
            "runs_per_setting": args.runs_per_setting,
            "gpt_model": args.gpt_model,
            "claude_model": args.claude_model,
            "include_openai": include_openai,
            "include_bedrock": include_bedrock,
        },
        output_dir="./results",
        output_filename=args.output_filename,
        verbose=True,
    )

    runner = ExperimentRunner(LLMValidationStrategy(), config)
    runner.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
