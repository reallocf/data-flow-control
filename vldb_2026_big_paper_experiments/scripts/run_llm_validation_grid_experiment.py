#!/usr/bin/env python3
"""Run LLM validation grid experiment across queries and databases."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from experiment_harness import ExperimentConfig, ExperimentRunner  # noqa: E402

from vldb_experiments.strategies.llm_validation_common import (  # noqa: E402
    DEFAULT_POLICY_COUNTS,
    DEFAULT_RUNS_PER_SETTING,
)
from vldb_experiments.strategies.llm_validation_grid_strategy import (  # noqa: E402
    DEFAULT_DATABASE_SFS,
    LLMValidationGridStrategy,
)
from vldb_experiments.strategies.llm_validation_strategy import TPCH_QUERIES_ALL  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Run LLM policy-validation grid experiment.")
    parser.add_argument(
        "--database-sfs",
        type=float,
        nargs="+",
        default=DEFAULT_DATABASE_SFS,
        help=f"TPC-H scale factors for the database axis (default: {DEFAULT_DATABASE_SFS}).",
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
        "--disable-gpt-query-only",
        action="store_true",
        help="Disable the GPT query-only baseline while keeping GPT query+results enabled.",
    )
    parser.add_argument(
        "--disable-gpt-query-results",
        action="store_true",
        help="Disable the GPT query+results baseline while keeping GPT query-only enabled.",
    )
    parser.add_argument(
        "--disable-bedrock",
        action="store_true",
        help="Disable Opus baselines.",
    )
    parser.add_argument(
        "--disable-opus-query-only",
        action="store_true",
        help="Disable the Opus query-only baseline while keeping Opus query+results enabled.",
    )
    parser.add_argument(
        "--disable-opus-query-results",
        action="store_true",
        help="Disable the Opus query+results baseline while keeping Opus query-only enabled.",
    )
    parser.add_argument(
        "--output-filename",
        default="llm_validation_grid_results.csv",
        help="CSV output filename.",
    )
    args = parser.parse_args()

    include_openai = not args.disable_openai
    include_bedrock = not args.disable_bedrock
    include_gpt_query_only = include_openai and not args.disable_gpt_query_only
    include_gpt_query_results = include_openai and not args.disable_gpt_query_results
    include_opus_query_only = include_bedrock and not args.disable_opus_query_only
    include_opus_query_results = include_bedrock and not args.disable_opus_query_results
    if not any([include_gpt_query_only, include_gpt_query_results, include_opus_query_only, include_opus_query_results]):
        raise ValueError("At least one LLM baseline must be enabled.")

    settings_per_query_policy_db = 1
    settings_per_query_policy_db += int(include_gpt_query_only)
    settings_per_query_policy_db += int(include_gpt_query_results)
    settings_per_query_policy_db += int(include_opus_query_only)
    settings_per_query_policy_db += int(include_opus_query_results)
    total_settings = len(args.database_sfs) * len(args.queries) * len(args.policy_counts) * settings_per_query_policy_db
    total_executions = total_settings * args.runs_per_setting

    print("Running LLM validation grid experiment:")
    print(f"  Database sfs: {args.database_sfs}")
    print(f"  Queries: {args.queries}")
    print(f"  Policy counts: {args.policy_counts}")
    print(f"  Settings/query-policy-db: {settings_per_query_policy_db}")
    print(f"  Include OpenAI: {include_openai}")
    print(f"  Include Bedrock: {include_bedrock}")
    print(f"  Include GPT query-only: {include_gpt_query_only}")
    print(f"  Include GPT query-results: {include_gpt_query_results}")
    print(f"  Include Opus query-only: {include_opus_query_only}")
    print(f"  Include Opus query-results: {include_opus_query_results}")
    print(f"  Runs per setting: {args.runs_per_setting}")
    print(f"  Warmups per setting: {args.warmup_per_setting}")
    print(f"  Total measured executions: {total_executions}")

    config = ExperimentConfig(
        num_executions=total_executions,
        num_warmup_runs=0,
        warmup_mode="per_setting",
        warmup_runs_per_setting=args.warmup_per_setting,
        strategy_config={
            "database_sfs": args.database_sfs,
            "policy_counts": args.policy_counts,
            "queries": args.queries,
            "runs_per_setting": args.runs_per_setting,
            "gpt_model": args.gpt_model,
            "claude_model": args.claude_model,
            "include_openai": include_openai,
            "include_bedrock": include_bedrock,
            "include_gpt_query_only": include_gpt_query_only,
            "include_gpt_query_results": include_gpt_query_results,
            "include_opus_query_only": include_opus_query_only,
            "include_opus_query_results": include_opus_query_results,
        },
        output_dir="./results",
        output_filename=args.output_filename,
        verbose=True,
    )

    runner = ExperimentRunner(LLMValidationGridStrategy(), config)
    runner.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
