#!/usr/bin/env python3
"""Run synthetic LLM validation grid experiment across random datasets."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from experiment_harness import ExperimentConfig, ExperimentRunner  # noqa: E402

from vldb_experiments.strategies.synthetic_llm_validation_grid_strategy import (  # noqa: E402
    DEFAULT_SYNTHETIC_DATASET_COUNT,
    DEFAULT_SYNTHETIC_POLICY_THRESHOLD,
    DEFAULT_SYNTHETIC_QUERY_NUMS,
    DEFAULT_SYNTHETIC_ROWS_PER_TABLE,
    SyntheticLLMValidationGridStrategy,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run synthetic LLM validation grid experiment.")
    parser.add_argument("--dataset-count", type=int, default=DEFAULT_SYNTHETIC_DATASET_COUNT)
    parser.add_argument("--rows-per-table", type=int, default=DEFAULT_SYNTHETIC_ROWS_PER_TABLE)
    parser.add_argument("--policy-threshold", type=float, default=DEFAULT_SYNTHETIC_POLICY_THRESHOLD)
    parser.add_argument("--query-nums", type=int, nargs="+", default=DEFAULT_SYNTHETIC_QUERY_NUMS)
    parser.add_argument("--policy-counts", type=int, nargs="+", default=[1])
    parser.add_argument("--runs-per-setting", type=int, default=1)
    parser.add_argument("--warmup-per-setting", type=int, default=0)
    parser.add_argument("--gpt-model", default="gpt-5.2")
    parser.add_argument("--claude-model", default="claude-4.6-opus")
    parser.add_argument("--disable-openai", action="store_true")
    parser.add_argument("--disable-gpt-query-only", action="store_true")
    parser.add_argument("--disable-gpt-query-results", action="store_true")
    parser.add_argument("--disable-bedrock", action="store_true")
    parser.add_argument("--disable-opus-query-only", action="store_true")
    parser.add_argument("--disable-opus-query-results", action="store_true")
    parser.add_argument(
        "--output-filename",
        default="synthetic_llm_validation_grid_results.csv",
    )
    args = parser.parse_args()

    include_openai = not args.disable_openai
    include_bedrock = not args.disable_bedrock
    include_gpt_query_only = include_openai and not args.disable_gpt_query_only
    include_gpt_query_results = include_openai and not args.disable_gpt_query_results
    include_opus_query_only = include_bedrock and not args.disable_opus_query_only
    include_opus_query_results = include_bedrock and not args.disable_opus_query_results

    settings_per_dataset_policy = 1
    settings_per_dataset_policy += int(include_gpt_query_only)
    settings_per_dataset_policy += int(include_gpt_query_results)
    settings_per_dataset_policy += int(include_opus_query_only)
    settings_per_dataset_policy += int(include_opus_query_results)
    total_settings = args.dataset_count * len(args.policy_counts) * settings_per_dataset_policy
    total_settings *= len(args.query_nums)
    total_executions = total_settings * args.runs_per_setting

    print("Running synthetic LLM validation grid experiment:")
    print(f"  Dataset count: {args.dataset_count}")
    print(f"  Rows/table: {args.rows_per_table}")
    print(f"  Query nums: {args.query_nums}")
    print(f"  Policy threshold: {args.policy_threshold}")
    print(f"  Policy counts: {args.policy_counts}")
    print(f"  Settings/dataset-policy: {settings_per_dataset_policy}")
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
            "dataset_count": args.dataset_count,
            "rows_per_table": args.rows_per_table,
            "query_nums": args.query_nums,
            "policy_threshold": args.policy_threshold,
            "policy_counts": args.policy_counts,
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
    ExperimentRunner(SyntheticLLMValidationGridStrategy(), config).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
