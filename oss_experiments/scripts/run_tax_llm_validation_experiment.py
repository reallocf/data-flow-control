#!/usr/bin/env python3
"""Run tax-domain LLM validation experiment."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

from dotenv import load_dotenv

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from experiment_harness import ExperimentConfig, ExperimentRunner  # noqa: E402

from oss_experiments.data_setup import (  # noqa: E402
    DEFAULT_BATCH_ROW_COUNT,
    DEFAULT_INPUT_ROW_COUNT,
    EXPERIMENT_MODE_AGG,
    EXPERIMENT_MODES,
    QUERY_SETS,
)
from oss_experiments.llm_clients import (  # noqa: E402
    DEFAULT_BEDROCK_MAX_TOKENS,
    DEFAULT_CLAUDE_MODEL,
    DEFAULT_GPT_MODEL,
    SUPPORTED_REASONING_EFFORTS,
)
from oss_experiments.prompts import INCLUDED_CONTEXTS  # noqa: E402
from oss_experiments.strategies.tax_llm_validation_strategy import (  # noqa: E402
    DEFAULT_LLM_MAX_ATTEMPTS,
    DEFAULT_RUNS_PER_SETTING,
    TaxLLMValidationStrategy,
)


def _count_settings(
    *,
    num_queries: int,
    dfc_only: bool,
    include_openai: bool,
    include_bedrock: bool,
    num_contexts: int,
    num_efforts: int,
) -> int:
    per_query = 1
    if not dfc_only:
        llm_settings = 0
        if include_openai:
            llm_settings += num_contexts * num_efforts
        if include_bedrock:
            llm_settings += num_contexts * num_efforts
        per_query += llm_settings
    return num_queries * per_query


def main() -> int:
    parser = argparse.ArgumentParser(description="Run tax LLM validation experiment.")
    parser.add_argument("--input-row-count", type=int, default=DEFAULT_INPUT_ROW_COUNT)
    parser.add_argument("--batch-row-count", type=int, default=DEFAULT_BATCH_ROW_COUNT)
    parser.add_argument(
        "--mode",
        choices=list(EXPERIMENT_MODES),
        default=EXPERIMENT_MODE_AGG,
        help="agg: merchant rollup queries; select: row-level INSERT..SELECT queries",
    )
    parser.add_argument("--query-sets", nargs="+", default=list(QUERY_SETS))
    parser.add_argument("--query-ids", nargs="+", default=None)
    parser.add_argument("--included-contexts", nargs="+", default=list(INCLUDED_CONTEXTS))
    parser.add_argument(
        "--reasoning-efforts",
        nargs="+",
        default=list(SUPPORTED_REASONING_EFFORTS),
    )
    parser.add_argument("--runs-per-setting", type=int, default=DEFAULT_RUNS_PER_SETTING)
    parser.add_argument("--llm-max-attempts", type=int, default=DEFAULT_LLM_MAX_ATTEMPTS)
    parser.add_argument("--warmup-per-setting", type=int, default=0)
    parser.add_argument("--gpt-model", default=DEFAULT_GPT_MODEL)
    parser.add_argument(
        "--claude-model",
        default=DEFAULT_CLAUDE_MODEL,
    )
    parser.add_argument("--bedrock-max-tokens", type=int, default=DEFAULT_BEDROCK_MAX_TOKENS)
    parser.add_argument("--disable-openai", action="store_true")
    parser.add_argument("--disable-bedrock", action="store_true")
    parser.add_argument("--dfc-only", action="store_true")
    parser.add_argument("--fail-on-cache-hit", action="store_true")
    parser.add_argument(
        "--output-filename",
        default="tax_llm_validation_results.csv",
    )
    args = parser.parse_args()

    load_dotenv(project_root / ".env")
    load_dotenv(project_root.parent / ".env")

    include_openai = not args.disable_openai and not args.dfc_only
    include_bedrock = not args.disable_bedrock and not args.dfc_only

    query_ids = args.query_ids
    num_queries = 0
    for _query_set in args.query_sets:
        count = 10 if not query_ids else len(query_ids)
        num_queries += count

    total_settings = _count_settings(
        num_queries=num_queries,
        dfc_only=args.dfc_only,
        include_openai=include_openai,
        include_bedrock=include_bedrock,
        num_contexts=len(args.included_contexts),
        num_efforts=len(args.reasoning_efforts),
    )
    total_executions = total_settings * args.runs_per_setting

    openai_client = None
    bedrock_client = None
    if include_openai:
        from openai import OpenAI

        openai_client = OpenAI()
    if include_bedrock:
        import boto3

        bedrock_client = boto3.client("bedrock-runtime")

    print("Running tax LLM validation experiment:")
    print(f"  Mode: {args.mode}")
    print(f"  Input rows: {args.input_row_count}")
    print(f"  Batch rows: {args.batch_row_count}")
    print(f"  Query sets: {args.query_sets}")
    print(f"  Query ids: {query_ids or 'all'}")
    print(f"  Included contexts: {args.included_contexts}")
    print(f"  Reasoning efforts: {args.reasoning_efforts}")
    print(f"  DFC only: {args.dfc_only}")
    print(f"  Include OpenAI: {include_openai} (model={args.gpt_model})")
    print(f"  Include Bedrock: {include_bedrock} (model={args.claude_model})")
    print(f"  Bedrock max tokens: {args.bedrock_max_tokens}")
    print(f"  LLM max attempts: {args.llm_max_attempts}")
    print(f"  Runs per setting: {args.runs_per_setting}")
    print(f"  Total measured executions: {total_executions}")

    config = ExperimentConfig(
        num_executions=total_executions,
        num_warmup_runs=0,
        warmup_mode="per_setting",
        warmup_runs_per_setting=args.warmup_per_setting,
        strategy_config={
            "mode": args.mode,
            "input_row_count": args.input_row_count,
            "batch_row_count": args.batch_row_count,
            "query_sets": args.query_sets,
            "query_ids": query_ids,
            "included_contexts": args.included_contexts,
            "reasoning_efforts": args.reasoning_efforts,
            "runs_per_setting": args.runs_per_setting,
            "llm_max_attempts": args.llm_max_attempts,
            "gpt_model": args.gpt_model,
            "claude_model": args.claude_model,
            "bedrock_max_tokens": args.bedrock_max_tokens,
            "include_openai": include_openai,
            "include_bedrock": include_bedrock,
            "dfc_only": args.dfc_only,
            "fail_on_cache_hit": args.fail_on_cache_hit,
            "openai_client": openai_client,
            "bedrock_client": bedrock_client,
        },
        output_dir="./results",
        output_filename=args.output_filename,
        verbose=True,
    )
    ExperimentRunner(TaxLLMValidationStrategy(), config).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
