#!/usr/bin/env python3
"""Run LLM validation experiment across TPC-H queries and policy counts."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from vldb_experiments.llm_validation_runner import (  # noqa: E402
    MAX_INFERENCE_CONCURRENCY,
    run_incremental_parallel,
)
from vldb_experiments.strategies.llm_validation_common import (  # noqa: E402
    DEFAULT_BEDROCK_MODEL_SPECS,
    DEFAULT_POLICY_COUNTS,
    DEFAULT_RUNS_PER_SETTING,
    DEFAULT_TPCH_SF,
    POLICY_VARIANT_FIXED_4,
    POLICY_VARIANT_GROWING,
    SUPPORTED_POLICY_VARIANTS,
)
from vldb_experiments.strategies.llm_validation_strategy import (  # noqa: E402
    DEFAULT_PROMPT_MODES,
    PROMPT_MODE_QUERY_RESULTS,
    SUPPORTED_PROMPT_MODES,
    TPCH_QUERIES_ALL,
    LLMValidationStrategy,
    build_validation_settings,
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
        "--policy-variant",
        choices=[variant.replace("_", "-") for variant in SUPPORTED_POLICY_VARIANTS],
        default=POLICY_VARIANT_GROWING.replace("_", "-"),
        help=(
            "Policy catalog to use: growing-violations reproduces the 4/13 through 9/13 "
            "labels; fixed-4-violations keeps exactly 4/13 labels at every policy count."
        ),
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
        help="Deprecated; must remain 0 because warmups add paid calls not used by Figure 1.",
    )
    parser.add_argument(
        "--prompt-modes",
        nargs="+",
        choices=[mode.replace("_", "-") for mode in SUPPORTED_PROMPT_MODES],
        default=[mode.replace("_", "-") for mode in DEFAULT_PROMPT_MODES],
        help=(
            "LLM prompt variants (default: query-only and query-results, including "
            "the Figure 1 footnote ablation)."
        ),
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        help=(
            "Run one query, one policy count, one query-results request per selected model, "
            "and no warmups."
        ),
    )
    parser.add_argument(
        "--models",
        nargs="+",
        choices=[spec["key"] for spec in DEFAULT_BEDROCK_MODEL_SPECS],
        default=[spec["key"] for spec in DEFAULT_BEDROCK_MODEL_SPECS],
        help="Bedrock models to run (default: astra sol_6_1 luna fable_5_1 opus_5_5).",
    )
    for spec in DEFAULT_BEDROCK_MODEL_SPECS:
        parser.add_argument(
            f"--{spec['key'].replace('_', '-')}-model-id",
            default=spec["model_id"],
            help=f"Bedrock inference profile ID or ARN for {spec['display_name']}.",
        )
    parser.add_argument(
        "--output-filename",
        default=None,
        help="CSV output filename.",
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=MAX_INFERENCE_CONCURRENCY,
        help=(
            "Maximum concurrent inference requests "
            f"(default and hard limit: {MAX_INFERENCE_CONCURRENCY})."
        ),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace an existing output CSV instead of resuming successful executions.",
    )
    args = parser.parse_args()

    if args.runs_per_setting < 1:
        parser.error("--runs-per-setting must be at least 1")
    if args.warmup_per_setting != 0:
        parser.error("--warmup-per-setting must be 0; Figure 1 does not use paid warmups")
    if not 1 <= args.max_concurrency <= MAX_INFERENCE_CONCURRENCY:
        parser.error(f"--max-concurrency must be between 1 and {MAX_INFERENCE_CONCURRENCY}")
    if not args.queries:
        parser.error("--queries must contain at least one query")
    if not args.policy_counts:
        parser.error("--policy-counts must contain at least one count")

    prompt_modes = [mode.replace("-", "_") for mode in args.prompt_modes]
    policy_variant = args.policy_variant.replace("-", "_")
    if args.smoke_test:
        args.queries = args.queries[:1]
        args.policy_counts = args.policy_counts[:1]
        args.runs_per_setting = 1
        args.warmup_per_setting = 0
        prompt_modes = [PROMPT_MODE_QUERY_RESULTS]

    if args.output_filename:
        output_filename = args.output_filename
    elif policy_variant == POLICY_VARIANT_FIXED_4:
        output_filename = (
            "llm_validation_smoke_fixed4_results.csv"
            if args.smoke_test
            else "llm_validation_results_fixed4_astra_sol61_luna_fable51_opus55.csv"
        )
    else:
        output_filename = (
            "llm_validation_smoke_results.csv"
            if args.smoke_test
            else "llm_validation_results_astra_sol61_luna_fable51_opus55.csv"
        )

    model_specs_by_key = {spec["key"]: spec for spec in DEFAULT_BEDROCK_MODEL_SPECS}
    models = []
    for key in args.models:
        model = dict(model_specs_by_key[key])
        model["model_id"] = getattr(args, f"{key}_model_id")
        models.append(model)
    settings = build_validation_settings(
        args.queries,
        args.policy_counts,
        [str(model["key"]) for model in models],
        prompt_modes,
    )
    settings_per_query_policy = 1 + len(prompt_modes) * len(models)
    total_settings = len(settings)
    total_executions = total_settings * args.runs_per_setting
    paid_llm_requests = (
        len(args.queries)
        * len(args.policy_counts)
        * len(models)
        * len(prompt_modes)
        * args.runs_per_setting
    )

    print("Running LLM validation experiment:")
    print(f"  TPC-H sf: {args.tpch_sf}")
    print(f"  Queries: {args.queries}")
    print(f"  Policy counts: {args.policy_counts}")
    print(f"  Policy variant: {policy_variant.replace('_', '-')}")
    print(f"  Prompt modes: {[mode.replace('_', '-') for mode in prompt_modes]}")
    print(f"  Settings/query-policy: {settings_per_query_policy}")
    print("  Models:")
    for model in models:
        print(f"    {model['display_name']}: {model['model_id']} via Bedrock")
    print(f"  Runs per setting: {args.runs_per_setting}")
    print(f"  Warmups per setting: {args.warmup_per_setting}")
    print(f"  Maximum concurrent inference requests: {args.max_concurrency}")
    print(f"  Total measured executions: {total_executions}")
    print(f"  Paid LLM requests: {paid_llm_requests}")
    print("  Cache control: explicit uncacheable checkpoints for OpenAI models; no nonces")
    print("  Results are appended and synced after every completed execution.")

    strategy_config = {
        "tpch_sf": args.tpch_sf,
        "policy_counts": args.policy_counts,
        "policy_variant": policy_variant,
        "queries": args.queries,
        "runs_per_setting": args.runs_per_setting,
        "models": models,
        "prompt_modes": prompt_modes,
    }
    output_path = Path("./results") / output_filename
    completed, total = run_incremental_parallel(
        LLMValidationStrategy(),
        strategy_config,
        output_path,
        max_concurrency=args.max_concurrency,
        resume=not args.overwrite,
    )
    print(f"Completed {completed}/{total} executions. Results: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
