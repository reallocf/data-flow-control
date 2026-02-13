#!/usr/bin/env python3
"""Run multi-source join chain experiment (No Policy vs DFC)."""

import argparse
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from experiment_harness import ExperimentConfig, ExperimentRunner

from vldb_experiments import MultiSourceStrategy

DEFAULT_SOURCE_COUNTS = [2, 4, 8, 16, 32]
DEFAULT_JOIN_COUNTS = [2, 4, 8, 16, 32]
DEFAULT_NUM_ROWS = 10_000
DEFAULT_WARMUP_PER_SETTING = 1
DEFAULT_RUNS_PER_SETTING = 5
DEFAULT_OUTPUT_PREFIX = "multi_source_results"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run multi-source join chain experiments.")
    parser.add_argument(
        "--sources",
        type=int,
        nargs="+",
        default=DEFAULT_SOURCE_COUNTS,
        help="Source counts to test (default: 2 4 8 16 32)",
    )
    parser.add_argument(
        "--joins",
        type=int,
        nargs="+",
        default=DEFAULT_JOIN_COUNTS,
        help="Join counts to test (default: 2 4 8 16 32)",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=DEFAULT_NUM_ROWS,
        help="Rows per table (default: 10000)",
    )
    parser.add_argument(
        "--runs-per-setting",
        type=int,
        default=DEFAULT_RUNS_PER_SETTING,
        help="Measured runs per source count (default: 5)",
    )
    parser.add_argument(
        "--warmup-per-setting",
        type=int,
        default=DEFAULT_WARMUP_PER_SETTING,
        help="Warmup runs per source count (default: 1)",
    )
    parser.add_argument(
        "--suffix",
        type=str,
        required=True,
        help="Suffix to append to outputs (required).",
    )
    args = parser.parse_args()

    source_counts = args.sources
    join_counts = args.joins
    valid_pairs = [
        (join_count, source_count)
        for join_count in join_counts
        for source_count in source_counts
        if source_count <= join_count
    ]
    runs_per_setting = args.runs_per_setting
    warmup_per_setting = args.warmup_per_setting

    num_warmup_runs = len(valid_pairs) * warmup_per_setting
    num_executions = len(valid_pairs) * runs_per_setting

    print("Running multi-source experiment:")
    print(f"  Sources: {source_counts}")
    print(f"  Joins: {join_counts}")
    print(f"  Rows per table: {args.rows}")
    print(f"  Warmup runs: {num_warmup_runs} ({warmup_per_setting} per source count)")
    print(f"  Measured runs: {num_executions} ({runs_per_setting} per source count)")
    print("  Approaches: No Policy, DFC")

    output_filename = f"{DEFAULT_OUTPUT_PREFIX}_{args.suffix}.csv"

    config = ExperimentConfig(
        num_executions=num_executions,
        num_warmup_runs=num_warmup_runs,
        database_config={
            "database": ":memory:",
        },
        strategy_config={
            "source_counts": source_counts,
            "join_counts": join_counts,
            "num_rows": args.rows,
            "warmup_per_setting": warmup_per_setting,
            "runs_per_setting": runs_per_setting,
        },
        output_dir="./results",
        output_filename=output_filename,
        verbose=True,
    )

    runner = ExperimentRunner(MultiSourceStrategy(), config)
    print("Starting experiments...", flush=True)
    runner.run()

    print("\nExperiments completed!")
    print(f"Results saved to: {config.output_dir}/{config.output_filename}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
