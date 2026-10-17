#!/usr/bin/env python3
"""Run TPC-H Q01 policy complexity experiments."""

import argparse
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from experiment_harness import ExperimentConfig, ExperimentRunner  # noqa: E402

from vldb_experiments import TPCHPolicyComplexityStrategy  # noqa: E402

DEFAULT_COMPLEXITY_TERMS = [1, 10, 100, 1000]
DEFAULT_WARMUP_PER_LEVEL = 1
DEFAULT_RUNS_PER_LEVEL = 5


def main() -> int:
    parser = argparse.ArgumentParser(description="Run TPC-H Q01 policy complexity experiments.")
    parser.add_argument(
        "--sf",
        type=int,
        default=1,
        help="TPC-H scale factor to run (default: 1)",
    )
    parser.add_argument(
        "--query",
        type=int,
        default=1,
        help="TPC-H query number to run (default: 1)",
    )
    parser.add_argument(
        "--complexity-terms",
        type=int,
        nargs="+",
        default=DEFAULT_COMPLEXITY_TERMS,
        help="Predicate term counts to test (default: 1 10 100 1000)",
    )
    parser.add_argument(
        "--runs-per-level",
        type=int,
        default=DEFAULT_RUNS_PER_LEVEL,
        help="Number of measured runs per complexity level (default: 5)",
    )
    parser.add_argument(
        "--warmup-per-level",
        type=int,
        default=DEFAULT_WARMUP_PER_LEVEL,
        help="Number of warmup runs per complexity level (default: 1)",
    )
    args = parser.parse_args()

    complexity_terms = args.complexity_terms
    runs_per_level = args.runs_per_level
    warmup_per_level = args.warmup_per_level

    num_executions = len(complexity_terms) * runs_per_level

    print("Running TPC-H Q01 policy complexity experiment:")
    print(f"  Scale factor: {args.sf}")
    print(f"  Query: Q{args.query:02d}")
    print(f"  Complexity terms: {complexity_terms}")
    print(f"  Warmup runs per setting: {warmup_per_level}")
    print(f"  Measured runs: {num_executions} ({runs_per_level} per level)")
    print("  Approaches: No Policy, DFC, Logical, Physical")

    db_path = f"./results/tpch_q{args.query:02d}_policy_complexity_sf{args.sf}.db"
    output_filename = f"tpch_q{args.query:02d}_policy_complexity_sf{args.sf}.csv"

    config = ExperimentConfig(
        num_executions=num_executions,
        num_warmup_runs=0,
        warmup_mode="per_setting",
        warmup_runs_per_setting=warmup_per_level,
        database_config={
            "database": ":memory:",
        },
        strategy_config={
            "tpch_sf": args.sf,
            "tpch_db_path": db_path,
            "tpch_query": args.query,
            "complexity_terms": complexity_terms,
            "warmup_per_level": warmup_per_level,
            "runs_per_level": runs_per_level,
        },
        output_dir="./results",
        output_filename=output_filename,
        verbose=True,
    )

    strategy = TPCHPolicyComplexityStrategy()
    runner = ExperimentRunner(strategy, config)

    print("Starting experiments...", flush=True)
    runner.run()

    print("\nExperiments completed!")
    print(f"Results saved to: {config.output_dir}/{config.output_filename}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
