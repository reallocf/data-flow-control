#!/usr/bin/env python3
"""Run TPC-H Q01 policy many-ORs experiments."""

import argparse
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from experiment_harness import ExperimentConfig, ExperimentRunner

from vldb_experiments import TPCHPolicyManyORsStrategy

DEFAULT_OR_COUNTS = [0, 1, 10, 100, 1000]
DEFAULT_WARMUP_PER_LEVEL = 1
DEFAULT_RUNS_PER_LEVEL = 5


def main() -> int:
    parser = argparse.ArgumentParser(description="Run TPC-H Q01 policy many-ORs experiments.")
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
        "--or-counts",
        type=int,
        nargs="+",
        default=DEFAULT_OR_COUNTS,
        help="OR clause counts to test (default: 0 1 10 100 1000)",
    )
    parser.add_argument(
        "--runs-per-level",
        type=int,
        default=DEFAULT_RUNS_PER_LEVEL,
        help="Number of measured runs per OR count (default: 5)",
    )
    parser.add_argument(
        "--warmup-per-level",
        type=int,
        default=DEFAULT_WARMUP_PER_LEVEL,
        help="Number of warmup runs per OR count (default: 1)",
    )
    args = parser.parse_args()

    or_counts = args.or_counts
    runs_per_level = args.runs_per_level
    warmup_per_level = args.warmup_per_level

    num_warmup_runs = len(or_counts) * warmup_per_level
    num_executions = len(or_counts) * runs_per_level

    print("Running TPC-H Q01 policy many-ORs experiment:")
    print(f"  Scale factor: {args.sf}")
    print(f"  Query: Q{args.query:02d}")
    print(f"  OR counts: {or_counts}")
    print(f"  Warmup runs: {num_warmup_runs} ({warmup_per_level} per level)")
    print(f"  Measured runs: {num_executions} ({runs_per_level} per level)")
    print("  Approaches: No Policy, DFC, Logical")

    db_path = f"./results/tpch_q{args.query:02d}_policy_many_ors_sf{args.sf}.db"
    output_filename = f"tpch_q{args.query:02d}_policy_many_ors_sf{args.sf}.csv"

    config = ExperimentConfig(
        num_executions=num_executions,
        num_warmup_runs=num_warmup_runs,
        database_config={
            "database": db_path,
        },
        strategy_config={
            "tpch_sf": args.sf,
            "tpch_db_path": db_path,
            "tpch_query": args.query,
            "or_counts": or_counts,
            "warmup_per_level": warmup_per_level,
            "runs_per_level": runs_per_level,
        },
        output_dir="./results",
        output_filename=output_filename,
        verbose=True,
    )

    strategy = TPCHPolicyManyORsStrategy()
    runner = ExperimentRunner(strategy, config)

    print("Starting experiments...", flush=True)
    runner.run()

    print("\nExperiments completed!")
    print(f"Results saved to: {config.output_dir}/{config.output_filename}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
