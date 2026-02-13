#!/usr/bin/env python3
"""Run TPC-H Q01 policy count experiments."""

import argparse
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from experiment_harness import ExperimentConfig, ExperimentRunner

from vldb_experiments import TPCHPolicyCountStrategy

DEFAULT_POLICY_COUNTS = [1, 10, 100, 1000]
DEFAULT_WARMUP_PER_POLICY = 1
DEFAULT_RUNS_PER_POLICY = 5


def main() -> int:
    parser = argparse.ArgumentParser(description="Run TPC-H Q01 policy count experiments.")
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
        "--policy-counts",
        type=int,
        nargs="+",
        default=DEFAULT_POLICY_COUNTS,
        help="Policy counts to test (default: 1 10 100 1000)",
    )
    parser.add_argument(
        "--runs-per-policy",
        type=int,
        default=DEFAULT_RUNS_PER_POLICY,
        help="Number of measured runs per policy count (default: 5)",
    )
    parser.add_argument(
        "--warmup-per-policy",
        type=int,
        default=DEFAULT_WARMUP_PER_POLICY,
        help="Number of warmup runs per policy count (default: 1)",
    )
    args = parser.parse_args()

    policy_counts = args.policy_counts
    runs_per_policy = args.runs_per_policy
    warmup_per_policy = args.warmup_per_policy

    num_warmup_runs = len(policy_counts) * warmup_per_policy
    num_executions = len(policy_counts) * runs_per_policy

    print("Running TPC-H Q01 policy count experiment:")
    print(f"  Scale factor: {args.sf}")
    print(f"  Query: Q{args.query:02d}")
    print(f"  Policy counts: {policy_counts}")
    print(f"  Warmup runs: {num_warmup_runs} ({warmup_per_policy} per policy count)")
    print(f"  Measured runs: {num_executions} ({runs_per_policy} per policy count)")
    print("  Approaches: DFC, Logical")

    db_path = f"./results/tpch_q01_policy_count_sf{args.sf}.db"
    output_filename = f"tpch_q{args.query:02d}_policy_count_sf{args.sf}.csv"

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
            "policy_counts": policy_counts,
            "warmup_per_policy": warmup_per_policy,
            "runs_per_policy": runs_per_policy,
        },
        output_dir="./results",
        output_filename=output_filename,
        verbose=True,
    )

    strategy = TPCHPolicyCountStrategy()
    runner = ExperimentRunner(strategy, config)

    print("Starting experiments...", flush=True)
    runner.run()

    print("\nExperiments completed!")
    print(f"Results saved to: {config.output_dir}/{config.output_filename}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
