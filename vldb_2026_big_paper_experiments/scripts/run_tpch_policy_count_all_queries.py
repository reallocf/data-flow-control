#!/usr/bin/env python3
"""Run TPC-H policy count experiments across all supported queries."""

import sys
from pathlib import Path
import argparse

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from experiment_harness import ExperimentRunner, ExperimentConfig
from vldb_experiments import TPCHPolicyCountAllQueriesStrategy
from vldb_experiments.strategies.tpch_strategy import TPCH_QUERIES


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run TPC-H policy count experiment (fixed count) over all queries."
    )
    parser.add_argument(
        "--sf",
        type=int,
        nargs="+",
        default=[1, 10],
        help="TPC-H scale factors to run (default: 1 10)",
    )
    parser.add_argument(
        "--policy-count",
        type=int,
        default=1000,
        help="Policy count to apply for all queries (default: 1000)",
    )
    parser.add_argument(
        "--output-suffix",
        default="",
        help="Suffix appended to the output CSV filename (e.g., _breakdown).",
    )
    args = parser.parse_args()

    num_queries = len(TPCH_QUERIES)
    num_warmup_runs = num_queries
    num_executions = num_queries * 5

    print("Running TPC-H policy count (fixed) across all queries:")
    print(f"  Queries: {num_queries} ({', '.join(f'Q{q:02d}' for q in TPCH_QUERIES)})")
    print(f"  Policy count: {args.policy_count}")
    print(f"  Warm-up runs: {num_warmup_runs} (1 per query)")
    print(f"  Measured runs: {num_executions} (5 per query)")
    print("  Approaches: no_policy, DFC, Logical")

    for scale_factor in args.sf:
        print(f"\n=== Scale factor {scale_factor} ===", flush=True)

        db_path = f"./results/tpch_policy_count_all_sf{scale_factor}.db"
        output_filename = f"tpch_policy_count_all_sf{scale_factor}{args.output_suffix}.csv"

        config = ExperimentConfig(
            num_executions=num_executions,
            num_warmup_runs=num_warmup_runs,
            database_config={
                "database": db_path,
            },
            strategy_config={
                "tpch_sf": scale_factor,
                "tpch_db_path": db_path,
                "policy_count": args.policy_count,
            },
            output_dir="./results",
            output_filename=output_filename,
            verbose=True,
        )

        strategy = TPCHPolicyCountAllQueriesStrategy()
        runner = ExperimentRunner(strategy, config)

        print("Starting experiments...", flush=True)
        runner.run()

        print("\nExperiments completed!")
        print(f"Results saved to: {config.output_dir}/{config.output_filename}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
