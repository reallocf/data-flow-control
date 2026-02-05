#!/usr/bin/env python3
"""Run TPC-H multi-database experiments (DuckDB + external engines)."""

import argparse
from pathlib import Path
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description="Run TPC-H multi-database experiments.")
    parser.add_argument(
        "--sf",
        type=float,
        nargs="+",
        default=[1],
        help="TPC-H scale factors to run (default: 1)",
    )
    parser.add_argument(
        "--suffix",
        required=True,
        help="Suffix appended to the output CSV filename (e.g., _umbra).",
    )
    parser.add_argument(
        "--engine",
        choices=["umbra", "postgres", "sqlite", "datafusion", "all"],
        default="all",
        help="External engine to run (default: all).",
    )
    args = parser.parse_args()

    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root / "src"))

    from experiment_harness import ExperimentConfig, ExperimentRunner

    from vldb_experiments import TPCHMultiDBStrategy
    from vldb_experiments.strategies.tpch_strategy import TPCH_QUERIES

    num_queries = len(TPCH_QUERIES)
    num_warmup_runs = num_queries
    num_executions = num_queries * 5

    print("Running TPC-H multi-database experiments:")
    print(f"  Queries: {num_queries} ({', '.join(f'Q{q:02d}' for q in TPCH_QUERIES)})")
    print(f"  Warm-up runs: {num_warmup_runs} (1 per query)")
    print(f"  Measured runs: {num_executions} (5 per query)")
    print("  Approaches: DuckDB no_policy/DFC/Logical + external no_policy engines")

    for scale_factor in args.sf:
        print(f"\n=== Scale factor {scale_factor} ===", flush=True)

        db_path = f"./results/tpch_sf{scale_factor}.db"
        output_filename = f"tpch_multi_db_sf{scale_factor}{args.suffix}.csv"

        config = ExperimentConfig(
            num_executions=num_executions,
            num_warmup_runs=num_warmup_runs,
            database_config={
                "database": db_path,
            },
            strategy_config={
                "tpch_sf": scale_factor,
                "tpch_db_path": db_path,
                "external_engines": None if args.engine == "all" else [args.engine],
            },
            output_dir="./results",
            output_filename=output_filename,
            verbose=True,
        )

        strategy = TPCHMultiDBStrategy()
        runner = ExperimentRunner(strategy, config)

        print("Starting experiments...", flush=True)
        runner.run()

        print("\nExperiments completed!")
        print(f"Results saved to: {config.output_dir}/{config.output_filename}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
