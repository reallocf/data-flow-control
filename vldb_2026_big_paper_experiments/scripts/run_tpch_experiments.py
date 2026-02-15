#!/usr/bin/env python3
"""Script to run TPC-H experiments."""

import argparse
from pathlib import Path
import sys

# Add src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from experiment_harness import ExperimentConfig, ExperimentRunner  # noqa: E402

from vldb_experiments import TPCHStrategy  # noqa: E402
from vldb_experiments.strategies.tpch_strategy import TPCH_QUERIES  # noqa: E402


def main():
    """Run TPC-H experiments."""
    parser = argparse.ArgumentParser(description="Run TPC-H experiments.")
    parser.add_argument(
        "--sf",
        type=int,
        nargs="+",
        default=[1, 10],
        help="TPC-H scale factors to run (default: 1 10)",
    )
    parser.add_argument(
        "--output-suffix",
        default="",
        help="Suffix appended to the output CSV filename (e.g., _breakdown).",
    )
    parser.add_argument(
        "--runs-per-query",
        type=int,
        default=5,
        help="Number of executions per query (default: 5).",
    )
    parser.add_argument(
        "--warmup-runs",
        type=int,
        default=1,
        help="Warm-up runs per query setting (default: 1).",
    )
    args = parser.parse_args()

    # Experiment structure: one execution per query
    num_queries = len(TPCH_QUERIES)
    num_executions = num_queries * args.runs_per_query
    warmup_per_query = args.warmup_runs

    print("Running TPC-H experiments:")
    print(f"  Queries: {num_queries} ({', '.join(f'Q{q:02d}' for q in TPCH_QUERIES)})")
    print(f"  Total executions: {num_executions} ({args.runs_per_query} per query)")
    print(f"  Warm-up runs per query: {warmup_per_query}")
    print("  Approaches: no_policy, DFC, Logical (CTE), Physical (SmokedDuck)")
    print("  Policies:")
    print("    - Q1-Q12, Q14, Q18-Q19: lineitem_policy (max(lineitem.l_quantity) >= 1)")
    print()

    for scale_factor in args.sf:
        print(f"\n=== Scale factor {scale_factor} ===", flush=True)

        db_path = f"./results/tpch_sf{scale_factor}.db"

        config = ExperimentConfig(
            num_executions=num_executions,
            num_warmup_runs=0,
            warmup_mode="per_setting",
            warmup_runs_per_setting=warmup_per_query,
            database_config={
                "database": db_path,
            },
            strategy_config={
                "tpch_sf": scale_factor,
                "tpch_db_path": db_path,
            },
            output_dir="./results",
            output_filename=f"tpch_results_sf{scale_factor}{args.output_suffix}.csv",
            verbose=True,
        )

        strategy = TPCHStrategy()
        runner = ExperimentRunner(strategy, config)

        print("Starting experiments...", flush=True)
        runner.run()

        print("\nExperiments completed!")
        print(f"Results saved to: {config.output_dir}/{config.output_filename}")

        # Check correctness
        import csv

        correctness_failures = []
        with open(f"{config.output_dir}/{config.output_filename}") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("correctness_match", "").lower() == "false":
                    correctness_failures.append({
                        "execution": row.get("execution_number"),
                        "query": row.get("query_name", ""),
                        "error": row.get("correctness_error", ""),
                    })

        if correctness_failures:
            print(f"\n⚠️  WARNING: {len(correctness_failures)} correctness failures detected!")
            for failure in correctness_failures[:10]:
                print(f"  Execution {failure['execution']} ({failure['query']}): {failure['error']}")
            if len(correctness_failures) > 10:
                print(f"  ... and {len(correctness_failures) - 10} more")
        else:
            print("\n✓ All correctness checks passed!")

    return 0


if __name__ == "__main__":
    sys.exit(main())
