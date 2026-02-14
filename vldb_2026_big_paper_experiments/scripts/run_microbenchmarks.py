#!/usr/bin/env python3
"""Script to run microbenchmark experiments."""

import argparse
from pathlib import Path
import sys

# Add src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from experiment_harness import ExperimentConfig, ExperimentRunner

from vldb_experiments import MicrobenchmarkStrategy
from vldb_experiments.query_definitions import get_query_order


def main():
    """Run microbenchmark experiments."""
    parser = argparse.ArgumentParser(description="Run microbenchmark experiments.")
    parser.add_argument(
        "--policy-count",
        type=int,
        default=1,
        help="Number of policies to register per run.",
    )
    parser.add_argument(
        "--output-filename",
        default=None,
        help="CSV filename for results (default: microbenchmark_results_policy{policy_count}.csv).",
    )
    parser.add_argument(
        "--num-variations",
        type=int,
        default=4,
        help="Number of variation values per query type.",
    )
    parser.add_argument(
        "--num-runs-per-variation",
        type=int,
        default=5,
        help="Number of runs per variation value.",
    )
    parser.add_argument(
        "--warmup-runs",
        type=int,
        default=2,
        help="Number of warm-up runs.",
    )
    parser.add_argument(
        "--disable-physical",
        action="store_true",
        help="Disable physical (SmokedDuck) baseline.",
    )
    parser.add_argument(
        "--query-types",
        default=None,
        help="Comma-separated list of query types to run (e.g., GROUP_BY,JOIN).",
    )
    args = parser.parse_args()

    output_filename = args.output_filename
    if output_filename is None:
        output_filename = f"microbenchmark_results_policy{args.policy_count}.csv"

    # Experiment structure: 4 variations × 5 runs = 20 executions per query type
    num_variations = args.num_variations
    num_runs_per_variation = args.num_runs_per_variation
    num_executions_per_query = num_variations * num_runs_per_variation
    num_warmup_runs = args.warmup_runs

    # Calculate total executions needed
    if args.query_types:
        query_order = [q.strip() for q in args.query_types.split(",") if q.strip()]
    else:
        query_order = get_query_order()
    num_query_types = len(query_order)
    total_executions = num_executions_per_query * num_query_types

    print("Running microbenchmark experiments with variations:")
    print(f"  Query types: {num_query_types} ({', '.join(query_order)})")
    print(f"  Variations per query: {num_variations} (x values, Zipfian distributed)")
    print(f"  Runs per variation: {num_runs_per_variation}")
    print(f"  Executions per query: {num_executions_per_query} ({num_variations} × {num_runs_per_variation})")
    print(f"  Total executions: {total_executions}")
    print(f"  Warm-up runs: {num_warmup_runs}")
    print("  Approaches: no_policy, DFC, Logical (CTE), Physical (SmokedDuck)")
    print(f"  Policy count: {args.policy_count}")
    print("  Variations:")
    print("    - SELECT/WHERE/ORDER_BY: Vary policy threshold (zipfian, 4 values)")
    print("    - JOIN: Vary join matches (zipfian, 4 values)")
    print("    - GROUP_BY: Vary number of groups (zipfian, 4 values)")
    print(f"  Charts will show averages of {num_runs_per_variation} runs per x value")
    print()

    # Configure experiment
    config = ExperimentConfig(
        num_executions=total_executions,
        num_warmup_runs=num_warmup_runs,
        database_config={
            "database": ":memory:",
        },
        output_dir="./results",
        output_filename=output_filename,
        verbose=True,
    )

    # Create and run strategy
    strategy = MicrobenchmarkStrategy(
        policy_count=args.policy_count,
        num_variations=num_variations,
        num_runs_per_variation=num_runs_per_variation,
        enable_physical=None if not args.disable_physical else False,
        query_types=query_order,
    )
    runner = ExperimentRunner(strategy, config)

    print("Starting experiments...")
    collector = runner.run()

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
                    "query_type": row.get("query_type"),
                    "error": row.get("correctness_error", "")
                })

    if correctness_failures:
        print(f"\n⚠️  WARNING: {len(correctness_failures)} correctness failures detected!")
        for failure in correctness_failures[:5]:  # Show first 5
            print(f"  Execution {failure['execution']} ({failure['query_type']}): {failure['error']}")
        if len(correctness_failures) > 5:
            print(f"  ... and {len(correctness_failures) - 5} more")
    else:
        print("\n✓ All correctness checks passed!")

    return 0


if __name__ == "__main__":
    sys.exit(main())
    output_filename = args.output_filename
    if output_filename is None:
        output_filename = f"microbenchmark_results_policy{args.policy_count}.csv"
