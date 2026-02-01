#!/usr/bin/env python3
"""Script to run microbenchmark experiments."""

import sys
from pathlib import Path

# Add src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from experiment_harness import ExperimentRunner, ExperimentConfig
from vldb_experiments import MicrobenchmarkStrategy
from vldb_experiments.query_definitions import get_query_order


def main():
    """Run microbenchmark experiments."""
    # Experiment structure: 4 variations × 5 runs = 20 executions per query type
    num_variations = 4
    num_runs_per_variation = 5
    num_executions_per_query = num_variations * num_runs_per_variation
    num_warmup_runs = 2
    
    # Calculate total executions needed
    num_query_types = len(get_query_order())
    total_executions = num_executions_per_query * num_query_types
    
    print(f"Running microbenchmark experiments with variations:")
    print(f"  Query types: {num_query_types}")
    print(f"  Variations per query: {num_variations} (x values, Zipfian distributed)")
    print(f"  Runs per variation: {num_runs_per_variation}")
    print(f"  Executions per query: {num_executions_per_query} ({num_variations} × {num_runs_per_variation})")
    print(f"  Total executions: {total_executions}")
    print(f"  Warm-up runs: {num_warmup_runs}")
    print(f"  Approaches: no_policy, DFC, Logical (CTE), Physical (SmokedDuck)")
    print(f"  Variations:")
    print(f"    - SELECT/WHERE/ORDER_BY: Vary policy threshold (zipfian, 4 values)")
    print(f"    - JOIN: Vary join matches (zipfian, 4 values)")
    print(f"    - GROUP_BY: Vary number of groups (zipfian, 4 values)")
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
        output_filename="microbenchmark_results.csv",
        verbose=True,
    )
    
    # Create and run strategy
    strategy = MicrobenchmarkStrategy()
    runner = ExperimentRunner(strategy, config)
    
    print("Starting experiments...")
    collector = runner.run()
    
    print(f"\nExperiments completed!")
    print(f"Results saved to: {config.output_dir}/{config.output_filename}")
    
    # Check correctness
    import csv
    correctness_failures = []
    with open(f"{config.output_dir}/{config.output_filename}", 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('correctness_match', '').lower() == 'false':
                correctness_failures.append({
                    'execution': row.get('execution_number'),
                    'query_type': row.get('query_type'),
                    'error': row.get('correctness_error', '')
                })
    
    if correctness_failures:
        print(f"\n⚠️  WARNING: {len(correctness_failures)} correctness failures detected!")
        for failure in correctness_failures[:5]:  # Show first 5
            print(f"  Execution {failure['execution']} ({failure['query_type']}): {failure['error']}")
        if len(correctness_failures) > 5:
            print(f"  ... and {len(correctness_failures) - 5} more")
    else:
        print(f"\n✓ All correctness checks passed!")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
