#!/usr/bin/env python3
"""Run wide-table microbenchmark experiment varying table width."""

import argparse
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from experiment_harness import ExperimentConfig, ExperimentRunner  # noqa: E402

from vldb_experiments import MicrobenchmarkTableWidthStrategy  # noqa: E402

DEFAULT_TABLE_WIDTHS = [32, 64, 128, 256]
DEFAULT_NUM_ROWS = 1_000_000
DEFAULT_WARMUP_PER_WIDTH = 1
DEFAULT_RUNS_PER_WIDTH = 5


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run wide-table microbenchmark width sweep."
    )
    parser.add_argument(
        "--num-rows",
        type=int,
        default=DEFAULT_NUM_ROWS,
        help="Number of rows in wide_data (default: 1000000)",
    )
    parser.add_argument(
        "--table-widths",
        type=int,
        nargs="+",
        default=DEFAULT_TABLE_WIDTHS,
        help="Even table widths to test (default: 32 64 128 256)",
    )
    parser.add_argument(
        "--warmup-per-width",
        type=int,
        default=DEFAULT_WARMUP_PER_WIDTH,
        help="Warmup runs per width (default: 1)",
    )
    parser.add_argument(
        "--runs-per-width",
        type=int,
        default=DEFAULT_RUNS_PER_WIDTH,
        help="Measured runs per width (default: 5)",
    )
    parser.add_argument(
        "--output-filename",
        default="microbenchmark_table_width.csv",
        help="CSV output filename (default: microbenchmark_table_width.csv)",
    )
    parser.add_argument(
        "--suffix",
        default="",
        help="Optional suffix appended to output filename before .csv",
    )
    args = parser.parse_args()

    for width in args.table_widths:
        if width < 2 or width % 2 != 0:
            raise ValueError(f"table width must be even and >= 2, got {width}")

    num_warmup_runs = 0
    num_executions = len(args.table_widths) * args.runs_per_width
    output_filename = args.output_filename
    if args.suffix:
        stem, ext = Path(output_filename).stem, Path(output_filename).suffix
        output_filename = f"{stem}_{args.suffix}{ext or '.csv'}"

    print("Running wide-table width microbenchmark:")
    print(f"  Rows: {args.num_rows}")
    print(f"  Widths: {args.table_widths}")
    print(f"  Warmup runs per width: {args.warmup_per_width}")
    print(f"  Measured runs per width: {args.runs_per_width}")
    total_strategy_executions = num_executions + (len(args.table_widths) * args.warmup_per_width)
    print(f"  Total measured executions: {num_executions}")
    print(f"  Total strategy executions (including warmups): {total_strategy_executions}")
    print("  Execution order: for each width -> warmup(s), then measured runs")
    print("  Approaches: No Policy, DFC, Logical, Physical")

    config = ExperimentConfig(
        num_executions=num_executions,
        num_warmup_runs=num_warmup_runs,
        warmup_mode="per_setting",
        warmup_runs_per_setting=args.warmup_per_width,
        database_config={"database": ":memory:"},
        strategy_config={
            "num_rows": args.num_rows,
            "table_widths": args.table_widths,
            "warmup_per_width": args.warmup_per_width,
            "runs_per_width": args.runs_per_width,
        },
        output_dir="./results",
        output_filename=output_filename,
        verbose=True,
    )

    strategy = MicrobenchmarkTableWidthStrategy()
    runner = ExperimentRunner(strategy, config)

    print("Starting experiments...", flush=True)
    runner.run()

    print("\nExperiments completed!")
    print(f"Results saved to: {config.output_dir}/{config.output_filename}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
