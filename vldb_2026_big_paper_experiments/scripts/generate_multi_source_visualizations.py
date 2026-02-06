#!/usr/bin/env python3
"""Generate multi-source experiment visualizations."""

import argparse
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from vldb_experiments.visualizations import (
    create_multi_source_exec_time_chart,
    create_multi_source_heatmap_chart,
    load_results,
)

DEFAULT_RESULTS_PREFIX = "multi_source_results"
DEFAULT_OUTPUT_PREFIX = "multi_source"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate multi-source experiment visualizations.")
    parser.add_argument(
        "--suffix",
        type=str,
        required=True,
        help="Suffix used for input/output files (required).",
    )
    parser.add_argument(
        "--results-dir",
        type=str,
        default="./results",
        help="Directory containing CSV results (default: ./results)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./results",
        help="Directory to write charts (default: ./results)",
    )
    args = parser.parse_args()

    csv_path = Path(args.results_dir) / f"{DEFAULT_RESULTS_PREFIX}_{args.suffix}.csv"
    df = load_results(str(csv_path))

    create_multi_source_exec_time_chart(
        df,
        output_dir=args.output_dir,
        output_filename=f"{DEFAULT_OUTPUT_PREFIX}_exec_time_{args.suffix}.png",
    )
    create_multi_source_heatmap_chart(
        df,
        output_dir=args.output_dir,
        output_filename=f"{DEFAULT_OUTPUT_PREFIX}_heatmap_{args.suffix}.png",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
