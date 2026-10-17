#!/usr/bin/env python3
"""Generate the optimized TPC-H policy count figure."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from vldb_experiments.visualizations import create_policy_count_chart, load_results  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate optimized TPC-H policy count figure.")
    parser.add_argument(
        "--csv",
        default="results/tpch_q01_policy_count_sf1_optimized.csv",
        help="Input CSV path (default: results/tpch_q01_policy_count_sf1_optimized.csv)",
    )
    parser.add_argument(
        "--output-dir",
        default="./results",
        help="Output directory (default: ./results)",
    )
    parser.add_argument(
        "--output-filename",
        default="tpch_q01_policy_count_optimized.png",
        help="Output image filename (default: tpch_q01_policy_count_optimized.png)",
    )
    args = parser.parse_args()

    df = load_results(args.csv)
    create_policy_count_chart(
        df,
        output_dir=args.output_dir,
        output_filename=args.output_filename,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
