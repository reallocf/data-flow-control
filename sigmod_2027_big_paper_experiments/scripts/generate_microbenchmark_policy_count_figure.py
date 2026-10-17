#!/usr/bin/env python3
"""Generate policy-count figures for microbenchmark CSVs."""

import argparse
from pathlib import Path
import sys

import pandas as pd

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from vldb_experiments.visualizations import create_microbenchmark_policy_count_chart  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate policy-count figures from microbenchmark CSV."
    )
    parser.add_argument(
        "--csv",
        default="results/microbenchmark_join_group_by_policy_count.csv",
        help="Input CSV path (default: results/microbenchmark_join_group_by_policy_count.csv)",
    )
    parser.add_argument(
        "--output-filename",
        default="microbenchmark_group_by_policy_count.png",
        help="Output image filename (default: microbenchmark_group_by_policy_count.png)",
    )
    parser.add_argument(
        "--separate-by-join-count",
        action="store_true",
        help="Generate one output figure per join_count value.",
    )
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    if "execution_number" in df.columns:
        df = df[df["execution_number"].astype(str).str.isdigit()].copy()

    if args.separate_by_join_count and "join_count" in df.columns:
        join_counts = sorted(df["join_count"].dropna().astype(int).unique().tolist())
        for join_count in join_counts:
            join_df = df[df["join_count"] == join_count].copy()
            output_filename = (
                Path(args.output_filename).stem
                + f"_join{join_count}"
                + Path(args.output_filename).suffix
            )
            create_microbenchmark_policy_count_chart(
                join_df,
                output_dir="./results",
                output_filename=output_filename,
            )
    else:
        create_microbenchmark_policy_count_chart(
            df,
            output_dir="./results",
            output_filename=args.output_filename,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
