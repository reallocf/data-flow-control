#!/usr/bin/env python3
"""Generate the TPC-H self-join alias-policy figure."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from vldb_experiments.visualizations import (  # noqa: E402
    create_tpch_self_join_chart,
    load_results,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate TPC-H self-join alias-policy figure.")
    parser.add_argument(
        "--csv",
        default="results/tpch_q01_self_join_policy_sf1.csv",
        help="Input CSV path (default: results/tpch_q01_self_join_policy_sf1.csv)",
    )
    parser.add_argument(
        "--output-dir",
        default="./results",
        help="Output directory (default: ./results)",
    )
    parser.add_argument(
        "--output-filename",
        default="tpch_q01_self_join_policy.png",
        help="Output image filename (default: tpch_q01_self_join_policy.png)",
    )
    args = parser.parse_args()

    df = load_results(args.csv)
    create_tpch_self_join_chart(
        df,
        output_dir=args.output_dir,
        output_filename=args.output_filename,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
