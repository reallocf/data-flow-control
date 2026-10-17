#!/usr/bin/env python3
"""Generate multi-db per-engine average-overhead summary charts."""

import argparse
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from vldb_experiments.visualizations import (  # noqa: E402
    create_multi_db_engine_summary_capped_chart,
    create_multi_db_engine_summary_chart,
    load_results,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate multi-db average-overhead charts by engine."
    )
    parser.add_argument(
        "csv_path",
        help="Path to multi-db CSV results.",
    )
    parser.add_argument(
        "--output-dir",
        default="./results",
        help="Directory to write chart images (default: ./results).",
    )
    parser.add_argument(
        "--suffix",
        default="",
        help="Suffix appended to output filenames (default: none).",
    )
    parser.add_argument(
        "--duckdb-cap-pct",
        type=float,
        default=200.0,
        help="Cap used for DuckDB in capped chart (default: 200).",
    )
    args = parser.parse_args()

    suffix = f"_{args.suffix}" if args.suffix else ""
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = load_results(args.csv_path)
    create_multi_db_engine_summary_chart(
        df,
        output_dir=str(output_dir),
        output_filename=f"tpch_multi_db_engine_summary{suffix}.png",
    )
    create_multi_db_engine_summary_capped_chart(
        df,
        output_dir=str(output_dir),
        output_filename=f"tpch_multi_db_engine_summary_capped{suffix}.png",
        duckdb_cap_pct=args.duckdb_cap_pct,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
