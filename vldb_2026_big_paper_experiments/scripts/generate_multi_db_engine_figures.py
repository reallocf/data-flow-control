#!/usr/bin/env python3
"""Generate per-engine multi-db figures with optional DuckDB exclusion."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def _drop_duckdb_columns(df: pd.DataFrame) -> pd.DataFrame:
    drop_cols = [
        c
        for c in df.columns
        if c.startswith(("no_policy_", "dfc_", "logical_"))
        or c in {"correctness_match", "correctness_error"}
        or c in {"no_policy_rows", "dfc_rows", "logical_rows"}
    ]
    return df.drop(columns=drop_cols, errors="ignore")


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate per-engine multi-db figures.")
    parser.add_argument("csv_path", help="Path to the multi-db CSV file.")
    parser.add_argument("--engine", required=True, help="Engine name for labeling output files.")
    parser.add_argument("--suffix", required=True, help="Suffix appended to output files.")
    parser.add_argument(
        "--output-dir",
        default="./results",
        help="Directory to write figures/CSVs (default: ./results).",
    )
    parser.add_argument(
        "--exclude-duckdb",
        action="store_true",
        help="Generate an additional figure with DuckDB columns removed.",
    )
    args = parser.parse_args()

    from vldb_experiments.visualizations import create_tpch_multi_db_chart, load_results

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    df = load_results(args.csv_path)

    output_filename = f"tpch_multi_db_{args.engine}_only_{args.suffix}.png"
    create_tpch_multi_db_chart(
        df,
        output_dir=str(output_dir),
        output_filename=output_filename,
        title_suffix=f"{args.engine.title()}",
    )

    if args.exclude_duckdb:
        filtered = _drop_duckdb_columns(pd.read_csv(args.csv_path))
        noduckdb_csv = output_dir / f"tpch_multi_db_{args.engine}_only_noduckdb_{args.suffix}.csv"
        filtered.to_csv(noduckdb_csv, index=False)
        noduckdb_df = load_results(str(noduckdb_csv))
        noduckdb_fig = f"tpch_multi_db_{args.engine}_only_noduckdb_{args.suffix}.png"
        create_tpch_multi_db_chart(
            noduckdb_df,
            output_dir=str(output_dir),
            output_filename=noduckdb_fig,
            title_suffix=f"{args.engine.title()} (No DuckDB)",
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
