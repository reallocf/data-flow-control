#!/usr/bin/env python3
"""Generate heatmaps for the LLM validation grid experiment."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from vldb_experiments.visualizations import (  # noqa: E402
    create_llm_validation_grid_heatmap,
    load_results,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate LLM validation grid heatmaps.")
    parser.add_argument(
        "--input-csv",
        default="results/llm_validation_grid_results.csv",
        help="Input CSV path.",
    )
    parser.add_argument(
        "--output-dir",
        default="results",
        help="Output directory for generated figures.",
    )
    args = parser.parse_args()

    df = load_results(args.input_csv)
    approach_order = [
        "gpt_query_only",
        "gpt_query_results",
        "opus_query_only",
        "opus_query_results",
    ]
    approaches = [a for a in approach_order if a in set(df["approach"].astype(str))]
    policy_counts = sorted(df["policy_count"].dropna().astype(int).unique().tolist())

    for approach in approaches:
        for policy_count in policy_counts:
            output_filename = f"llm_validation_grid_{approach}_p{policy_count}.png"
            create_llm_validation_grid_heatmap(
                df,
                approach=approach,
                policy_count=policy_count,
                output_dir=args.output_dir,
                output_filename=output_filename,
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
