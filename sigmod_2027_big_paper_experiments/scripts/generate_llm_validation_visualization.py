#!/usr/bin/env python3
"""Generate the Figure 1 latency/F1 chart from LLM validation results."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root / "scripts"))

from generate_llm_validation_table import generate_table  # noqa: E402
import pandas as pd  # noqa: E402

from vldb_experiments.visualizations_final import (  # noqa: E402
    create_llm_validation_latency_f1_chart,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate the LLM validation latency/F1 figure from raw CSV results."
    )
    parser.add_argument(
        "--csv",
        default="results/llm_validation_results_astra_sol61_luna_fable51_opus55.csv",
        help="Raw experiment CSV path.",
    )
    parser.add_argument(
        "--summary-csv",
        default="results/llm_validation_table_astra_sol61_luna_fable51_opus55.csv",
        help="Aggregated summary CSV path.",
    )
    parser.add_argument(
        "--output",
        default="results/llm_validation_latency_f1_astra_sol61_luna_fable51_opus55.png",
        help="Output figure path.",
    )
    parser.add_argument(
        "--prompt-mode",
        choices=("query-only", "query-results"),
        default="query-results",
        help="Prompt variant to plot alongside the DFC baseline.",
    )
    parser.add_argument(
        "--scale-markers-by-policy-count",
        action="store_true",
        help="Scale marker area as 1 + log2(policy count).",
    )
    parser.add_argument(
        "--metric",
        choices=("f1", "balanced-accuracy"),
        default="f1",
        help="Accuracy metric for the vertical axis (default: f1).",
    )
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    summary_path = generate_table(csv_path, Path(args.summary_csv))
    create_llm_validation_latency_f1_chart(
        pd.read_csv(summary_path),
        output_path=Path(args.output),
        prompt_mode=args.prompt_mode.replace("-", "_"),
        scale_markers_by_policy_count=args.scale_markers_by_policy_count,
        score_metric=args.metric.replace("-", "_"),
    )
    print(summary_path)
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
