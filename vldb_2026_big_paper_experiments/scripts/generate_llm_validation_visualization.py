#!/usr/bin/env python3
"""Generate visualization(s) for the LLM validation experiment."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src" / "vldb_experiments"))

from visualizations import (  # noqa: E402
    create_llm_validation_accuracy_chart,
    load_results,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate LLM validation figures from CSV results.")
    parser.add_argument(
        "--csv",
        default="results/llm_validation_results_gpt52_full.csv",
        help="Input CSV path (default: results/llm_validation_results_gpt52_full.csv)",
    )
    parser.add_argument(
        "--output-filename",
        default="llm_validation_accuracy.png",
        help="Output image filename (default: llm_validation_accuracy.png)",
    )
    parser.add_argument(
        "--output-dir",
        default="./results",
        help="Output directory (default: ./results)",
    )
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = load_results(str(csv_path))
    create_llm_validation_accuracy_chart(
        df,
        output_dir=args.output_dir,
        output_filename=args.output_filename,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
