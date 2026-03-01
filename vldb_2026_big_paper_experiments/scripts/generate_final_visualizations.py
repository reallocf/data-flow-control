#!/usr/bin/env python3
"""Generate paper-final figures from CSVs in final_results/."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from vldb_experiments.visualizations_final import generate_all_final_visualizations  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate final paper figures from final_results CSVs.")
    parser.add_argument(
        "--final-results-dir",
        default=str(project_root / "final_results"),
        help="Directory containing final CSVs and receiving final PNGs.",
    )
    args = parser.parse_args()

    outputs = generate_all_final_visualizations(args.final_results_dir)
    for output in outputs:
        print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
