#!/usr/bin/env python3
"""Generate figures and cache audit for tax LLM validation results."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from oss_experiments.visualizations import (  # noqa: E402
    create_cache_audit_csv,
    create_confusion_by_model_figure,
    create_f1_by_query_figure,
    create_f1_runtime_scatter_by_reasoning_figure,
    create_f1_runtime_scatter_figure,
    create_runtime_tokens_figure,
    load_results,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate tax LLM validation visualizations.")
    parser.add_argument(
        "--input-csv",
        default="results/tax_llm_validation_results.csv",
    )
    parser.add_argument("--output-dir", default="results")
    parser.add_argument(
        "--query-set-scatter",
        default=None,
        help="If set (e.g. 10_row_queries), also emit F1/latency scatter with reasoning-effort markers",
    )
    parser.add_argument("--include-cache-contaminated", action="store_true")
    args = parser.parse_args()

    df = load_results(args.input_csv)
    include = args.include_cache_contaminated
    create_f1_by_query_figure(df, output_dir=args.output_dir, include_cache_contaminated=include)
    create_f1_runtime_scatter_figure(
        df, output_dir=args.output_dir, include_cache_contaminated=include
    )
    if args.query_set_scatter:
        create_f1_runtime_scatter_by_reasoning_figure(
            df,
            output_dir=args.output_dir,
            query_set=args.query_set_scatter,
            include_cache_contaminated=include,
        )
    create_confusion_by_model_figure(
        df, output_dir=args.output_dir, include_cache_contaminated=include
    )
    create_runtime_tokens_figure(df, output_dir=args.output_dir, include_cache_contaminated=include)
    create_cache_audit_csv(df, output_dir=args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
