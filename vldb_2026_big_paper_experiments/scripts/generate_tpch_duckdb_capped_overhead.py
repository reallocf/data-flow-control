#!/usr/bin/env python3
"""Generate DuckDB-only TPC-H percent-overhead charts with capped values."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import TYPE_CHECKING

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from vldb_experiments.visualizations import load_results  # noqa: E402

if TYPE_CHECKING:
    import pandas as pd


def create_tpch_duckdb_capped_overhead_chart(
    df: pd.DataFrame,
    *,
    cap_pct: float,
    output_path: Path,
    title_suffix: str = "",
) -> None:
    """Create a per-query capped overhead bar chart for DuckDB 1Phase, 2Phase, and Logical."""
    if "no_policy_exec_time_ms" not in df.columns and "no_policy_time_ms" in df.columns:
        df["no_policy_exec_time_ms"] = df["no_policy_time_ms"]
    if "dfc_1phase_exec_time_ms" not in df.columns and "dfc_exec_time_ms" in df.columns:
        df["dfc_1phase_exec_time_ms"] = df["dfc_exec_time_ms"]
    if "dfc_1phase_exec_time_ms" not in df.columns and "dfc_time_ms" in df.columns:
        df["dfc_1phase_exec_time_ms"] = df["dfc_time_ms"]
    if "dfc_2phase_exec_time_ms" not in df.columns and "dfc_2phase_time_ms" in df.columns:
        df["dfc_2phase_exec_time_ms"] = df["dfc_2phase_time_ms"]
    if "logical_exec_time_ms" not in df.columns and "logical_time_ms" in df.columns:
        df["logical_exec_time_ms"] = df["logical_time_ms"]

    required_cols = {
        "query_num",
        "no_policy_exec_time_ms",
        "dfc_1phase_exec_time_ms",
        "dfc_2phase_exec_time_ms",
        "logical_exec_time_ms",
    }
    if not required_cols.issubset(df.columns):
        missing = sorted(required_cols - set(df.columns))
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

    grouped = (
        df.groupby("query_num", as_index=True)[
            [
                "no_policy_exec_time_ms",
                "dfc_1phase_exec_time_ms",
                "dfc_2phase_exec_time_ms",
                "logical_exec_time_ms",
            ]
        ]
        .mean()
        .sort_index()
    )

    dfc_1phase_overhead = (
        (grouped["dfc_1phase_exec_time_ms"] - grouped["no_policy_exec_time_ms"])
        / grouped["no_policy_exec_time_ms"]
    ) * 100.0
    dfc_2phase_overhead = (
        (grouped["dfc_2phase_exec_time_ms"] - grouped["no_policy_exec_time_ms"])
        / grouped["no_policy_exec_time_ms"]
    ) * 100.0
    logical_overhead = (
        (grouped["logical_exec_time_ms"] - grouped["no_policy_exec_time_ms"])
        / grouped["no_policy_exec_time_ms"]
    ) * 100.0

    # Cap only the top-end to preserve negative-overhead visibility.
    dfc_1phase_plot = dfc_1phase_overhead.clip(upper=cap_pct)
    dfc_2phase_plot = dfc_2phase_overhead.clip(upper=cap_pct)
    logical_plot = logical_overhead.clip(upper=cap_pct)

    x_positions = list(range(len(grouped.index)))
    bar_width = 0.25

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(
        [x - bar_width for x in x_positions],
        dfc_1phase_plot,
        width=bar_width,
        label="1Phase",
        color="#ff7f0e",
    )
    ax.bar(
        x_positions,
        dfc_2phase_plot,
        width=bar_width,
        label="2Phase",
        color="#9467bd",
    )
    ax.bar(
        [x + bar_width for x in x_positions],
        logical_plot,
        width=bar_width,
        label="Logical",
        color="#2ca02c",
    )

    ax.set_xticks(x_positions)
    ax.set_xticklabels([f"Q{int(q):02d}" for q in grouped.index], fontsize=9)
    ax.set_xlabel("TPC-H Query", fontsize=12)
    ax.set_ylabel("Overhead vs No Policy (%)", fontsize=12)
    title = f"DuckDB TPC-H Overhead by Query (Capped at {cap_pct:.0f}%)"
    if title_suffix:
        title = f"{title} ({title_suffix})"
    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.grid(axis="y", alpha=0.3)
    ax.legend(loc="best", fontsize=10)

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved chart to {output_path}")


def _default_output_name(csv_path: Path, cap_pct: float) -> str:
    stem = csv_path.stem
    sf_token = "unknown"
    if "sf" in stem:
        sf_token = stem.split("sf", 1)[1]
    return f"tpch_duckdb_percent_overhead_capped{int(cap_pct)}_sf{sf_token}.png"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate capped DuckDB-only TPC-H percent-overhead charts."
    )
    parser.add_argument(
        "csv_paths",
        nargs="+",
        help="One or more TPC-H CSV files (e.g., results/tpch_results_sf1.csv).",
    )
    parser.add_argument(
        "--output-dir",
        default="./results",
        help="Directory to write charts (default: ./results).",
    )
    parser.add_argument(
        "--cap-pct",
        type=float,
        default=1000.0,
        help="Upper cap for displayed percent overhead (default: 1000).",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    for csv_path_str in args.csv_paths:
        csv_path = Path(csv_path_str)
        df = load_results(str(csv_path))
        output_filename = _default_output_name(csv_path, args.cap_pct)

        sf_value = ""
        if "tpch_sf" in df.columns and not df["tpch_sf"].dropna().empty:
            sf_value = f"SF={df['tpch_sf'].dropna().iloc[0]}"

        create_tpch_duckdb_capped_overhead_chart(
            df,
            cap_pct=args.cap_pct,
            output_path=output_dir / output_filename,
            title_suffix=sf_value,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
