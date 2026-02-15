#!/usr/bin/env python3
"""Generate visualization for wide-table width microbenchmark results."""

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate wide-table width microbenchmark chart."
    )
    parser.add_argument(
        "--csv",
        default="results/microbenchmark_table_width.csv",
        help="Input CSV path (default: results/microbenchmark_table_width.csv)",
    )
    parser.add_argument(
        "--output-filename",
        default="microbenchmark_table_width.png",
        help="Output image filename (default: microbenchmark_table_width.png)",
    )
    parser.add_argument(
        "--overhead-output-filename",
        default="microbenchmark_table_width_overhead.png",
        help=(
            "Output image filename for percent-overhead chart "
            "(default: microbenchmark_table_width_overhead.png)"
        ),
    )
    parser.add_argument(
        "--suffix",
        default="",
        help="Optional suffix appended to output filename before extension.",
    )
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    if "execution_number" in df.columns:
        df = df[df["execution_number"].astype(str).str.isdigit()].copy()
    if "run_num" in df.columns:
        df = df[df["run_num"].fillna(0) > 0].copy()

    required = {
        "table_width",
        "no_policy_exec_time_ms",
        "dfc_exec_time_ms",
        "logical_exec_time_ms",
        "physical_exec_time_ms",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns in CSV: {sorted(missing)}")

    grouped = df.groupby("table_width", as_index=True).mean(numeric_only=True).sort_index()
    if grouped.empty:
        raise ValueError("No data available for plotting")

    fig, ax = plt.subplots(figsize=(10, 7))
    series = [
        ("no_policy_exec_time_ms", "No Policy", "#1f77b4"),
        ("dfc_exec_time_ms", "DFC", "#ff7f0e"),
        ("logical_exec_time_ms", "Logical", "#2ca02c"),
        ("physical_exec_time_ms", "Physical", "#d62728"),
    ]
    for col, label, color in series:
        ax.plot(
            grouped.index,
            grouped[col],
            marker="o",
            linewidth=2,
            markersize=6,
            label=label,
            color=color,
        )

    ax.set_xlabel("Table Width (Number of Columns)", fontsize=12)
    ax.set_ylabel("Average Execution Time (ms)", fontsize=12)
    ax.set_title("Wide-Table Aggregation Performance vs Table Width", fontsize=14, fontweight="bold")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=10)

    plt.tight_layout()
    output_filename = args.output_filename
    if args.suffix:
        stem, ext = Path(output_filename).stem, Path(output_filename).suffix
        output_filename = f"{stem}_{args.suffix}{ext or '.png'}"

    output_path = Path("./results") / output_filename
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)

    baseline = grouped["no_policy_exec_time_ms"]
    overhead = pd.DataFrame(index=grouped.index)
    overhead["DFC"] = ((grouped["dfc_exec_time_ms"] - baseline) / baseline) * 100.0
    overhead["Logical"] = ((grouped["logical_exec_time_ms"] - baseline) / baseline) * 100.0
    overhead["Physical"] = ((grouped["physical_exec_time_ms"] - baseline) / baseline) * 100.0

    fig_overhead, ax_overhead = plt.subplots(figsize=(10, 7))
    overhead_series = [
        ("DFC", "#ff7f0e"),
        ("Logical", "#2ca02c"),
        ("Physical", "#d62728"),
    ]
    for col, color in overhead_series:
        ax_overhead.plot(
            overhead.index,
            overhead[col],
            marker="o",
            linewidth=2,
            markersize=6,
            label=col,
            color=color,
        )

    ax_overhead.axhline(0.0, color="black", linestyle="--", linewidth=1, alpha=0.8)
    ax_overhead.set_xlabel("Table Width (Number of Columns)", fontsize=12)
    ax_overhead.set_ylabel("Percent Overhead vs No Policy (%)", fontsize=12)
    ax_overhead.set_title(
        "Wide-Table Aggregation Percent Overhead vs No Policy",
        fontsize=14,
        fontweight="bold",
    )
    ax_overhead.set_xscale("log")
    ax_overhead.grid(True, alpha=0.3)
    ax_overhead.legend(loc="best", fontsize=10)

    plt.tight_layout()
    overhead_output_filename = args.overhead_output_filename
    if args.suffix:
        stem, ext = Path(overhead_output_filename).stem, Path(overhead_output_filename).suffix
        overhead_output_filename = f"{stem}_{args.suffix}{ext or '.png'}"
    overhead_output_path = Path("./results") / overhead_output_filename
    fig_overhead.savefig(str(overhead_output_path), dpi=150, bbox_inches="tight")
    plt.close(fig_overhead)

    print(f"Saved chart to {output_path}")
    print(f"Saved overhead chart to {overhead_output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
