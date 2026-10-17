#!/usr/bin/env python3
"""Generate 1Phase/2Phase ratio heatmap for phase-competition microbenchmark."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
from matplotlib.colors import TwoSlopeNorm
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def _load_runs(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    if "execution_number" in df.columns:
        df = df[df["execution_number"].astype(str).str.isdigit()].copy()
    if "run_num" in df.columns:
        df = df[df["run_num"].fillna(0).astype(int) > 0].copy()
    return df


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate 1Phase/2Phase ratio heatmap from phase-competition CSV."
    )
    parser.add_argument(
        "--csv",
        default="results/microbenchmark_phase_competition.csv",
        help="Input CSV path (default: results/microbenchmark_phase_competition.csv)",
    )
    parser.add_argument(
        "--output-filename",
        default="microbenchmark_phase_competition_heatmap.png",
        help="Output figure filename (default: microbenchmark_phase_competition_heatmap.png)",
    )
    parser.add_argument(
        "--x-dimension",
        choices=["row_count", "join_fanout", "base_aggregate_columns"],
        default="join_fanout",
        help="Column to use for x-axis (default: join_fanout)",
    )
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = _load_runs(csv_path)
    x_dimension = args.x_dimension
    required_cols = {
        x_dimension,
        "policy_column_count",
        "dfc_1phase_exec_time_ms",
        "dfc_2phase_exec_time_ms",
        "correctness_match",
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    bad = df[df["correctness_match"].astype(str).str.lower() != "true"]
    if not bad.empty:
        raise ValueError(
            f"Found {len(bad)} rows where 1Phase and 2Phase results do not match. "
            "Aborting heatmap generation."
        )

    grouped = df.groupby([x_dimension, "policy_column_count"], as_index=False)[
        ["dfc_1phase_exec_time_ms", "dfc_2phase_exec_time_ms"]
    ].mean()
    grouped["partial_push_faster_pct"] = (
        grouped["dfc_1phase_exec_time_ms"] / grouped["dfc_2phase_exec_time_ms"] - 1.0
    ) * 100.0

    x_values = sorted(grouped[x_dimension].astype(int).unique().tolist())
    policy_values = sorted(grouped["policy_column_count"].astype(int).unique().tolist())
    faster_pct_matrix = np.full((len(policy_values), len(x_values)), np.nan)

    for _, row in grouped.iterrows():
        j = x_values.index(int(row[x_dimension]))
        p = policy_values.index(int(row["policy_column_count"]))
        faster_pct_matrix[p, j] = float(row["partial_push_faster_pct"])

    valid = faster_pct_matrix[np.isfinite(faster_pct_matrix)]
    if valid.size == 0:
        raise ValueError("No valid ratio values to plot.")
    max_abs = min(max(abs(float(valid.min())), abs(float(valid.max()))), 100.0)
    if max_abs == 0.0:
        max_abs = 1.0
    faster_pct_matrix = np.clip(faster_pct_matrix, -max_abs, max_abs)

    fig, ax = plt.subplots(figsize=(10, 7))
    im = ax.imshow(
        faster_pct_matrix,
        aspect="auto",
        origin="lower",
        cmap="RdBu_r",
        norm=TwoSlopeNorm(vmin=-max_abs, vcenter=0.0, vmax=max_abs),
    )

    ax.set_xticks(range(len(x_values)))
    ax.set_xticklabels(x_values)
    ax.set_yticks(range(len(policy_values)))
    ax.set_yticklabels(policy_values)
    if x_dimension == "base_aggregate_columns":
        ax.set_xlabel("Base Query Columns Summed", fontsize=12)
    elif x_dimension == "join_fanout":
        ax.set_xlabel("Join Fanout", fontsize=12)
    else:
        ax.set_xlabel("Number of Rows", fontsize=12)
    ax.set_ylabel("Policy Columns Summed", fontsize=12)
    ax.set_title("Partial-Push Speedup Over Full-Push", fontsize=14, fontweight="bold")

    for y_idx in range(len(policy_values)):
        for x_idx in range(len(x_values)):
            val = faster_pct_matrix[y_idx, x_idx]
            if np.isfinite(val):
                ax.text(x_idx, y_idx, f"{val:+.0f}%", ha="center", va="center", fontsize=9)

    cbar = fig.colorbar(im, ax=ax)
    cbar.set_ticks([])
    cbar.ax.text(
        1.55,
        0.0,
        "Full-Push",
        transform=cbar.ax.transAxes,
        ha="left",
        va="bottom",
        fontsize=11,
    )
    cbar.ax.text(
        1.55,
        1.0,
        "Partial-Push",
        transform=cbar.ax.transAxes,
        ha="left",
        va="top",
        fontsize=11,
    )
    cbar.ax.text(
        1.55,
        0.5,
        "% Faster",
        transform=cbar.ax.transAxes,
        ha="left",
        va="center",
        fontsize=11,
    )
    plt.tight_layout()

    output_path = Path("./results") / args.output_filename
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved heatmap to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
