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

    grouped = (
        df.groupby([x_dimension, "policy_column_count"], as_index=False)[
            ["dfc_1phase_exec_time_ms", "dfc_2phase_exec_time_ms"]
        ]
        .mean()
    )
    grouped["ratio_1phase_to_2phase"] = (
        grouped["dfc_1phase_exec_time_ms"] / grouped["dfc_2phase_exec_time_ms"]
    )
    grouped["ratio_delta"] = grouped["ratio_1phase_to_2phase"] - 1.0

    x_values = sorted(grouped[x_dimension].astype(int).unique().tolist())
    policy_values = sorted(grouped["policy_column_count"].astype(int).unique().tolist())
    ratio_matrix = np.full((len(policy_values), len(x_values)), np.nan)
    delta_matrix = np.full((len(policy_values), len(x_values)), np.nan)

    for _, row in grouped.iterrows():
        j = x_values.index(int(row[x_dimension]))
        p = policy_values.index(int(row["policy_column_count"]))
        ratio_matrix[p, j] = float(row["ratio_1phase_to_2phase"])
        delta_matrix[p, j] = float(row["ratio_delta"])

    valid = delta_matrix[np.isfinite(delta_matrix)]
    if valid.size == 0:
        raise ValueError("No valid ratio values to plot.")
    delta_matrix = np.clip(delta_matrix, -1.0, 1.0)

    fig, ax = plt.subplots(figsize=(10, 7))
    im = ax.imshow(
        delta_matrix,
        aspect="auto",
        origin="lower",
        cmap="RdBu_r",
        norm=TwoSlopeNorm(vmin=-1.0, vcenter=0.0, vmax=1.0),
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
    ax.set_title("1Phase / 2Phase Execution Time Ratio", fontsize=14, fontweight="bold")

    for y_idx in range(len(policy_values)):
        for x_idx in range(len(x_values)):
            val = delta_matrix[y_idx, x_idx]
            if np.isfinite(val):
                ax.text(x_idx, y_idx, f"{val:+.2f}", ha="center", va="center", fontsize=9)

    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label("Ratio Delta ((1Phase / 2Phase) - 1), clipped to [-1, 1]", fontsize=11)
    plt.tight_layout()

    output_path = Path("./results") / args.output_filename
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved heatmap to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
