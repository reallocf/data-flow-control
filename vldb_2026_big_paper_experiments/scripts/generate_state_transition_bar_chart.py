#!/usr/bin/env python3
"""Generate a bar chart for the state-transition experiment timings."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from vldb_experiments.visualizations import load_results  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a state-transition timing bar chart.")
    parser.add_argument(
        "--csv",
        default="results/state_transition_results.csv",
        help="Input CSV path (default: results/state_transition_results.csv)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output image path (default: derived from the CSV contents)",
    )
    args = parser.parse_args()

    df = load_results(args.csv)
    if df.empty:
        raise ValueError("No execution rows found in CSV")

    num_updates = int(df["num_updates"].iloc[0])
    labels = ["No Policy", "1Phase"]
    means = [
        (df["no_policy_time_ms"] / df["num_updates"]).mean(),
        (df["dfc_1phase_time_ms"] / df["num_updates"]).mean(),
    ]
    stds = [
        (df["no_policy_time_ms"] / df["num_updates"]).std(ddof=1),
        (df["dfc_1phase_time_ms"] / df["num_updates"]).std(ddof=1),
    ]
    colors = ["#4C78A8", "#F58518"]

    if "gpt_5_2_time_ms" in df.columns:
        labels.append("GPT-5.2")
        means.append((df["gpt_5_2_time_ms"] / df["num_updates"]).mean())
        stds.append((df["gpt_5_2_time_ms"] / df["num_updates"]).std(ddof=1))
        colors.append("#54A24B")

    fig, ax = plt.subplots(figsize=(9, 5))

    ax.bar(labels, means, color=colors, width=0.6)
    ax.set_ylabel("Average Time Per Update (ms)")
    ax.set_yscale("log")
    ax.grid(axis="y", alpha=0.3)

    output_path = Path(args.output) if args.output else Path(f"results/state_transition_timing_{num_updates}_updates.png")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
