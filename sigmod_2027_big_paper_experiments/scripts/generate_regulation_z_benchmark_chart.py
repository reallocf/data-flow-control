#!/usr/bin/env python3
"""Generate the update-latency figure for the Regulation Z benchmark."""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

MODEL_KEYS = ["astra", "sol_6_1", "opus_5_5", "fable_5_1"]


def _format_time(seconds: float) -> str:
    precision = 3 if seconds < 0.1 else 2
    return f"{seconds:.{precision}f} s"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        default="results/regulation_z_benchmark_results.csv",
    )
    parser.add_argument(
        "--output",
        default="results/regulation_z_benchmark_latency.png",
    )
    args = parser.parse_args()
    df = pd.read_csv(args.input)
    if df.empty:
        raise ValueError("No Regulation Z benchmark rows found")

    labels = ["DFC", "Astra 6", "Sol 6.1", "Opus 5.5", "Fable 5.1", "No Policy"]
    keys = ["dfc", *MODEL_KEYS, "no_policy"]
    colors = ["#F58518", "#4C78A8", "#72B7B2", "#E45756", "#B279A2", "#9D9D9D"]
    times = [(df[f"{key}_time_ms"] / df["num_updates"] / 1000).median() for key in keys]
    fig, time_ax = plt.subplots(figsize=(5.4, 3.4))
    time_bars = time_ax.barh(labels, times, color=colors)
    time_ax.set_xlabel("Average Latency (s)")
    time_ax.grid(axis="x", alpha=0.3)
    time_ax.set_xlim(0, 14)
    label_offset = 0.15
    for label, bar, seconds in zip(labels, time_bars, times, strict=True):
        inside_bar = label in {"Opus 5.5", "Fable 5.1"}
        time_ax.text(
            seconds - label_offset if inside_bar else seconds + label_offset,
            bar.get_y() + bar.get_height() / 2,
            _format_time(seconds),
            ha="right" if inside_bar else "left",
            va="center",
            color="white" if inside_bar else "black",
            fontsize=8,
        )
    time_ax.invert_yaxis()
    fig.tight_layout()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=300, bbox_inches="tight")
    plt.close(fig)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
