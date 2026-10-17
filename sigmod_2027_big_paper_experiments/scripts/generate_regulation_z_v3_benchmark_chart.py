#!/usr/bin/env python3
"""Generate the Regulation Z v3 accuracy-versus-latency scatter plot."""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
import math
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

SERIES = {
    "dfc": ("DFC", "#D62728", "*"),
    "astra": ("Astra 6", "#10A37F", "o"),
    "sol_6_1": ("Sol 6.1", "#10A37F", "o"),
    "luna": ("Luna 6", "#10A37F", "o"),
    "fable_5_1": ("Fable 5.1", "#D97757", "o"),
    "opus_5_5": ("Opus 5.5", "#D97757", "o"),
    "glm_5_3": ("GLM 5.3", "#000000", "o"),
    # Blue from https://www.kimi.ai/resources/kimi-brand.
    "kimi_k3": ("Kimi K3", "#007CFF", "o"),
    "minimax_m2_5": ("MiniMax M2.5", "#F45B94", "o"),
    "no_policy": ("No Policy", "#9D9D9D", "X"),
}


@dataclass(frozen=True)
class ApproachMetrics:
    accuracy: float
    average_latency_seconds: float


def _as_bool(value: str) -> bool:
    if value == "True":
        return True
    if value == "False":
        return False
    raise ValueError(f"Expected boolean CSV value, got {value!r}")


def summarize_progress(path: Path) -> dict[str, ApproachMetrics]:
    grouped: dict[str, list[dict[str, str]]] = {}
    with path.open(encoding="utf-8", newline="") as input_file:
        for row in csv.DictReader(input_file):
            grouped.setdefault(row["approach"], []).append(row)

    metrics = {}
    for approach, rows in grouped.items():
        correct = sum(
            not row.get("error")
            and row.get("parse_failure") != "True"
            and _as_bool(row["expected_allow"]) == _as_bool(row["actual_allow"])
            for row in rows
        )
        timed = [float(row["elapsed_ms"]) for row in rows if row.get("elapsed_ms")]
        metrics[approach] = ApproachMetrics(
            accuracy=correct / len(rows),
            average_latency_seconds=sum(timed) / len(timed) / 1000.0 if timed else float("nan"),
        )
    return metrics


def save_scatter(
    metrics: dict[str, ApproachMetrics],
    output: Path,
    *,
    log_latency: bool = False,
    no_ties_layout: bool = False,
    show_random_chance: bool = False,
) -> None:
    series = {k: v for k, v in SERIES.items() if k in metrics}
    fig, axis = plt.subplots(figsize=(5.0625, 2.5))
    for key, (_label, color, marker) in series.items():
        point = metrics[key]
        axis.scatter(
            point.average_latency_seconds,
            point.accuracy * 100,
            color=color,
            marker=marker,
            s=200 if key == "dfc" else (70 if key == "fable_5_1" else 95),
            edgecolors="white",
            linewidth=0.8,
            zorder=4 if key == "fable_5_1" else 3,
            clip_on=False,
        )

    offsets = {
        "DFC": (8, -12),
        "No Policy": (-10, -15),
        "Astra 6": (15, 30),
        "Sol 6.1": (-15, 50),
        "Luna 6": (20, -60),
        "Fable 5.1": (35, 0),
        "Opus 5.5": (0, -35),
        "GLM 5.3": (-20, 25),
        "Kimi K3": (0, -25),
        "MiniMax M2.5": (0, 25),
    }
    if log_latency:
        offsets.update(
            {
                "No Policy": (8, -10),
                "Sol 6.1": (-12, 16),
                "Astra 6": (-4, 38),
                "Luna 6": (-5, -40),
                "Opus 5.5": (-12, -14),
                "Fable 5.1": (8, 16),
                "GLM 5.3": (0, 40),
                "Kimi K3": (-8, -20),
                "MiniMax M2.5": (0, -48),
            }
        )
    if no_ties_layout:
        offsets.update(
            {
                "Sol 6.1": (-10, -32),
                "Astra 6": (-10, -12),
                "Luna 6": (-5, -72),
                "Opus 5.5": (-10, -52),
                "Fable 5.1": (-5, -90),
                "GLM 5.3": (8, -48),
                "Kimi K3": (8, -72),
                "MiniMax M2.5": (0, -22),
            }
        )
    for key, (label, _color, _marker) in series.items():
        point = metrics[key]
        dx, dy = offsets[label]
        axis.annotate(
            label,
            (point.average_latency_seconds, point.accuracy * 100),
            xytext=(dx, dy),
            textcoords="offset points",
            ha="left" if dx > 0 else "right",
            va="center",
            arrowprops={"arrowstyle": "-", "color": "#666666", "linewidth": 0.8},
            fontsize=8,
            bbox={"facecolor": "white", "edgecolor": "none", "pad": 0.5}
            if no_ties_layout
            else None,
        )

    axis.set_xlabel("Average Latency (s)")
    axis.set_ylabel("Accuracy (%)")
    max_latency = max(point.average_latency_seconds for point in metrics.values())
    if log_latency:
        min_latency = min(point.average_latency_seconds for point in metrics.values())
        if min_latency <= 0:
            raise ValueError("Log latency requires positive measurements")
        axis.set_xscale("log")
        axis.xaxis.set_major_formatter(FuncFormatter(lambda value, _position: f"{value:g}"))
        axis.set_xlim(10 ** math.floor(math.log10(min_latency)), max_latency * 2.5)
    else:
        axis.set_xlim(-max_latency * 0.12, max_latency * 1.12)
    axis.set_ylim(0, 100)
    if show_random_chance:
        axis.axhline(50, color="#C0C0C0", linestyle="--", linewidth=1, zorder=1)
        axis.annotate(
            "random chance",
            xy=(0.27, 50),
            xycoords=axis.get_yaxis_transform(),
            xytext=(0, 3),
            textcoords="offset points",
            ha="left",
            va="bottom",
            fontsize=8,
            color="#999999",
        )
    axis.grid(alpha=0.25)
    fig.tight_layout()
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=300, bbox_inches="tight")
    fig.savefig(output.with_suffix(".pdf"), bbox_inches="tight")
    plt.close(fig)


def save_bars(metrics: dict[str, ApproachMetrics], output: Path) -> None:
    keys = [key for key in SERIES if key in metrics]
    fig, (accuracy, latency) = plt.subplots(1, 2, figsize=(10, 5), sharey=True)
    colors = [SERIES[key][1] for key in keys]
    ys = list(range(len(keys)))
    accuracy.barh(ys, [metrics[k].accuracy * 100 for k in keys], color=colors)
    latency.barh(ys, [metrics[k].average_latency_seconds for k in keys], color=colors)
    accuracy.set_yticks(ys, [SERIES[k][0] for k in keys])
    accuracy.invert_yaxis()
    accuracy.set_xlim(0, 112)
    accuracy.set_xticks([0, 25, 50, 75, 100])
    accuracy.set_xlabel("Accuracy (%)")
    latency.set_xlabel("Average Latency (s)")
    latency.set_xlim(0, max(metrics[k].average_latency_seconds for k in keys) * 1.25)
    for i, key in enumerate(keys):
        point = metrics[key]
        accuracy.text(point.accuracy * 100 + 1, i, f"{point.accuracy:.0%}", va="center", fontsize=9)
        latency.annotate(
            f"{point.average_latency_seconds:.3f}"
            if key in {"dfc", "no_policy"}
            else f"{point.average_latency_seconds:.1f}",
            (point.average_latency_seconds, i),
            xytext=(4, 0),
            textcoords="offset points",
            va="center",
            fontsize=9,
        )
    for axis in (accuracy, latency):
        axis.grid(axis="x", alpha=0.2)
        axis.set_axisbelow(True)
    fig.tight_layout()
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=300, bbox_inches="tight")
    fig.savefig(output.with_suffix(".pdf"), bbox_inches="tight")
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log-latency", action="store_true", help="Use a log latency axis for the scatterplot."
    )
    parser.add_argument(
        "--no-ties-layout",
        action="store_true",
        help="Place labels below the high-accuracy no-ties results.",
    )
    parser.add_argument(
        "--show-random-chance", action="store_true", help="Mark random chance at 50% accuracy."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("results/regulation_z_v3_closed_100_2026-10-12.progress.csv"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("results/regulation_z_v3_accuracy_latency_scatter.png"),
    )
    args = parser.parse_args()
    metrics = summarize_progress(args.input)
    save_scatter(
        metrics,
        args.output,
        log_latency=args.log_latency,
        no_ties_layout=args.no_ties_layout,
        show_random_chance=args.show_random_chance,
    )
    if not args.log_latency:
        save_bars(metrics, args.output.with_name(args.output.stem + "_bars.png"))
    for key in SERIES:
        if key not in metrics:
            continue
        point = metrics[key]
        print(
            f"{SERIES[key][0]}: accuracy={point.accuracy:.3f}, "
            f"average latency={point.average_latency_seconds:.3f}s"
        )
    print(f"Saved {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
