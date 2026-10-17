#!/usr/bin/env python3
"""Generate publication figures for the Regulation Z v2 benchmark."""

from __future__ import annotations

import argparse
import math
from pathlib import Path

from matplotlib.patches import Patch
import matplotlib.pyplot as plt
import pandas as pd

MODEL_KEYS = [
    "astra",
    "sol_6_1",
    "luna",
    "opus_5_5",
    "fable_5_1",
    "haiku_5_5",
    "gpt_oss_120b",
    "kimi_k3",
    "glm_5_3",
    "deepseek_v3_2",
    "qwen3_coder_480b",
]
SERIES_CONFIG = {
    "dfc": ("DFC", "#F58518", "o"),
    "astra": ("Astra 6", "#4C78A8", "s"),
    "sol_6_1": ("Sol 6.1", "#4C78A8", "^"),
    "luna": ("Luna 6", "#4C78A8", "P"),
    "opus_5_5": ("Opus 5.5", "#4C78A8", "D"),
    "fable_5_1": ("Fable 5.1", "#4C78A8", "v"),
    "haiku_5_5": ("Haiku 5.5", "#4C78A8", "*"),
    "gpt_oss_120b": ("GPT-OSS 120B", "#54A24B", "s"),
    "kimi_k3": ("Kimi K3", "#54A24B", "^"),
    "glm_5_3": ("GLM 5.3", "#54A24B", "P"),
    "deepseek_v3_2": ("DeepSeek V3.2", "#54A24B", "D"),
    "qwen3_coder_480b": ("Qwen3 Coder 480B", "#54A24B", "v"),
    "no_policy": ("No Policy", "#9D9D9D", "X"),
}


def _format_time(seconds: float) -> str:
    precision = 3 if seconds < 0.1 else 2
    return f"{seconds:.{precision}f} s"


def _metrics(frame: pd.DataFrame, keys: list[str]) -> tuple[list[float], list[float]]:
    required_columns = [f"{key}_time_ms" for key in keys]
    required_columns.extend(f"{key}_accuracy" for key in keys)
    missing = [column for column in required_columns if column not in frame]
    if missing:
        raise ValueError(f"Missing result columns: {', '.join(missing)}")
    times = []
    for key in keys:
        times.append((frame[f"{key}_time_ms"] / frame["num_updates"] / 1000).median())
    accuracies = [frame[f"{key}_accuracy"].median() * 100 for key in keys]
    return times, accuracies


def _merge_model_metrics(frame: pd.DataFrame, additional: pd.DataFrame) -> pd.DataFrame:
    """Add model metrics without replacing baselines measured by the main run."""
    columns = [
        column
        for key in MODEL_KEYS
        if f"{key}_time_ms" in additional
        for column in additional.columns
        if column.startswith(f"{key}_")
    ]
    if not columns:
        raise ValueError("Merged result CSV has no model metrics")
    collisions = sorted(set(columns) & set(frame.columns))
    if collisions:
        raise ValueError(
            "Merged model metrics already exist in the main input: " + ", ".join(collisions)
        )
    values = additional.iloc[0]
    added = pd.DataFrame(
        {column: [values[column]] * len(frame) for column in columns},
        index=frame.index,
    )
    return pd.concat([frame, added], axis=1)


def _latency_axis(
    axis: plt.Axes,
    times: list[float],
    max_seconds: float,
    labels: list[str],
    colors: list[str],
) -> None:
    bars = axis.barh(labels, times, color=colors)
    axis.set_xlabel("Average Latency (s)")
    axis.grid(axis="x", alpha=0.3)
    axis.set_xlim(0, max_seconds)
    label_offset = max_seconds * 0.012
    for bar, seconds in zip(bars, times, strict=True):
        inside_bar = seconds > max_seconds * 0.72
        axis.text(
            seconds - label_offset if inside_bar else seconds + label_offset,
            bar.get_y() + bar.get_height() / 2,
            _format_time(seconds),
            ha="right" if inside_bar else "left",
            va="center",
            color="white" if inside_bar else "black",
            fontsize=8,
        )
    axis.invert_yaxis()


def _accuracy_axis(
    axis: plt.Axes,
    frame: pd.DataFrame,
    accuracies: list[float],
    keys: list[str],
    labels: list[str],
    colors: list[str],
) -> None:
    bars = axis.barh(labels, accuracies, color=colors)
    axis.set_xlabel("Accuracy (%)")
    axis.set_xlim(0, 105)
    axis.grid(axis="x", alpha=0.3)
    for bar, accuracy in zip(bars, accuracies, strict=True):
        axis.text(
            min(accuracy + 1.0, 102.0),
            bar.get_y() + bar.get_height() / 2,
            f"{accuracy:.0f}%",
            ha="left",
            va="center",
            fontsize=8,
        )
    for index, key in enumerate(keys):
        false_positives = int(frame.get(f"{key}_false_positives", pd.Series([0])).median())
        false_negatives = int(frame.get(f"{key}_false_negatives", pd.Series([0])).median())
        if key == "no_policy":
            false_negatives = int(frame["invalid_updates"].median())
        parts = []
        if false_positives:
            parts.append(
                f"{false_positives} false positive" + ("s" if false_positives != 1 else "")
            )
        if false_negatives:
            separator = "\n" if false_negatives >= 10 else " "
            parts.append(
                f"{false_negatives} false{separator}negative"
                + ("s" if false_negatives != 1 else "")
            )
        if parts:
            axis.text(
                max(accuracies[index] - 2.0, 2.0),
                bars[index].get_y() + bars[index].get_height() / 2,
                ", ".join(parts),
                ha="right",
                va="center",
                fontsize=7,
                color="white",
            )


def _save_latency(
    output: Path,
    times: list[float],
    max_seconds: float,
    labels: list[str],
    colors: list[str],
) -> None:
    fig, axis = plt.subplots(figsize=(5.4, 3.4))
    _latency_axis(axis, times, max_seconds, labels, colors)
    fig.tight_layout()
    fig.savefig(output, dpi=300, bbox_inches="tight")
    plt.close(fig)


def _save_combined(
    output: Path,
    frame: pd.DataFrame,
    times: list[float],
    accuracies: list[float],
    max_seconds: float,
    keys: list[str],
    labels: list[str],
    colors: list[str],
) -> None:
    fig, (accuracy_axis, latency_axis) = plt.subplots(
        1,
        2,
        figsize=(11.0, max(4.2, len(labels) * 0.43)),
        sharey=True,
        gridspec_kw={"width_ratios": [1.0, 1.35]},
    )
    _accuracy_axis(accuracy_axis, frame, accuracies, keys, labels, colors)
    accuracy_axis.set_title("(a) Decision accuracy", loc="left")
    _latency_axis(latency_axis, times, max_seconds, labels, colors)
    latency_axis.set_title("(b) Update latency", loc="left")
    latency_axis.tick_params(axis="y", labelleft=False)
    legend = [
        Patch(facecolor="#F58518", label="DFC"),
        Patch(facecolor="#4C78A8", label="Closed model"),
        Patch(facecolor="#54A24B", label="Open-weight model"),
        Patch(facecolor="#9D9D9D", label="No Policy"),
    ]
    fig.legend(handles=legend, loc="upper center", ncol=4, frameon=False)
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    fig.savefig(output, dpi=300, bbox_inches="tight")
    plt.close(fig)


def _save_scatter(
    output: Path,
    times: list[float],
    accuracies: list[float],
    labels: list[str],
    colors: list[str],
    markers: list[str],
) -> None:
    latency_ms = [seconds * 1000 for seconds in times]
    fig, axis = plt.subplots(figsize=(6.2, 3.8))
    for x, y, color, marker in zip(latency_ms, accuracies, colors, markers, strict=True):
        axis.scatter(x, y, color=color, marker=marker, s=95, edgecolors="white", linewidth=0.8)
    offsets = {
        "DFC": (12, -18),
        "Astra 6": (-12, 15),
        "Sol 6.1": (-16, -22),
        "Luna 6": (12, -18),
        "Opus 5.5": (-12, 15),
        "Fable 5.1": (12, -18),
        "Haiku 5.5": (12, 15),
        "No Policy": (14, 15),
        "GPT-OSS 120B": (12, 15),
        "Kimi K3": (-12, -18),
        "GLM 5.3": (12, -18),
        "DeepSeek V3.2": (-12, 15),
        "Qwen3 Coder 480B": (12, -18),
    }
    for label, x, y in zip(labels, latency_ms, accuracies, strict=True):
        dx, dy = offsets[label]
        axis.annotate(
            label,
            (x, y),
            xytext=(dx, dy),
            textcoords="offset points",
            ha="left" if dx > 0 else "right",
            va="center",
            arrowprops={"arrowstyle": "-", "color": "#666666", "linewidth": 0.8},
            fontsize=8,
        )
    axis.set_xscale("log")
    axis.set_xlabel("Average Latency (ms)")
    axis.set_ylabel("Accuracy (%)")
    axis.set_ylim(47, 103)
    axis.grid(alpha=0.25)
    fig.tight_layout()
    fig.savefig(output, dpi=300, bbox_inches="tight")
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        default="results/regulation_z_v2_benchmark_results.csv",
    )
    parser.add_argument(
        "--output",
        default="results/regulation_z_v2_benchmark_latency.png",
    )
    parser.add_argument(
        "--combined-output",
        default="results/regulation_z_v2_benchmark_accuracy_time.png",
    )
    parser.add_argument(
        "--scatter-output",
        default="results/regulation_z_v2_benchmark_accuracy_latency_scatter.png",
    )
    parser.add_argument(
        "--dfc-override",
        help="Optional Rust DFC summary CSV whose DFC metrics replace the input row's metrics.",
    )
    parser.add_argument(
        "--merge-input",
        action="append",
        default=[],
        help="Additional one-row result CSV whose metrics are merged into the main input.",
    )
    parser.add_argument(
        "--model-override",
        action="append",
        default=[],
        metavar="KEY=CSV",
        help="One-row result CSV whose metrics replace a single model in the main input.",
    )
    parser.add_argument("--max-seconds", type=float)
    args = parser.parse_args()
    frame = pd.read_csv(args.input)
    if frame.empty:
        raise ValueError("No Regulation Z v2 benchmark rows found")
    for filename in args.merge_input:
        additional = pd.read_csv(filename)
        if len(additional) != 1:
            raise ValueError("Merged result CSVs must contain exactly one summary row")
        frame = _merge_model_metrics(frame, additional)
    if args.dfc_override:
        override = pd.read_csv(args.dfc_override)
        if len(override) != 1:
            raise ValueError("DFC override must contain exactly one summary row")
        for column in (
            "dfc_correct",
            "dfc_accuracy",
            "dfc_rewrite_time_ms",
            "dfc_execution_time_ms",
            "dfc_time_ms",
        ):
            frame[column] = override.iloc[0][column]
    for value in args.model_override:
        try:
            key, filename = value.split("=", 1)
        except ValueError as error:
            raise ValueError("Model overrides must use KEY=CSV") from error
        if key not in MODEL_KEYS:
            raise ValueError(f"Unknown model override key: {key}")
        override = pd.read_csv(filename)
        if len(override) != 1:
            raise ValueError("Model override must contain exactly one summary row")
        columns = [column for column in override if column.startswith(f"{key}_")]
        if not columns:
            raise ValueError(f"Model override has no {key}_ columns")
        for column in columns:
            frame[column] = override.iloc[0][column]

    keys = ["dfc", *[key for key in MODEL_KEYS if f"{key}_time_ms" in frame], "no_policy"]
    labels = [SERIES_CONFIG[key][0] for key in keys]
    colors = [SERIES_CONFIG[key][1] for key in keys]
    markers = [SERIES_CONFIG[key][2] for key in keys]
    times, accuracies = _metrics(frame, keys)
    max_seconds = args.max_seconds or math.ceil(max(times) * 1.12)
    outputs = [Path(args.output), Path(args.combined_output), Path(args.scatter_output)]
    for output in outputs:
        output.parent.mkdir(parents=True, exist_ok=True)
    _save_latency(outputs[0], times, max_seconds, labels, colors)
    _save_combined(outputs[1], frame, times, accuracies, max_seconds, keys, labels, colors)
    _save_scatter(outputs[2], times, accuracies, labels, colors, markers)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
