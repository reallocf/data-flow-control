"""Paper-final visualization entrypoints driven from final_results/ CSVs only."""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from vldb_experiments.visualizations import (
    FINAL_ANNOTATION_FONTSIZE,
    FINAL_AXIS_LABEL_FONTSIZE,
    FINAL_TICK_FONTSIZE,
    FULL_PUSH_LABEL,
    PARTIAL_PUSH_LABEL,
    _with_exec_time_columns,
    create_multi_source_heatmap_chart,
    load_results,
)

# PVLDB 2026 uses ACM sigconf geometry: 8.5in paper, 54pt side margins,
# and 2pc column separation, giving a single-column width of about 3.337in.
COLUMN_WIDTH_IN = 3.336758
COLUMN_TICK_FONTSIZE = 7
COLUMN_AXIS_LABEL_FONTSIZE = 8
COLUMN_LEGEND_FONTSIZE = 7
COLUMN_ANNOTATION_FONTSIZE = 6
COLUMN_LINE_WIDTH = 1.4
COLUMN_MARKER_SIZE = 4.0

FINAL_MARKERS = {
    "No Policy": "o",
    FULL_PUSH_LABEL: "s",
    f"{FULL_PUSH_LABEL} Optimized": "^",
    PARTIAL_PUSH_LABEL: "D",
    "Logical": "P",
    "Physical": "X",
}

RELATIVE_PERF_CMAP = matplotlib.colors.LinearSegmentedColormap.from_list(
    "lightened_relative_perf",
    ["#5fa8d3", "#f7f7f7", "#e76f51"],
)
MULTI_SOURCE_BLUE_CMAP = matplotlib.colors.LinearSegmentedColormap.from_list(
    "multi_source_blue",
    ["#8ecae6", "#023e8a"],
)


def create_tpch_duckdb_capped_overhead_chart(
    df: pd.DataFrame,
    *,
    cap_pct: float,
    output_path: Path,
    x_min: float | None = None,
) -> None:
    df = _with_exec_time_columns(df)
    required_cols = {
        "query_num",
        "no_policy_exec_time_ms",
        "dfc_1phase_exec_time_ms",
        "dfc_2phase_exec_time_ms",
        "logical_exec_time_ms",
        "physical_exec_time_ms",
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns for capped TPCH chart: {sorted(missing)}")

    grouped = (
        df.groupby("query_num", as_index=True)[
            [
                "no_policy_exec_time_ms",
                "dfc_1phase_exec_time_ms",
                "dfc_2phase_exec_time_ms",
                "logical_exec_time_ms",
                "physical_exec_time_ms",
            ]
        ]
        .mean()
        .sort_index()
    )

    baseline = grouped["no_policy_exec_time_ms"]
    dfc_1phase_overhead = ((grouped["dfc_1phase_exec_time_ms"] - baseline) / baseline) * 100.0
    dfc_2phase_overhead = ((grouped["dfc_2phase_exec_time_ms"] - baseline) / baseline) * 100.0
    logical_overhead = ((grouped["logical_exec_time_ms"] - baseline) / baseline) * 100.0
    physical_overhead = ((grouped["physical_exec_time_ms"] - baseline) / baseline) * 100.0

    dfc_1phase_plot = dfc_1phase_overhead.clip(upper=cap_pct)
    dfc_2phase_plot = dfc_2phase_overhead.clip(upper=cap_pct)
    logical_plot = logical_overhead.clip(upper=cap_pct)
    physical_plot = physical_overhead.where(~grouped.index.isin([4, 18])).clip(upper=cap_pct)

    y_positions = np.arange(len(grouped.index))
    bar_height = 0.18
    offsets = [-1.5 * bar_height, -0.5 * bar_height, 0.5 * bar_height, 1.5 * bar_height]

    fig, ax = plt.subplots(figsize=(COLUMN_WIDTH_IN, 4.0625))
    dfc_1phase_bars = ax.barh(
        y_positions + offsets[0],
        dfc_1phase_plot,
        height=bar_height,
        label=FULL_PUSH_LABEL,
        color="#ff7f0e",
    )
    dfc_2phase_bars = ax.barh(
        y_positions + offsets[1],
        dfc_2phase_plot,
        height=bar_height,
        label=PARTIAL_PUSH_LABEL,
        color="#9467bd",
    )
    logical_bars = ax.barh(
        y_positions + offsets[2],
        logical_plot,
        height=bar_height,
        label="Logical",
        color="#2ca02c",
    )
    physical_bars = ax.barh(
        y_positions + offsets[3],
        physical_plot,
        height=bar_height,
        label="Physical",
        color="#d62728",
    )

    ax.set_yticks(y_positions)
    ax.set_yticklabels([f"Q{int(q):02d}" for q in grouped.index], fontsize=COLUMN_TICK_FONTSIZE)
    ax.invert_yaxis()
    ax.set_xlabel("Overhead vs No Policy (%)", fontsize=COLUMN_AXIS_LABEL_FONTSIZE)
    if x_min is None:
        x_min = min(-50.0, float(dfc_1phase_plot.min()) - 5.0)
    ax.set_xlim(left=x_min, right=cap_pct)
    ax.grid(axis="x", alpha=0.3)
    ax.legend(
        loc="lower right",
        ncol=2,
        fontsize=COLUMN_LEGEND_FONTSIZE,
        frameon=True,
        framealpha=0.85,
        handlelength=1.4,
        columnspacing=0.9,
    )
    ax.tick_params(axis="x", labelsize=COLUMN_TICK_FONTSIZE)

    def _annotate_capped(bars, original_values):
        for bar, original in zip(bars, original_values):
            if pd.isna(original) or original <= cap_pct:
                continue
            x = cap_pct
            y = bar.get_y() - (bar.get_height() * 0.2)
            ax.text(
                x,
                y,
                f"{original:.0f}%",
                ha="right",
                va="bottom",
                rotation=0,
                fontsize=COLUMN_ANNOTATION_FONTSIZE,
                clip_on=False,
            )

    _annotate_capped(dfc_1phase_bars, dfc_1phase_overhead)
    _annotate_capped(dfc_2phase_bars, dfc_2phase_overhead)
    _annotate_capped(logical_bars, logical_overhead)
    _annotate_capped(physical_bars, physical_overhead)

    for query_num in [4, 18]:
        if query_num not in grouped.index:
            continue
        y = y_positions[list(grouped.index).index(query_num)] + offsets[3]
        ax.text(
            cap_pct * 0.015,
            y,
            "N/A",
            color="#d62728",
            fontsize=COLUMN_ANNOTATION_FONTSIZE,
            fontweight="bold",
            ha="left",
            va="center",
        )

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def create_tpch_duckdb_provenance_capped_overhead_chart(
    df: pd.DataFrame,
    *,
    output_path: Path,
    show_dfc_label: bool = True,
    zero_dfc_bar: bool = False,
) -> None:
    df = _with_exec_time_columns(df)
    required_cols = {
        "query_num",
        "no_policy_exec_time_ms",
        "dfc_1phase_exec_time_ms",
        "logical_exec_time_ms",
        "physical_exec_time_ms",
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns for provenance TPCH chart: {sorted(missing)}")

    grouped = (
        df.groupby("query_num", as_index=True)[
            [
                "no_policy_exec_time_ms",
                "dfc_1phase_exec_time_ms",
                "logical_exec_time_ms",
                "physical_exec_time_ms",
            ]
        ]
        .mean()
        .sort_index()
    )

    baseline = grouped["no_policy_exec_time_ms"]
    dfc_overhead = ((grouped["dfc_1phase_exec_time_ms"] - baseline) / baseline) * 100.0
    logical_overhead = ((grouped["logical_exec_time_ms"] - baseline) / baseline) * 100.0
    physical_overhead = ((grouped["physical_exec_time_ms"] - baseline) / baseline) * 100.0
    physical_overhead = physical_overhead.where(~grouped.index.isin([4, 18]))

    complete_overheads = pd.DataFrame(
        {
            "dfc": dfc_overhead,
            "logical": logical_overhead,
            "physical": physical_overhead,
        }
    ).dropna()
    if complete_overheads.empty:
        raise ValueError("No TPCH queries have complete DFC, logical provenance, and physical provenance results.")

    plot_specs = [
        ("SOA Data Flow 1", float(complete_overheads["logical"].mean()), "#2ca02c"),
        ("SOA Data Flow 2", float(complete_overheads["physical"].mean()), "#d62728"),
        ("DFC Rewriter", float(complete_overheads["dfc"].mean()), "#ff7f0e"),
    ]

    labels = [label for label, _, _ in plot_specs]
    original_values = [value for _, value, _ in plot_specs]
    plot_values = [
        0.0 if label == "DFC Rewriter" and zero_dfc_bar else value
        for label, value in zip(labels, original_values)
    ]
    colors = [color for _, _, color in plot_specs]
    x_positions = list(range(len(labels)))

    fig, ax = plt.subplots(figsize=(12, 4.5))
    bars = ax.bar(x_positions, plot_values, width=0.8, color=colors)

    ax.set_xticks(x_positions)
    ax.set_xticklabels(labels, fontsize=FINAL_TICK_FONTSIZE)
    ax.set_ylabel("Overhead vs No Policy (%)", fontsize=FINAL_AXIS_LABEL_FONTSIZE)
    ax.set_yscale("symlog", linthresh=1.0, linscale=1.0)
    positive_values = [value for value in plot_values if value > 0]
    if not positive_values:
        raise ValueError("Cannot create log-scale provenance chart without positive overhead values.")
    ax.set_ylim(bottom=0.0, top=10000.0)
    ax.set_yticks([0.0, 1.0, 10.0, 100.0, 1000.0, 10000.0])
    ax.set_yticklabels(["0", "1", "10", "100", "1000", "10000"])
    ax.grid(axis="y", alpha=0.3)
    ax.tick_params(axis="y", labelsize=FINAL_TICK_FONTSIZE)

    for label, bar, original, plot_value in zip(labels, bars, original_values, plot_values):
        if pd.isna(original):
            continue
        if plot_value <= 0:
            continue
        if label == "DFC Rewriter" and not show_dfc_label:
            continue
        x = bar.get_x() + bar.get_width() / 2.0
        if label.startswith("SOA Data Flow"):
            y = plot_value / 1.15
            vertical_alignment = "top"
            text_color = "white"
            clip_on = True
        else:
            y = plot_value * 1.15
            vertical_alignment = "bottom"
            text_color = "black"
            clip_on = False
        ax.text(
            x,
            y,
            f"{original:.2f}%" if abs(original) < 1.0 else f"{original:.0f}%",
            ha="center",
            va=vertical_alignment,
            rotation=0,
            fontsize=FINAL_ANNOTATION_FONTSIZE * 3,
            color=text_color,
            clip_on=clip_on,
        )

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def create_phase_competition_heatmap(
    df: pd.DataFrame,
    *,
    output_path: Path,
) -> None:
    plot_df = df.copy()
    if "run_num" in plot_df.columns:
        plot_df = plot_df[plot_df["run_num"].fillna(0).astype(int) > 0].copy()
    required = {
        "join_fanout",
        "policy_column_count",
        "dfc_1phase_exec_time_ms",
        "dfc_2phase_exec_time_ms",
        "correctness_match",
    }
    missing = required - set(plot_df.columns)
    if missing:
        raise ValueError(f"Missing required columns for phase competition heatmap: {sorted(missing)}")
    bad = plot_df[plot_df["correctness_match"].astype(str).str.lower() != "true"]
    if not bad.empty:
        raise ValueError("Phase competition CSV contains correctness mismatches.")

    grouped = (
        plot_df.groupby(["join_fanout", "policy_column_count"], as_index=False)[
            ["dfc_1phase_exec_time_ms", "dfc_2phase_exec_time_ms"]
        ]
        .mean()
    )
    grouped["relative_perf"] = grouped["dfc_1phase_exec_time_ms"] / grouped["dfc_2phase_exec_time_ms"]

    fanouts = sorted(grouped["join_fanout"].astype(int).unique().tolist())
    policy_counts = sorted(grouped["policy_column_count"].astype(int).unique().tolist())
    heatmap = pd.DataFrame(index=policy_counts, columns=fanouts, dtype=float)
    for _, row in grouped.iterrows():
        x_idx = fanouts.index(int(row["join_fanout"]))
        y_idx = policy_counts.index(int(row["policy_column_count"]))
        heatmap.iat[y_idx, x_idx] = float(row["relative_perf"])

    log_heatmap = np.log2(heatmap.astype(float))
    finite_vals = log_heatmap.values[np.isfinite(log_heatmap.values)]

    fig, ax = plt.subplots(figsize=(COLUMN_WIDTH_IN, 2.55))
    max_abs = float(np.nanmax(np.abs(finite_vals)))
    cmap = RELATIVE_PERF_CMAP
    cmap.set_bad(color="#f0f0f0")
    norm = matplotlib.colors.TwoSlopeNorm(vmin=-max_abs, vcenter=0.0, vmax=max_abs)
    ax.imshow(log_heatmap.astype(float), cmap=cmap, norm=norm, aspect="auto")
    ax.set_facecolor("white")
    ax.set_xticks(range(len(fanouts)))
    ax.set_xticklabels(fanouts, fontsize=COLUMN_TICK_FONTSIZE)
    ax.set_yticks(range(len(policy_counts)))
    ax.set_yticklabels(policy_counts, fontsize=COLUMN_TICK_FONTSIZE)
    ax.set_xticks(np.arange(-0.5, len(fanouts), 1), minor=True)
    ax.set_yticks(np.arange(-0.5, len(policy_counts), 1), minor=True)
    ax.set_xlabel("Join Fanout", fontsize=COLUMN_AXIS_LABEL_FONTSIZE)
    ax.set_ylabel("Policy Columns", fontsize=COLUMN_AXIS_LABEL_FONTSIZE)
    ax.set_xlim(-0.5, len(fanouts) - 0.5)
    ax.set_ylim(-0.5, len(policy_counts) - 0.5)
    ax.grid(which="minor", color="#d0d0d0", linestyle="-", linewidth=0.8)
    ax.tick_params(which="minor", bottom=False, left=False)
    for y_idx in range(len(policy_counts)):
        for x_idx in range(len(fanouts)):
            val = heatmap.iat[y_idx, x_idx]
            if np.isfinite(val):
                log_val = log_heatmap.iat[y_idx, x_idx]
                ax.text(
                    x_idx,
                    y_idx,
                    f"{log_val:.2f}",
                    ha="center",
                    va="center",
                    fontsize=COLUMN_ANNOTATION_FONTSIZE,
                    color="black",
                )

    sm = plt.cm.ScalarMappable(norm=norm, cmap=cmap)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, fraction=0.046, pad=0.03)
    cbar.set_label("Relative Execution Time", fontsize=COLUMN_AXIS_LABEL_FONTSIZE)
    cbar.ax.tick_params(labelsize=COLUMN_TICK_FONTSIZE)
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def create_state_transition_chart(
    df: pd.DataFrame,
    *,
    output_path: Path,
) -> None:
    if df.empty:
        raise ValueError("No state-transition rows found.")

    labels = ["No Policy", FULL_PUSH_LABEL]
    means = [
        (df["no_policy_time_ms"] / df["num_updates"]).mean(),
        (df["dfc_1phase_time_ms"] / df["num_updates"]).mean(),
    ]
    colors = ["#4C78A8", "#F58518"]

    if "gpt_5_2_time_ms" in df.columns:
        labels.append("GPT-5.2")
        means.append((df["gpt_5_2_time_ms"] / df["num_updates"]).mean())
        colors.append("#54A24B")
    if "opus_4_6_time_ms" in df.columns:
        labels.append("Opus 4.6")
        means.append((df["opus_4_6_time_ms"] / df["num_updates"]).mean())
        colors.append("#B279A2")

    fig, ax = plt.subplots(figsize=(COLUMN_WIDTH_IN, 1.65))
    ax.barh(labels, means, color=colors, height=0.55)
    ax.set_xlabel("Update Time (ms)", fontsize=COLUMN_AXIS_LABEL_FONTSIZE)
    ax.set_xscale("log")
    ax.grid(axis="x", alpha=0.3)
    ax.tick_params(axis="y", labelsize=COLUMN_TICK_FONTSIZE)
    ax.minorticks_off()
    ax.tick_params(axis="x", which="major", labelsize=COLUMN_TICK_FONTSIZE, length=3)
    ax.tick_params(axis="x", which="minor", length=0)

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def create_llm_validation_latency_f1_chart(
    df: pd.DataFrame,
    *,
    output_path: Path,
) -> None:
    required_cols = {
        "dfc_f1",
        "dfc_avg_runtime_ms",
        "gpt_52_query_results_f1",
        "gpt_52_query_results_avg_runtime_ms",
        "opus_46_query_results_f1",
        "opus_46_query_results_avg_runtime_ms",
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns for LLM validation scatter chart: {sorted(missing)}")

    plot_df = df.copy()
    if plot_df.empty:
        raise ValueError("No LLM validation rows found.")

    series_specs = [
        ("DFC", "dfc_avg_runtime_ms", "dfc_f1", "#2ca02c", "o"),
        ("GPT 5.2", "gpt_52_query_results_avg_runtime_ms", "gpt_52_query_results_f1", "#4C78A8", "s"),
        ("Opus 4.6", "opus_46_query_results_avg_runtime_ms", "opus_46_query_results_f1", "#E45756", "^"),
    ]

    fig, ax = plt.subplots(figsize=(COLUMN_WIDTH_IN, 2.25))
    for label, runtime_col, f1_col, color, marker in series_specs:
        ax.scatter(
            plot_df[runtime_col],
            plot_df[f1_col] * 100.0,
            s=28,
            marker=marker,
            label=label,
            color=color,
            edgecolors="white",
            linewidths=0.35,
            zorder=3,
        )

    ax.set_xlabel("Latency (ms)", fontsize=COLUMN_AXIS_LABEL_FONTSIZE)
    ax.set_ylabel("F1 (%)", fontsize=COLUMN_AXIS_LABEL_FONTSIZE)
    ax.set_ylim(0.0, 105.0)
    ax.set_yticks([0.0, 25.0, 50.0, 75.0, 100.0])
    ax.set_yticklabels(["0", "25", "50", "75", "100"], fontsize=COLUMN_TICK_FONTSIZE)
    ax.set_xlim(left=0.0)
    ax.xaxis.set_major_formatter(matplotlib.ticker.StrMethodFormatter("{x:,.0f}"))
    ax.grid(True, alpha=0.3)
    ax.tick_params(axis="x", labelsize=COLUMN_TICK_FONTSIZE)
    ax.tick_params(axis="both", which="both", length=0)
    ax.text(
        0.03,
        0.89,
        "DFC",
        transform=ax.transAxes,
        color="#2ca02c",
        fontsize=COLUMN_AXIS_LABEL_FONTSIZE,
        fontweight="bold",
        ha="left",
        va="top",
    )
    ax.text(
        0.16,
        0.54,
        "GPT 5.2",
        transform=ax.transAxes,
        color="#4C78A8",
        fontsize=COLUMN_AXIS_LABEL_FONTSIZE,
        fontweight="bold",
        ha="left",
        va="center",
    )
    ax.text(
        0.62,
        0.66,
        "Opus 4.6",
        transform=ax.transAxes,
        color="#E45756",
        fontsize=COLUMN_AXIS_LABEL_FONTSIZE,
        fontweight="bold",
        ha="left",
        va="center",
    )

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def create_multi_source_overhead_line_chart(
    df: pd.DataFrame,
    *,
    output_path: Path,
) -> None:
    required_cols = {"source_count", "join_count", "no_policy_exec_time_ms", "dfc_1phase_exec_time_ms"}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"Missing required columns for multi-source line chart: {sorted(required_cols - set(df.columns))}")

    plot_df = df.copy()
    if "run_num" in plot_df.columns:
        plot_df = plot_df[plot_df["run_num"].fillna(0) > 0].copy()

    plot_df = plot_df.dropna(subset=["source_count", "join_count"])
    if plot_df.empty:
        raise ValueError("No data available for multi-source line chart.")

    plot_df["overhead_pct"] = (
        (plot_df["dfc_1phase_exec_time_ms"] - plot_df["no_policy_exec_time_ms"])
        / plot_df["no_policy_exec_time_ms"]
    ) * 100.0

    grouped = (
        plot_df.groupby(["join_count", "source_count"], as_index=False)
        .agg({"overhead_pct": "mean"})
        .sort_values(["source_count", "join_count"])
    )

    fig, ax = plt.subplots(figsize=(COLUMN_WIDTH_IN, 2.2))
    source_values = sorted(grouped["source_count"].astype(int).unique().tolist())
    cmap = MULTI_SOURCE_BLUE_CMAP
    norm = matplotlib.colors.Normalize(vmin=min(source_values), vmax=max(source_values))

    for idx, source_count in enumerate(source_values):
        source_df = grouped[grouped["source_count"] == source_count].sort_values("join_count")
        if source_df.empty:
            continue
        ax.plot(
            source_df["join_count"],
            source_df["overhead_pct"],
            marker=["o", "s", "^", "D", "P", "X", "v", "<", ">"][idx % 9],
            linewidth=COLUMN_LINE_WIDTH,
            markersize=COLUMN_MARKER_SIZE,
            color=cmap(norm(source_count)),
        )

    ax.set_xlabel("Number of Joins", fontsize=COLUMN_AXIS_LABEL_FONTSIZE)
    ax.set_ylabel("Overhead (%)", fontsize=COLUMN_AXIS_LABEL_FONTSIZE)
    ax.tick_params(axis="both", labelsize=COLUMN_TICK_FONTSIZE)
    ax.grid(True, alpha=0.3)
    sm = plt.cm.ScalarMappable(norm=norm, cmap=cmap)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, fraction=0.046, pad=0.03)
    cbar.set_label("Sources", fontsize=COLUMN_AXIS_LABEL_FONTSIZE)
    cbar.ax.tick_params(labelsize=COLUMN_TICK_FONTSIZE)

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def create_policy_count_self_join_combined_chart(
    policy_df: pd.DataFrame,
    self_join_df: pd.DataFrame,
    *,
    output_path: Path,
) -> None:
    axis_label_fontsize = COLUMN_AXIS_LABEL_FONTSIZE
    tick_fontsize = COLUMN_TICK_FONTSIZE
    legend_fontsize = COLUMN_LEGEND_FONTSIZE
    policy_df = _with_exec_time_columns(policy_df.copy())
    self_join_df = self_join_df.copy()

    policy_required = {"policy_count", "dfc_1phase_exec_time_ms"}
    missing_policy = policy_required - set(policy_df.columns)
    if missing_policy:
        raise ValueError(f"Missing required policy-count columns: {sorted(missing_policy)}")

    self_join_required = {
        "self_join_count",
        "no_policy_time_ms",
        "dfc_1phase_time_ms",
        "dfc_1phase_optimized_time_ms",
    }
    missing_self_join = self_join_required - set(self_join_df.columns)
    if missing_self_join:
        raise ValueError(f"Missing required self-join columns: {sorted(missing_self_join)}")

    policy_plot_cols = ["policy_count", "dfc_1phase_exec_time_ms"]
    if "dfc_1phase_optimized_exec_time_ms" in policy_df.columns:
        policy_plot_cols.append("dfc_1phase_optimized_exec_time_ms")
    if "dfc_2phase_exec_time_ms" in policy_df.columns:
        policy_plot_cols.append("dfc_2phase_exec_time_ms")
    if "logical_exec_time_ms" in policy_df.columns:
        policy_plot_cols.append("logical_exec_time_ms")
    if "physical_exec_time_ms" in policy_df.columns:
        policy_plot_cols.append("physical_exec_time_ms")
    policy_grouped = (
        policy_df[policy_plot_cols]
        .dropna(subset=["policy_count"])
        .groupby("policy_count", as_index=True)
        .mean(numeric_only=True)
        .sort_index()
    )

    self_join_grouped = (
        self_join_df[
            ["self_join_count", "no_policy_time_ms", "dfc_1phase_time_ms", "dfc_1phase_optimized_time_ms"]
        ]
        .dropna(subset=["self_join_count"])
        .groupby("self_join_count", as_index=True)
        .mean(numeric_only=True)
        .sort_index()
    )
    self_join_baseline = self_join_grouped["no_policy_time_ms"]
    self_join_grouped["dfc_1phase_overhead_pct"] = (
        (self_join_grouped["dfc_1phase_time_ms"] - self_join_baseline) / self_join_baseline
    ) * 100.0
    self_join_grouped["dfc_1phase_optimized_overhead_pct"] = (
        (self_join_grouped["dfc_1phase_optimized_time_ms"] - self_join_baseline) / self_join_baseline
    ) * 100.0

    fig, (ax_left, ax_right) = plt.subplots(2, 1, figsize=(COLUMN_WIDTH_IN, 2.95))

    ax_left.plot(
        policy_grouped.index,
        policy_grouped["dfc_1phase_exec_time_ms"] / 1000.0,
        marker=FINAL_MARKERS[FULL_PUSH_LABEL],
        linewidth=COLUMN_LINE_WIDTH,
        markersize=COLUMN_MARKER_SIZE,
        label=FULL_PUSH_LABEL,
        color="#ff7f0e",
    )
    if "dfc_1phase_optimized_exec_time_ms" in policy_grouped.columns:
        ax_left.plot(
            policy_grouped.index,
            policy_grouped["dfc_1phase_optimized_exec_time_ms"] / 1000.0,
            marker=FINAL_MARKERS[f"{FULL_PUSH_LABEL} Optimized"],
            linewidth=COLUMN_LINE_WIDTH,
            markersize=COLUMN_MARKER_SIZE,
            label=f"{FULL_PUSH_LABEL} Optimized",
            color="#8c564b",
        )
    if "dfc_2phase_exec_time_ms" in policy_grouped.columns:
        ax_left.plot(
            policy_grouped.index,
            policy_grouped["dfc_2phase_exec_time_ms"] / 1000.0,
            marker=FINAL_MARKERS[PARTIAL_PUSH_LABEL],
            linewidth=COLUMN_LINE_WIDTH,
            markersize=COLUMN_MARKER_SIZE,
            label=PARTIAL_PUSH_LABEL,
            color="#9467bd",
        )
    if "logical_exec_time_ms" in policy_grouped.columns:
        ax_left.plot(
            policy_grouped.index,
            policy_grouped["logical_exec_time_ms"] / 1000.0,
            marker=FINAL_MARKERS["Logical"],
            linewidth=COLUMN_LINE_WIDTH,
            markersize=COLUMN_MARKER_SIZE,
            label="Logical",
            color="#2ca02c",
        )
    if "physical_exec_time_ms" in policy_grouped.columns:
        ax_left.plot(
            policy_grouped.index,
            policy_grouped["physical_exec_time_ms"] / 1000.0,
            marker=FINAL_MARKERS["Physical"],
            linewidth=COLUMN_LINE_WIDTH,
            markersize=COLUMN_MARKER_SIZE,
            label="Physical",
            color="#1f77b4",
        )
    ax_left.set_xscale("log")
    y_formatter = matplotlib.ticker.ScalarFormatter(useOffset=False)
    y_formatter.set_scientific(False)
    ax_left.yaxis.set_major_formatter(y_formatter)
    ax_left.set_xlabel("# Policies", fontsize=axis_label_fontsize)
    ax_left.set_ylabel("Time (s)", fontsize=axis_label_fontsize)
    ax_left.grid(True, alpha=0.3)
    ax_left.legend(
        loc="lower center",
        bbox_to_anchor=(0.5, 1.01),
        ncol=2,
        fontsize=legend_fontsize,
        frameon=False,
        handlelength=1.5,
    )
    ax_left.minorticks_off()
    ax_left.tick_params(axis="both", which="major", labelsize=tick_fontsize, length=3)
    ax_left.tick_params(axis="both", which="minor", length=0)

    ax_right.plot(
        self_join_grouped.index,
        self_join_grouped["dfc_1phase_overhead_pct"],
        marker=FINAL_MARKERS[FULL_PUSH_LABEL],
        linewidth=COLUMN_LINE_WIDTH,
        markersize=COLUMN_MARKER_SIZE,
        label=FULL_PUSH_LABEL,
        color="#ff7f0e",
    )
    ax_right.plot(
        self_join_grouped.index,
        self_join_grouped["dfc_1phase_optimized_overhead_pct"],
        marker=FINAL_MARKERS[f"{FULL_PUSH_LABEL} Optimized"],
        linewidth=COLUMN_LINE_WIDTH,
        markersize=COLUMN_MARKER_SIZE,
        label=f"{FULL_PUSH_LABEL} Optimized",
        color="#8c564b",
    )
    ax_right.set_xscale("log")
    right_y_formatter = matplotlib.ticker.ScalarFormatter(useOffset=False)
    right_y_formatter.set_scientific(False)
    ax_right.yaxis.set_major_formatter(right_y_formatter)
    ax_right.set_xlabel("# Self-Joins", fontsize=axis_label_fontsize)
    ax_right.set_ylabel("Overhead (%)", fontsize=axis_label_fontsize)
    ax_right.grid(True, alpha=0.3)
    ax_right.minorticks_off()
    ax_right.tick_params(axis="both", which="major", labelsize=tick_fontsize, length=3)
    ax_right.tick_params(axis="both", which="minor", length=0)

    plt.tight_layout(h_pad=0.55)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def create_microbenchmark_combined_chart(
    df: pd.DataFrame,
    *,
    output_path: Path,
) -> None:
    axis_label_fontsize = COLUMN_AXIS_LABEL_FONTSIZE
    tick_fontsize = COLUMN_TICK_FONTSIZE
    legend_fontsize = COLUMN_LEGEND_FONTSIZE
    line_width = COLUMN_LINE_WIDTH
    marker_size = COLUMN_MARKER_SIZE

    plot_df = _with_exec_time_columns(df.copy())
    plot_df = plot_df.drop(
        columns=[
            "physical_exec_time_ms",
            "physical_time_ms",
            "physical_base_capture_time_ms",
            "physical_lineage_query_time_ms",
            "physical_rewrite_time_ms",
            "physical_runtime_ms",
        ],
        errors="ignore",
    )

    colors = {
        "No Policy": "#1f77b4",
        FULL_PUSH_LABEL: "#ff7f0e",
        PARTIAL_PUSH_LABEL: "#9467bd",
        "Logical": "#2ca02c",
    }
    approach_columns = {
        "No Policy": "no_policy_exec_time_ms",
        FULL_PUSH_LABEL: "dfc_1phase_exec_time_ms",
        PARTIAL_PUSH_LABEL: "dfc_2phase_exec_time_ms",
        "Logical": "logical_exec_time_ms",
    }
    query_specs = [
        ("SIMPLE_AGG", "# Input Rows", "variation_num_rows"),
        ("GROUP_BY", "# Groups", "variation_num_groups"),
        ("JOIN", "# Join Matches", "variation_join_matches"),
    ]

    fig, stacked_axes = plt.subplots(3, 1, figsize=(COLUMN_WIDTH_IN, 4.55))
    axes = dict(zip([spec[0] for spec in query_specs], stacked_axes))

    for query_type, x_label, x_col in query_specs:
        ax = axes[query_type]
        query_df = plot_df[plot_df["query_type"] == query_type].copy()
        if query_df.empty or x_col not in query_df.columns:
            continue

        grouped = (
            query_df.groupby(x_col, as_index=True)
            .mean(numeric_only=True)
            .sort_index()
        )

        for approach, col in approach_columns.items():
            if col not in grouped.columns:
                continue
            series = grouped[col].dropna()
            if series.empty:
                continue
            ax.plot(
                series.index,
                series.values,
                marker=FINAL_MARKERS[approach],
                linewidth=line_width,
                markersize=marker_size,
                label=approach,
                color=colors[approach],
            )

        ax.set_xscale("log")
        ax.set_xlabel(x_label, fontsize=axis_label_fontsize)
        ax.set_ylabel("Time (ms)", fontsize=axis_label_fontsize)
        ax.grid(True, alpha=0.3)
        ax.minorticks_off()
        ax.tick_params(axis="both", which="major", labelsize=tick_fontsize, length=3)
        ax.tick_params(axis="both", which="minor", length=0)

    handles, labels = stacked_axes[0].get_legend_handles_labels()
    stacked_axes[0].legend(
        handles,
        labels,
        loc="upper left",
        ncol=2,
        fontsize=legend_fontsize,
        frameon=True,
        framealpha=0.85,
        handlelength=1.5,
        columnspacing=0.9,
    )

    plt.tight_layout(h_pad=0.55)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def create_multi_db_engine_summary_capped_chart(
    df: pd.DataFrame,
    output_dir: str = "./results",
    output_filename: str = "tpch_multi_db_engine_summary_capped.png",
    duckdb_cap_pct: float = 300.0,
) -> None:
    df = _with_exec_time_columns(df)
    if "query_num" not in df.columns:
        raise ValueError("Missing query_num column for multi-db engine summary chart.")

    duckdb_cols = (
        "dfc_1phase_exec_time_ms" if "dfc_1phase_exec_time_ms" in df.columns else "dfc_1phase_time_ms",
        "dfc_2phase_exec_time_ms" if "dfc_2phase_exec_time_ms" in df.columns else "dfc_2phase_time_ms",
        "logical_exec_time_ms" if "logical_exec_time_ms" in df.columns else "logical_time_ms",
        "no_policy_exec_time_ms" if "no_policy_exec_time_ms" in df.columns else "no_policy_time_ms",
    )
    engines = {
        "DuckDB": duckdb_cols,
        "Umbra": ("umbra_dfc_1phase_time_ms", "umbra_dfc_2phase_time_ms", "umbra_logical_time_ms", "umbra_time_ms"),
        "Postgres": ("postgres_dfc_1phase_time_ms", "postgres_dfc_2phase_time_ms", "postgres_logical_time_ms", "postgres_time_ms"),
        "DataFusion": (
            "datafusion_dfc_1phase_time_ms",
            "datafusion_dfc_2phase_time_ms",
            "datafusion_logical_time_ms",
            "datafusion_time_ms",
        ),
        "SQL Server": (
            "sqlserver_dfc_1phase_time_ms",
            "sqlserver_dfc_2phase_time_ms",
            "sqlserver_logical_time_ms",
            "sqlserver_time_ms",
        ),
    }

    records: list[dict[str, float | str]] = []
    engine_order: list[str] = []
    for engine, (dfc_1phase_col, dfc_2phase_col, logical_col, baseline_col) in engines.items():
        if not all(col in df.columns for col in (dfc_1phase_col, dfc_2phase_col, logical_col, baseline_col)):
            continue
        baseline_by_query = df.groupby("query_num", as_index=True)[baseline_col].mean(numeric_only=True)
        baseline_by_query = baseline_by_query[baseline_by_query > 0]
        if baseline_by_query.empty:
            continue
        engine_added = False
        for label, col in [(FULL_PUSH_LABEL, dfc_1phase_col), (PARTIAL_PUSH_LABEL, dfc_2phase_col), ("Logical", logical_col)]:
            approach_by_query = df.groupby("query_num", as_index=True)[col].mean(numeric_only=True)
            approach_by_query = approach_by_query.reindex(baseline_by_query.index)
            valid_mask = approach_by_query > 0
            overhead_by_query = (
                approach_by_query[valid_mask] / baseline_by_query[valid_mask]
            ).replace([float("inf"), float("-inf")], pd.NA).dropna()
            if overhead_by_query.empty:
                continue
            avg_overhead = float((overhead_by_query.mean() - 1.0) * 100.0)
            records.append(
                {
                    "engine": engine,
                    "approach": label,
                    "avg_overhead": avg_overhead,
                    "plot_overhead": min(avg_overhead, duckdb_cap_pct) if engine == "DuckDB" else avg_overhead,
                }
            )
            engine_added = True
        if engine_added:
            engine_order.append(engine)

    if not records:
        raise ValueError("No data available for multi-db engine summary chart.")

    summary_df = pd.DataFrame.from_records(records)
    summary_df["engine"] = pd.Categorical(summary_df["engine"], categories=engine_order, ordered=True)
    summary_df = summary_df.sort_values(["engine", "approach"])

    fig, ax = plt.subplots(figsize=(COLUMN_WIDTH_IN, 2.95))
    y_positions = range(len(engine_order))
    bar_height = 0.22
    offsets = {FULL_PUSH_LABEL: -bar_height, PARTIAL_PUSH_LABEL: 0.0, "Logical": bar_height}
    colors = {FULL_PUSH_LABEL: "#ff7f0e", PARTIAL_PUSH_LABEL: "#9467bd", "Logical": "#2ca02c"}

    for approach in [FULL_PUSH_LABEL, PARTIAL_PUSH_LABEL, "Logical"]:
        subset = summary_df[summary_df["approach"] == approach]
        if subset.empty:
            continue
        ys = [engine_order.index(str(e)) + offsets[approach] for e in subset["engine"]]
        bars = ax.barh(
            ys,
            subset["plot_overhead"],
            height=bar_height,
            label=approach,
            color=colors[approach],
        )
        for bar, original in zip(bars, subset["avg_overhead"]):
            if original <= duckdb_cap_pct:
                continue
            y_offset = 1 if approach == "Logical" else 0
            ax.annotate(
                f"{original:.0f}%",
                xy=(duckdb_cap_pct, bar.get_y()),
                xytext=(0, y_offset),
                textcoords="offset points",
                ha="right",
                va="bottom",
                fontsize=COLUMN_ANNOTATION_FONTSIZE,
                clip_on=False,
            )

    ax.set_yticks(list(y_positions))
    ax.set_yticklabels(engine_order, fontsize=COLUMN_TICK_FONTSIZE)
    ax.invert_yaxis()
    ax.set_xlabel("Overhead vs No Policy (%)", fontsize=COLUMN_AXIS_LABEL_FONTSIZE)
    ax.grid(axis="x", alpha=0.3)
    ax.legend(
        loc="lower center",
        bbox_to_anchor=(0.5, 1.01),
        ncol=3,
        fontsize=COLUMN_LEGEND_FONTSIZE,
        frameon=False,
        handlelength=1.4,
        columnspacing=0.8,
    )
    ax.set_xlim(left=-75.0, right=duckdb_cap_pct)
    ax.tick_params(axis="x", labelsize=COLUMN_TICK_FONTSIZE)

    plt.tight_layout(rect=(0, 0, 1, 0.91))
    output_path = Path(output_dir) / output_filename
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)


def generate_all_final_visualizations(final_results_dir: str | Path) -> list[Path]:
    final_dir = Path(final_results_dir)
    final_dir.mkdir(parents=True, exist_ok=True)
    output_paths: list[Path] = []

    tpch_sf10_df = load_results(str(final_dir / "tpch_results_sf10.csv"))
    create_tpch_duckdb_capped_overhead_chart(
        tpch_sf10_df,
        cap_pct=1000.0,
        output_path=final_dir / "tpch_duckdb_percent_overhead_capped1000_sf10.png",
    )
    output_paths.append(final_dir / "tpch_duckdb_percent_overhead_capped1000_sf10.png")
    create_tpch_duckdb_capped_overhead_chart(
        tpch_sf10_df,
        cap_pct=500.0,
        output_path=final_dir / "tpch_duckdb_percent_overhead_capped500_sf10.png",
    )
    output_paths.append(final_dir / "tpch_duckdb_percent_overhead_capped500_sf10.png")
    create_tpch_duckdb_capped_overhead_chart(
        tpch_sf10_df,
        cap_pct=300.0,
        output_path=final_dir / "tpch_duckdb_percent_overhead_capped300_sf10.png",
        x_min=-25.0,
    )
    output_paths.append(final_dir / "tpch_duckdb_percent_overhead_capped300_sf10.png")
    create_tpch_duckdb_provenance_capped_overhead_chart(
        tpch_sf10_df,
        output_path=final_dir / "tpch_duckdb_provenance_percent_overhead_log_sf10.png",
    )
    output_paths.append(final_dir / "tpch_duckdb_provenance_percent_overhead_log_sf10.png")
    create_tpch_duckdb_provenance_capped_overhead_chart(
        tpch_sf10_df,
        output_path=final_dir / "tpch_duckdb_provenance_percent_overhead_log_sf10_no_dfc_label.png",
        show_dfc_label=False,
        zero_dfc_bar=True,
    )
    output_paths.append(final_dir / "tpch_duckdb_provenance_percent_overhead_log_sf10_no_dfc_label.png")

    tpch_multi_db_df = load_results(str(final_dir / "tpch_multi_db_sf1_default_merged.csv"))
    create_multi_db_engine_summary_capped_chart(
        tpch_multi_db_df,
        output_dir=str(final_dir),
        output_filename="tpch_multi_db_engine_summary_capped_final.png",
        duckdb_cap_pct=200.0,
    )
    output_paths.append(final_dir / "tpch_multi_db_engine_summary_capped_final.png")

    micro_df = load_results(str(final_dir / "microbenchmark_results_policy1.csv"))
    create_microbenchmark_combined_chart(
        micro_df,
        output_path=final_dir / "microbenchmark_combined_policy1.png",
    )
    output_paths.append(final_dir / "microbenchmark_combined_policy1.png")

    phase_df = load_results(str(final_dir / "microbenchmark_phase_competition.csv"))
    create_phase_competition_heatmap(
        phase_df,
        output_path=final_dir / "microbenchmark_phase_competition_heatmap.png",
    )
    output_paths.append(final_dir / "microbenchmark_phase_competition_heatmap.png")

    multi_source_df = load_results(str(final_dir / "multi_source_tpch_results.csv"))
    create_multi_source_heatmap_chart(
        multi_source_df,
        output_dir=str(final_dir),
        output_filename="multi_source_tpch_heatmap.png",
    )
    output_paths.append(final_dir / "multi_source_tpch_heatmap.png")
    create_multi_source_overhead_line_chart(
        multi_source_df,
        output_path=final_dir / "multi_source_tpch_overhead_lines.png",
    )
    output_paths.append(final_dir / "multi_source_tpch_overhead_lines.png")

    optimized_df = load_results(str(final_dir / "tpch_q01_policy_count_sf1_optimized.csv"))
    self_join_df = load_results(str(final_dir / "tpch_q01_self_join_policy_sf0.001.csv"))
    create_policy_count_self_join_combined_chart(
        optimized_df,
        self_join_df,
        output_path=final_dir / "tpch_q01_policy_count_self_join_combined.png",
    )
    output_paths.append(final_dir / "tpch_q01_policy_count_self_join_combined.png")

    state_df = load_results(str(final_dir / "state_transition_llm_results.csv"))
    create_state_transition_chart(
        state_df,
        output_path=final_dir / "state_transition_timing_100_updates.png",
    )
    output_paths.append(final_dir / "state_transition_timing_100_updates.png")

    llm_validation_df = pd.read_csv(final_dir / "llm_validation_table.csv")
    create_llm_validation_latency_f1_chart(
        llm_validation_df,
        output_path=final_dir / "llm_validation_metrics_stacked.png",
    )
    output_paths.append(final_dir / "llm_validation_metrics_stacked.png")

    return output_paths
