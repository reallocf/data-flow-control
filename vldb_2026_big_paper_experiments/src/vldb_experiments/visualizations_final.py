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
    FINAL_LEGEND_FONTSIZE,
    FINAL_TICK_FONTSIZE,
    FULL_PUSH_LABEL,
    PARTIAL_PUSH_LABEL,
    _with_exec_time_columns,
    create_multi_db_engine_summary_capped_chart,
    create_multi_source_heatmap_chart,
    load_results,
)


def create_tpch_duckdb_capped_overhead_chart(
    df: pd.DataFrame,
    *,
    cap_pct: float,
    output_path: Path,
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

    x_positions = list(range(len(grouped.index)))
    bar_width = 0.2

    fig, ax = plt.subplots(figsize=(12, 6))
    dfc_1phase_bars = ax.bar(
        [x - bar_width for x in x_positions], dfc_1phase_plot, width=bar_width, label=FULL_PUSH_LABEL, color="#ff7f0e"
    )
    dfc_2phase_bars = ax.bar(
        x_positions, dfc_2phase_plot, width=bar_width, label=PARTIAL_PUSH_LABEL, color="#9467bd"
    )
    logical_bars = ax.bar(
        [x + bar_width for x in x_positions], logical_plot, width=bar_width, label="Logical", color="#2ca02c"
    )
    physical_bars = ax.bar(
        [x + (2 * bar_width) for x in x_positions], physical_plot, width=bar_width, label="Physical", color="#d62728"
    )

    ax.set_xticks(x_positions)
    ax.set_xticklabels([f"Q{int(q):02d}" for q in grouped.index], fontsize=FINAL_TICK_FONTSIZE)
    ax.set_xlabel("TPC-H Query", fontsize=FINAL_AXIS_LABEL_FONTSIZE)
    ax.set_ylabel("Overhead vs No Policy (%)", fontsize=FINAL_AXIS_LABEL_FONTSIZE)
    ax.set_ylim(top=cap_pct)
    ax.grid(axis="y", alpha=0.3)
    ax.legend(loc="upper right", bbox_to_anchor=(0.99, 0.95), fontsize=FINAL_LEGEND_FONTSIZE)
    ax.tick_params(axis="y", labelsize=FINAL_TICK_FONTSIZE)

    def _annotate_capped(bars, original_values):
        for bar, original in zip(bars, original_values):
            if pd.isna(original) or original <= cap_pct:
                continue
            x = bar.get_x() + bar.get_width() / 2.0
            y = cap_pct
            ax.text(
                x,
                y,
                f"{original:.0f}%",
                ha="center",
                va="bottom",
                rotation=0,
                fontsize=FINAL_ANNOTATION_FONTSIZE,
                clip_on=False,
            )

    _annotate_capped(dfc_1phase_bars, dfc_1phase_overhead)
    _annotate_capped(dfc_2phase_bars, dfc_2phase_overhead)
    _annotate_capped(logical_bars, logical_overhead)
    _annotate_capped(physical_bars, physical_overhead)

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

    fig, ax = plt.subplots(figsize=(10, 7))
    cmap = matplotlib.colors.LinearSegmentedColormap.from_list(
        "light_blue_red",
        ["#8bb6e3", "#e38b8b"],
    )
    cmap.set_bad(color="#f0f0f0")
    im = ax.imshow(
        heatmap.values,
        aspect="auto",
        origin="lower",
        cmap=cmap,
        vmin=0.5,
        vmax=2.0,
    )
    ax.set_xticks(range(len(fanouts)))
    ax.set_xticklabels(fanouts, fontsize=FINAL_TICK_FONTSIZE)
    ax.set_yticks(range(len(policy_counts)))
    ax.set_yticklabels(policy_counts, fontsize=FINAL_TICK_FONTSIZE)
    ax.set_xlabel("Join Fanout", fontsize=FINAL_AXIS_LABEL_FONTSIZE)
    ax.set_ylabel("Policy Columns Summed", fontsize=FINAL_AXIS_LABEL_FONTSIZE)
    for y_idx in range(len(policy_counts)):
        for x_idx in range(len(fanouts)):
            val = heatmap.iat[y_idx, x_idx]
            if np.isfinite(val):
                ax.text(
                    x_idx,
                    y_idx,
                    f"{val:.2f}",
                    ha="center",
                    va="center",
                    fontsize=FINAL_ANNOTATION_FONTSIZE,
                )

    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label("Execution Time Ratio", fontsize=FINAL_AXIS_LABEL_FONTSIZE)
    cbar.ax.tick_params(labelsize=FINAL_TICK_FONTSIZE)
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

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(labels, means, color=colors, width=0.6)
    ax.set_ylabel("Update Time (ms)", fontsize=FINAL_AXIS_LABEL_FONTSIZE)
    ax.set_yscale("log")
    ax.grid(axis="y", alpha=0.3)
    ax.tick_params(axis="both", labelsize=FINAL_TICK_FONTSIZE)
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

    fig, ax = plt.subplots(figsize=(10, 6))
    source_values = sorted(grouped["source_count"].astype(int).unique().tolist())
    cmap = plt.get_cmap("tab10")

    for idx, source_count in enumerate(source_values):
        source_df = grouped[grouped["source_count"] == source_count].sort_values("join_count")
        if source_df.empty:
            continue
        ax.plot(
            source_df["join_count"],
            source_df["overhead_pct"],
            marker="o",
            linewidth=2,
            markersize=6,
            label=f"{source_count}",
            color=cmap(idx % 10),
        )

    ax.set_xlabel("Number of Joins", fontsize=FINAL_AXIS_LABEL_FONTSIZE)
    ax.set_ylabel("Overhead vs No Policy (%)", fontsize=FINAL_AXIS_LABEL_FONTSIZE)
    ax.tick_params(axis="both", labelsize=FINAL_TICK_FONTSIZE)
    ax.grid(True, alpha=0.3)
    ax.legend(title="# Sources", fontsize=FINAL_LEGEND_FONTSIZE, title_fontsize=FINAL_LEGEND_FONTSIZE, loc="best")

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
    axis_label_fontsize = FINAL_AXIS_LABEL_FONTSIZE * 2
    tick_fontsize = FINAL_TICK_FONTSIZE * 2
    legend_fontsize = FINAL_LEGEND_FONTSIZE * 2
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

    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(18, 6))

    ax_left.plot(
        policy_grouped.index,
        policy_grouped["dfc_1phase_exec_time_ms"] / 1000.0,
        marker="o",
        linewidth=4,
        markersize=6,
        label=FULL_PUSH_LABEL,
        color="#ff7f0e",
    )
    if "dfc_1phase_optimized_exec_time_ms" in policy_grouped.columns:
        ax_left.plot(
            policy_grouped.index,
            policy_grouped["dfc_1phase_optimized_exec_time_ms"] / 1000.0,
            marker="o",
            linewidth=4,
            markersize=6,
            label=f"{FULL_PUSH_LABEL} Optimized",
            color="#8c564b",
        )
    if "dfc_2phase_exec_time_ms" in policy_grouped.columns:
        ax_left.plot(
            policy_grouped.index,
            policy_grouped["dfc_2phase_exec_time_ms"] / 1000.0,
            marker="o",
            linewidth=4,
            markersize=6,
            label=PARTIAL_PUSH_LABEL,
            color="#9467bd",
        )
    if "logical_exec_time_ms" in policy_grouped.columns:
        ax_left.plot(
            policy_grouped.index,
            policy_grouped["logical_exec_time_ms"] / 1000.0,
            marker="o",
            linewidth=4,
            markersize=6,
            label="Logical",
            color="#2ca02c",
        )
    if "physical_exec_time_ms" in policy_grouped.columns:
        ax_left.plot(
            policy_grouped.index,
            policy_grouped["physical_exec_time_ms"] / 1000.0,
            marker="o",
            linewidth=4,
            markersize=6,
            label="Physical",
            color="#1f77b4",
        )
    ax_left.set_xscale("log")
    y_formatter = matplotlib.ticker.ScalarFormatter(useOffset=False)
    y_formatter.set_scientific(False)
    ax_left.yaxis.set_major_formatter(y_formatter)
    ax_left.set_xlabel("# Policies", fontsize=axis_label_fontsize)
    ax_left.set_ylabel("Execution Time (s)", fontsize=axis_label_fontsize)
    ax_left.grid(True, alpha=0.3)
    ax_left.legend(loc="best", fontsize=legend_fontsize)
    ax_left.tick_params(axis="both", labelsize=tick_fontsize)

    ax_right.plot(
        self_join_grouped.index,
        self_join_grouped["dfc_1phase_overhead_pct"],
        marker="o",
        linewidth=4,
        markersize=6,
        label=FULL_PUSH_LABEL,
        color="#ff7f0e",
    )
    ax_right.plot(
        self_join_grouped.index,
        self_join_grouped["dfc_1phase_optimized_overhead_pct"],
        marker="o",
        linewidth=4,
        markersize=6,
        label=f"{FULL_PUSH_LABEL} Optimized",
        color="#8c564b",
    )
    ax_right.set_xscale("log")
    ax_right.set_ylim(bottom=0)
    right_y_formatter = matplotlib.ticker.ScalarFormatter(useOffset=False)
    right_y_formatter.set_scientific(False)
    ax_right.yaxis.set_major_formatter(right_y_formatter)
    ax_right.set_xlabel("# Self-Joins", fontsize=axis_label_fontsize)
    ax_right.set_ylabel("Overhead (%)", fontsize=axis_label_fontsize)
    ax_right.grid(True, alpha=0.3)
    ax_right.tick_params(axis="both", labelsize=tick_fontsize)

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def create_microbenchmark_combined_chart(
    df: pd.DataFrame,
    *,
    output_path: Path,
) -> None:
    axis_label_fontsize = FINAL_AXIS_LABEL_FONTSIZE * 2
    tick_fontsize = FINAL_TICK_FONTSIZE * 2
    legend_fontsize = FINAL_LEGEND_FONTSIZE * 2
    line_width = 4
    marker_size = 8

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

    fig = plt.figure(figsize=(20, 16))
    grid = fig.add_gridspec(2, 4)
    axes = {
        "SIMPLE_AGG": fig.add_subplot(grid[0, 0:2]),
        "GROUP_BY": fig.add_subplot(grid[0, 2:4]),
        "JOIN": fig.add_subplot(grid[1, 1:3]),
    }

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
                marker="o",
                linewidth=line_width,
                markersize=marker_size,
                label=approach,
                color=colors[approach],
            )

        ax.set_xscale("log")
        ax.set_ylim(bottom=0)
        ax.set_xlabel(x_label, fontsize=axis_label_fontsize)
        ax.set_ylabel("Execution Time (ms)", fontsize=axis_label_fontsize)
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis="both", labelsize=tick_fontsize)
        if query_type == "SIMPLE_AGG":
            ax.legend(loc="best", fontsize=legend_fontsize)

    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches="tight")
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

    return output_paths
