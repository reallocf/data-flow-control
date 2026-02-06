"""Generate visualizations from experiment results as static PNG images using matplotlib."""

from pathlib import Path
from typing import Optional

import matplotlib

matplotlib.use("Agg")  # Use non-interactive backend
import matplotlib.pyplot as plt
import pandas as pd


def load_results(csv_path: str) -> pd.DataFrame:
    """Load experiment results from CSV file.

    Args:
        csv_path: Path to CSV file

    Returns:
        DataFrame with experiment results
    """
    df = pd.read_csv(csv_path)
    # Filter out non-numeric execution numbers (like summary rows)
    df = df[df["execution_number"].astype(str).str.isdigit()]
    # Convert execution_number to int
    df["execution_number"] = df["execution_number"].astype(int)
    return df


def get_variation_x_axis(query_type: str) -> tuple[str, str]:
    """Get the X-axis column and label for a query type.

    Args:
        query_type: Query type (SELECT, WHERE, JOIN, GROUP_BY, ORDER_BY)

    Returns:
        Tuple of (column_name, axis_label)
    """
    if query_type in ["SELECT", "WHERE", "ORDER_BY"]:
        return "variation_rows_to_remove", "Rows Removed by Policy"
    if query_type == "JOIN":
        return "variation_join_matches", "Join Matches"
    if query_type == "GROUP_BY":
        return "variation_num_groups", "Number of Groups"
    return "variation_num", "Variation Number"


def create_operator_chart(
    query_type: str,
    df: pd.DataFrame,
    output_dir: str = "./results",
    output_template: str = "{query_type}_performance.png",
    policy_count: int | None = None,
) -> Optional[plt.Figure]:
    """Create a performance chart for a specific operator.

    Args:
        query_type: Query type (SELECT, WHERE, JOIN, GROUP_BY, ORDER_BY)
        df: DataFrame with all results
        output_dir: Directory to save chart

    Returns:
        Matplotlib figure object, or None if no data
    """
    # Filter to this query type
    query_df = df[df["query_type"] == query_type].copy()

    if len(query_df) == 0:
        print(f"No data found for {query_type}")
        return None

    # Get X-axis column
    x_col, x_label = get_variation_x_axis(query_type)

    # Check if X-axis column exists and has data
    if x_col not in query_df.columns or query_df[x_col].isna().all():
        # Try fallback columns
        if "variation_num" in query_df.columns and not query_df["variation_num"].isna().all():
            print(f"Warning: {x_col} column missing or empty for {query_type}, using variation_num")
            x_col = "variation_num"
            x_label = "Variation Number"
        else:
            print(f"Warning: {x_col} column missing or empty for {query_type}, using execution_number")
            x_col = "execution_number"
            x_label = "Execution Number"

    # Prepare data for plotting using exec-only times when available
    exec_columns = [
        "no_policy_exec_time_ms",
        "dfc_exec_time_ms",
        "logical_exec_time_ms",
        "physical_time_ms",
    ]
    total_columns = ["no_policy_time_ms", "dfc_time_ms", "logical_time_ms", "physical_time_ms"]
    available_exec_cols = [col for col in exec_columns if col in query_df.columns]
    available_time_cols = available_exec_cols or [col for col in total_columns if col in query_df.columns]

    if not available_time_cols:
        print(f"No time columns found for {query_type}")
        return None

    # Create long format for plotting
    plot_data = []
    for idx, row in query_df.iterrows():
        # Get X-axis value, with fallbacks
        if x_col in query_df.columns and pd.notna(row[x_col]):
            x_val = row[x_col]
        elif "variation_num" in query_df.columns and pd.notna(row.get("variation_num")):
            x_val = row["variation_num"]
        else:
            # Use execution number as fallback
            x_val = row.get("execution_number", idx)

        # Convert to numeric if possible
        try:
            x_val = float(x_val)
        except (ValueError, TypeError):
            x_val = float(idx)

        for col in available_time_cols:
            # Map column names directly to approach names
            approach_map = {
                "no_policy_time_ms": "No Policy",
                "no_policy_exec_time_ms": "No Policy",
                "dfc_time_ms": "DFC",
                "dfc_exec_time_ms": "DFC",
                "logical_time_ms": "Logical",
                "logical_exec_time_ms": "Logical",
                "physical_time_ms": "Physical",
            }
            approach = approach_map.get(col, col.replace("_time_ms", "").replace("_", " ").title())

            time_val = row[col]
            if pd.notna(time_val) and time_val > 0:
                plot_data.append({
                    x_label: x_val,
                    "Execution Time (ms)": float(time_val),
                    "Approach": approach,
                })

    if not plot_data:
        print(f"No valid time data for {query_type}")
        return None

    plot_df = pd.DataFrame(plot_data)

    # Average the runs for each x value and approach
    # Group by x value and approach, then average the execution times
    plot_df_averaged = plot_df.groupby([x_label, "Approach"], as_index=False).agg({
        "Execution Time (ms)": "mean"
    })

    # Sort by Approach and then X-axis value so lines connect properly
    plot_df_averaged = plot_df_averaged.sort_values(["Approach", x_label])

    # Determine if we should use log scale for Y-axis
    # Only use log scale if all values are positive and range is large
    max_time = plot_df_averaged["Execution Time (ms)"].max()
    min_time = plot_df_averaged["Execution Time (ms)"].min()
    use_log_scale = (
        min_time > 0 and
        max_time > 1000 and
        max_time / min_time > 10
    )

    # Create matplotlib figure
    fig, ax = plt.subplots(figsize=(10, 7))

    # Define colors for each approach
    colors = {
        "No Policy": "#1f77b4",
        "No Policy exec": "#1f77b4",
        "DFC": "#ff7f0e",
        "DFC rewrite": "#ff7f0e",
        "DFC exec": "#ffbb78",
        "Logical": "#2ca02c",
        "Logical rewrite": "#2ca02c",
        "Logical exec": "#98df8a",
        "Physical": "#d62728",
    }

    # Plot each approach (using averaged data)
    approaches = ["No Policy", "DFC", "Logical", "Physical"]

    for approach in approaches:
        approach_data = plot_df_averaged[plot_df_averaged["Approach"] == approach]
        if len(approach_data) > 0:
            # Sort by x-axis value for clean lines
            approach_data = approach_data.sort_values(x_label)
            ax.plot(
                approach_data[x_label],
                approach_data["Execution Time (ms)"],
                marker="o",
                linewidth=2,
                markersize=6,
                label=approach,
                color=colors.get(approach)
            )

    # Set labels and title
    ax.set_xlabel(x_label, fontsize=12)
    ax.set_ylabel("Execution Time (ms)", fontsize=12)
    ax.set_title(f"{query_type} Query Performance", fontsize=14, fontweight="bold")

    # Set log scale for x-axis (variation parameters often span large ranges)
    ax.set_xscale("log")

    # Set log scale for y-axis if needed
    if use_log_scale:
        ax.set_yscale("log")

    # Add legend
    ax.legend(loc="best", fontsize=10)

    # Add grid
    ax.grid(True, alpha=0.3)

    # Tight layout
    plt.tight_layout()

    # Save chart as PNG
    output_path = Path(output_dir) / output_template.format(
        query_type=query_type.lower(),
        policy_count=policy_count if policy_count is not None else "unknown",
    )
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)

    print(f"Saved chart to {output_path}")
    return fig


def _apply_suffix(template: str, suffix: str) -> str:
    if not suffix:
        return template
    if "{suffix}" in template:
        return template
    if template.endswith(".png"):
        return f"{template[:-4]}_{suffix}.png"
    return f"{template}_{suffix}"


def create_all_charts(
    csv_path: str = "./results/microbenchmark_results.csv",
    output_dir: str = "./results",
    tpch_avg_template: str = "tpch_average_times_sf{sf}.png",
    tpch_overhead_template: str = "tpch_percent_overhead_sf{sf}.png",
    operator_template: str = "{query_type}_performance_policy{policy_count}.png",
    tpch_breakdown_template: str = "tpch_rewrite_exec_breakdown_sf{sf}.png",
    tpch_multi_db_template: str = "tpch_multi_db_sf{sf}.png",
    tpch_avg_log_template: str = "tpch_average_times_log_sf{sf}.png",
    tpch_breakdown_log_template: str = "tpch_rewrite_exec_breakdown_log_sf{sf}.png",
    suffix: str = "",
) -> None:
    """Create visualizations for all operators.

    Args:
        csv_path: Path to CSV file with results
        output_dir: Directory to save charts
    """
    # Load data
    print(f"Loading results from {csv_path}...")
    df = load_results(csv_path)
    print(f"Loaded {len(df)} execution results")

    tpch_avg_template = _apply_suffix(tpch_avg_template, suffix)
    tpch_overhead_template = _apply_suffix(tpch_overhead_template, suffix)
    operator_template = _apply_suffix(operator_template, suffix)
    tpch_breakdown_template = _apply_suffix(tpch_breakdown_template, suffix)
    tpch_multi_db_template = _apply_suffix(tpch_multi_db_template, suffix)
    tpch_avg_log_template = _apply_suffix(tpch_avg_log_template, suffix)
    tpch_breakdown_log_template = _apply_suffix(tpch_breakdown_log_template, suffix)

    external_time_cols = [
        col
        for col in df.columns
        if col.endswith("_time_ms")
        and col
        not in {
            "no_policy_time_ms",
            "dfc_time_ms",
            "logical_time_ms",
            "physical_time_ms",
            "no_policy_exec_time_ms",
            "dfc_rewrite_time_ms",
            "dfc_exec_time_ms",
            "logical_rewrite_time_ms",
            "logical_exec_time_ms",
        }
    ]

    if external_time_cols and {"query_num", "tpch_sf"}.issubset(df.columns):
        print("\nCreating TPC-H multi-db charts by scale factor...")
        for sf in sorted(df["tpch_sf"].dropna().unique()):
            sf_df = df[df["tpch_sf"] == sf]
            output_filename = tpch_multi_db_template.format(sf=sf)
            create_tpch_multi_db_chart(
                sf_df,
                output_dir,
                output_filename=output_filename,
                title_suffix=f"SF={sf}",
            )
        print(f"\nAll charts saved to {output_dir}/")
        return

    if "query_type" in df.columns:
        query_types = df["query_type"].unique()
        print(f"Found query types: {query_types}")
        policy_count_value = None
        if "policy_count" in df.columns:
            unique_counts = sorted(df["policy_count"].dropna().unique().tolist())
            if len(unique_counts) == 1:
                try:
                    policy_count_value = int(unique_counts[0])
                except (ValueError, TypeError):
                    policy_count_value = None

        # Create chart for each query type
        for query_type in sorted(query_types):
            print(f"\nCreating chart for {query_type}...")
            create_operator_chart(
                query_type,
                df,
                output_dir,
                output_template=operator_template,
                policy_count=policy_count_value,
            )
        print(f"\nAll charts saved to {output_dir}/")
        return

    if {"source_count", "join_count", "no_policy_exec_time_ms", "dfc_exec_time_ms"}.issubset(df.columns):
        print("\nCreating multi-source execution time chart...")
        create_multi_source_exec_time_chart(df, output_dir=output_dir)
        create_multi_source_heatmap_chart(df, output_dir=output_dir)
        print(f"\nAll charts saved to {output_dir}/")
        return

    if (
        {"no_policy_exec_time_ms", "dfc_exec_time_ms", "logical_exec_time_ms"}.issubset(df.columns)
        or {"no_policy_time_ms", "dfc_time_ms", "logical_time_ms"}.issubset(df.columns)
    ):
        if "tpch_sf" in df.columns:
            print("\nCreating TPC-H summary charts by scale factor...")
            create_tpch_summary_charts_by_sf(
                df,
                output_dir,
                avg_template=tpch_avg_template,
                overhead_template=tpch_overhead_template,
                avg_log_template=tpch_avg_log_template,
            )
            if {
                "no_policy_exec_time_ms",
                "dfc_rewrite_time_ms",
                "dfc_exec_time_ms",
                "logical_rewrite_time_ms",
                "logical_exec_time_ms",
            }.issubset(df.columns):
                for sf in sorted(df["tpch_sf"].dropna().unique()):
                    sf_df = df[df["tpch_sf"] == sf]
                    output_filename = tpch_breakdown_template.format(sf=sf)
                    create_tpch_rewrite_exec_breakdown_chart(
                        sf_df,
                        output_dir,
                        output_filename=output_filename,
                        title_suffix=f"SF={sf}",
                    )
                    output_filename = tpch_breakdown_log_template.format(sf=sf)
                    create_tpch_rewrite_exec_breakdown_chart(
                        sf_df,
                        output_dir,
                        output_filename=output_filename,
                        title_suffix=f"SF={sf}",
                        log_scale=True,
                    )
        else:
            print("\nCreating TPC-H summary chart...")
            create_tpch_summary_chart(df, output_dir)
        print(f"\nAll charts saved to {output_dir}/")
        return

    if {"policy_count", "dfc_exec_time_ms", "logical_exec_time_ms"}.issubset(df.columns):
        print("\nCreating policy count line chart...")
        output_filename = _apply_suffix("tpch_q01_policy_count.png", suffix)
        create_policy_count_chart(df, output_dir, output_filename=output_filename)
        print(f"\nAll charts saved to {output_dir}/")
        return

    print("No supported chart type found for this CSV.")
    return


def create_tpch_summary_chart(
    df: pd.DataFrame,
    output_dir: str = "./results",
    output_filename: str = "tpch_average_times.png",
    title_suffix: str = "",
    plot_mode: str = "average_time",
    log_scale: bool = False,
) -> Optional[plt.Figure]:
    """Create a grouped bar chart for TPC-H results by query and approach."""
    if "query_num" not in df.columns:
        print("No query_num column found; cannot create TPC-H summary chart.")
        return None

    time_columns = [
        "no_policy_exec_time_ms",
        "dfc_exec_time_ms",
        "logical_exec_time_ms",
    ]
    if not set(time_columns).issubset(df.columns):
        print("Exec time columns missing; falling back to total time columns.")
        time_columns = ["no_policy_time_ms", "dfc_time_ms", "logical_time_ms"]

    grouped = df.groupby("query_num", as_index=True)[time_columns].mean().sort_index()
    if plot_mode not in {"average_time", "percent_overhead"}:
        raise ValueError(f"Unsupported plot_mode: {plot_mode}")

    if plot_mode == "percent_overhead":
        # Compute % overhead relative to no-policy per query.
        grouped["dfc_overhead_pct"] = (
            (grouped[time_columns[1]] - grouped[time_columns[0]])
            / grouped[time_columns[0]]
        ) * 100.0
        grouped["logical_overhead_pct"] = (
            (grouped[time_columns[2]] - grouped[time_columns[0]])
            / grouped[time_columns[0]]
        ) * 100.0

    query_nums = grouped.index.astype(int).tolist()
    x_positions = list(range(len(query_nums)))
    bar_width = 0.25 if plot_mode == "average_time" else 0.35

    fig, ax = plt.subplots(figsize=(12, 6))
    if plot_mode == "average_time":
        colors = ["#1f77b4", "#ff7f0e", "#2ca02c"]
        ax.bar(
            [x - bar_width for x in x_positions],
            grouped[time_columns[0]],
            width=bar_width,
            label="No Policy",
            color=colors[0],
        )
        ax.bar(
            x_positions,
            grouped[time_columns[1]],
            width=bar_width,
            label="DFC",
            color=colors[1],
        )
        ax.bar(
            [x + bar_width for x in x_positions],
            grouped[time_columns[2]],
            width=bar_width,
            label="Logical",
            color=colors[2],
        )
        ax.set_ylabel("Average Execution Time (ms)", fontsize=12)
        if log_scale:
            ax.set_yscale("log")
        title = "TPC-H Average Execution Time by Query and Approach"
    else:
        colors = ["#ff7f0e", "#2ca02c"]
        ax.bar(
            [x - bar_width / 2 for x in x_positions],
            grouped["dfc_overhead_pct"],
            width=bar_width,
            label="DFC",
            color=colors[0],
        )
        ax.bar(
            [x + bar_width / 2 for x in x_positions],
            grouped["logical_overhead_pct"],
            width=bar_width,
            label="Logical",
            color=colors[1],
        )
        ax.set_ylabel("Overhead vs No Policy (%)", fontsize=12)
        title = "TPC-H Overhead vs No Policy by Query"

    ax.set_xticks(x_positions)
    ax.set_xticklabels([f"Q{q:02d}" for q in query_nums], fontsize=9)
    ax.set_xlabel("TPC-H Query", fontsize=12)
    if title_suffix:
        title = f"{title} ({title_suffix})"
    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.legend(loc="best", fontsize=10)
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    output_path = Path(output_dir) / output_filename
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved chart to {output_path}")
    return fig


def create_tpch_multi_db_chart(
    df: pd.DataFrame,
    output_dir: str = "./results",
    output_filename: str = "tpch_multi_db.png",
    title_suffix: str | None = None,
) -> Optional[plt.Figure]:
    """Create a multi-db bar chart of per-query average times."""
    if "query_num" not in df.columns:
        print("No query_num column found for multi-db chart.")
        return None

    base_series = [
        ("no_policy_exec_time_ms", "DuckDB No Policy", "#1f77b4"),
        ("dfc_exec_time_ms", "DuckDB DFC", "#ff7f0e"),
        ("logical_exec_time_ms", "DuckDB Logical", "#2ca02c"),
    ]

    time_cols = {
        col
        for col in df.columns
        if col.endswith("_time_ms")
        and not col.endswith("_rewrite_time_ms")
        and col
        not in {
            "no_policy_time_ms",
            "dfc_time_ms",
            "logical_time_ms",
            "physical_time_ms",
        }
    }

    series = []
    for col, label, color in base_series:
        if col in time_cols:
            series.append((col, label, color))

    external_colors = ["#9467bd", "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"]
    external_cols = [col for col in time_cols if col not in {c for c, _, _ in base_series}]

    def _engine_name(col: str) -> str:
        if col.endswith("_dfc_time_ms"):
            return col[: -len("_dfc_time_ms")]
        if col.endswith("_logical_time_ms"):
            return col[: -len("_logical_time_ms")]
        return col[: -len("_time_ms")]

    engine_prefixes = sorted({_engine_name(col) for col in external_cols})
    ordered_external_cols: list[str] = []
    for engine in engine_prefixes:
        ordered_external_cols.extend(
            [
                f"{engine}_time_ms",
                f"{engine}_dfc_time_ms",
                f"{engine}_logical_time_ms",
            ]
        )
    ordered_external_cols = [col for col in ordered_external_cols if col in external_cols]

    for idx, col in enumerate(ordered_external_cols):
        if col.endswith("_dfc_time_ms"):
            engine = col[: -len("_dfc_time_ms")]
            label = f"{engine.replace('_', ' ').title()} DFC"
        elif col.endswith("_logical_time_ms"):
            engine = col[: -len("_logical_time_ms")]
            label = f"{engine.replace('_', ' ').title()} Logical"
        else:
            engine = col[: -len("_time_ms")]
            label = f"{engine.replace('_', ' ').title()} No Policy"
        series.append((col, label, external_colors[idx % len(external_colors)]))

    if not series:
        print("No time columns found for multi-db chart.")
        return None

    series_cols = []
    for col, _, _ in series:
        if col not in series_cols:
            series_cols.append(col)
    grouped = df.groupby("query_num")[series_cols].mean()
    grouped = grouped.sort_index()

    fig, ax = plt.subplots(figsize=(14, 7))
    num_series = len(series)
    width = 0.8 / num_series
    x = range(len(grouped.index))

    for idx, (col, label, color) in enumerate(series):
        offsets = [pos + (idx - (num_series - 1) / 2) * width for pos in x]
        ax.bar(offsets, grouped[col], width=width, label=label, color=color)

    labels = [f"Q{int(q):02d}" for q in grouped.index]
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=0)
    ax.set_xlabel("TPC-H Query", fontsize=12)
    ax.set_ylabel("Average Execution Time (ms)", fontsize=12)
    title = "TPC-H Multi-DB Average Execution Time by Query"
    if title_suffix:
        title = f"{title} ({title_suffix})"
    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.legend(loc="best", fontsize=9, ncol=2)
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    output_path = Path(output_dir) / output_filename
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved chart to {output_path}")
    return fig


def create_tpch_summary_charts_by_sf(
    df: pd.DataFrame,
    output_dir: str = "./results",
    avg_template: str = "tpch_average_times_sf{sf}.png",
    overhead_template: str = "tpch_percent_overhead_sf{sf}.png",
    avg_log_template: str | None = None,
) -> None:
    """Create TPC-H summary charts separated by scale factor."""
    if "tpch_sf" not in df.columns:
        print("No tpch_sf column found; cannot split charts by scale factor.")
        return

    for sf in sorted(df["tpch_sf"].dropna().unique()):
        sf_df = df[df["tpch_sf"] == sf]
        title_suffix = f"SF={sf}"

        output_filename = avg_template.format(sf=sf)
        create_tpch_summary_chart(
            sf_df,
            output_dir,
            output_filename,
            title_suffix,
            plot_mode="average_time",
        )
        if avg_log_template:
            output_filename = avg_log_template.format(sf=sf)
            create_tpch_summary_chart(
                sf_df,
                output_dir,
                output_filename,
                title_suffix,
                plot_mode="average_time",
                log_scale=True,
            )

        output_filename = overhead_template.format(sf=sf)
        create_tpch_summary_chart(
            sf_df,
            output_dir,
            output_filename,
            title_suffix,
            plot_mode="percent_overhead",
        )


def create_tpch_rewrite_exec_breakdown_chart(
    df: pd.DataFrame,
    output_dir: str = "./results",
    output_filename: str = "tpch_rewrite_exec_breakdown_sf{sf}.png",
    title_suffix: str = "",
    log_scale: bool = False,
) -> Optional[plt.Figure]:
    """Create a stacked bar chart for TPC-H rewrite vs exec breakdown."""
    required_cols = {
        "query_num",
        "no_policy_exec_time_ms",
        "dfc_rewrite_time_ms",
        "dfc_exec_time_ms",
        "logical_rewrite_time_ms",
        "logical_exec_time_ms",
    }
    if not required_cols.issubset(df.columns):
        print("Missing required columns for TPC-H rewrite/exec breakdown chart.")
        return None

    grouped = (
        df.groupby("query_num", as_index=True)[
            [
                "no_policy_exec_time_ms",
                "dfc_rewrite_time_ms",
                "dfc_exec_time_ms",
                "logical_rewrite_time_ms",
                "logical_exec_time_ms",
            ]
        ]
        .mean()
        .sort_index()
    )

    if grouped.empty:
        print("No data available for TPC-H rewrite/exec breakdown chart.")
        return None

    query_nums = grouped.index.astype(int).tolist()
    x_positions = list(range(len(query_nums)))
    bar_width = 0.25

    fig, ax = plt.subplots(figsize=(12, 6))

    # No policy exec (single bar)
    ax.bar(
        [x - bar_width for x in x_positions],
        grouped["no_policy_exec_time_ms"],
        width=bar_width,
        label="No Policy exec",
        color="#1f77b4",
    )

    # DFC stacked: rewrite + exec
    ax.bar(
        x_positions,
        grouped["dfc_rewrite_time_ms"],
        width=bar_width,
        label="DFC rewrite",
        color="#ff7f0e",
    )
    ax.bar(
        x_positions,
        grouped["dfc_exec_time_ms"],
        width=bar_width,
        bottom=grouped["dfc_rewrite_time_ms"],
        label="DFC exec",
        color="#ffbb78",
    )

    # Logical stacked: rewrite + exec
    ax.bar(
        [x + bar_width for x in x_positions],
        grouped["logical_rewrite_time_ms"],
        width=bar_width,
        label="Logical rewrite",
        color="#2ca02c",
    )
    ax.bar(
        [x + bar_width for x in x_positions],
        grouped["logical_exec_time_ms"],
        width=bar_width,
        bottom=grouped["logical_rewrite_time_ms"],
        label="Logical exec",
        color="#98df8a",
    )

    ax.set_xticks(x_positions)
    ax.set_xticklabels([f"Q{q:02d}" for q in query_nums], fontsize=9)
    ax.set_xlabel("TPC-H Query", fontsize=12)
    ax.set_ylabel("Average Execution Time (ms)", fontsize=12)
    if log_scale:
        ax.set_yscale("log")

    title = "TPC-H Rewrite vs Execution Breakdown"
    if title_suffix:
        title = f"{title} ({title_suffix})"
    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.legend(loc="best", fontsize=9)
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    output_path = Path(output_dir) / output_filename
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved chart to {output_path}")
    return fig




def create_policy_count_chart(
    df: pd.DataFrame,
    output_dir: str = "./results",
    output_filename: str = "tpch_q01_policy_count.png",
) -> Optional[plt.Figure]:
    """Create a line chart for TPC-H Q01 policy count experiment."""
    required_cols = {"policy_count", "dfc_exec_time_ms", "logical_exec_time_ms"}
    if not required_cols.issubset(df.columns):
        print("Missing required columns for policy count chart.")
        return None

    plot_df = df[["policy_count", "dfc_exec_time_ms", "logical_exec_time_ms"]].copy()
    plot_df = plot_df.dropna(subset=["policy_count"])

    grouped = plot_df.groupby("policy_count", as_index=True).mean(numeric_only=True)
    grouped = grouped.sort_index()

    if grouped.empty:
        print("No data available for policy count chart.")
        return None

    fig, ax = plt.subplots(figsize=(9, 6))
    ax.plot(
        grouped.index,
        grouped["dfc_exec_time_ms"],
        marker="o",
        linewidth=2,
        markersize=6,
        label="DFC",
        color="#ff7f0e",
    )
    ax.plot(
        grouped.index,
        grouped["logical_exec_time_ms"],
        marker="o",
        linewidth=2,
        markersize=6,
        label="Logical",
        color="#2ca02c",
    )

    ax.set_xlabel("Number of Policies", fontsize=12)
    ax.set_ylabel("Average Execution Time (ms)", fontsize=12)
    query_label = None
    if "query_num" in df.columns:
        unique_queries = df["query_num"].dropna().unique().tolist()
        if len(unique_queries) == 1:
            query_label = f"Q{int(unique_queries[0]):02d}"
    if query_label:
        ax.set_title(
            f"TPC-H {query_label} Execution Time vs Policy Count",
            fontsize=14,
            fontweight="bold",
        )
    else:
        ax.set_title("TPC-H Execution Time vs Policy Count", fontsize=14, fontweight="bold")
    ax.set_xscale("log")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=10)

    plt.tight_layout()
    if output_filename == "tpch_q01_policy_count.png" and query_label:
        output_filename = f"tpch_{query_label.lower()}_policy_count.png"
    output_path = Path(output_dir) / output_filename
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved chart to {output_path}")
    return fig


def create_multi_source_exec_time_chart(
    df: pd.DataFrame,
    output_dir: str = "./results",
    output_filename: str = "multi_source_exec_time.png",
) -> Optional[plt.Figure]:
    """Create a line chart for multi-source join chain execution times."""
    required_cols = {"source_count", "no_policy_exec_time_ms", "dfc_exec_time_ms"}
    if not required_cols.issubset(df.columns):
        print("Missing required columns for multi-source chart.")
        return None

    plot_df = df[list(required_cols)].copy()
    if "run_num" in df.columns:
        plot_df = df[df["run_num"].fillna(0) > 0].copy()

    plot_df = plot_df.dropna(subset=["source_count"])
    if plot_df.empty:
        print("No data available for multi-source chart.")
        return None

    grouped = plot_df.groupby("source_count", as_index=True).mean(numeric_only=True)
    grouped = grouped.sort_index()

    fig, ax = plt.subplots(figsize=(9, 6))
    ax.plot(
        grouped.index,
        grouped["no_policy_exec_time_ms"],
        marker="o",
        linewidth=2,
        markersize=6,
        label="No Policy",
        color="#1f77b4",
    )
    ax.plot(
        grouped.index,
        grouped["dfc_exec_time_ms"],
        marker="o",
        linewidth=2,
        markersize=6,
        label="DFC",
        color="#ff7f0e",
    )

    ax.set_xlabel("Number of Sources (Linear Join Chain)", fontsize=12)
    ax.set_ylabel("Average Execution Time (ms)", fontsize=12)
    ax.set_title("Multi-Source Join Chain Execution Time", fontsize=14, fontweight="bold")
    ax.set_xscale("log")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=10)

    plt.tight_layout()
    output_path = Path(output_dir) / output_filename
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved chart to {output_path}")
    return fig


def create_multi_source_heatmap_chart(
    df: pd.DataFrame,
    output_dir: str = "./results",
    output_filename: str = "multi_source_heatmap.png",
) -> Optional[plt.Figure]:
    """Create a heatmap of DFC vs No Policy execution time ratio."""
    required_cols = {"source_count", "join_count", "no_policy_exec_time_ms", "dfc_exec_time_ms"}
    if not required_cols.issubset(df.columns):
        print("Missing required columns for multi-source heatmap.")
        return None

    plot_df = df[list(required_cols)].copy()
    if "run_num" in df.columns:
        plot_df = df[df["run_num"].fillna(0) > 0].copy()

    plot_df = plot_df.dropna(subset=["source_count", "join_count"])
    if plot_df.empty:
        print("No data available for multi-source heatmap.")
        return None

    plot_df["relative_perf"] = plot_df["dfc_exec_time_ms"] / plot_df["no_policy_exec_time_ms"]

    grouped = (
        plot_df.groupby(["join_count", "source_count"], as_index=False)
        .agg({"relative_perf": "mean"})
    )

    join_values = sorted(grouped["join_count"].unique().tolist())
    source_values = sorted(grouped["source_count"].unique().tolist())

    heatmap = pd.DataFrame(index=source_values, columns=join_values, dtype=float)
    for _, row in grouped.iterrows():
        heatmap.at[row["source_count"], row["join_count"]] = row["relative_perf"]

    for source in source_values:
        for join in join_values:
            if source > join:
                heatmap.at[source, join] = float("nan")

    fig, ax = plt.subplots(figsize=(8, 6))
    cmap = matplotlib.colors.LinearSegmentedColormap.from_list(
        "light_red_yellow",
        ["#e38b8b", "#fff3b0"],
    )
    cmap.set_bad(color="#f0f0f0")

    im = ax.imshow(
        heatmap.values,
        origin="lower",
        aspect="auto",
        cmap=cmap,
    )

    ax.set_xticks(range(len(join_values)))
    ax.set_xticklabels(join_values)
    ax.set_yticks(range(len(source_values)))
    ax.set_yticklabels(source_values)
    ax.set_xlabel("Number of Joins", fontsize=12)
    ax.set_ylabel("Number of Sources", fontsize=12)
    ax.set_title("Multi-Source Relative Performance (DFC / No Policy)", fontsize=14, fontweight="bold")

    for y_idx, source in enumerate(source_values):
        for x_idx, join in enumerate(join_values):
            value = heatmap.at[source, join]
            if pd.notna(value):
                ax.text(x_idx, y_idx, f"{value:.2f}", ha="center", va="center", fontsize=9)

    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label("Execution Time Ratio", fontsize=11)

    ax.grid(False)
    plt.tight_layout()
    output_path = Path(output_dir) / output_filename
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved chart to {output_path}")
    return fig


def main():
    """Main entry point for visualization script."""
    import argparse
    from pathlib import Path

    parser = argparse.ArgumentParser(description="Generate experiment visualizations.")
    parser.add_argument(
        "csv_path",
        nargs="?",
        default="./results/microbenchmark_results.csv",
        help="Path to the CSV file with results.",
    )
    parser.add_argument(
        "output_dir",
        nargs="?",
        default="./results",
        help="Directory to save charts.",
    )
    parser.add_argument(
        "--tpch-avg-template",
        default="tpch_average_times_sf{sf}.png",
        help="Filename template for TPC-H average time charts (use {sf}).",
    )
    parser.add_argument(
        "--tpch-overhead-template",
        default="tpch_percent_overhead_sf{sf}.png",
        help="Filename template for TPC-H overhead charts (use {sf}).",
    )
    parser.add_argument(
        "--tpch-avg-log-template",
        default="tpch_average_times_log_sf{sf}.png",
        help="Filename template for log-scale TPC-H average time charts (use {sf}).",
    )
    parser.add_argument(
        "--tpch-breakdown-template",
        default="tpch_rewrite_exec_breakdown_sf{sf}.png",
        help="Filename template for TPC-H breakdown charts (use {sf}).",
    )
    parser.add_argument(
        "--tpch-breakdown-log-template",
        default="tpch_rewrite_exec_breakdown_log_sf{sf}.png",
        help="Filename template for log-scale TPC-H breakdown charts (use {sf}).",
    )
    parser.add_argument(
        "--tpch-multi-db-template",
        default="tpch_multi_db_sf{sf}.png",
        help="Filename template for multi-db TPC-H charts (use {sf}).",
    )
    parser.add_argument(
        "--suffix",
        required=True,
        help="Suffix appended to all output figures to avoid overwriting.",
    )
    parser.add_argument(
        "--operator-template",
        default="{query_type}_performance_policy{policy_count}.png",
        help="Filename template for operator charts (use {query_type}, {policy_count}).",
    )
    args = parser.parse_args()

    # Create output directory if it doesn't exist
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    create_all_charts(
        args.csv_path,
        args.output_dir,
        tpch_avg_template=args.tpch_avg_template,
        tpch_overhead_template=args.tpch_overhead_template,
        operator_template=args.operator_template,
        tpch_breakdown_template=args.tpch_breakdown_template,
        tpch_multi_db_template=args.tpch_multi_db_template,
        tpch_avg_log_template=args.tpch_avg_log_template,
        tpch_breakdown_log_template=args.tpch_breakdown_log_template,
        suffix=args.suffix,
    )


if __name__ == "__main__":
    main()
