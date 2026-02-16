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
    if query_type == "JOIN_GROUP_BY":
        return "variation_join_count", "Number of Joins"
    return "variation_num", "Variation Number"


def _with_exec_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with exec-time columns normalized and preferred."""
    normalized = df.copy()

    if "no_policy_exec_time_ms" not in normalized.columns and "no_policy_time_ms" in normalized.columns:
        normalized["no_policy_exec_time_ms"] = normalized["no_policy_time_ms"]
    if "dfc_1phase_exec_time_ms" not in normalized.columns:
        if "dfc_exec_time_ms" in normalized.columns:
            normalized["dfc_1phase_exec_time_ms"] = normalized["dfc_exec_time_ms"]
        elif "dfc_1phase_time_ms" in normalized.columns:
            normalized["dfc_1phase_exec_time_ms"] = normalized["dfc_1phase_time_ms"]
        elif "dfc_time_ms" in normalized.columns:
            normalized["dfc_1phase_exec_time_ms"] = normalized["dfc_time_ms"]
    if "dfc_2phase_exec_time_ms" not in normalized.columns and "dfc_2phase_time_ms" in normalized.columns:
        normalized["dfc_2phase_exec_time_ms"] = normalized["dfc_2phase_time_ms"]
    if "logical_exec_time_ms" not in normalized.columns and "logical_time_ms" in normalized.columns:
        normalized["logical_exec_time_ms"] = normalized["logical_time_ms"]
    if "physical_exec_time_ms" not in normalized.columns:
        if {"physical_base_capture_time_ms", "physical_lineage_query_time_ms"}.issubset(normalized.columns):
            normalized["physical_exec_time_ms"] = (
                normalized["physical_base_capture_time_ms"].fillna(0.0)
                + normalized["physical_lineage_query_time_ms"].fillna(0.0)
            )
        elif "physical_runtime_ms" in normalized.columns:
            normalized["physical_exec_time_ms"] = normalized["physical_runtime_ms"]
        elif "physical_time_ms" in normalized.columns:
            normalized["physical_exec_time_ms"] = normalized["physical_time_ms"]

    # Backward-compat alias for older plotting paths that still read dfc_exec_time_ms.
    if "dfc_exec_time_ms" not in normalized.columns and "dfc_1phase_exec_time_ms" in normalized.columns:
        normalized["dfc_exec_time_ms"] = normalized["dfc_1phase_exec_time_ms"]

    return normalized


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
    query_df = _with_exec_time_columns(query_df)

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

    # Prepare data for plotting using exec-only columns.
    physical_time_col = "physical_exec_time_ms"
    time_columns = [
        "no_policy_exec_time_ms",
        "dfc_1phase_exec_time_ms",
        "dfc_2phase_exec_time_ms",
        "logical_exec_time_ms",
        physical_time_col,
    ]
    available_time_cols = [col for col in time_columns if col in query_df.columns]

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
                "no_policy_exec_time_ms": "No Policy",
                "dfc_1phase_exec_time_ms": "1Phase",
                "dfc_2phase_exec_time_ms": "2Phase",
                "logical_exec_time_ms": "Logical",
                "physical_exec_time_ms": "Physical",
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
        "1Phase": "#ff7f0e",
        "1Phase rewrite": "#ff7f0e",
        "1Phase exec": "#ffbb78",
        "2Phase": "#9467bd",
        "Logical": "#2ca02c",
        "Logical rewrite": "#2ca02c",
        "Logical exec": "#98df8a",
        "Physical": "#d62728",
    }

    # Plot each approach (using averaged data)
    approaches = ["No Policy", "1Phase", "2Phase", "Logical", "Physical"]

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


def create_operator_overhead_chart(
    query_type: str,
    df: pd.DataFrame,
    output_dir: str = "./results",
    output_template: str = "{query_type}_percent_overhead_policy{policy_count}.png",
    policy_count: int | None = None,
) -> Optional[plt.Figure]:
    """Create a percent-overhead chart vs No Policy for a specific operator."""
    query_df = df[df["query_type"] == query_type].copy()
    query_df = _with_exec_time_columns(query_df)
    if len(query_df) == 0:
        print(f"No data found for {query_type}")
        return None

    x_col, x_label = get_variation_x_axis(query_type)
    if x_col not in query_df.columns or query_df[x_col].isna().all():
        if "variation_num" in query_df.columns and not query_df["variation_num"].isna().all():
            x_col = "variation_num"
            x_label = "Variation Number"
        else:
            x_col = "execution_number"
            x_label = "Execution Number"

    physical_time_col = "physical_exec_time_ms"
    overhead_sources = {
        "1Phase": "dfc_1phase_exec_time_ms",
        "2Phase": "dfc_2phase_exec_time_ms",
        "Logical": "logical_exec_time_ms",
        "Physical": physical_time_col,
    }

    if "no_policy_exec_time_ms" not in query_df.columns:
        print(f"No no_policy_exec_time_ms column found for {query_type}")
        return None

    plot_data = []
    for idx, row in query_df.iterrows():
        baseline = row.get("no_policy_exec_time_ms")
        if pd.isna(baseline) or baseline <= 0:
            continue

        if x_col in query_df.columns and pd.notna(row[x_col]):
            x_val = row[x_col]
        elif "variation_num" in query_df.columns and pd.notna(row.get("variation_num")):
            x_val = row["variation_num"]
        else:
            x_val = row.get("execution_number", idx)

        try:
            x_val = float(x_val)
        except (ValueError, TypeError):
            x_val = float(idx)

        for approach, col in overhead_sources.items():
            if col not in query_df.columns:
                continue
            value = row.get(col)
            if pd.isna(value) or value <= 0:
                continue
            overhead_pct = ((float(value) - float(baseline)) / float(baseline)) * 100.0
            plot_data.append(
                {
                    x_label: x_val,
                    "Percent Overhead (%)": overhead_pct,
                    "Approach": approach,
                }
            )

    if not plot_data:
        print(f"No valid overhead data for {query_type}")
        return None

    plot_df = pd.DataFrame(plot_data)
    plot_df_averaged = plot_df.groupby([x_label, "Approach"], as_index=False).agg(
        {"Percent Overhead (%)": "mean"}
    )
    plot_df_averaged = plot_df_averaged.sort_values(["Approach", x_label])

    fig, ax = plt.subplots(figsize=(10, 7))
    colors = {
        "1Phase": "#ff7f0e",
        "2Phase": "#9467bd",
        "Logical": "#2ca02c",
        "Physical": "#d62728",
    }
    for approach in ["1Phase", "2Phase", "Logical", "Physical"]:
        approach_data = plot_df_averaged[plot_df_averaged["Approach"] == approach]
        if len(approach_data) == 0:
            continue
        approach_data = approach_data.sort_values(x_label)
        ax.plot(
            approach_data[x_label],
            approach_data["Percent Overhead (%)"],
            marker="o",
            linewidth=2,
            markersize=6,
            label=approach,
            color=colors.get(approach),
        )

    ax.set_xlabel(x_label, fontsize=12)
    ax.set_ylabel("Percent Overhead (%)", fontsize=12)
    ax.set_title(f"{query_type} Percent Overhead vs No Policy", fontsize=14, fontweight="bold")
    ax.set_xscale("log")
    ax.axhline(y=0.0, color="#555555", linestyle="--", linewidth=1)
    ax.legend(loc="best", fontsize=10)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()

    output_path = Path(output_dir) / output_template.format(
        query_type=query_type.lower(),
        policy_count=policy_count if policy_count is not None else "unknown",
    )
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved chart to {output_path}")
    return fig


def create_operator_overhead_chart_dfc_physical(
    query_type: str,
    df: pd.DataFrame,
    output_dir: str = "./results",
    output_template: str = "{query_type}_percent_overhead_1phase_physical_policy{policy_count}.png",
    policy_count: int | None = None,
) -> Optional[plt.Figure]:
    """Create a percent-overhead chart vs No Policy for 1Phase and Physical only."""
    query_df = df[df["query_type"] == query_type].copy()
    query_df = _with_exec_time_columns(query_df)
    if len(query_df) == 0:
        print(f"No data found for {query_type}")
        return None

    x_col, x_label = get_variation_x_axis(query_type)
    if x_col not in query_df.columns or query_df[x_col].isna().all():
        if "variation_num" in query_df.columns and not query_df["variation_num"].isna().all():
            x_col = "variation_num"
            x_label = "Variation Number"
        else:
            x_col = "execution_number"
            x_label = "Execution Number"

    physical_time_col = "physical_exec_time_ms"
    overhead_sources = {
        "1Phase": "dfc_1phase_exec_time_ms",
        "Physical": physical_time_col,
    }

    if "no_policy_exec_time_ms" not in query_df.columns:
        print(f"No no_policy_exec_time_ms column found for {query_type}")
        return None

    plot_data = []
    for idx, row in query_df.iterrows():
        baseline = row.get("no_policy_exec_time_ms")
        if pd.isna(baseline) or baseline <= 0:
            continue

        if x_col in query_df.columns and pd.notna(row[x_col]):
            x_val = row[x_col]
        elif "variation_num" in query_df.columns and pd.notna(row.get("variation_num")):
            x_val = row["variation_num"]
        else:
            x_val = row.get("execution_number", idx)

        try:
            x_val = float(x_val)
        except (ValueError, TypeError):
            x_val = float(idx)

        for approach, col in overhead_sources.items():
            if col not in query_df.columns:
                continue
            value = row.get(col)
            if pd.isna(value) or value <= 0:
                continue
            overhead_pct = ((float(value) - float(baseline)) / float(baseline)) * 100.0
            plot_data.append(
                {
                    x_label: x_val,
                    "Percent Overhead (%)": overhead_pct,
                    "Approach": approach,
                }
            )

    if not plot_data:
        print(f"No valid overhead data for {query_type}")
        return None

    plot_df = pd.DataFrame(plot_data)
    plot_df_averaged = plot_df.groupby([x_label, "Approach"], as_index=False).agg(
        {"Percent Overhead (%)": "mean"}
    )
    plot_df_averaged = plot_df_averaged.sort_values(["Approach", x_label])

    fig, ax = plt.subplots(figsize=(10, 7))
    colors = {
        "1Phase": "#ff7f0e",
        "Physical": "#d62728",
    }
    for approach in ["1Phase", "Physical"]:
        approach_data = plot_df_averaged[plot_df_averaged["Approach"] == approach]
        if len(approach_data) == 0:
            continue
        approach_data = approach_data.sort_values(x_label)
        ax.plot(
            approach_data[x_label],
            approach_data["Percent Overhead (%)"],
            marker="o",
            linewidth=2,
            markersize=6,
            label=approach,
            color=colors.get(approach),
        )

    ax.set_xlabel(x_label, fontsize=12)
    ax.set_ylabel("Percent Overhead (%)", fontsize=12)
    ax.set_title(f"{query_type} Percent Overhead vs No Policy (1Phase/Physical)", fontsize=14, fontweight="bold")
    ax.set_xscale("log")
    ax.axhline(y=0.0, color="#555555", linestyle="--", linewidth=1)
    ax.legend(loc="best", fontsize=10)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()

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
    operator_overhead_template: str = "{query_type}_percent_overhead_policy{policy_count}.png",
    operator_overhead_dfc_physical_template: str = "{query_type}_percent_overhead_1phase_physical_policy{policy_count}.png",
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
    operator_overhead_template = _apply_suffix(operator_overhead_template, suffix)
    operator_overhead_dfc_physical_template = _apply_suffix(operator_overhead_dfc_physical_template, suffix)
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
            "dfc_1phase_time_ms",
            "dfc_2phase_time_ms",
            "logical_time_ms",
            "physical_time_ms",
            "physical_runtime_ms",
            "physical_exec_time_ms",
            "physical_rewrite_time_ms",
            "physical_base_capture_time_ms",
            "physical_lineage_query_time_ms",
            "no_policy_exec_time_ms",
            "dfc_rewrite_time_ms",
            "dfc_exec_time_ms",
            "dfc_1phase_rewrite_time_ms",
            "dfc_1phase_exec_time_ms",
            "dfc_2phase_rewrite_time_ms",
            "dfc_2phase_exec_time_ms",
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
            create_operator_overhead_chart(
                query_type,
                df,
                output_dir,
                output_template=operator_overhead_template,
                policy_count=policy_count_value,
            )
            create_operator_overhead_chart_dfc_physical(
                query_type,
                df,
                output_dir,
                output_template=operator_overhead_dfc_physical_template,
                policy_count=policy_count_value,
            )
        print(f"\nAll charts saved to {output_dir}/")
        return

    if {"source_count", "join_count", "no_policy_exec_time_ms", "dfc_1phase_exec_time_ms"}.issubset(df.columns):
        print("\nCreating multi-source execution time chart...")
        create_multi_source_exec_time_chart(df, output_dir=output_dir)
        create_multi_source_heatmap_chart(df, output_dir=output_dir)
        print(f"\nAll charts saved to {output_dir}/")
        return

    if {"complexity_terms", "no_policy_exec_time_ms", "dfc_1phase_exec_time_ms", "logical_exec_time_ms"}.issubset(df.columns):
        print("\nCreating policy complexity overhead chart...")
        output_filename = _apply_suffix("tpch_q01_policy_complexity_overhead.png", suffix)
        create_policy_complexity_overhead_chart(df, output_dir=output_dir, output_filename=output_filename)
        print(f"\nAll charts saved to {output_dir}/")
        return

    if {"or_count", "no_policy_exec_time_ms", "dfc_1phase_exec_time_ms", "logical_exec_time_ms"}.issubset(df.columns):
        print("\nCreating policy OR-chain overhead chart...")
        output_filename = _apply_suffix("tpch_q01_policy_many_ors_overhead.png", suffix)
        create_policy_many_ors_overhead_chart(df, output_dir=output_dir, output_filename=output_filename)
        print(f"\nAll charts saved to {output_dir}/")
        return

    if (
        {"no_policy_exec_time_ms", "dfc_1phase_exec_time_ms", "logical_exec_time_ms"}.issubset(df.columns)
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
                "dfc_1phase_rewrite_time_ms",
                "dfc_1phase_exec_time_ms",
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

    if {"policy_count", "dfc_1phase_exec_time_ms", "logical_exec_time_ms"}.issubset(df.columns):
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

    df = _with_exec_time_columns(df)
    physical_col = "physical_exec_time_ms"
    time_columns = [
        "no_policy_exec_time_ms",
        "dfc_1phase_exec_time_ms",
        "dfc_2phase_exec_time_ms",
        "logical_exec_time_ms",
        physical_col,
    ]
    time_columns = [col for col in time_columns if col in df.columns]
    if len(time_columns) < 4:
        print("Missing required time columns for TPC-H summary chart.")
        return None

    grouped = df.groupby("query_num", as_index=True)[time_columns].mean().sort_index()
    if plot_mode not in {"average_time", "percent_overhead"}:
        raise ValueError(f"Unsupported plot_mode: {plot_mode}")

    if plot_mode == "percent_overhead":
        # Compute % overhead relative to no-policy per query.
        grouped["dfc_overhead_pct"] = (
            (grouped[time_columns[1]] - grouped[time_columns[0]])
            / grouped[time_columns[0]]
        ) * 100.0
        grouped["dfc_2phase_overhead_pct"] = (
            (grouped[time_columns[2]] - grouped[time_columns[0]])
            / grouped[time_columns[0]]
        ) * 100.0
        grouped["logical_overhead_pct"] = (
            (grouped[time_columns[3]] - grouped[time_columns[0]])
            / grouped[time_columns[0]]
        ) * 100.0
        if len(time_columns) > 4:
            grouped["physical_overhead_pct"] = (
                (grouped[time_columns[4]] - grouped[time_columns[0]])
                / grouped[time_columns[0]]
            ) * 100.0

    query_nums = grouped.index.astype(int).tolist()
    x_positions = list(range(len(query_nums)))
    bar_width = 0.16 if plot_mode == "average_time" else 0.2

    fig, ax = plt.subplots(figsize=(12, 6))
    if plot_mode == "average_time":
        colors = ["#1f77b4", "#ff7f0e", "#9467bd", "#2ca02c", "#d62728"]
        ax.bar(
            [x - 2 * bar_width for x in x_positions],
            grouped[time_columns[0]],
            width=bar_width,
            label="No Policy",
            color=colors[0],
        )
        ax.bar(
            [x - bar_width for x in x_positions],
            grouped[time_columns[1]],
            width=bar_width,
            label="1Phase",
            color=colors[1],
        )
        ax.bar(
            x_positions,
            grouped[time_columns[2]],
            width=bar_width,
            label="2Phase",
            color=colors[2],
        )
        ax.bar(
            [x + bar_width for x in x_positions],
            grouped[time_columns[3]],
            width=bar_width,
            label="Logical",
            color=colors[3],
        )
        if len(time_columns) > 4:
            ax.bar(
                [x + 2 * bar_width for x in x_positions],
                grouped[time_columns[4]],
                width=bar_width,
                label="Physical",
                color=colors[4],
            )
        ax.set_ylabel("Average Execution Time (ms)", fontsize=12)
        if log_scale:
            ax.set_yscale("log")
        title = "TPC-H Average Execution Time by Query and Approach"
    else:
        colors = ["#ff7f0e", "#9467bd", "#2ca02c", "#d62728"]
        ax.bar(
            [x - 1.5 * bar_width for x in x_positions],
            grouped["dfc_overhead_pct"],
            width=bar_width,
            label="1Phase",
            color=colors[0],
        )
        ax.bar(
            [x - 0.5 * bar_width for x in x_positions],
            grouped["dfc_2phase_overhead_pct"],
            width=bar_width,
            label="2Phase",
            color=colors[1],
        )
        ax.bar(
            [x + 0.5 * bar_width for x in x_positions],
            grouped["logical_overhead_pct"],
            width=bar_width,
            label="Logical",
            color=colors[2],
        )
        if "physical_overhead_pct" in grouped.columns:
            ax.bar(
                [x + 1.5 * bar_width for x in x_positions],
                grouped["physical_overhead_pct"],
                width=bar_width,
                label="Physical",
                color=colors[3],
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
        ("dfc_1phase_exec_time_ms", "DuckDB 1Phase", "#ff7f0e"),
        ("dfc_2phase_exec_time_ms", "DuckDB 2Phase", "#9467bd"),
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
            "dfc_1phase_time_ms",
            "dfc_2phase_time_ms",
            "logical_time_ms",
            "physical_time_ms",
            "physical_runtime_ms",
            "no_policy_exec_time_ms",
            "dfc_1phase_exec_time_ms",
            "dfc_2phase_exec_time_ms",
            "logical_exec_time_ms",
            "physical_exec_time_ms",
        }
    }

    series = []
    for col, label, color in base_series:
        if col in time_cols:
            series.append((col, label, color))

    external_colors = ["#9467bd", "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"]
    external_cols = [col for col in time_cols if col not in {c for c, _, _ in base_series}]

    def _engine_name(col: str) -> str:
        if col.endswith("_dfc_1phase_time_ms"):
            return col[: -len("_dfc_1phase_time_ms")]
        if col.endswith("_dfc_2phase_time_ms"):
            return col[: -len("_dfc_2phase_time_ms")]
        if col.endswith("_logical_time_ms"):
            return col[: -len("_logical_time_ms")]
        return col[: -len("_time_ms")]

    engine_prefixes = sorted({_engine_name(col) for col in external_cols})
    ordered_external_cols: list[str] = []
    for engine in engine_prefixes:
        ordered_external_cols.extend(
            [
                f"{engine}_time_ms",
                f"{engine}_dfc_1phase_time_ms",
                f"{engine}_dfc_2phase_time_ms",
                f"{engine}_logical_time_ms",
            ]
        )
    ordered_external_cols = [col for col in ordered_external_cols if col in external_cols]

    for idx, col in enumerate(ordered_external_cols):
        if col.endswith("_dfc_1phase_time_ms"):
            engine = col[: -len("_dfc_1phase_time_ms")]
            label = f"{engine.replace('_', ' ').title()} 1Phase"
        elif col.endswith("_dfc_2phase_time_ms"):
            engine = col[: -len("_dfc_2phase_time_ms")]
            label = f"{engine.replace('_', ' ').title()} 2Phase"
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
        "dfc_1phase_rewrite_time_ms",
        "dfc_1phase_exec_time_ms",
        "logical_rewrite_time_ms",
        "logical_exec_time_ms",
    }
    if not required_cols.issubset(df.columns):
        print("Missing required columns for TPC-H rewrite/exec breakdown chart.")
        return None

    grouped_cols = [
        "no_policy_exec_time_ms",
        "dfc_1phase_rewrite_time_ms",
        "dfc_1phase_exec_time_ms",
        "logical_rewrite_time_ms",
        "logical_exec_time_ms",
    ]
    if "dfc_2phase_exec_time_ms" in df.columns:
        grouped_cols.append("dfc_2phase_exec_time_ms")
    grouped = df.groupby("query_num", as_index=True)[grouped_cols].mean().sort_index()

    if grouped.empty:
        print("No data available for TPC-H rewrite/exec breakdown chart.")
        return None

    query_nums = grouped.index.astype(int).tolist()
    x_positions = list(range(len(query_nums)))
    bar_width = 0.2

    fig, ax = plt.subplots(figsize=(12, 6))

    # No policy exec (single bar)
    ax.bar(
        [x - bar_width for x in x_positions],
        grouped["no_policy_exec_time_ms"],
        width=bar_width,
        label="No Policy exec",
        color="#1f77b4",
    )

    # 1Phase stacked: rewrite + exec
    ax.bar(
        list(x_positions),
        grouped["dfc_1phase_rewrite_time_ms"],
        width=bar_width,
        label="1Phase rewrite",
        color="#ff7f0e",
    )
    ax.bar(
        list(x_positions),
        grouped["dfc_1phase_exec_time_ms"],
        width=bar_width,
        bottom=grouped["dfc_1phase_rewrite_time_ms"],
        label="1Phase exec",
        color="#ffbb78",
    )

    # 2Phase exec-only (rewrite intentionally excluded)
    if "dfc_2phase_exec_time_ms" in grouped.columns:
        ax.bar(
            [x + bar_width for x in x_positions],
            grouped["dfc_2phase_exec_time_ms"],
            width=bar_width,
            label="2Phase exec",
            color="#9467bd",
        )

    # Logical stacked: rewrite + exec
    ax.bar(
        [x + (2 * bar_width) for x in x_positions],
        grouped["logical_rewrite_time_ms"],
        width=bar_width,
        label="Logical rewrite",
        color="#2ca02c",
    )
    ax.bar(
        [x + (2 * bar_width) for x in x_positions],
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
    required_cols = {"policy_count", "dfc_1phase_exec_time_ms", "logical_exec_time_ms"}
    if not required_cols.issubset(df.columns):
        print("Missing required columns for policy count chart.")
        return None

    plot_cols = ["policy_count", "dfc_1phase_exec_time_ms", "logical_exec_time_ms"]
    if "dfc_2phase_exec_time_ms" in df.columns:
        plot_cols.append("dfc_2phase_exec_time_ms")
    if "physical_exec_time_ms" in df.columns:
        plot_cols.append("physical_exec_time_ms")
    plot_df = df[plot_cols].copy()
    plot_df = plot_df.dropna(subset=["policy_count"])

    grouped = plot_df.groupby("policy_count", as_index=True).mean(numeric_only=True)
    grouped = grouped.sort_index()

    if grouped.empty:
        print("No data available for policy count chart.")
        return None

    fig, ax = plt.subplots(figsize=(9, 6))
    ax.plot(
        grouped.index,
        grouped["dfc_1phase_exec_time_ms"],
        marker="o",
        linewidth=2,
        markersize=6,
        label="1Phase",
        color="#ff7f0e",
    )
    if "dfc_2phase_exec_time_ms" in grouped.columns:
        ax.plot(
            grouped.index,
            grouped["dfc_2phase_exec_time_ms"],
            marker="o",
            linewidth=2,
            markersize=6,
            label="2Phase",
            color="#9467bd",
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
    if "physical_exec_time_ms" in grouped.columns:
        ax.plot(
            grouped.index,
            grouped["physical_exec_time_ms"],
            marker="o",
            linewidth=2,
            markersize=6,
            label="Physical",
            color="#1f77b4",
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


def create_microbenchmark_policy_count_chart(
    df: pd.DataFrame,
    output_dir: str = "./results",
    output_filename: str = "microbenchmark_group_by_policy_count.png",
) -> Optional[plt.Figure]:
    """Create policy-count line chart for the GROUP BY microbenchmark."""
    required_cols = {"policy_count", "dfc_1phase_exec_time_ms", "logical_exec_time_ms"}
    if not required_cols.issubset(df.columns):
        print("Missing required columns for microbenchmark policy count chart.")
        return None

    plot_df = df.copy()
    if "query_type" in plot_df.columns:
        if (plot_df["query_type"] == "GROUP_BY").any():
            plot_df = plot_df[plot_df["query_type"] == "GROUP_BY"].copy()
        elif (plot_df["query_type"] == "JOIN_GROUP_BY").any():
            plot_df = plot_df[plot_df["query_type"] == "JOIN_GROUP_BY"].copy()
    if "run_num" in plot_df.columns:
        plot_df = plot_df[plot_df["run_num"].fillna(0) > 0].copy()

    plot_cols = ["policy_count", "dfc_1phase_exec_time_ms", "logical_exec_time_ms"]
    if "dfc_2phase_exec_time_ms" in plot_df.columns:
        plot_cols.append("dfc_2phase_exec_time_ms")
    if "physical_exec_time_ms" in plot_df.columns:
        plot_cols.append("physical_exec_time_ms")
    plot_df = plot_df[plot_cols].copy().dropna(subset=["policy_count"])
    grouped = plot_df.groupby("policy_count", as_index=True).mean(numeric_only=True).sort_index()

    if grouped.empty:
        print("No data available for microbenchmark policy count chart.")
        return None

    fig, ax = plt.subplots(figsize=(9, 6))
    ax.plot(
        grouped.index,
        grouped["dfc_1phase_exec_time_ms"],
        marker="o",
        linewidth=2,
        markersize=6,
        label="1Phase",
        color="#ff7f0e",
    )
    if "dfc_2phase_exec_time_ms" in grouped.columns:
        ax.plot(
            grouped.index,
            grouped["dfc_2phase_exec_time_ms"],
            marker="o",
            linewidth=2,
            markersize=6,
            label="2Phase",
            color="#9467bd",
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
    if "physical_exec_time_ms" in grouped.columns:
        ax.plot(
            grouped.index,
            grouped["physical_exec_time_ms"],
            marker="o",
            linewidth=2,
            markersize=6,
            label="Physical",
            color="#1f77b4",
        )

    ax.set_xlabel("Number of Policies", fontsize=12)
    ax.set_ylabel("Average Execution Time (ms)", fontsize=12)
    ax.set_title(
        "Microbenchmark Execution Time vs Policy Count",
        fontsize=14,
        fontweight="bold",
    )
    ax.set_xscale("log")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=10)

    plt.tight_layout()
    output_path = Path(output_dir) / output_filename
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved chart to {output_path}")
    return fig


def _prepare_policy_overhead_df(df: pd.DataFrame, x_col: str) -> Optional[pd.DataFrame]:
    required_cols = {x_col, "no_policy_exec_time_ms", "dfc_1phase_exec_time_ms", "logical_exec_time_ms"}
    if not required_cols.issubset(df.columns):
        print(f"Missing required columns for policy overhead chart: {required_cols - set(df.columns)}")
        return None

    plot_df = df.copy()
    if "run_num" in plot_df.columns:
        plot_df = plot_df[plot_df["run_num"].fillna(0) > 0].copy()

    plot_cols = list(required_cols)
    if "dfc_2phase_exec_time_ms" in plot_df.columns:
        plot_cols.append("dfc_2phase_exec_time_ms")
    if "physical_exec_time_ms" in plot_df.columns:
        plot_cols.append("physical_exec_time_ms")
    plot_df = plot_df[plot_cols].copy()
    plot_df = plot_df.dropna(subset=[x_col])
    if plot_df.empty:
        print("No data available for policy overhead chart.")
        return None

    grouped = plot_df.groupby(x_col, as_index=True).mean(numeric_only=True).sort_index()
    grouped = grouped[grouped["no_policy_exec_time_ms"] > 0]
    if grouped.empty:
        print("No valid no-policy timings available for policy overhead chart.")
        return None

    grouped["dfc_1phase_overhead"] = grouped["dfc_1phase_exec_time_ms"] / grouped["no_policy_exec_time_ms"]
    if "dfc_2phase_exec_time_ms" in grouped.columns:
        grouped["dfc_2phase_overhead"] = grouped["dfc_2phase_exec_time_ms"] / grouped["no_policy_exec_time_ms"]
    grouped["logical_overhead"] = grouped["logical_exec_time_ms"] / grouped["no_policy_exec_time_ms"]
    if "physical_exec_time_ms" in grouped.columns:
        grouped["physical_overhead"] = grouped["physical_exec_time_ms"] / grouped["no_policy_exec_time_ms"]
    return grouped


def _apply_overhead_xscale(ax: plt.Axes, x_values: pd.Index) -> None:
    if (x_values <= 0).any():
        ax.set_xscale("symlog", linthresh=1, base=10)
        ax.set_xlim(left=0)
    else:
        ax.set_xscale("log")
        ax.set_xlim(left=max(min(x_values), 1e-6))

    unique_values = sorted(set(x_values.tolist()))
    if len(unique_values) <= 10:
        ax.set_xticks(unique_values)


def _policy_overhead_title(df: pd.DataFrame, base_title: str) -> str:
    query_label = None
    if "query_num" in df.columns:
        unique_queries = df["query_num"].dropna().unique().tolist()
        if len(unique_queries) == 1:
            query_label = f"Q{int(unique_queries[0]):02d}"
    if query_label:
        return f"{base_title} ({query_label})"
    return base_title


def create_policy_complexity_overhead_chart(
    df: pd.DataFrame,
    output_dir: str = "./results",
    output_filename: str = "tpch_q01_policy_complexity_overhead.png",
) -> Optional[plt.Figure]:
    """Create a relative overhead chart for policy complexity scaling."""
    grouped = _prepare_policy_overhead_df(df, "complexity_terms")
    if grouped is None:
        return None

    fig, ax = plt.subplots(figsize=(9, 6))
    ax.plot(
        grouped.index,
        grouped["dfc_1phase_overhead"],
        marker="o",
        linewidth=2,
        markersize=6,
        label="1Phase / No Policy",
        color="#ff7f0e",
    )
    if "dfc_2phase_overhead" in grouped.columns:
        ax.plot(
            grouped.index,
            grouped["dfc_2phase_overhead"],
            marker="o",
            linewidth=2,
            markersize=6,
            label="2Phase / No Policy",
            color="#9467bd",
        )
    ax.plot(
        grouped.index,
        grouped["logical_overhead"],
        marker="o",
        linewidth=2,
        markersize=6,
        label="Logical / No Policy",
        color="#2ca02c",
    )
    if "physical_overhead" in grouped.columns:
        ax.plot(
            grouped.index,
            grouped["physical_overhead"],
            marker="o",
            linewidth=2,
            markersize=6,
            label="Physical / No Policy",
            color="#1f77b4",
        )
    ax.axhline(1.0, color="#1f77b4", linestyle="--", linewidth=1, label="No Policy")

    ax.set_xlabel("Predicate Complexity (Term Count)", fontsize=12)
    ax.set_ylabel("Relative Overhead (Exec Time Ratio)", fontsize=12)
    ax.set_title(_policy_overhead_title(df, "TPC-H Policy Complexity Overhead"), fontsize=14, fontweight="bold")
    _apply_overhead_xscale(ax, grouped.index)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=9)

    plt.tight_layout()
    output_path = Path(output_dir) / output_filename
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved chart to {output_path}")
    return fig


def create_policy_many_ors_overhead_chart(
    df: pd.DataFrame,
    output_dir: str = "./results",
    output_filename: str = "tpch_q01_policy_many_ors_overhead.png",
) -> Optional[plt.Figure]:
    """Create a relative overhead chart for policy OR-clause scaling."""
    grouped = _prepare_policy_overhead_df(df, "or_count")
    if grouped is None:
        return None

    fig, ax = plt.subplots(figsize=(9, 6))
    ax.plot(
        grouped.index,
        grouped["dfc_1phase_overhead"],
        marker="o",
        linewidth=2,
        markersize=6,
        label="1Phase / No Policy",
        color="#ff7f0e",
    )
    if "dfc_2phase_overhead" in grouped.columns:
        ax.plot(
            grouped.index,
            grouped["dfc_2phase_overhead"],
            marker="o",
            linewidth=2,
            markersize=6,
            label="2Phase / No Policy",
            color="#9467bd",
        )
    ax.plot(
        grouped.index,
        grouped["logical_overhead"],
        marker="o",
        linewidth=2,
        markersize=6,
        label="Logical / No Policy",
        color="#2ca02c",
    )
    if "physical_overhead" in grouped.columns:
        ax.plot(
            grouped.index,
            grouped["physical_overhead"],
            marker="o",
            linewidth=2,
            markersize=6,
            label="Physical / No Policy",
            color="#1f77b4",
        )
    ax.axhline(1.0, color="#1f77b4", linestyle="--", linewidth=1, label="No Policy")

    ax.set_xlabel("Number of OR Clauses", fontsize=12)
    ax.set_ylabel("Relative Overhead (Exec Time Ratio)", fontsize=12)
    ax.set_title(_policy_overhead_title(df, "TPC-H Policy OR-Chain Overhead"), fontsize=14, fontweight="bold")
    _apply_overhead_xscale(ax, grouped.index)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=9)

    plt.tight_layout()
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
    required_cols = {"source_count", "no_policy_exec_time_ms", "dfc_1phase_exec_time_ms"}
    if not required_cols.issubset(df.columns):
        print("Missing required columns for multi-source chart.")
        return None

    plot_cols = list(required_cols)
    if "dfc_2phase_exec_time_ms" in df.columns:
        plot_cols.append("dfc_2phase_exec_time_ms")
    plot_df = df[plot_cols].copy()
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
        grouped["dfc_1phase_exec_time_ms"],
        marker="o",
        linewidth=2,
        markersize=6,
        label="1Phase",
        color="#ff7f0e",
    )
    if "dfc_2phase_exec_time_ms" in grouped.columns:
        ax.plot(
            grouped.index,
            grouped["dfc_2phase_exec_time_ms"],
            marker="o",
            linewidth=2,
            markersize=6,
            label="2Phase",
            color="#9467bd",
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
    """Create a heatmap of 1Phase vs No Policy execution time ratio."""
    required_cols = {"source_count", "join_count", "no_policy_exec_time_ms", "dfc_1phase_exec_time_ms"}
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

    plot_df["relative_perf"] = plot_df["dfc_1phase_exec_time_ms"] / plot_df["no_policy_exec_time_ms"]

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
    ax.set_title("Multi-Source Relative Performance (1Phase / No Policy)", fontsize=14, fontweight="bold")

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


def create_multi_db_engine_summary_chart(
    df: pd.DataFrame,
    output_dir: str = "./results",
    output_filename: str = "tpch_multi_db_engine_summary.png",
    title_suffix: str | None = None,
) -> Optional[plt.Figure]:
    """Create a per-engine summary chart of average overhead."""
    df = _with_exec_time_columns(df)
    if "query_num" not in df.columns:
        print("Missing query_num column for multi-db engine summary chart.")
        return None

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
        "DataFusion": ("datafusion_dfc_1phase_time_ms", "datafusion_dfc_2phase_time_ms", "datafusion_logical_time_ms", "datafusion_time_ms"),
        "SQL Server": (
            "sqlserver_dfc_1phase_time_ms",
            "sqlserver_dfc_2phase_time_ms",
            "sqlserver_logical_time_ms",
            "sqlserver_time_ms",
        ),
    }

    available = {
        name: cols for name, cols in engines.items() if all(col in df.columns for col in cols)
    }
    if not available:
        print("No engine time columns found for multi-db engine summary chart.")
        return None

    records: list[dict[str, float | str]] = []
    valid_engines: list[str] = []
    for engine, (dfc_1phase_col, dfc_2phase_col, logical_col, baseline_col) in available.items():
        baseline_by_query = df.groupby("query_num", as_index=True)[baseline_col].mean(numeric_only=True)
        baseline_by_query = baseline_by_query[baseline_by_query > 0]
        if baseline_by_query.empty:
            continue
        engine_added = False
        for label, col in [("1Phase", dfc_1phase_col), ("2Phase", dfc_2phase_col), ("Logical", logical_col)]:
            approach_by_query = df.groupby("query_num", as_index=True)[col].mean(numeric_only=True)
            approach_by_query = approach_by_query.reindex(baseline_by_query.index)
            valid_mask = approach_by_query > 0
            overhead_by_query = (
                approach_by_query[valid_mask] / baseline_by_query[valid_mask]
            ).replace([float("inf"), float("-inf")], pd.NA).dropna()
            if overhead_by_query.empty:
                continue
            overall_avg = float((overhead_by_query.mean() - 1.0) * 100.0)
            records.append(
                {
                    "engine": engine,
                    "approach": label,
                    "avg_overhead": overall_avg,
                }
            )
            engine_added = True
        if engine_added:
            valid_engines.append(engine)

    if not records:
        print("No data available for multi-db engine summary chart.")
        return None

    summary_df = pd.DataFrame.from_records(records)
    engine_order = [engine for engine in engines if engine in valid_engines]
    summary_df["engine"] = pd.Categorical(summary_df["engine"], categories=engine_order, ordered=True)
    summary_df = summary_df.sort_values(["engine", "approach"])

    fig, ax = plt.subplots(figsize=(9, 6))

    x_positions = range(len(engine_order))
    bar_width = 0.24
    offsets = {"1Phase": -bar_width, "2Phase": 0.0, "Logical": bar_width}
    colors = {"1Phase": "#ff7f0e", "2Phase": "#9467bd", "Logical": "#2ca02c"}

    for approach in ["1Phase", "2Phase", "Logical"]:
        subset = summary_df[summary_df["approach"] == approach]
        if subset.empty:
            continue
        xs = [engine_order.index(e) + offsets[approach] for e in subset["engine"]]
        ax.bar(
            xs,
            subset["avg_overhead"],
            width=bar_width,
            label=approach,
            color=colors[approach],
        )

    ax.set_xticks(list(x_positions))
    ax.set_xticklabels(engine_order, fontsize=10)
    ax.set_ylabel("Avg Overhead (%)", fontsize=12)
    title = "TPC-H Average Overhead by Engine (Per-Query Avg)"
    if title_suffix:
        title = f"{title} ({title_suffix})"
    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.grid(axis="y", alpha=0.3)
    ax.legend(loc="best", fontsize=10)

    plt.tight_layout()
    output_path = Path(output_dir) / output_filename
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved chart to {output_path}")
    return fig


def create_multi_db_engine_summary_capped_chart(
    df: pd.DataFrame,
    output_dir: str = "./results",
    output_filename: str = "tpch_multi_db_engine_summary_capped.png",
    duckdb_cap_pct: float = 300.0,
    title_suffix: str | None = None,
) -> Optional[plt.Figure]:
    """Create a per-engine summary chart with DuckDB overhead capped."""
    df = _with_exec_time_columns(df)
    if "query_num" not in df.columns:
        print("Missing query_num column for multi-db engine summary chart.")
        return None

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
        "DataFusion": ("datafusion_dfc_1phase_time_ms", "datafusion_dfc_2phase_time_ms", "datafusion_logical_time_ms", "datafusion_time_ms"),
        "SQL Server": (
            "sqlserver_dfc_1phase_time_ms",
            "sqlserver_dfc_2phase_time_ms",
            "sqlserver_logical_time_ms",
            "sqlserver_time_ms",
        ),
    }

    available = {
        name: cols for name, cols in engines.items() if all(col in df.columns for col in cols)
    }
    if not available:
        print("No engine time columns found for multi-db engine summary chart.")
        return None

    records: list[dict[str, float | str]] = []
    valid_engines: list[str] = []
    for engine, (dfc_1phase_col, dfc_2phase_col, logical_col, baseline_col) in available.items():
        baseline_by_query = df.groupby("query_num", as_index=True)[baseline_col].mean(numeric_only=True)
        baseline_by_query = baseline_by_query[baseline_by_query > 0]
        if baseline_by_query.empty:
            continue
        engine_added = False
        for label, col in [("1Phase", dfc_1phase_col), ("2Phase", dfc_2phase_col), ("Logical", logical_col)]:
            approach_by_query = df.groupby("query_num", as_index=True)[col].mean(numeric_only=True)
            approach_by_query = approach_by_query.reindex(baseline_by_query.index)
            valid_mask = approach_by_query > 0
            overhead_by_query = (
                approach_by_query[valid_mask] / baseline_by_query[valid_mask]
            ).replace([float("inf"), float("-inf")], pd.NA).dropna()
            if overhead_by_query.empty:
                continue
            overall_avg = float((overhead_by_query.mean() - 1.0) * 100.0)
            if engine == "DuckDB":
                overall_avg = min(overall_avg, duckdb_cap_pct)
            records.append(
                {
                    "engine": engine,
                    "approach": label,
                    "avg_overhead": overall_avg,
                }
            )
            engine_added = True
        if engine_added:
            valid_engines.append(engine)

    if not records:
        print("No data available for multi-db engine summary chart.")
        return None

    summary_df = pd.DataFrame.from_records(records)
    engine_order = [engine for engine in engines if engine in valid_engines]
    summary_df["engine"] = pd.Categorical(summary_df["engine"], categories=engine_order, ordered=True)
    summary_df = summary_df.sort_values(["engine", "approach"])

    fig, ax = plt.subplots(figsize=(9, 6))

    x_positions = range(len(engine_order))
    bar_width = 0.24
    offsets = {"1Phase": -bar_width, "2Phase": 0.0, "Logical": bar_width}
    colors = {"1Phase": "#ff7f0e", "2Phase": "#9467bd", "Logical": "#2ca02c"}

    for approach in ["1Phase", "2Phase", "Logical"]:
        subset = summary_df[summary_df["approach"] == approach]
        if subset.empty:
            continue
        xs = [engine_order.index(e) + offsets[approach] for e in subset["engine"]]
        ax.bar(
            xs,
            subset["avg_overhead"],
            width=bar_width,
            label=approach,
            color=colors[approach],
        )

    ax.set_xticks(list(x_positions))
    ax.set_xticklabels(engine_order, fontsize=10)
    ax.set_ylabel("Avg Overhead (%)", fontsize=12)
    title = f"TPC-H Average Overhead by Engine (DuckDB capped at {duckdb_cap_pct:.0f}%)"
    if title_suffix:
        title = f"{title} ({title_suffix})"
    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.grid(axis="y", alpha=0.3)
    ax.legend(loc="best", fontsize=10)
    ax.set_ylim(top=duckdb_cap_pct)

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
    parser.add_argument(
        "--operator-overhead-template",
        default="{query_type}_percent_overhead_policy{policy_count}.png",
        help="Filename template for operator overhead charts (use {query_type}, {policy_count}).",
    )
    parser.add_argument(
        "--operator-overhead-dfc-physical-template",
        default="{query_type}_percent_overhead_1phase_physical_policy{policy_count}.png",
        help="Filename template for 1Phase/Physical-only operator overhead charts (use {query_type}, {policy_count}).",
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
        operator_overhead_template=args.operator_overhead_template,
        operator_overhead_dfc_physical_template=args.operator_overhead_dfc_physical_template,
        tpch_breakdown_template=args.tpch_breakdown_template,
        tpch_multi_db_template=args.tpch_multi_db_template,
        tpch_avg_log_template=args.tpch_avg_log_template,
        tpch_breakdown_log_template=args.tpch_breakdown_log_template,
        suffix=args.suffix,
    )


if __name__ == "__main__":
    main()
