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
    output_dir: str = "./results"
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

    # Prepare data for plotting
    # We need to melt the time columns into a single column
    time_columns = ["no_policy_time_ms", "dfc_time_ms", "logical_time_ms", "physical_time_ms"]
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
                "no_policy_time_ms": "No Policy",
                "dfc_time_ms": "DFC",
                "logical_time_ms": "Logical",
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
        "DFC": "#ff7f0e",
        "Logical": "#2ca02c",
        "Physical": "#d62728"
    }

    # Plot each approach (using averaged data)
    for approach in ["No Policy", "DFC", "Logical", "Physical"]:
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
    output_path = Path(output_dir) / f"{query_type.lower()}_performance.png"
    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)

    print(f"Saved chart to {output_path}")
    return fig


def create_all_charts(
    csv_path: str = "./results/microbenchmark_results.csv",
    output_dir: str = "./results"
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

    # Get unique query types
    query_types = df["query_type"].unique()
    print(f"Found query types: {query_types}")

    # Create chart for each query type
    for query_type in sorted(query_types):
        print(f"\nCreating chart for {query_type}...")
        create_operator_chart(query_type, df, output_dir)

    print(f"\nAll charts saved to {output_dir}/")


def main():
    """Main entry point for visualization script."""
    from pathlib import Path
    import sys

    # Default paths
    csv_path = "./results/microbenchmark_results.csv"
    output_dir = "./results"

    # Allow command-line arguments
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    if len(sys.argv) > 2:
        output_dir = sys.argv[2]

    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    create_all_charts(csv_path, output_dir)


if __name__ == "__main__":
    main()
