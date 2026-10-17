"""Visualization helpers for tax LLM validation results."""

from __future__ import annotations

from pathlib import Path

from matplotlib.lines import Line2D
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, confusion_matrix, f1_score

TRUTHY_VALUES = {"true", "1", "1.0", "yes"}


def load_results(csv_path: str) -> pd.DataFrame:
    return pd.read_csv(csv_path)


def _as_bool_series(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.astype(bool)
    return series.astype(str).str.strip().str.lower().isin(TRUTHY_VALUES)


def _clean_df(df: pd.DataFrame, include_cache_contaminated: bool) -> tuple[pd.DataFrame, int]:
    if include_cache_contaminated or "cache_contaminated" not in df.columns:
        return df, 0
    contaminated = _as_bool_series(df["cache_contaminated"])
    excluded = int(contaminated.sum())
    cleaned = df[~contaminated].copy()
    return cleaned, excluded


def _llm_df(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["approach"].astype(str) == "llm"].copy()


def _metric_label(y_true: pd.Series, y_pred: pd.Series) -> str:
    if y_true.nunique() > 1 and y_pred.nunique() > 1:
        return "f1"
    return "accuracy"


def _plot_label(row: pd.Series) -> str:
    if str(row["approach"]) == "dfc":
        return "dfc"
    if str(row.get("provider", "")) == "bedrock":
        return "opus-4.8 (bedrock)"
    return str(row["model_name"])


def _defense_legend_label(plot_label: str) -> str:
    if plot_label == "opus-4.8 (bedrock)":
        return "opus-4.8"
    return plot_label


def _query_set_sort_key(query_set: str) -> tuple[int, str]:
    try:
        return (int(str(query_set).split("_", maxsplit=1)[0]), str(query_set))
    except ValueError:
        return (10**9, str(query_set))


def create_f1_by_query_figure(
    df: pd.DataFrame,
    *,
    output_dir: str,
    output_filename: str = "tax_llm_validation_f1_by_query.png",
    include_cache_contaminated: bool = False,
) -> None:
    cleaned, excluded = _clean_df(df, include_cache_contaminated)
    if excluded:
        print(f"Excluded {excluded} cache-contaminated rows from F1 figure.")
    llm = _llm_df(cleaned)
    if llm.empty:
        print("No LLM rows available for F1 figure.")
        return

    llm = llm.dropna(subset=["ground_truth_violation", "predicted_violation", "parse_ok"])
    llm = llm[_as_bool_series(llm["parse_ok"])]
    llm["y_true"] = _as_bool_series(llm["ground_truth_violation"])
    llm["y_pred"] = _as_bool_series(llm["predicted_violation"])

    groups = (
        llm.groupby(
            [
                "query_set",
                "query_id",
                "model_name",
                "included_context",
                "requested_reasoning_effort",
            ],
            dropna=False,
        )
        .apply(
            lambda g: f1_score(g["y_true"], g["y_pred"])
            if _metric_label(g["y_true"], g["y_pred"]) == "f1"
            else accuracy_score(g["y_true"], g["y_pred"]),
            include_groups=False,
        )
        .reset_index(name="score")
    )

    query_sets = sorted(groups["query_set"].unique(), key=_query_set_sort_key)
    fig, axes = plt.subplots(1, len(query_sets), figsize=(6 * len(query_sets), 5), squeeze=False)
    for idx, query_set in enumerate(query_sets):
        ax = axes[0][idx]
        subset = groups[groups["query_set"] == query_set]
        pivot = subset.pivot_table(
            index="query_id",
            columns=["model_name", "included_context", "requested_reasoning_effort"],
            values="score",
            aggfunc="mean",
        )
        pivot.plot(kind="bar", ax=ax, rot=45)
        ax.set_title(f"{query_set}")
        ax.set_ylabel("F1 / Accuracy")
        ax.set_xlabel("Query id")
    fig.tight_layout()
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    fig.savefig(Path(output_dir) / output_filename, dpi=150)
    plt.close(fig)


def _runtime_scatter_summary(
    plot_df: pd.DataFrame,
    *,
    group_cols: list[str],
) -> pd.DataFrame:
    summary_rows = []
    for keys, group in plot_df.groupby(group_cols, dropna=False):
        row = dict(zip(group_cols, keys, strict=True))
        row["runtime_ms"] = group["runtime_ms"].mean()
        row["accuracy"] = accuracy_score(group["y_true"], group["y_pred"])
        summary_rows.append(row)
    return pd.DataFrame(summary_rows)


def _prepare_f1_runtime_scatter_df(
    df: pd.DataFrame,
    *,
    include_cache_contaminated: bool,
    query_set: str | None = None,
) -> tuple[pd.DataFrame, int]:
    cleaned, excluded = _clean_df(df, include_cache_contaminated)
    if cleaned.empty:
        return cleaned, excluded

    required = [
        "approach",
        "model_name",
        "query_set",
        "included_context",
        "requested_reasoning_effort",
        "ground_truth_violation",
        "predicted_violation",
        "parse_ok",
        "runtime_ms",
    ]
    plot_df = cleaned.dropna(subset=required).copy()
    if query_set is not None:
        plot_df = plot_df[plot_df["query_set"].astype(str) == query_set].copy()
    plot_df = plot_df[_as_bool_series(plot_df["parse_ok"])]
    if plot_df.empty:
        return plot_df, excluded

    plot_df["plot_label"] = plot_df.apply(_plot_label, axis=1)
    plot_df["y_true"] = _as_bool_series(plot_df["ground_truth_violation"])
    plot_df["y_pred"] = _as_bool_series(plot_df["predicted_violation"])
    return plot_df, excluded


def _reasoning_effort_sort_key(effort: str) -> tuple[int, str]:
    order = {"none": 0, "low": 1, "medium": 2, "high": 3, "xhigh": 4}
    return (order.get(str(effort).lower(), 99), str(effort))


def _reasoning_effort_marker(effort: str) -> str:
    marker_map = {
        "none": "v",
        "low": "v",
        "medium": "o",
        "high": "s",
        "xhigh": "D",
    }
    return marker_map.get(str(effort).lower(), "o")


_DFC_MARKER = "*"
_SCATTER_AXIS_LABEL_FONTSIZE = 21
_SCATTER_TICK_LABEL_FONTSIZE = 16.5
_SCATTER_POINT_SIZE = 135
_SCATTER_LEGEND_MARKER_SIZE = 12
_DFC_POINT_SIZE = _SCATTER_POINT_SIZE * 1.5
_DFC_LEGEND_MARKER_SIZE = _SCATTER_LEGEND_MARKER_SIZE * 1.5


def _add_side_by_side_legends(
    ax,
    *,
    left_handles,
    left_title: str,
    right_handles,
    right_title: str,
) -> None:
    right_legend = ax.legend(
        handles=right_handles,
        title=right_title,
        loc="upper right",
        bbox_to_anchor=(1.0, 1.0),
        borderaxespad=0.0,
    )
    ax.add_artist(right_legend)
    ax.figure.canvas.draw()
    right_bbox = right_legend.get_window_extent().transformed(ax.transAxes.inverted())

    left_legend = ax.legend(
        handles=left_handles,
        title=left_title,
        loc="upper right",
        bbox_to_anchor=(right_bbox.x0 - 0.01, 1.0),
        borderaxespad=0.0,
    )
    ax.add_artist(left_legend)


def create_f1_runtime_scatter_figure(
    df: pd.DataFrame,
    *,
    output_dir: str,
    output_filename: str = "tax_llm_validation_f1_runtime_scatter.png",
    include_cache_contaminated: bool = False,
) -> None:
    plot_df, excluded = _prepare_f1_runtime_scatter_df(
        df, include_cache_contaminated=include_cache_contaminated
    )
    if excluded:
        print(f"Excluded {excluded} cache-contaminated rows from F1/runtime scatter.")
    if plot_df.empty:
        print("No parseable rows available for F1/runtime scatter.")
        return

    summary = _runtime_scatter_summary(
        plot_df,
        group_cols=[
            "plot_label",
            "query_set",
            "included_context",
            "requested_reasoning_effort",
        ],
    )
    if summary.empty:
        return

    labels = ["dfc", *sorted(label for label in summary["plot_label"].unique() if label != "dfc")]
    color_map = dict(zip(labels, plt.get_cmap("tab10").colors, strict=False))
    marker_map = {
        "1_row_queries": "o",
        "10_row_queries": "^",
        "100_row_queries": "s",
    }

    fig, ax = plt.subplots(figsize=(8, 5))
    for label in labels:
        subset = summary[summary["plot_label"] == label]
        for query_set in sorted(subset["query_set"].dropna().unique(), key=_query_set_sort_key):
            query_subset = subset[subset["query_set"] == query_set]
            ax.scatter(
                query_subset["runtime_ms"],
                query_subset["accuracy"] * 100.0,
                label=label if query_set == subset["query_set"].iloc[0] else None,
                color=color_map[label],
                marker=_DFC_MARKER if label == "dfc" else marker_map.get(str(query_set), "o"),
                s=_DFC_POINT_SIZE if label == "dfc" else _SCATTER_POINT_SIZE,
                alpha=0.85,
                edgecolors="black",
                linewidths=0.5,
            )

    if (summary["runtime_ms"] > 0).all():
        ax.set_xscale("log")
        ax.set_xlabel("Mean runtime (ms, log scale)", fontsize=_SCATTER_AXIS_LABEL_FONTSIZE)
    else:
        ax.set_xlabel("Mean runtime (ms)", fontsize=_SCATTER_AXIS_LABEL_FONTSIZE)
    ax.set_ylabel("Accuracy (%)", fontsize=_SCATTER_AXIS_LABEL_FONTSIZE)
    ax.tick_params(axis="both", labelsize=_SCATTER_TICK_LABEL_FONTSIZE)
    ax.set_ylim(-2, 105)
    ax.set_title("Accuracy vs runtime")
    ax.grid(True, which="both", axis="both", alpha=0.25)
    model_handles = [
        Line2D(
            [0],
            [0],
            marker=_DFC_MARKER if label == "dfc" else "o",
            color=color_map[label],
            markerfacecolor=color_map[label],
            markeredgecolor="black",
            label=_defense_legend_label(label),
            linestyle="None",
            markersize=_DFC_LEGEND_MARKER_SIZE if label == "dfc" else _SCATTER_LEGEND_MARKER_SIZE,
        )
        for label in labels
        if label in set(summary["plot_label"])
    ]
    query_set_handles = [
        Line2D(
            [0],
            [0],
            marker=marker,
            color="black",
            label=query_set,
            linestyle="None",
            markersize=_SCATTER_LEGEND_MARKER_SIZE,
        )
        for query_set, marker in marker_map.items()
        if query_set in set(summary["query_set"])
    ]
    if "dfc" in set(summary["plot_label"]):
        query_set_handles.append(
            Line2D(
                [0],
                [0],
                marker=_DFC_MARKER,
                color=color_map["dfc"],
                markerfacecolor=color_map["dfc"],
                markeredgecolor="black",
                label="dfc",
                linestyle="None",
                markersize=_DFC_LEGEND_MARKER_SIZE,
            )
        )
    _add_side_by_side_legends(
        ax,
        left_handles=query_set_handles,
        left_title="Query set",
        right_handles=model_handles,
        right_title="Defense",
    )
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    fig.savefig(Path(output_dir) / output_filename, dpi=150)
    plt.close(fig)


def create_f1_runtime_scatter_by_reasoning_figure(
    df: pd.DataFrame,
    *,
    output_dir: str,
    query_set: str = "10_row_queries",
    output_filename: str | None = None,
    include_cache_contaminated: bool = False,
) -> None:
    plot_df, excluded = _prepare_f1_runtime_scatter_df(
        df,
        include_cache_contaminated=include_cache_contaminated,
        query_set=query_set,
    )
    if excluded:
        print(f"Excluded {excluded} cache-contaminated rows from F1/latency scatter ({query_set}).")
    if plot_df.empty:
        print(f"No parseable rows available for F1/latency scatter ({query_set}).")
        return

    summary = _runtime_scatter_summary(
        plot_df,
        group_cols=[
            "plot_label",
            "included_context",
            "requested_reasoning_effort",
        ],
    )
    if summary.empty:
        return

    if output_filename is None:
        output_filename = f"tax_llm_validation_f1_runtime_scatter_{query_set}_by_reasoning.png"

    labels = ["dfc", *sorted(label for label in summary["plot_label"].unique() if label != "dfc")]
    color_map = dict(zip(labels, plt.get_cmap("tab10").colors, strict=False))
    efforts = sorted(
        summary["requested_reasoning_effort"].dropna().unique(),
        key=_reasoning_effort_sort_key,
    )

    fig, ax = plt.subplots(figsize=(8, 5))
    for label in labels:
        subset = summary[summary["plot_label"] == label]
        for effort in efforts:
            effort_subset = subset[subset["requested_reasoning_effort"] == effort]
            if effort_subset.empty:
                continue
            ax.scatter(
                effort_subset["runtime_ms"],
                effort_subset["accuracy"] * 100.0,
                color=color_map[label],
                marker=_DFC_MARKER if label == "dfc" else _reasoning_effort_marker(str(effort)),
                s=_DFC_POINT_SIZE if label == "dfc" else _SCATTER_POINT_SIZE,
                alpha=0.85,
                edgecolors="black",
                linewidths=0.5,
            )

    ax.set_xlabel("Latency (ms)", fontsize=_SCATTER_AXIS_LABEL_FONTSIZE)
    ax.set_ylabel("Accuracy (%)", fontsize=_SCATTER_AXIS_LABEL_FONTSIZE)
    ax.tick_params(axis="both", labelsize=_SCATTER_TICK_LABEL_FONTSIZE)
    ax.set_ylim(-2, 105)
    ax.grid(True, which="both", axis="both", alpha=0.25)
    model_handles = [
        Line2D(
            [0],
            [0],
            marker=_DFC_MARKER if label == "dfc" else "o",
            color=color_map[label],
            markerfacecolor=color_map[label],
            markeredgecolor="black",
            label=_defense_legend_label(label),
            linestyle="None",
            markersize=_DFC_LEGEND_MARKER_SIZE if label == "dfc" else _SCATTER_LEGEND_MARKER_SIZE,
        )
        for label in labels
        if label in set(summary["plot_label"])
    ]
    effort_handles = [
        Line2D(
            [0],
            [0],
            marker=_reasoning_effort_marker(str(effort)),
            color="black",
            label=str(effort),
            linestyle="None",
            markersize=_SCATTER_LEGEND_MARKER_SIZE,
        )
        for effort in efforts
        if str(effort).lower() != "none"
    ]
    if "dfc" in set(summary["plot_label"]):
        effort_handles.insert(
            0,
            Line2D(
                [0],
                [0],
                marker=_DFC_MARKER,
                color=color_map["dfc"],
                markerfacecolor=color_map["dfc"],
                markeredgecolor="black",
                label="dfc",
                linestyle="None",
                markersize=_DFC_LEGEND_MARKER_SIZE,
            ),
        )
    _add_side_by_side_legends(
        ax,
        left_handles=effort_handles,
        left_title="Reasoning effort",
        right_handles=model_handles,
        right_title="Defense",
    )
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    fig.savefig(Path(output_dir) / output_filename, dpi=150)
    plt.close(fig)


def create_confusion_by_model_figure(
    df: pd.DataFrame,
    *,
    output_dir: str,
    output_filename: str = "tax_llm_validation_confusion_by_model.png",
    include_cache_contaminated: bool = False,
) -> None:
    cleaned, excluded = _clean_df(df, include_cache_contaminated)
    if excluded:
        print(f"Excluded {excluded} cache-contaminated rows from confusion figure.")
    llm = _llm_df(cleaned)
    if llm.empty:
        print("No LLM rows available for confusion figure.")
        return

    llm = llm.dropna(subset=["ground_truth_violation", "predicted_violation", "parse_ok"])
    llm = llm[_as_bool_series(llm["parse_ok"])]
    llm["y_true"] = _as_bool_series(llm["ground_truth_violation"])
    llm["y_pred"] = _as_bool_series(llm["predicted_violation"])

    combos = llm[["model_name", "included_context", "requested_reasoning_effort"]].drop_duplicates()
    n = len(combos)
    cols = min(3, n)
    rows = int(np.ceil(n / cols))
    fig, axes = plt.subplots(rows, cols, figsize=(5 * cols, 4 * rows), squeeze=False)
    for i, (_, combo) in enumerate(combos.iterrows()):
        ax = axes[i // cols][i % cols]
        subset = llm[
            (llm["model_name"] == combo["model_name"])
            & (llm["included_context"] == combo["included_context"])
            & (llm["requested_reasoning_effort"] == combo["requested_reasoning_effort"])
        ]
        cm = confusion_matrix(subset["y_true"], subset["y_pred"], labels=[False, True])
        ax.imshow(cm, cmap="Blues")
        ax.set_xticks([0, 1], labels=["TN/FP pred", "TP"])
        ax.set_yticks([0, 1], labels=["False", "True"])
        for r in range(2):
            for c in range(2):
                ax.text(c, r, str(cm[r, c]), ha="center", va="center")
        ax.set_title(
            f"{combo['model_name']}\n{combo['included_context']} / {combo['requested_reasoning_effort']}"
        )
    for j in range(i + 1, rows * cols):
        axes[j // cols][j % cols].axis("off")
    fig.tight_layout()
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    fig.savefig(Path(output_dir) / output_filename, dpi=150)
    plt.close(fig)


def create_runtime_tokens_figure(
    df: pd.DataFrame,
    *,
    output_dir: str,
    output_filename: str = "tax_llm_validation_runtime_tokens.png",
    include_cache_contaminated: bool = False,
) -> None:
    cleaned, excluded = _clean_df(df, include_cache_contaminated)
    if excluded:
        print(f"Excluded {excluded} cache-contaminated rows from runtime/tokens figure.")
    if cleaned.empty:
        return

    summary = (
        cleaned.groupby(
            [
                "query_set",
                "approach",
                "model_name",
                "included_context",
                "requested_reasoning_effort",
            ],
            dropna=False,
        )
        .agg(runtime_ms=("runtime_ms", "mean"), input_tokens=("input_tokens", "mean"))
        .reset_index()
    )
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    for query_set in sorted(summary["query_set"].dropna().unique(), key=_query_set_sort_key):
        subset = summary[summary["query_set"] == query_set]
        label = (
            subset["model_name"].astype(str)
            + " / "
            + subset["included_context"].astype(str)
            + " / "
            + subset["requested_reasoning_effort"].astype(str)
        )
        axes[0].bar(label, subset["runtime_ms"], alpha=0.7, label=query_set)
        axes[1].bar(label, subset["input_tokens"], alpha=0.7, label=query_set)
    axes[0].set_title("Mean runtime (ms)")
    axes[1].set_title("Mean input tokens")
    for ax in axes:
        ax.tick_params(axis="x", rotation=90)
    fig.tight_layout()
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    fig.savefig(Path(output_dir) / output_filename, dpi=150)
    plt.close(fig)


def create_cache_audit_csv(
    df: pd.DataFrame,
    *,
    output_dir: str,
    output_filename: str = "tax_llm_validation_cache_audit.csv",
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    llm = _llm_df(df)
    if llm.empty:
        audit = pd.DataFrame()
    else:
        contaminated = _as_bool_series(llm["cache_contaminated"])
        audit = (
            llm.assign(cache_contaminated=contaminated)
            .groupby(["provider", "model_name"], dropna=False)
            .agg(
                rows=("model_name", "count"),
                contaminated_rows=("cache_contaminated", "sum"),
                max_cached_tokens=("cached_input_tokens", "max"),
                mean_cached_tokens=("cached_input_tokens", "mean"),
            )
            .reset_index()
        )
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    audit.to_csv(Path(output_dir) / output_filename, index=False)
    return audit
