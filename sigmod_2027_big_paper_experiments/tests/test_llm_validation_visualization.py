"""Tests for the LLM validation latency/F1 visualization."""

from pathlib import Path
from typing import Any

from matplotlib.axes import Axes
import pandas as pd
import pytest

from vldb_experiments.visualizations_final import (
    create_llm_validation_latency_f1_chart,
)


def _summary_data() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "policy_count": [1],
            "dfc_f1": [1.0],
            "dfc_balanced_accuracy": [1.0],
            "dfc_avg_runtime_ms": [100.0],
            "astra_query_only_f1": [0.8],
            "astra_query_only_balanced_accuracy": [0.75],
            "astra_query_only_avg_runtime_ms": [2_000.0],
            "astra_query_results_f1": [0.7],
            "astra_query_results_balanced_accuracy": [0.65],
            "astra_query_results_avg_runtime_ms": [4_000.0],
        }
    )


@pytest.mark.parametrize("prompt_mode", ["query_only", "query_results"])
def test_create_llm_validation_chart_supports_both_prompt_modes(
    tmp_path: Path, prompt_mode: str
) -> None:
    output_path = tmp_path / f"{prompt_mode}.png"

    create_llm_validation_latency_f1_chart(
        _summary_data(), output_path=output_path, prompt_mode=prompt_mode
    )

    assert output_path.is_file()
    assert output_path.stat().st_size > 0


def test_create_llm_validation_chart_scales_markers_by_policy_count(
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "scaled.png"

    create_llm_validation_latency_f1_chart(
        _summary_data(),
        output_path=output_path,
        scale_markers_by_policy_count=True,
    )

    assert output_path.is_file()
    assert output_path.stat().st_size > 0


def test_create_llm_validation_chart_supports_balanced_accuracy(
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "balanced-accuracy.png"

    create_llm_validation_latency_f1_chart(
        _summary_data(),
        output_path=output_path,
        score_metric="balanced_accuracy",
    )

    assert output_path.is_file()
    assert output_path.stat().st_size > 0


def test_scaled_markers_draw_largest_first(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    summary = pd.concat([_summary_data(), _summary_data()], ignore_index=True)
    summary.loc[1, "policy_count"] = 32
    marker_sizes: list[float] = []
    original_scatter = Axes.scatter

    def capture_scatter(self: Axes, *args: Any, **kwargs: Any) -> Any:
        marker_sizes.append(float(kwargs["s"]))
        return original_scatter(self, *args, **kwargs)

    monkeypatch.setattr(Axes, "scatter", capture_scatter)

    create_llm_validation_latency_f1_chart(
        summary,
        output_path=tmp_path / "draw-order.png",
        scale_markers_by_policy_count=True,
    )

    assert marker_sizes == sorted(marker_sizes, reverse=True)


def test_create_llm_validation_chart_rejects_unknown_prompt_mode(
    tmp_path: Path,
) -> None:
    with pytest.raises(
        ValueError,
        match=r"prompt_mode must be either 'query_only' or 'query_results'\.",
    ):
        create_llm_validation_latency_f1_chart(
            _summary_data(), output_path=tmp_path / "invalid.png", prompt_mode="both"
        )
