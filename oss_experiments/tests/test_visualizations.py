"""Tests for tax LLM validation visualization helpers."""

from __future__ import annotations

import pandas as pd

from oss_experiments.visualizations import (
    create_f1_runtime_scatter_by_reasoning_figure,
    create_f1_runtime_scatter_figure,
)


def test_create_f1_runtime_scatter_figure(tmp_path):
    rows = []
    for query_id, violation in (("q00", False), ("q01", True)):
        rows.append(
            {
                "approach": "dfc",
                "provider": "none",
                "model_name": "none",
                "query_set": "1_row_queries",
                "query_id": query_id,
                "included_context": "none",
                "requested_reasoning_effort": "none",
                "ground_truth_violation": violation,
                "predicted_violation": violation,
                "parse_ok": True,
                "runtime_ms": 1.0,
                "cache_contaminated": False,
            }
        )
        for provider, model_name in (
            ("openai", "gpt-5.5"),
            (
                "bedrock",
                "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/e7ygwg2bquz8",
            ),
        ):
            rows.append(
                {
                    "approach": "llm",
                    "provider": provider,
                    "model_name": model_name,
                    "query_set": "1_row_queries",
                    "query_id": query_id,
                    "included_context": "query_and_results",
                    "requested_reasoning_effort": "medium",
                    "ground_truth_violation": violation,
                    "predicted_violation": violation,
                    "parse_ok": True,
                    "runtime_ms": 100.0,
                    "cache_contaminated": False,
                }
            )

    create_f1_runtime_scatter_figure(pd.DataFrame(rows), output_dir=str(tmp_path))

    output = tmp_path / "tax_llm_validation_f1_runtime_scatter.png"
    assert output.exists()
    assert output.stat().st_size > 0


def test_create_f1_runtime_scatter_by_reasoning_figure(tmp_path):
    rows = []
    for query_id, violation in (("q00", False), ("q01", True)):
        rows.append(
            {
                "approach": "dfc",
                "provider": "none",
                "model_name": "none",
                "query_set": "10_row_queries",
                "query_id": query_id,
                "included_context": "none",
                "requested_reasoning_effort": "none",
                "ground_truth_violation": violation,
                "predicted_violation": violation,
                "parse_ok": True,
                "runtime_ms": 2.0,
                "cache_contaminated": False,
            }
        )
        for effort in ("low", "medium", "high", "xhigh"):
            rows.append(
                {
                    "approach": "llm",
                    "provider": "openai",
                    "model_name": "gpt-5.5",
                    "query_set": "10_row_queries",
                    "query_id": query_id,
                    "included_context": "query_and_results",
                    "requested_reasoning_effort": effort,
                    "ground_truth_violation": violation,
                    "predicted_violation": violation,
                    "parse_ok": True,
                    "runtime_ms": 100.0 if effort == "medium" else 200.0,
                    "cache_contaminated": False,
                }
            )

    create_f1_runtime_scatter_by_reasoning_figure(
        pd.DataFrame(rows),
        output_dir=str(tmp_path),
        query_set="10_row_queries",
    )

    output = tmp_path / "tax_llm_validation_f1_runtime_scatter_10_row_queries_by_reasoning.png"
    assert output.exists()
    assert output.stat().st_size > 0
