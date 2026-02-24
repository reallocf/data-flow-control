"""Smoke test for LLM validation strategy (OpenAI-only)."""

from __future__ import annotations

import os

from experiment_harness import ExperimentConfig, ExperimentRunner
import pytest

from vldb_experiments.strategies.llm_validation_strategy import LLMValidationStrategy
from vldb_experiments.strategies.tpch_strategy import _ensure_smokedduck


def test_llm_validation_smoke_openai(tmp_path, monkeypatch) -> None:
    """Run a minimal smoke configuration through the full experiment runner."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        pytest.skip("OPENAI_API_KEY is not set; skipping OpenAI smoke test")

    try:
        _ensure_smokedduck()
    except Exception as exc:
        pytest.skip(f"SmokedDuck is unavailable for smoke test: {exc}")

    monkeypatch.setenv("OPENAI_API_KEY", api_key)

    config = ExperimentConfig(
        num_executions=3,  # 1Phase + GPT query_only + GPT query_results
        num_warmup_runs=0,
        warmup_mode="per_setting",
        warmup_runs_per_setting=0,
        strategy_config={
            "tpch_sf": 1.0,
            "queries": [1],
            "policy_counts": [1],
            "runs_per_setting": 1,
            "gpt_model": "gpt-5.2",
            "include_openai": True,
            "include_bedrock": False,
        },
        output_dir=str(tmp_path),
        output_filename="llm_validation_smoke.csv",
        verbose=True,
    )

    runner = ExperimentRunner(LLMValidationStrategy(), config)
    collector = runner.run()

    assert len(collector.results) == 3
    assert all(result.error in (None, "") for result in collector.results)
