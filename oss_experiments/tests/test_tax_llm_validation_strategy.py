"""Tests for TaxLLMValidationStrategy."""

from __future__ import annotations

from unittest.mock import MagicMock

from experiment_harness import ExperimentConfig, ExperimentContext, ExperimentRunner

from oss_experiments.data_setup import QUERY_SET_ONE_ROW, QUERY_SET_TEN_ROW, QUERY_SETS
from oss_experiments.llm_clients import LLMResponse
from oss_experiments.strategies.tax_llm_validation_strategy import TaxLLMValidationStrategy


def _llm_response(
    *,
    raw_response: str = '{"violates_policy": false}',
    predicted_violation: bool | None = False,
    parse_ok: bool = True,
    cache_contaminated: bool = False,
) -> LLMResponse:
    return LLMResponse(
        raw_response=raw_response,
        predicted_violation=predicted_violation,
        parse_ok=parse_ok,
        runtime_ms=10.0,
        input_tokens=100,
        output_tokens=5,
        cached_input_tokens=1 if cache_contaminated else 0,
        cache_write_input_tokens=0,
        cache_read_input_tokens=1 if cache_contaminated else 0,
        cache_contaminated=cache_contaminated,
        request_id="req-1",
        requested_reasoning_effort="medium",
        effective_reasoning_effort="medium",
        temperature_effective="provider_default",
        prompt_chars=50,
    )


def _dfc_only_config(*, num_queries: int = len(QUERY_SETS) * 10, runs: int = 1) -> ExperimentConfig:
    return ExperimentConfig(
        num_executions=num_queries * runs,
        num_warmup_runs=0,
        warmup_mode="per_setting",
        strategy_config={
            "query_sets": list(QUERY_SETS),
            "runs_per_setting": runs,
            "dfc_only": True,
            "db_path": ":memory:",
        },
        output_dir="./results",
        output_filename="test_tax_llm_validation_results.csv",
        verbose=False,
    )


def test_settings_count_dfc_only():
    strategy = TaxLLMValidationStrategy()
    config = _dfc_only_config()
    context = ExperimentContext(strategy_config=config.strategy_config)
    strategy.setup(context)
    assert len(strategy.settings) == len(QUERY_SETS) * 10


def test_settings_count_full_grid():
    strategy = TaxLLMValidationStrategy()
    context = ExperimentContext(
        strategy_config={
            "query_sets": [QUERY_SET_ONE_ROW],
            "query_ids": ["q00", "q01"],
            "included_contexts": ["query_and_results", "query_and_results_and_input"],
            "reasoning_efforts": ["medium", "max"],
            "runs_per_setting": 1,
            "dfc_only": False,
            "include_openai": True,
            "include_bedrock": True,
            "openai_client": MagicMock(),
            "bedrock_client": MagicMock(),
            "db_path": ":memory:",
        }
    )
    strategy.setup(context)
    # 2 queries * (1 dfc + 2 models * 2 contexts * 2 efforts) = 2 * 9 = 18
    assert len(strategy.settings) == 18


def test_dfc_only_runner_produces_expected_outcomes(tmp_path):
    output_dir = tmp_path / "results"
    num_queries = 10
    config = ExperimentConfig(
        num_executions=num_queries,
        num_warmup_runs=0,
        warmup_mode="per_setting",
        strategy_config={
            "query_sets": [QUERY_SET_TEN_ROW],
            "runs_per_setting": 1,
            "dfc_only": True,
            "db_path": str(tmp_path / "tax.db"),
        },
        output_dir=str(output_dir),
        output_filename="dfc_only.csv",
        verbose=False,
    )
    collector = ExperimentRunner(TaxLLMValidationStrategy(), config).run()
    assert len(collector.results) == num_queries
    for result in collector.results:
        assert result.error is None
        metrics = result.custom_metrics
        assert metrics["dfc_truth_matches_expected"] is True

    for query_set in [QUERY_SET_TEN_ROW]:
        subset = [r for r in collector.results if r.custom_metrics["query_set"] == query_set]
        kills = sum(1 for r in subset if r.custom_metrics["dfc_killed"])
        passes = sum(1 for r in subset if not r.custom_metrics["dfc_killed"])
        assert kills == 5
        assert passes == 5


def test_llm_execution_with_mock_client(tmp_path, monkeypatch):
    mock_response = _llm_response()

    def fake_invoke(_self, _prompt, *, nonce, requested_reasoning_effort):
        _ = (nonce, requested_reasoning_effort)
        return mock_response

    monkeypatch.setattr(
        "oss_experiments.llm_clients.OpenAILLMClient.invoke",
        fake_invoke,
    )

    config = ExperimentConfig(
        num_executions=2,
        num_warmup_runs=0,
        warmup_mode="per_setting",
        strategy_config={
            "query_sets": [QUERY_SET_ONE_ROW],
            "query_ids": ["q00"],
            "included_contexts": ["query_and_results"],
            "reasoning_efforts": ["medium"],
            "runs_per_setting": 1,
            "dfc_only": False,
            "include_openai": True,
            "include_bedrock": False,
            "openai_client": MagicMock(),
            "gpt_model": "gpt-5.5",
            "db_path": str(tmp_path / "tax_llm.db"),
        },
        output_dir=str(tmp_path),
        output_filename="llm_mock.csv",
        verbose=False,
    )
    collector = ExperimentRunner(TaxLLMValidationStrategy(), config).run()
    llm_rows = [r for r in collector.results if r.custom_metrics.get("approach") == "llm"]
    assert len(llm_rows) == 1
    assert llm_rows[0].custom_metrics["parse_ok"] is True
    assert llm_rows[0].custom_metrics["cached_input_tokens"] == 0
    assert llm_rows[0].custom_metrics["llm_attempts"] == 1


def test_llm_retries_provider_exception(tmp_path, monkeypatch):
    attempts = {"count": 0}

    def fake_invoke(_self, _prompt, *, nonce, requested_reasoning_effort):
        _ = (nonce, requested_reasoning_effort)
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("temporary provider failure")
        return _llm_response()

    monkeypatch.setattr(
        "oss_experiments.llm_clients.OpenAILLMClient.invoke",
        fake_invoke,
    )

    config = ExperimentConfig(
        num_executions=2,
        num_warmup_runs=0,
        warmup_mode="per_setting",
        strategy_config={
            "query_sets": [QUERY_SET_ONE_ROW],
            "query_ids": ["q00"],
            "included_contexts": ["query_and_results"],
            "reasoning_efforts": ["medium"],
            "runs_per_setting": 1,
            "llm_max_attempts": 3,
            "dfc_only": False,
            "include_openai": True,
            "include_bedrock": False,
            "openai_client": MagicMock(),
            "gpt_model": "gpt-5.5",
            "db_path": str(tmp_path / "tax_llm_retry_exception.db"),
        },
        output_dir=str(tmp_path),
        output_filename="llm_retry_exception.csv",
        verbose=False,
    )
    collector = ExperimentRunner(TaxLLMValidationStrategy(), config).run()
    llm_rows = [r for r in collector.results if r.custom_metrics.get("approach") == "llm"]
    metrics = llm_rows[0].custom_metrics
    assert attempts["count"] == 2
    assert metrics["parse_ok"] is True
    assert metrics["llm_attempts"] == 2
    assert "temporary provider failure" in metrics["llm_retry_errors"]


def test_llm_retries_parse_failure(tmp_path, monkeypatch):
    responses = [
        _llm_response(raw_response="", predicted_violation=None, parse_ok=False),
        _llm_response(),
    ]

    def fake_invoke(_self, _prompt, *, nonce, requested_reasoning_effort):
        _ = (nonce, requested_reasoning_effort)
        return responses.pop(0)

    monkeypatch.setattr(
        "oss_experiments.llm_clients.OpenAILLMClient.invoke",
        fake_invoke,
    )

    config = ExperimentConfig(
        num_executions=2,
        num_warmup_runs=0,
        warmup_mode="per_setting",
        strategy_config={
            "query_sets": [QUERY_SET_ONE_ROW],
            "query_ids": ["q00"],
            "included_contexts": ["query_and_results"],
            "reasoning_efforts": ["medium"],
            "runs_per_setting": 1,
            "llm_max_attempts": 3,
            "dfc_only": False,
            "include_openai": True,
            "include_bedrock": False,
            "openai_client": MagicMock(),
            "gpt_model": "gpt-5.5",
            "db_path": str(tmp_path / "tax_llm_retry_parse.db"),
        },
        output_dir=str(tmp_path),
        output_filename="llm_retry_parse.csv",
        verbose=False,
    )
    collector = ExperimentRunner(TaxLLMValidationStrategy(), config).run()
    llm_rows = [r for r in collector.results if r.custom_metrics.get("approach") == "llm"]
    metrics = llm_rows[0].custom_metrics
    assert metrics["parse_ok"] is True
    assert metrics["llm_attempts"] == 2
    assert "parse_failed" in metrics["llm_retry_errors"]


def test_llm_retries_cache_contamination_when_guard_enabled(tmp_path, monkeypatch):
    responses = [
        _llm_response(cache_contaminated=True),
        _llm_response(),
    ]

    def fake_invoke(_self, _prompt, *, nonce, requested_reasoning_effort):
        _ = (nonce, requested_reasoning_effort)
        return responses.pop(0)

    monkeypatch.setattr(
        "oss_experiments.llm_clients.OpenAILLMClient.invoke",
        fake_invoke,
    )

    config = ExperimentConfig(
        num_executions=2,
        num_warmup_runs=0,
        warmup_mode="per_setting",
        strategy_config={
            "query_sets": [QUERY_SET_ONE_ROW],
            "query_ids": ["q00"],
            "included_contexts": ["query_and_results"],
            "reasoning_efforts": ["medium"],
            "runs_per_setting": 1,
            "llm_max_attempts": 3,
            "dfc_only": False,
            "include_openai": True,
            "include_bedrock": False,
            "openai_client": MagicMock(),
            "gpt_model": "gpt-5.5",
            "fail_on_cache_hit": True,
            "db_path": str(tmp_path / "tax_llm_retry_cache.db"),
        },
        output_dir=str(tmp_path),
        output_filename="llm_retry_cache.csv",
        verbose=False,
    )
    collector = ExperimentRunner(TaxLLMValidationStrategy(), config).run()
    llm_rows = [r for r in collector.results if r.custom_metrics.get("approach") == "llm"]
    metrics = llm_rows[0].custom_metrics
    assert metrics["cache_contaminated"] is False
    assert metrics["llm_attempts"] == 2
    assert "cache contamination detected" in metrics["llm_retry_errors"]
