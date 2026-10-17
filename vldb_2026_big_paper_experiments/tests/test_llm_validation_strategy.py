"""Tests for llm_validation strategy configuration."""

from dataclasses import replace
import json
import sys
from types import SimpleNamespace

from data_flow_control import Policy, Resolution
from data_flow_control.adapters.duckdb import DuckDBAdapter
from data_flow_control.connection import Connection
import duckdb

from agent_harness.config import HarnessConfig
from agent_harness.llm_factory import create_chat_model
from vldb_experiments.strategies.llm_validation_common import (
    DEFAULT_BEDROCK_MODEL_SPECS,
    POLICY_VARIANT_FIXED_4,
    fixed_4_violation_policy_specs,
    policy_specs,
)
from vldb_experiments.strategies.llm_validation_strategy import (
    DEFAULT_PROMPT_MODES,
    PROMPT_MODE_QUERY_ONLY,
    PROMPT_MODE_QUERY_RESULTS,
    LLMValidationStrategy,
    _build_policies,
    _build_query_only_prompt,
    _build_query_results_prompt,
    _policy_catalog,
    _response_usage,
    build_validation_settings,
)
from vldb_experiments.strategies.regulation_z_benchmark import build_openai_invoke_request


def test_llm_validation_policy_catalog_size_is_fixed() -> None:
    """Keep policy count fixed while iterating on policy content."""
    assert len(_policy_catalog()) == 32


def test_llm_validation_matches_figure_one_quantity_threshold() -> None:
    assert _policy_catalog()[0] == (
        "sum(lineitem.l_quantity - 30) <= 0",
        "Average quantity should remain at or below 30.",
    )


def test_llm_validation_policy_catalog_avoids_avg() -> None:
    assert all("avg(" not in constraint.lower() for constraint, _ in _policy_catalog())


def test_fixed_4_policy_variant_only_replaces_new_violation_policies() -> None:
    growing = policy_specs()
    fixed = fixed_4_violation_policy_specs()

    assert len(fixed) == 32
    assert fixed[0] == growing[0]
    assert {
        index for index, (old, new) in enumerate(zip(growing, fixed), start=1) if old != new
    } == {2, 3, 5, 9, 17}
    assert [fixed[index - 1].constraint for index in [2, 3, 5, 9, 17]] == [
        "sum(lineitem.l_quantity - 10.5) >= 0",
        "sum(lineitem.l_quantity - 10) >= 0",
        "sum(lineitem.l_discount - 0.055) <= 0",
        "sum(lineitem.l_discount - 0.055) <= 0",
        "sum(lineitem.l_discount - 0.055) <= 0",
    ]
    assert all("avg(" not in spec.constraint.lower() for spec in fixed)


def test_build_policies_selects_fixed_4_variant() -> None:
    policies = _build_policies(32, POLICY_VARIANT_FIXED_4)

    assert policies[0].constraint == "sum(lineitem.l_quantity - 30) <= 0"
    assert policies[1].constraint == "sum(lineitem.l_quantity - 10.5) >= 0"
    assert policies[31].constraint == "sum(lineitem.l_discount - 0.06) <= 0"


def test_llm_validation_preserves_multi_source_policy_applicability() -> None:
    policies = _build_policies(17)

    assert policies[2].sources == ["lineitem", "part"]
    assert policies[4].sources == ["lineitem", "partsupp"]
    assert policies[8].sources == ["lineitem", "part", "region"]
    assert policies[16].sources == ["lineitem", "region"]


def test_llm_validation_policy_sources_and_constraints_are_stable() -> None:
    assert [(list(spec.sources), spec.constraint) for spec in policy_specs()] == [
        (["lineitem"], "sum(lineitem.l_quantity - 30) <= 0"),
        (["lineitem"], "sum(lineitem.l_quantity - 14) >= 0"),
        (["lineitem", "part"], "sum(lineitem.l_quantity - 20) >= 0"),
        (["lineitem"], "sum(lineitem.l_quantity - 10) >= 0"),
        (["lineitem", "partsupp"], "max(lineitem.l_discount) <= 0.07"),
        (["lineitem"], "sum(lineitem.l_discount - 0.01) >= 0"),
        (["lineitem"], "sum(lineitem.l_tax - 0.01) >= 0"),
        (["lineitem"], "sum(lineitem.l_extendedprice - 10000) >= 0"),
        (["lineitem", "part", "region"], "max(lineitem.l_discount) <= 0.07"),
        (
            ["lineitem"],
            "sum(lineitem.l_extendedprice - 1000 * lineitem.l_quantity) >= 0",
        ),
        (["lineitem", "part"], "sum(lineitem.l_quantity - 30) <= 0"),
        (["lineitem", "part"], "sum(lineitem.l_discount - 0.06) <= 0"),
        (["lineitem", "part"], "sum(lineitem.l_tax - 0.05) <= 0"),
        (["lineitem", "part"], "sum(lineitem.l_extendedprice - 60000) <= 0"),
        (["lineitem", "region"], "sum(lineitem.l_quantity - 30) <= 0"),
        (["lineitem", "region"], "sum(lineitem.l_discount - 0.06) <= 0"),
        (["lineitem", "region"], "max(lineitem.l_discount) <= 0.07"),
        (["lineitem", "region"], "sum(lineitem.l_tax - 0.05) <= 0"),
        (["lineitem", "region"], "sum(lineitem.l_extendedprice - 60000) <= 0"),
        (["lineitem", "partsupp"], "sum(lineitem.l_quantity - 30) <= 0"),
        (["lineitem", "partsupp"], "sum(lineitem.l_discount - 0.06) <= 0"),
        (["lineitem", "partsupp"], "sum(lineitem.l_tax - 0.05) <= 0"),
        (["lineitem", "partsupp"], "sum(lineitem.l_extendedprice - 60000) <= 0"),
        (["lineitem", "customer"], "sum(lineitem.l_quantity - 20) >= 0"),
        (["lineitem", "customer"], "sum(lineitem.l_discount - 0.09) <= 0"),
        (["lineitem", "customer"], "sum(lineitem.l_tax - 0.07) <= 0"),
        (["lineitem", "customer"], "sum(lineitem.l_extendedprice - 80000) <= 0"),
        (["lineitem", "supplier"], "sum(lineitem.l_quantity - 30) <= 0"),
        (["lineitem", "supplier"], "sum(lineitem.l_discount - 0.06) <= 0"),
        (["lineitem", "supplier"], "sum(lineitem.l_tax - 0.05) <= 0"),
        (["lineitem", "supplier"], "sum(lineitem.l_extendedprice - 60000) <= 0"),
        (["lineitem"], "sum(lineitem.l_discount - 0.06) <= 0"),
    ]


def test_default_models_use_bedrock_inference_profiles() -> None:
    assert DEFAULT_BEDROCK_MODEL_SPECS == [
        {
            "key": "astra",
            "display_name": "Astra 6",
            "provider": "bedrock",
            "bedrock_provider": "openai",
            "input_usd_per_million": 11.0,
            "output_usd_per_million": 55.0,
            "cache_read_usd_per_million": 1.1,
            "cache_write_usd_per_million": 13.75,
            "model_id": (
                "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/ybl9l7bkcovm"
            ),
        },
        {
            "key": "sol_6_1",
            "display_name": "Sol 6.1",
            "provider": "bedrock",
            "bedrock_provider": "openai",
            "input_usd_per_million": 2.2,
            "output_usd_per_million": 11.0,
            "cache_read_usd_per_million": 0.11,
            "cache_write_usd_per_million": 2.75,
            "model_id": (
                "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/793tfgrfes18"
            ),
        },
        {
            "key": "luna",
            "display_name": "Luna 6",
            "provider": "bedrock",
            "bedrock_provider": "openai",
            "input_usd_per_million": 0.11,
            "output_usd_per_million": 0.55,
            "cache_read_usd_per_million": 0.011,
            "cache_write_usd_per_million": 0.1375,
            "model_id": (
                "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/cn56cvd6ntch"
            ),
        },
        {
            "key": "fable_5_1",
            "display_name": "Fable 5.1",
            "provider": "bedrock",
            "bedrock_provider": "anthropic",
            "input_usd_per_million": 11.0,
            "output_usd_per_million": 55.0,
            "cache_read_usd_per_million": 0.275,
            "cache_write_usd_per_million": 13.75,
            "model_id": (
                "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/7mwcoskr36hk"
            ),
        },
        {
            "key": "opus_5_5",
            "display_name": "Opus 5.5",
            "provider": "bedrock",
            "bedrock_provider": "anthropic",
            "input_usd_per_million": 4.4,
            "output_usd_per_million": 22.0,
            "cache_read_usd_per_million": 0.22,
            "cache_write_usd_per_million": 5.5,
            "model_id": (
                "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/d0lzabehot7u"
            ),
        },
    ]


def test_bedrock_chat_model_uses_provider_sampling_defaults(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeChatBedrockConverse:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

    monkeypatch.setitem(
        sys.modules,
        "langchain_aws",
        SimpleNamespace(ChatBedrockConverse=FakeChatBedrockConverse),
    )
    config = replace(
        HarnessConfig.from_env(),
        provider="bedrock",
        bedrock_model_id=(
            "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/ybl9l7bkcovm"
        ),
        bedrock_provider="openai",
        aws_region="us-east-1",
    )

    create_chat_model(config)

    assert captured == {
        "model": (
            "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/ybl9l7bkcovm"
        ),
        "region_name": "us-east-1",
        "provider": "openai",
    }


def test_query_only_prompt_has_no_nonce() -> None:
    prompt = _build_query_only_prompt("SELECT 1", ["Policy"])

    assert prompt.startswith("You are validating whether query output would violate")
    assert "nonce" not in prompt.lower()


def test_query_results_prompt_has_no_nonce() -> None:
    prompt = _build_query_results_prompt("SELECT 1", ["Policy"], "[]")

    assert prompt.startswith("You are validating whether query output violates")
    assert "nonce" not in prompt.lower()


def test_openai_bedrock_request_disables_prompt_caching() -> None:
    request = build_openai_invoke_request("profile", "benchmark prompt")
    body = json.loads(request["body"])

    assert body["prompt_cache_options"] == {"mode": "explicit"}
    assert body["messages"][0]["content"] == [
        {
            "type": "text",
            "text": "Evaluate this independent benchmark case.\n",
            "prompt_cache_breakpoint": {"mode": "explicit"},
        },
        {"type": "text", "text": "benchmark prompt"},
    ]


def test_llm_validation_builds_rust_remove_policies() -> None:
    policies = _build_policies(2)

    assert len(policies) == 2
    assert all(isinstance(policy, Policy) for policy in policies)
    assert all(policy.on_fail is Resolution.REMOVE for policy in policies)


def test_llm_validation_clears_multi_source_rust_policies() -> None:
    raw_conn = duckdb.connect(":memory:")
    raw_conn.execute("CREATE TABLE lineitem (l_quantity INTEGER)")
    raw_conn.execute("CREATE TABLE part (p_partkey INTEGER)")
    strategy = LLMValidationStrategy()
    strategy.dfc_rewriter = Connection(DuckDBAdapter(raw_conn))

    strategy._clear_and_register_policies(4)
    strategy._clear_and_register_policies(1)

    policies = strategy.dfc_rewriter.policies()
    assert [(policy.sources, policy.constraint) for policy in policies] == [
        (["lineitem"], "sum(lineitem.l_quantity - 30) <= 0")
    ]


def test_rust_dfc_detects_a_removed_query_result() -> None:
    raw_conn = duckdb.connect(":memory:")
    raw_conn.execute("CREATE TABLE lineitem (l_quantity INTEGER)")
    raw_conn.execute("INSERT INTO lineitem VALUES (40), (50)")
    strategy = LLMValidationStrategy()
    strategy.no_policy_conn = raw_conn
    strategy.dfc_conn = raw_conn
    strategy.dfc_rewriter = Connection(DuckDBAdapter(raw_conn))
    strategy.query_result_cache = {}
    strategy.query_result_counts = {}

    violation, _, _, policy_count = strategy._measure_dfc(
        1,
        1,
        "SELECT avg(l_quantity) AS avg_quantity FROM lineitem",
    )

    assert violation is True
    assert policy_count == 1


def test_validation_settings_can_select_one_paid_input() -> None:
    settings = build_validation_settings(
        [1],
        [1],
        ["astra"],
        [PROMPT_MODE_QUERY_RESULTS],
    )

    assert settings == [
        (1, 1, "dfc_1phase"),
        (1, 1, "astra_query_results"),
    ]


def test_default_prompt_modes_include_figure_and_footnote() -> None:
    assert DEFAULT_PROMPT_MODES == [PROMPT_MODE_QUERY_ONLY, PROMPT_MODE_QUERY_RESULTS]


def test_response_usage_reads_langchain_token_metadata() -> None:
    response = SimpleNamespace(
        usage_metadata={
            "input_tokens": 120,
            "output_tokens": 30,
            "input_token_details": {"cache_read": 4, "cache_creation": 6},
        }
    )

    assert _response_usage(response) == (120, 30, 4, 6)


def test_validation_settings_support_both_prompt_modes() -> None:
    settings = build_validation_settings(
        [1],
        [1],
        ["astra", "sol_6_1"],
        [PROMPT_MODE_QUERY_ONLY, PROMPT_MODE_QUERY_RESULTS],
    )

    assert settings == [
        (1, 1, "dfc_1phase"),
        (1, 1, "astra_query_only"),
        (1, 1, "astra_query_results"),
        (1, 1, "sol_6_1_query_only"),
        (1, 1, "sol_6_1_query_results"),
    ]
