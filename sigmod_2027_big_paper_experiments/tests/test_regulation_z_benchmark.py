from __future__ import annotations

import csv
from io import BytesIO
import json
import threading
import time

import duckdb
from experiment_harness import ExperimentContext
import pytest
from sql_rewriter import SQLRewriter

from vldb_experiments.strategies.regulation_z_benchmark import (
    POLICY_TEXT,
    PROGRESS_FIELDNAMES,
    BedrockPolicyGuard,
    RegulationZBenchmarkStrategy,
    _parse_allow_update,
    build_bedrock_request,
    build_policy_prompt,
    build_regulation_z_policy,
    generate_underwriting_workload,
    reset_underwriting_database,
)


def test_workload_is_balanced_and_covers_violation_types() -> None:
    workload = generate_underwriting_workload(num_applications=100, num_updates=100)
    assert sum(case.is_valid for case in workload) == 50
    assert sum(not case.is_valid for case in workload) == 50
    assert {case.case_type for case in workload} == {
        "valid_approval",
        "valid_decline",
        "unverified_income",
        "omitted_obligation",
        "incorrect_payment",
        "cross_application",
        "threshold_violation",
        "unverified_income_record",
        "unverified_obligations_record",
        "product_mismatch",
        "income_application_mismatch",
        "obligations_application_mismatch",
        "loan_terms_application_mismatch",
        "nonpositive_income",
        "incorrect_dti",
        "missing_income_record",
        "application_mismatch",
    }


def test_workload_supports_single_input_smoke_run() -> None:
    workload = generate_underwriting_workload(num_applications=1, num_updates=1)

    assert len(workload) == 1
    assert workload[0].application_id == 1
    assert workload[0].case_type == "valid_approval"


def test_workload_rejects_zero_inputs() -> None:
    with pytest.raises(ValueError) as error:
        generate_underwriting_workload(num_applications=1, num_updates=0)

    assert str(error.value) == "num_updates must be at least 1"


def test_workload_independently_varies_relational_evidence() -> None:
    workload = generate_underwriting_workload(num_applications=32, num_updates=32)
    cases = {case.case_type: case for case in workload}

    assert cases["income_application_mismatch"].income_application_id < 0
    assert (
        cases["income_application_mismatch"].obligations_application_id
        == cases["income_application_mismatch"].application_id
    )
    assert cases["obligations_application_mismatch"].obligations_application_id < 0
    assert (
        cases["obligations_application_mismatch"].income_application_id
        == cases["obligations_application_mismatch"].application_id
    )
    assert cases["loan_terms_application_mismatch"].loan_terms_application_id < 0
    assert cases["product_mismatch"].standard_product_id == 99
    assert cases["unverified_income_record"].income_third_party_verified is False
    assert cases["unverified_obligations_record"].obligations_third_party_verified is False
    assert cases["nonpositive_income"].candidate_income_used == 0
    assert (
        cases["incorrect_dti"].candidate_dti
        != cases["incorrect_dti"].candidate_obligations_used
        / cases["incorrect_dti"].candidate_income_used
    )
    assert cases["application_mismatch"].application_source_ids == (
        -cases["application_mismatch"].application_id,
    )
    assert cases["missing_income_record"].income_evidence_present is False
    assert "LEFT JOIN verified_income" in cases["missing_income_record"].sql


def test_no_policy_database_executes_every_candidate_update() -> None:
    workload = generate_underwriting_workload(num_applications=30, num_updates=30)
    conn = duckdb.connect(":memory:")
    reset_underwriting_database(conn, workload)

    for case in workload:
        conn.execute(case.sql)

    outcomes = conn.execute(
        "SELECT outcome FROM underwriting_decisions ORDER BY application_id"
    ).fetchall()
    assert [outcome[0] for outcome in outcomes] == [case.candidate_outcome for case in workload]
    conn.close()


def test_policy_uses_all_relational_sources_and_remove_resolution() -> None:
    policy = build_regulation_z_policy()
    assert policy.sources == [
        "applications",
        "verified_income",
        "verified_obligations",
        "loan_terms",
        "underwriting_standards",
    ]
    assert policy.sink == "underwriting_decisions"
    assert policy.sink_alias == "d"
    assert policy.on_fail.value == "REMOVE"


def test_dfc_policy_allows_only_oracle_valid_updates() -> None:
    workload = generate_underwriting_workload(num_applications=32, num_updates=32)
    conn = duckdb.connect(":memory:")
    reset_underwriting_database(conn, workload)
    rewriter = SQLRewriter(conn=conn)
    rewriter.register_policy(build_regulation_z_policy())

    actual = []
    for case in workload:
        conn.execute(rewriter.transform_query(case.sql))
        outcome = conn.execute(
            "SELECT outcome FROM underwriting_decisions WHERE application_id = ?",
            [case.application_id],
        ).fetchone()[0]
        actual.append(outcome != "PENDING")

    assert actual == [case.is_valid for case in workload]


def test_prompt_starts_with_unique_nonce_before_policy() -> None:
    cases = generate_underwriting_workload(num_applications=10, num_updates=10)
    prompt = build_policy_prompt(cases[0], "nonce-123")
    assert POLICY_TEXT == (
        "An APPROVED mortgage decision is allowed only when all of these are true:\n"
        "1. The application, verified income, verified obligations, loan terms, and underwriting\n"
        "   standard all belong to the same application and product.\n"
        "2. income_used is positive and no greater than verified_monthly_income. Applicant-claimed\n"
        "   income is not verified income.\n"
        "3. The verified income and verified obligations rows must be third party verified.\n"
        "4. obligations_used exactly equals existing monthly debt plus the proposed mortgage payment,\n"
        "   mortgage-related obligations, and any simultaneous-loan payment.\n"
        "5. dti equals obligations_used divided by income_used. Treat the dti as matching if it's\n"
        "   within 0.000001.\n"
        "6. dti is no greater than the product's max_dti.\n"
        "A DECLINED decision is allowed. Reject an update if any approval requirement is not met."
    )
    assert prompt.startswith("Run nonce: nonce-123\nPolicy:\n")
    assert POLICY_TEXT in prompt
    assert cases[0].sql in prompt


def test_bedrock_request_uses_provider_defaults_and_no_cache_controls() -> None:
    request = build_bedrock_request("us.openai.gpt-6-astra", "Run nonce: n\nPolicy:\n...")
    assert request == {
        "modelId": "us.openai.gpt-6-astra",
        "messages": [
            {
                "role": "user",
                "content": [{"text": "Run nonce: n\nPolicy:\n..."}],
            }
        ],
    }
    serialized = json.dumps(request)
    assert "temperature" not in serialized
    assert "cachePoint" not in serialized
    assert "cache_control" not in serialized


def test_parse_allow_update_requires_named_boolean() -> None:
    assert _parse_allow_update('{"allow_update": true}') is True
    assert _parse_allow_update('{"allow_update": false}') is False
    assert _parse_allow_update("true") is None


@pytest.mark.parametrize("tag", ["reasoning", "think"])
def test_decision_parser_ignores_reasoning_and_incomplete_reasoning(tag: str) -> None:
    assert (
        _parse_allow_update(
            f'<{tag}>Consider {{"allow_update": true}}</{tag}>\n{{"allow_update": false}}'
        )
        is False
    )
    assert _parse_allow_update(f'<{tag}>{{"allow_update": true}}') is None


@pytest.mark.parametrize(
    ("model_key", "profile_id"),
    [
        ("minimax_m2_5", "anonymous-profile-03"),
        ("grok_4_7", "anonymous-profile-06"),
    ],
)
def test_native_model_uses_bedrock_request_and_final_decision(model_key, profile_id) -> None:
    from vldb_experiments.strategies.regulation_z_v2_benchmark import (
        AVAILABLE_V2_BEDROCK_MODEL_SPECS,
    )

    spec = next(s for s in AVAILABLE_V2_BEDROCK_MODEL_SPECS if s["key"] == model_key)
    raw = '<reasoning>{"allow_update": true}</reasoning>\n{"allow_update": false}'

    class FakeClient:
        def invoke_model(self, **request):
            assert request["modelId"] == (
                f"arn:aws:bedrock:us-east-1:000000000000:application-inference-profile/{profile_id}"
            )
            assert json.loads(request["body"]) == {
                "messages": [{"role": "user", "content": "Check allocation"}],
                "max_tokens": 8192,
            }
            return {
                "body": BytesIO(
                    json.dumps(
                        {
                            "choices": [{"message": {"content": raw}}],
                            "usage": {"prompt_tokens": 45, "completion_tokens": 96},
                        }
                    ).encode()
                ),
                "ResponseMetadata": {"RequestId": "minimax-test"},
            }

    result = BedrockPolicyGuard(
        FakeClient(), str(spec["model_id"]), str(spec["bedrock_provider"])
    ).invoke("Check allocation")
    assert result.allow_update is False
    assert result.raw_response == raw
    assert (result.input_tokens, result.output_tokens) == (45, 96)
    assert (result.cache_read_input_tokens, result.cache_write_input_tokens) == (0, 0)
    assert result.request_id == "minimax-test"


def test_bedrock_guard_parses_response_and_cache_counters() -> None:
    class FakeClient:
        def converse(self, **request):
            assert request["messages"][0]["content"][0]["text"].startswith("Run nonce:")
            return {
                "output": {
                    "message": {
                        "content": [{"text": '{"allow_update": true}'}],
                    }
                },
                "usage": {
                    "inputTokens": 42,
                    "outputTokens": 7,
                    "cacheReadInputTokens": 0,
                    "cacheWriteInputTokens": 0,
                },
                "ResponseMetadata": {"RequestId": "request-1"},
            }

    result = BedrockPolicyGuard(FakeClient(), "model-id").invoke("Run nonce: n\nPolicy:\n...")
    assert result.allow_update is True
    assert result.raw_response == '{"allow_update": true}'
    assert result.input_tokens == 42
    assert result.output_tokens == 7
    assert result.cache_read_input_tokens == 0
    assert result.cache_write_input_tokens == 0
    assert result.request_id == "request-1"


def test_strategy_runs_all_approaches_with_bedrock_converse_client() -> None:
    class AlwaysBlockClient:
        def converse(self, **request):
            prompt = request["messages"][0]["content"][0]["text"]
            assert prompt.startswith("Run nonce:")
            return {
                "output": {
                    "message": {
                        "content": [{"text": '{"allow_update": false}'}],
                    }
                },
                "usage": {"inputTokens": 10, "outputTokens": 5},
            }

    context = ExperimentContext(
        execution_number=1,
        strategy_config={
            "num_applications": 20,
            "num_updates": 20,
            "bedrock_client": AlwaysBlockClient(),
            "models": [
                {
                    "key": "astra",
                    "display_name": "Astra 6",
                    "model_id": "us.openai.gpt-6-astra",
                }
            ],
        },
    )
    strategy = RegulationZBenchmarkStrategy()
    strategy.setup(context)
    try:
        result = strategy.execute(context)
    finally:
        strategy.teardown(context)

    assert result.custom_metrics["dfc_accuracy"] == 1.0
    assert result.custom_metrics["no_policy_accuracy"] == 0.5
    assert result.custom_metrics["astra_accuracy"] == 0.5
    assert result.custom_metrics["astra_parse_failures"] == 0
    assert result.custom_metrics["astra_cache_contaminated"] is False


def test_strategy_flushes_incremental_progress_after_each_update(tmp_path) -> None:
    class AlwaysAllowClient:
        def converse(self, **_request):
            return {
                "output": {"message": {"content": [{"text": '{"allow_update": true}'}]}},
                "usage": {"inputTokens": 10, "outputTokens": 5},
                "ResponseMetadata": {"RequestId": "request-1"},
            }

    progress_path = tmp_path / "progress.csv"
    context = ExperimentContext(
        execution_number=1,
        strategy_config={
            "num_applications": 1,
            "num_updates": 1,
            "bedrock_client": AlwaysAllowClient(),
            "progress_path": str(progress_path),
            "models": [
                {
                    "key": "astra",
                    "display_name": "Astra 6",
                    "model_id": "profile-arn",
                }
            ],
        },
    )
    strategy = RegulationZBenchmarkStrategy()
    strategy.setup(context)
    try:
        strategy.execute(context)
        with progress_path.open(encoding="utf-8", newline="") as progress_file:
            reader = csv.DictReader(progress_file)
            rows = list(reader)
            assert reader.fieldnames == PROGRESS_FIELDNAMES
    finally:
        strategy.teardown(context)

    assert [row["approach"] for row in rows] == ["no_policy", "dfc", "astra"]
    assert [row["update_number"] for row in rows] == ["1", "1", "1"]
    assert [row["correct"] for row in rows] == ["True", "True", "True"]
    assert rows[2]["model_id"] == "profile-arn"
    assert rows[2]["input_tokens"] == "10"
    assert rows[2]["output_tokens"] == "5"
    assert rows[2]["request_id"] == "request-1"
    assert rows[2]["error"] == ""


def test_strategy_flushes_model_error_before_run_fails(tmp_path) -> None:
    class FailingClient:
        def converse(self, **_request):
            raise RuntimeError("model unavailable")

    progress_path = tmp_path / "progress.csv"
    context = ExperimentContext(
        execution_number=1,
        strategy_config={
            "num_applications": 1,
            "num_updates": 1,
            "bedrock_client": FailingClient(),
            "progress_path": str(progress_path),
            "models": [
                {
                    "key": "astra",
                    "display_name": "Astra 6",
                    "model_id": "profile-arn",
                }
            ],
        },
    )
    strategy = RegulationZBenchmarkStrategy()
    strategy.setup(context)
    try:
        with pytest.raises(RuntimeError) as error:
            strategy.execute(context)
        assert str(error.value) == "model unavailable"
        with progress_path.open(encoding="utf-8", newline="") as progress_file:
            rows = list(csv.DictReader(progress_file))
    finally:
        strategy.teardown(context)

    assert [row["approach"] for row in rows] == ["no_policy", "dfc", "astra"]
    assert rows[2]["correct"] == ""
    assert rows[2]["error"] == "model unavailable"


def test_strategy_runs_bedrock_requests_concurrently() -> None:
    class ConcurrencyTrackingClient:
        def __init__(self) -> None:
            self.active = 0
            self.peak = 0
            self.lock = threading.Lock()

        def converse(self, **_request):
            with self.lock:
                self.active += 1
                self.peak = max(self.peak, self.active)
            time.sleep(0.02)
            with self.lock:
                self.active -= 1
            return {
                "output": {"message": {"content": [{"text": '{"allow_update": true}'}]}},
                "usage": {"inputTokens": 10, "outputTokens": 5},
            }

    client = ConcurrencyTrackingClient()
    context = ExperimentContext(
        execution_number=1,
        strategy_config={
            "num_applications": 12,
            "num_updates": 12,
            "max_concurrency": 4,
            "bedrock_client": client,
            "models": [
                {
                    "key": "astra",
                    "display_name": "Astra 6",
                    "model_id": "profile-arn",
                }
            ],
        },
    )
    strategy = RegulationZBenchmarkStrategy()
    strategy.setup(context)
    try:
        result = strategy.execute(context)
    finally:
        strategy.teardown(context)

    assert client.peak == 4
    assert result.custom_metrics["astra_input_tokens"] == 120
    assert result.custom_metrics["astra_output_tokens"] == 60
    assert result.custom_metrics["astra_wall_time_ms"] < result.custom_metrics["astra_llm_time_ms"]


def test_strategy_rejects_concurrency_above_fifty() -> None:
    context = ExperimentContext(
        strategy_config={
            "num_applications": 1,
            "num_updates": 1,
            "max_concurrency": 51,
        }
    )

    with pytest.raises(ValueError) as error:
        RegulationZBenchmarkStrategy().setup(context)

    assert str(error.value) == "max_concurrency must be between 1 and 50"
