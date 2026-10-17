"""Tests for prompt cache control request builders."""

from __future__ import annotations

import json

from oss_experiments.llm_clients import (
    DEFAULT_BEDROCK_MAX_TOKENS,
    build_bedrock_request,
    build_openai_request,
)
from oss_experiments.prompts import build_llm_prompt


def test_openai_request_nonce_first_in_prompt():
    nonce = "nonce-abc-123"
    prompt = build_llm_prompt(
        nonce=nonce,
        query_set="1_row_queries",
        query_id="q00",
        batch_id=0,
        rows_selected=1,
        sql="SELECT 1",
        policy_descriptions=["policy one"],
        included_context="query_and_results",
        expense_columns=["id"],
        expense_rows=[(1,)],
    )
    request = build_openai_request(
        model="gpt-5.5",
        prompt=prompt,
        nonce=nonce,
        requested_reasoning_effort="medium",
    )
    assert prompt.startswith(f"Run nonce: {nonce}")
    assert request["prompt_cache_key"] == nonce
    assert request["reasoning"] == {"effort": "medium"}
    assert request["input"] == prompt


def test_bedrock_request_has_no_cache_point():
    nonce = "nonce-bedrock-456"
    prompt = build_llm_prompt(
        nonce=nonce,
        query_set="100_row_queries",
        query_id="q01",
        batch_id=1,
        rows_selected=100,
        sql="SELECT 1",
        policy_descriptions=["policy one"],
        included_context="query_and_results_and_input",
        expense_columns=["id"],
        expense_rows=[(1,)],
        receipt_columns=["id"],
        receipt_rows=[(1,)],
    )
    request = build_bedrock_request(
        model_id="anthropic.claude-opus-4-8",
        prompt=prompt,
        requested_reasoning_effort="xhigh",
    )
    serialized = json.dumps(request)
    assert "cachePoint" not in serialized
    assert "cache_control" not in serialized
    assert request["additionalModelRequestFields"]["thinking"] == {"type": "adaptive"}
    assert request["additionalModelRequestFields"]["output_config"] == {"effort": "xhigh"}
    assert request["inferenceConfig"]["maxTokens"] == DEFAULT_BEDROCK_MAX_TOKENS
    assert "budget_tokens" not in serialized
    assert '"enabled"' not in serialized
    user_content = request["messages"][0]["content"][0]["text"]
    assert user_content.startswith(f"Run nonce: {nonce}")


def test_bedrock_request_max_tokens_can_be_overridden():
    request = build_bedrock_request(
        model_id="anthropic.claude-opus-4-8",
        prompt="Run nonce: nonce\nReturn JSON only.",
        requested_reasoning_effort="xhigh",
        max_tokens=16000,
    )
    assert request["inferenceConfig"]["maxTokens"] == 16000
