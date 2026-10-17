"""Tests for passthrough reasoning effort labels."""

from __future__ import annotations

import pytest

from oss_experiments.llm_clients import (
    SUPPORTED_REASONING_EFFORTS,
    build_bedrock_request,
    build_openai_request,
    normalize_reasoning_effort,
)


@pytest.mark.parametrize("effort", SUPPORTED_REASONING_EFFORTS)
def test_reasoning_effort_passthrough_openai(effort: str) -> None:
    request = build_openai_request(
        model="gpt-5.5",
        prompt="Run nonce: n\n{}",
        nonce="n",
        requested_reasoning_effort=effort,
    )
    assert request["reasoning"] == {"effort": effort}


@pytest.mark.parametrize("effort", SUPPORTED_REASONING_EFFORTS)
def test_reasoning_effort_passthrough_bedrock(effort: str) -> None:
    request = build_bedrock_request(
        model_id="anthropic.claude-opus-4-8",
        prompt="Run nonce: n\n{}",
        requested_reasoning_effort=effort,
    )
    assert request["additionalModelRequestFields"]["output_config"] == {"effort": effort}


def test_normalize_rejects_legacy_max_label() -> None:
    with pytest.raises(ValueError, match="unsupported reasoning effort"):
        normalize_reasoning_effort("max")
