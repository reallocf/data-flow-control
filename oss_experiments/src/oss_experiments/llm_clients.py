"""Direct OpenAI and Bedrock clients with cache-audit metadata."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from oss_experiments.prompts import parse_violation_prediction

APPROACH_DFC = "dfc"
APPROACH_LLM = "llm"

DEFAULT_GPT_MODEL = "gpt-5.5"
DEFAULT_CLAUDE_MODEL = (
    "arn:aws:bedrock:us-east-1:920736616554:application-inference-profile/e7ygwg2bquz8"
)
DEFAULT_BEDROCK_MAX_TOKENS = 8192

SUPPORTED_REASONING_EFFORTS = ("low", "medium", "high", "xhigh")


@dataclass
class LLMResponse:
    raw_response: str
    predicted_violation: bool | None
    parse_ok: bool
    runtime_ms: float
    input_tokens: int
    output_tokens: int
    cached_input_tokens: int
    cache_write_input_tokens: int
    cache_read_input_tokens: int
    cache_contaminated: bool
    request_id: str
    requested_reasoning_effort: str
    effective_reasoning_effort: str
    temperature_effective: str
    prompt_chars: int


def normalize_reasoning_effort(effort: str) -> str:
    normalized = effort.strip().lower()
    if normalized not in SUPPORTED_REASONING_EFFORTS:
        raise ValueError(
            f"unsupported reasoning effort: {effort!r}; "
            f"expected one of {SUPPORTED_REASONING_EFFORTS}"
        )
    return normalized


def build_openai_request(
    *,
    model: str,
    prompt: str,
    nonce: str,
    requested_reasoning_effort: str,
) -> dict[str, Any]:
    effort = normalize_reasoning_effort(requested_reasoning_effort)
    return {
        "model": model,
        "input": prompt,
        "reasoning": {"effort": effort},
        "prompt_cache_key": nonce,
        "text": {"format": {"type": "json_object"}},
    }


def build_bedrock_request(
    *,
    model_id: str,
    prompt: str,
    requested_reasoning_effort: str,
    max_tokens: int = DEFAULT_BEDROCK_MAX_TOKENS,
) -> dict[str, Any]:
    effort = normalize_reasoning_effort(requested_reasoning_effort)
    return {
        "modelId": model_id,
        "messages": [
            {
                "role": "user",
                "content": [{"text": prompt}],
            }
        ],
        "inferenceConfig": {
            "maxTokens": max_tokens,
        },
        "additionalModelRequestFields": {
            "thinking": {"type": "adaptive"},
            "output_config": {"effort": effort},
        },
    }


def _extract_openai_usage(response: Any) -> dict[str, int]:
    usage = getattr(response, "usage", None)
    if usage is None and isinstance(response, dict):
        usage = response.get("usage")
    cached = 0
    prompt_tokens = 0
    output_tokens = 0
    if usage is not None:
        prompt_tokens = int(
            getattr(usage, "input_tokens", 0) or getattr(usage, "prompt_tokens", 0) or 0
        )
        output_tokens = int(
            getattr(usage, "output_tokens", 0) or getattr(usage, "completion_tokens", 0) or 0
        )
        details = getattr(usage, "prompt_tokens_details", None) or getattr(
            usage, "input_tokens_details", None
        )
        if details is not None:
            cached = int(getattr(details, "cached_tokens", 0) or 0)
        elif isinstance(usage, dict):
            details_dict = usage.get("prompt_tokens_details") or usage.get("input_tokens_details")
            if isinstance(details_dict, dict):
                cached = int(details_dict.get("cached_tokens", 0) or 0)
    return {
        "input_tokens": prompt_tokens,
        "output_tokens": output_tokens,
        "cached_input_tokens": cached,
    }


def _extract_openai_text(response: Any) -> str:
    if hasattr(response, "output_text"):
        return str(response.output_text or "")
    output = getattr(response, "output", None)
    if isinstance(output, list):
        chunks: list[str] = []
        for item in output:
            content = getattr(item, "content", None) or (
                item.get("content") if isinstance(item, dict) else None
            )
            if isinstance(content, list):
                for part in content:
                    text = getattr(part, "text", None) or (
                        part.get("text") if isinstance(part, dict) else None
                    )
                    if text:
                        chunks.append(str(text))
        return "\n".join(chunks)
    return str(response)


def _extract_bedrock_usage(response: dict[str, Any]) -> dict[str, int]:
    usage = response.get("usage") or {}
    cached_read = int(usage.get("cacheReadInputTokens") or 0)
    cached_write = int(usage.get("cacheWriteInputTokens") or 0)
    return {
        "input_tokens": int(usage.get("inputTokens") or 0),
        "output_tokens": int(usage.get("outputTokens") or 0),
        "cached_input_tokens": cached_read,
        "cache_read_input_tokens": cached_read,
        "cache_write_input_tokens": cached_write,
    }


def _extract_bedrock_text(response: dict[str, Any]) -> str:
    output = response.get("output") or {}
    message = output.get("message") or {}
    content = message.get("content") or []
    parts: list[str] = []
    for block in content:
        if isinstance(block, dict) and "text" in block:
            parts.append(str(block["text"]))
    return "\n".join(parts)


class OpenAILLMClient:
    def __init__(self, client: Any, model: str) -> None:
        self._client = client
        self.model = model

    def invoke(
        self,
        prompt: str,
        *,
        nonce: str,
        requested_reasoning_effort: str,
    ) -> LLMResponse:
        import time

        effort = normalize_reasoning_effort(requested_reasoning_effort)
        request = build_openai_request(
            model=self.model,
            prompt=prompt,
            nonce=nonce,
            requested_reasoning_effort=effort,
        )
        start = time.perf_counter()
        response = self._client.responses.create(**request)
        runtime_ms = (time.perf_counter() - start) * 1000.0
        raw = _extract_openai_text(response)
        usage = _extract_openai_usage(response)
        predicted = parse_violation_prediction(raw)
        cached = usage["cached_input_tokens"]
        return LLMResponse(
            raw_response=raw,
            predicted_violation=predicted,
            parse_ok=predicted is not None,
            runtime_ms=runtime_ms,
            input_tokens=usage["input_tokens"],
            output_tokens=usage["output_tokens"],
            cached_input_tokens=cached,
            cache_write_input_tokens=0,
            cache_read_input_tokens=cached,
            cache_contaminated=cached > 0,
            request_id=str(getattr(response, "id", "") or ""),
            requested_reasoning_effort=effort,
            effective_reasoning_effort=effort,
            temperature_effective="provider_default",
            prompt_chars=len(prompt),
        )


class BedrockLLMClient:
    def __init__(
        self,
        client: Any,
        model_id: str,
        *,
        max_tokens: int = DEFAULT_BEDROCK_MAX_TOKENS,
    ) -> None:
        self._client = client
        self.model_id = model_id
        self.max_tokens = max_tokens

    def invoke(
        self,
        prompt: str,
        *,
        requested_reasoning_effort: str,
    ) -> LLMResponse:
        import time

        effort = normalize_reasoning_effort(requested_reasoning_effort)
        request = build_bedrock_request(
            model_id=self.model_id,
            prompt=prompt,
            requested_reasoning_effort=effort,
            max_tokens=self.max_tokens,
        )
        start = time.perf_counter()
        response = self._client.converse(**request)
        runtime_ms = (time.perf_counter() - start) * 1000.0
        raw = _extract_bedrock_text(response)
        usage = _extract_bedrock_usage(response)
        predicted = parse_violation_prediction(raw)
        cache_read = usage.get("cache_read_input_tokens", 0)
        cache_write = usage.get("cache_write_input_tokens", 0)
        contaminated = cache_read > 0 or cache_write > 0
        return LLMResponse(
            raw_response=raw,
            predicted_violation=predicted,
            parse_ok=predicted is not None,
            runtime_ms=runtime_ms,
            input_tokens=usage["input_tokens"],
            output_tokens=usage["output_tokens"],
            cached_input_tokens=cache_read,
            cache_write_input_tokens=cache_write,
            cache_read_input_tokens=cache_read,
            cache_contaminated=contaminated,
            request_id=str(response.get("ResponseMetadata", {}).get("RequestId", "")),
            requested_reasoning_effort=effort,
            effective_reasoning_effort=effort,
            temperature_effective="provider_default",
            prompt_chars=len(prompt),
        )
