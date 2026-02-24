"""LLM factory for OpenAI and AWS Bedrock LangChain chat models."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import HarnessConfig


def create_chat_model(config: HarnessConfig):
    """Create a LangChain chat model from provider config."""
    if config.provider == "openai":
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(
            model=config.openai_model,
            api_key=config.openai_api_key,
            temperature=1.0,
        )

    if config.provider == "bedrock":
        from langchain_aws import ChatBedrockConverse

        if config.aws_profile:
            os.environ["AWS_PROFILE"] = config.aws_profile

        kwargs = {
            "model": config.bedrock_model_id,
            "region_name": config.aws_region,
            "temperature": 1.0,
        }
        if config.bedrock_model_id.startswith("arn:"):
            # Inference profile ARNs require an explicit provider hint.
            kwargs["provider"] = os.getenv("BEDROCK_PROVIDER", "anthropic")

        return ChatBedrockConverse(**kwargs)

    raise ValueError(f"Unsupported provider: {config.provider}")
