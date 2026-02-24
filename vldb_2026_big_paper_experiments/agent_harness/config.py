"""Environment-driven configuration for the agent harness."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path

DEFAULT_SYSTEM_PROMPT = (
    "You are a SQL agent. Use the execute_sql tool to run SQL. "
    "Do not fabricate query results. "
    "Every SQL execution goes through SQLRewriter 1Phase."
)


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class HarnessConfig:
    """Configuration loaded from environment variables."""

    provider: str
    openai_model: str
    openai_api_key: str | None
    bedrock_model_id: str
    aws_region: str | None
    aws_profile: str | None
    db_path: str
    max_result_rows: int
    verbose: bool
    system_prompt: str
    dfc_policy: str | None
    dfc_policy_file: str | None

    @classmethod
    def from_env(cls) -> HarnessConfig:
        return cls(
            provider=os.getenv("AGENT_PROVIDER", "openai").strip().lower(),
            openai_model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
            openai_api_key=os.getenv("OPENAI_API_KEY"),
            bedrock_model_id=os.getenv(
                "BEDROCK_MODEL_ID",
                "anthropic.claude-3-5-sonnet-20241022-v2:0",
            ),
            aws_region=os.getenv("AWS_REGION"),
            aws_profile=os.getenv("AWS_PROFILE"),
            db_path=os.getenv("AGENT_DB_PATH", ":memory:"),
            max_result_rows=int(os.getenv("AGENT_MAX_RESULT_ROWS", "100")),
            verbose=_env_bool("AGENT_VERBOSE", default=True),
            system_prompt=os.getenv("AGENT_SYSTEM_PROMPT", DEFAULT_SYSTEM_PROMPT),
            dfc_policy=os.getenv("AGENT_DFC_POLICY"),
            dfc_policy_file=os.getenv("AGENT_DFC_POLICY_FILE"),
        )

    def validate(self) -> None:
        if self.provider not in {"openai", "bedrock"}:
            raise ValueError("AGENT_PROVIDER must be one of: openai, bedrock")
        if self.provider == "openai" and not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY is required when AGENT_PROVIDER=openai")
        if self.provider == "bedrock" and not self.aws_region:
            raise ValueError("AWS_REGION is required when AGENT_PROVIDER=bedrock")
        if self.max_result_rows <= 0:
            raise ValueError("AGENT_MAX_RESULT_ROWS must be > 0")
        if not self.system_prompt.strip():
            raise ValueError("AGENT_SYSTEM_PROMPT must not be empty")
        if self.dfc_policy_file:
            policy_file = Path(self.dfc_policy_file)
            if not policy_file.exists():
                raise ValueError(f"AGENT_DFC_POLICY_FILE does not exist: {policy_file}")
