"""LangChain agent construction and execution helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from langchain.agents import create_agent
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage

from .llm_factory import create_chat_model
from .sql_tool import SQLExecutionHarness, make_execute_sql_tool

if TYPE_CHECKING:
    from .config import HarnessConfig


@dataclass(frozen=True)
class AgentRunStats:
    """Telemetry captured during one agent loop execution."""

    llm_turns: int
    chars_sent_to_llm: int


def _message_content_len(content: Any) -> int:
    if isinstance(content, str):
        return len(content)
    if isinstance(content, list):
        total = 0
        for item in content:
            if isinstance(item, str):
                total += len(item)
            elif isinstance(item, dict):
                for value in item.values():
                    if isinstance(value, str):
                        total += len(value)
        return total
    return len(str(content))


class AgentTelemetryCallback(BaseCallbackHandler):
    """Collect simple LLM call telemetry for agent loops."""

    def __init__(self) -> None:
        super().__init__()
        self.llm_turns = 0
        self.chars_sent_to_llm = 0

    def on_chat_model_start(self, _serialized: dict, messages: list[list[BaseMessage]], **_kwargs: Any) -> Any:
        self.llm_turns += len(messages)
        for message_batch in messages:
            for message in message_batch:
                self.chars_sent_to_llm += _message_content_len(getattr(message, "content", ""))
        return None

    def on_llm_start(self, _serialized: dict, prompts: list[str], **_kwargs: Any) -> Any:
        self.llm_turns += len(prompts)
        self.chars_sent_to_llm += sum(len(prompt) for prompt in prompts)
        return None


def build_agent(config: HarnessConfig) -> tuple[Any, SQLExecutionHarness]:
    """Construct the LangChain agent and SQL harness."""
    llm = create_chat_model(config)
    sql_harness = SQLExecutionHarness(
        db_path=config.db_path,
        max_result_rows=config.max_result_rows,
    )
    execute_sql = make_execute_sql_tool(sql_harness)

    agent = create_agent(
        model=llm,
        tools=[execute_sql],
        system_prompt=config.system_prompt,
        debug=config.verbose,
    )
    return agent, sql_harness


def _message_text(message: BaseMessage) -> str:
    """Convert a LangChain message content value into plain text."""
    content = getattr(message, "content", "")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                text = item.get("text")
                if text:
                    parts.append(str(text))
        return "\n".join(parts).strip()
    return str(content)


def run_agent_loop(
    agent: Any,
    user_input: str,
    chat_history: list[BaseMessage],
    max_iterations: int = 25,
    return_stats: bool = False,
) -> str | tuple[str, AgentRunStats]:
    """Run the agent loop to completion and return the final assistant text."""
    telemetry = AgentTelemetryCallback()
    input_messages = [*chat_history, HumanMessage(content=user_input)]
    result = agent.invoke(
        {"messages": input_messages},
        config={"recursion_limit": max_iterations, "callbacks": [telemetry]},
    )
    messages = result.get("messages", [])

    output = ""
    if messages:
        output = _message_text(messages[-1])

    chat_history.clear()
    for message in messages:
        if isinstance(message, BaseMessage):
            chat_history.append(message)
    if not chat_history or not isinstance(chat_history[-1], AIMessage):
        chat_history.append(HumanMessage(content=user_input))
        chat_history.append(AIMessage(content=output))
    if return_stats:
        return output, AgentRunStats(
            llm_turns=telemetry.llm_turns,
            chars_sent_to_llm=telemetry.chars_sent_to_llm,
        )
    return output


def run_single_turn(agent: Any, user_input: str, chat_history: list[BaseMessage]) -> str:
    """Run one user turn through the agent loop."""
    return run_agent_loop(agent=agent, user_input=user_input, chat_history=chat_history)
