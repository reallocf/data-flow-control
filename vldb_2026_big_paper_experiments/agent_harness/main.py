"""LangChain agent harness with a single execute_sql tool via SQLRewriter (1Phase)."""

from __future__ import annotations

import argparse
import atexit
from pathlib import Path
import sys
from typing import Any

from .agent import build_agent, run_single_turn
from .config import HarnessConfig


def register_configured_policies(
    sql_harness: Any,
    config: HarnessConfig,
    cli_policies: list[str] | None,
    cli_policy_file: str | None,
) -> int:
    """Register policies configured via env and/or CLI."""
    registered = 0

    if config.dfc_policy:
        registered += sql_harness.register_policy_strings([config.dfc_policy])
    if config.dfc_policy_file:
        registered += sql_harness.register_policy_file(config.dfc_policy_file)

    if cli_policies:
        registered += sql_harness.register_policy_strings(cli_policies)
    if cli_policy_file:
        policy_file = Path(cli_policy_file)
        if not policy_file.exists():
            raise ValueError(f"--policy-file does not exist: {policy_file}")
        registered += sql_harness.register_policy_file(str(policy_file))

    return registered


def main() -> int:
    parser = argparse.ArgumentParser(description="Run SQL agent harness (OpenAI or Bedrock).")
    parser.add_argument(
        "--question",
        default=None,
        help="Single question to run. If omitted, starts interactive REPL.",
    )
    parser.add_argument(
        "--policy",
        action="append",
        default=[],
        help="Policy string to register on startup. Repeat for multiple policies.",
    )
    parser.add_argument(
        "--policy-file",
        default=None,
        help="Path to a file with one policy string per line.",
    )
    args = parser.parse_args()

    config = HarnessConfig.from_env()
    config.validate()

    agent, sql_harness = build_agent(config)
    atexit.register(sql_harness.close)
    registered_count = register_configured_policies(
        sql_harness=sql_harness,
        config=config,
        cli_policies=args.policy,
        cli_policy_file=args.policy_file,
    )

    chat_history: list[Any] = []

    if args.question:
        print(run_single_turn(agent, args.question, chat_history))
        return 0

    print(f"Agent harness started ({registered_count} policy/policies registered). Type 'exit' to quit.")
    while True:
        try:
            user_input = input("> ").strip()
        except EOFError:
            break
        if user_input.lower() in {"exit", "quit"}:
            break
        if not user_input:
            continue
        output = run_single_turn(agent, user_input, chat_history)
        print(output)

    return 0


if __name__ == "__main__":
    sys.exit(main())
