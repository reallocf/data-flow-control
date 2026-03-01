#!/usr/bin/env python3
"""Run the state-transition UPDATE experiment with 1Phase and GPT gating."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from experiment_harness import ExperimentConfig, ExperimentRunner  # noqa: E402

from vldb_experiments.strategies.state_transition_llm_strategy import (  # noqa: E402
    DEFAULT_GPT_MODEL,
    StateTransitionLLMStrategy,
)
from vldb_experiments.strategies.state_transition_strategy import (  # noqa: E402
    DEFAULT_MEASURED_RUNS,
    DEFAULT_NUM_ROWS,
    DEFAULT_NUM_UPDATES,
    DEFAULT_VALID_RATIO,
    DEFAULT_WARMUP_RUNS,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the state-transition UPDATE experiment with 1Phase and GPT gating.")
    parser.add_argument("--num-rows", type=int, default=DEFAULT_NUM_ROWS)
    parser.add_argument("--num-updates", type=int, default=DEFAULT_NUM_UPDATES)
    parser.add_argument("--valid-ratio", type=float, default=DEFAULT_VALID_RATIO)
    parser.add_argument("--gpt-model", type=str, default=DEFAULT_GPT_MODEL)
    parser.add_argument("--runs", type=int, default=DEFAULT_MEASURED_RUNS)
    parser.add_argument("--warmups", type=int, default=DEFAULT_WARMUP_RUNS)
    args = parser.parse_args()

    print("Running state-transition UPDATE experiment with 1Phase and GPT gating:")
    print(f"  Rows: {args.num_rows}")
    print(f"  Updates: {args.num_updates}")
    print(f"  Valid ratio target: {args.valid_ratio}")
    print(f"  GPT model: {args.gpt_model}")
    print(f"  Warmup runs: {args.warmups}")
    print(f"  Measured runs: {args.runs}")
    print("  Approaches: No Policy, 1Phase, GPT 5.2 gate")

    config = ExperimentConfig(
        num_executions=args.runs,
        num_warmup_runs=args.warmups,
        database_config={"database": ":memory:"},
        strategy_config={
            "num_rows": args.num_rows,
            "num_updates": args.num_updates,
            "valid_ratio": args.valid_ratio,
            "gpt_model": args.gpt_model,
        },
        output_dir="./results",
        output_filename="state_transition_llm_results.csv",
        verbose=True,
    )

    runner = ExperimentRunner(StateTransitionLLMStrategy(), config)
    print("Starting experiments...", flush=True)
    runner.run()
    print("\nExperiments completed!")
    print(f"Results saved to: {config.output_dir}/{config.output_filename}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
