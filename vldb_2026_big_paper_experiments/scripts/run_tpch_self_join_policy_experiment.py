#!/usr/bin/env python3
"""Run the TPC-H Q01 self-join alias-policy experiment."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root / "src" / "vldb_experiments" / "strategies"))

from experiment_harness import ExperimentConfig, ExperimentRunner  # noqa: E402
from tpch_self_join_policy_queries import DEFAULT_SELF_JOIN_COUNTS  # noqa: E402
from tpch_self_join_policy_strategy import (  # noqa: E402
    DEFAULT_RUNS_PER_SETTING,
    DEFAULT_WARMUP_PER_SETTING,
    TPCHSelfJoinPolicyStrategy,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the TPC-H self-join alias-policy experiment.")
    parser.add_argument(
        "--sf",
        type=float,
        default=1,
        help="TPC-H scale factor to run (default: 1)",
    )
    parser.add_argument(
        "--self-join-counts",
        type=int,
        nargs="+",
        default=DEFAULT_SELF_JOIN_COUNTS,
        help="Self-join counts to test (default: 1 10 100 1000)",
    )
    parser.add_argument(
        "--runs-per-setting",
        type=int,
        default=DEFAULT_RUNS_PER_SETTING,
        help="Number of measured runs per self-join count (default: 5)",
    )
    parser.add_argument(
        "--warmup-per-setting",
        type=int,
        default=DEFAULT_WARMUP_PER_SETTING,
        help="Number of warmup runs per self-join count (default: 1)",
    )
    args = parser.parse_args()

    self_join_counts = args.self_join_counts
    runs_per_setting = args.runs_per_setting
    warmup_per_setting = args.warmup_per_setting

    num_executions = len(self_join_counts) * runs_per_setting

    print("Running TPC-H Q01 self-join alias-policy experiment:")
    print(f"  Scale factor: {args.sf}")
    print(f"  Self-join counts: {self_join_counts}")
    print(f"  Warmup runs per setting: {warmup_per_setting}")
    print(f"  Measured runs: {num_executions} ({runs_per_setting} per self-join count)")
    print("  Approaches: No Policy, 1Phase, 1Phase Optimized")

    config = ExperimentConfig(
        num_executions=num_executions,
        num_warmup_runs=0,
        warmup_mode="per_setting",
        warmup_runs_per_setting=warmup_per_setting,
        database_config={"database": ":memory:"},
        strategy_config={
            "tpch_sf": args.sf,
            "self_join_counts": self_join_counts,
            "warmup_per_setting": warmup_per_setting,
            "runs_per_setting": runs_per_setting,
        },
        output_dir="./results",
        output_filename=f"tpch_q01_self_join_policy_sf{args.sf}.csv",
        verbose=True,
    )

    runner = ExperimentRunner(TPCHSelfJoinPolicyStrategy(), config)
    print("Starting experiments...", flush=True)
    runner.run()
    print("\nExperiments completed!")
    print(f"Results saved to: {config.output_dir}/{config.output_filename}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
