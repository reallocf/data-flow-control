#!/usr/bin/env python3
"""Run JOIN->GROUP_BY microbenchmark policy-count experiment."""

import argparse
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from experiment_harness import ExperimentConfig, ExperimentRunner  # noqa: E402

from vldb_experiments import MicrobenchmarkPolicyCountStrategy  # noqa: E402

DEFAULT_POLICY_COUNTS = [1, 10, 100, 1000]
DEFAULT_JOIN_COUNTS = [1]
DEFAULT_WARMUP_PER_SETTING = 1
DEFAULT_RUNS_PER_SETTING = 5
DEFAULT_NUM_ROWS = 1_000


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run JOIN->GROUP_BY microbenchmark policy-count experiment."
    )
    parser.add_argument(
        "--policy-counts",
        type=int,
        nargs="+",
        default=DEFAULT_POLICY_COUNTS,
        help="Policy counts to test (default: 1 10 100 1000)",
    )
    parser.add_argument(
        "--join-counts",
        type=int,
        nargs="+",
        default=DEFAULT_JOIN_COUNTS,
        help="Join counts to test (default: 1)",
    )
    parser.add_argument(
        "--runs-per-setting",
        type=int,
        default=DEFAULT_RUNS_PER_SETTING,
        help="Measured runs per (join_count, policy_count) setting (default: 5)",
    )
    parser.add_argument(
        "--warmup-per-setting",
        type=int,
        default=DEFAULT_WARMUP_PER_SETTING,
        help="Warmup runs per (join_count, policy_count) setting (default: 1)",
    )
    parser.add_argument(
        "--num-rows",
        type=int,
        default=DEFAULT_NUM_ROWS,
        help="Row count for join-group-by data (default: 1000)",
    )
    parser.add_argument(
        "--output-filename",
        default="microbenchmark_join_group_by_policy_count.csv",
        help="CSV output filename (default: microbenchmark_join_group_by_policy_count.csv)",
    )
    args = parser.parse_args()

    policy_counts = args.policy_counts
    join_counts = args.join_counts
    runs_per_setting = args.runs_per_setting
    warmup_per_setting = args.warmup_per_setting
    total_settings = len(policy_counts) * len(join_counts)
    num_executions = total_settings * runs_per_setting

    print("Running JOIN->GROUP_BY microbenchmark policy-count experiment:")
    print(f"  Join counts: {join_counts}")
    print(f"  Policy counts: {policy_counts}")
    print(f"  Settings: {total_settings} (join_count x policy_count)")
    print(f"  Warmup runs per setting: {warmup_per_setting}")
    print(f"  Measured runs: {num_executions} ({runs_per_setting} per setting)")
    print(f"  Rows: {args.num_rows}")
    print("  Approaches: DFC, Logical, Physical")

    config = ExperimentConfig(
        num_executions=num_executions,
        num_warmup_runs=0,
        warmup_mode="per_setting",
        warmup_runs_per_setting=warmup_per_setting,
        database_config={"database": ":memory:"},
        strategy_config={
            "policy_counts": policy_counts,
            "join_counts": join_counts,
            "warmup_per_setting": warmup_per_setting,
            "runs_per_setting": runs_per_setting,
            "num_rows": args.num_rows,
        },
        output_dir="./results",
        output_filename=args.output_filename,
        verbose=True,
    )

    strategy = MicrobenchmarkPolicyCountStrategy()
    runner = ExperimentRunner(strategy, config)
    print("Starting experiments...", flush=True)
    runner.run()

    print("\nExperiments completed!")
    print(f"Results saved to: {config.output_dir}/{config.output_filename}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
