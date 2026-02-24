#!/usr/bin/env python3
"""Run the 1Phase vs 2Phase phase-competition microbenchmark experiment."""

import argparse
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from experiment_harness import ExperimentConfig, ExperimentRunner  # noqa: E402

from vldb_experiments import MicrobenchmarkPhaseCompetitionStrategy  # noqa: E402

DEFAULT_ROW_COUNTS = [10000]
DEFAULT_JOIN_FANOUTS = [2, 4, 8, 16, 32, 64]
DEFAULT_BASE_QUERY_COLUMN_COUNTS = [128]
DEFAULT_POLICY_COLUMN_COUNTS = [2, 4, 8, 16, 32, 64, 128, 256, 512]
DEFAULT_WARMUP_PER_SETTING = 1
DEFAULT_RUNS_PER_SETTING = 5
DEFAULT_OUTPUT_FILENAME = "microbenchmark_phase_competition.csv"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run phase-competition microbenchmark (1Phase vs 2Phase)."
    )
    parser.add_argument(
        "--row-counts",
        type=int,
        nargs="+",
        default=DEFAULT_ROW_COUNTS,
        help="Row counts to test (default: 10000)",
    )
    parser.add_argument(
        "--policy-column-counts",
        type=int,
        nargs="+",
        default=DEFAULT_POLICY_COLUMN_COUNTS,
        help="Policy column counts to test (default: 2 4 8 16 32 64 128 256 512)",
    )
    parser.add_argument(
        "--join-fanouts",
        type=int,
        nargs="+",
        default=DEFAULT_JOIN_FANOUTS,
        help="Join fanout values for join_data (default: 2 4 8 16 32 64)",
    )
    parser.add_argument(
        "--base-query-column-counts",
        type=int,
        nargs="+",
        default=DEFAULT_BASE_QUERY_COLUMN_COUNTS,
        help="Base query aggregate column counts to test (default: 128)",
    )
    parser.add_argument(
        "--runs-per-setting",
        type=int,
        default=DEFAULT_RUNS_PER_SETTING,
        help="Measured runs per setting (default: 5)",
    )
    parser.add_argument(
        "--warmup-per-setting",
        type=int,
        default=DEFAULT_WARMUP_PER_SETTING,
        help="Warmup runs per setting (default: 1)",
    )
    parser.add_argument(
        "--output-filename",
        default=DEFAULT_OUTPUT_FILENAME,
        help=f"CSV output filename (default: {DEFAULT_OUTPUT_FILENAME})",
    )
    args = parser.parse_args()

    row_counts = args.row_counts
    join_fanouts = args.join_fanouts
    base_query_column_counts = args.base_query_column_counts
    policy_column_counts = args.policy_column_counts
    runs_per_setting = args.runs_per_setting
    warmup_per_setting = args.warmup_per_setting
    total_settings = (
        len(row_counts)
        * len(join_fanouts)
        * len(base_query_column_counts)
        * len(policy_column_counts)
    )
    num_executions = total_settings * runs_per_setting

    print("Running phase-competition microbenchmark experiment:")
    print(f"  Row counts: {row_counts}")
    print(f"  Join fanouts: {join_fanouts}")
    print(f"  Base query column counts: {base_query_column_counts}")
    print(f"  Policy column counts: {policy_column_counts}")
    print("  Fixed table width: 4096 columns")
    print(f"  Settings: {total_settings}")
    print(f"  Warmup runs per setting: {warmup_per_setting}")
    print(f"  Measured runs: {num_executions} ({runs_per_setting} per setting)")
    print("  Approaches: 1Phase, 2Phase")

    config = ExperimentConfig(
        num_executions=num_executions,
        num_warmup_runs=0,
        warmup_mode="per_setting",
        warmup_runs_per_setting=warmup_per_setting,
        database_config={"database": ":memory:"},
        strategy_config={
            "row_counts": row_counts,
            "join_fanouts": join_fanouts,
            "base_aggregate_columns_list": base_query_column_counts,
            "policy_column_counts": policy_column_counts,
            "warmup_per_setting": warmup_per_setting,
            "runs_per_setting": runs_per_setting,
            "total_columns": 4096,
        },
        output_dir="./results",
        output_filename=args.output_filename,
        verbose=True,
    )

    strategy = MicrobenchmarkPhaseCompetitionStrategy()
    runner = ExperimentRunner(strategy, config)
    print("Starting experiments...", flush=True)
    runner.run()
    print("\nExperiments completed!")
    print(f"Results saved to: {config.output_dir}/{config.output_filename}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
