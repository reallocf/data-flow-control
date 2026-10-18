"""Run the table/view/expanded comparison through the Rust-backed DFC engine."""

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiment_harness import ExperimentConfig, ExperimentRunner

from vldb_experiments.strategies.regulation_z_view_benchmark import ViewRegulationZStrategy


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--num-accounts", type=int, default=100)
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--warmups", type=int, default=0)
    parser.add_argument("--no-ties", action="store_true")
    parser.add_argument(
        "--output", type=Path, default=Path("results/regulation_z_dfc/view_comparison")
    )
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    ExperimentRunner(
        ViewRegulationZStrategy(),
        ExperimentConfig(
            num_executions=args.runs,
            num_warmup_runs=args.warmups,
            database_config={"database": ":memory:"},
            strategy_config={
                "num_accounts": args.num_accounts,
                "no_ties": args.no_ties,
                "output": str(args.output),
            },
            output_dir=str(args.output),
            output_filename="comparison.csv",
            verbose=True,
        ),
    ).run()


if __name__ == "__main__":
    main()
