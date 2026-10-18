"""Export expanded policies and compare both DFC-only approaches on v3 data."""

import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from experiment_harness import ExperimentConfig, ExperimentRunner

from vldb_experiments.strategies.regulation_z_expanded_benchmark import (
    ExpandedRegulationZStrategy,
    audit_expanded_policies,
    export_expanded_policies,
)
from vldb_experiments.strategies.regulation_z_payment_allocation_benchmark import (
    generate_payment_allocation_workload,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--num-accounts", type=int, default=100)
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--no-ties", action="store_true")
    parser.add_argument("--audit-policies", action="store_true")
    parser.add_argument("--output", type=Path, default=Path("results/regulation_z_dfc/expanded"))
    args = parser.parse_args()
    export_expanded_policies(args.output)
    mode = args.output / ("no_ties" if args.no_ties else "ties")
    mode.mkdir(exist_ok=True)
    config = ExperimentConfig(
        num_executions=args.runs,
        num_warmup_runs=args.warmups,
        database_config={"database": ":memory:"},
        strategy_config={
            "num_accounts": args.num_accounts,
            "no_ties": args.no_ties,
            "output": str(mode),
        },
        output_dir=str(mode),
        output_filename="comparison.csv",
        verbose=True,
    )
    ExperimentRunner(ExpandedRegulationZStrategy(), config).run()
    if args.audit_policies:
        workload = generate_payment_allocation_workload(
            num_accounts=args.num_accounts,
            num_updates=args.num_accounts,
            no_ties=args.no_ties,
        )
        audit_expanded_policies(workload, mode / "policy-audit.csv")


if __name__ == "__main__":
    main()
