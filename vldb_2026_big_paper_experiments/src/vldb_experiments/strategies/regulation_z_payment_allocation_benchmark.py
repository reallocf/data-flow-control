"""Regulation Z v3 disputed-balance payment-allocation benchmark."""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import csv
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import random
import time
from typing import TYPE_CHECKING, Any, TextIO
import uuid

from data_flow_control import Policy, Resolution
from data_flow_control.adapters.duckdb import DuckDBAdapter
from data_flow_control.connection import Connection
import duckdb
from experiment_harness import ExperimentResult

from vldb_experiments.strategies.regulation_z_53_full_policy_catalog import (
    FULL_REGULATION_Z_53_POLICY_SPECS,
)
from vldb_experiments.strategies.regulation_z_benchmark import BedrockPolicyGuard
from vldb_experiments.strategies.regulation_z_policy_text import (
    EVIDENCE_SEMANTICS,
    policy_decision_text,
)
from vldb_experiments.strategies.regulation_z_v2_benchmark import (
    AVAILABLE_V2_BEDROCK_MODEL_SPECS,
    DEFAULT_AWS_REGION,
    DEFAULT_MAX_CONCURRENCY,
    DEFAULT_MEASURED_RUNS,
    DEFAULT_V2_BEDROCK_MODEL_SPECS,
    DEFAULT_WARMUP_RUNS,
)
from vldb_experiments.strategies.regulation_z_v2_benchmark import (
    RegulationZV2BenchmarkStrategy as _BenchmarkBase,
)
from vldb_experiments.strategies.regulation_z_v3_evidence import (
    full_evidence_for_prompt,
    reset_full_evidence,
)

if TYPE_CHECKING:
    from experiment_harness import ExperimentContext

DEFAULT_NUM_ACCOUNTS = 100
DEFAULT_NUM_UPDATES = 100
DEFAULT_WORKLOAD_SEED = 20261012
DEFAULT_V3_BEDROCK_MODEL_SPECS = [
    spec for spec in DEFAULT_V2_BEDROCK_MODEL_SPECS if spec["key"] != "haiku_5_5"
]
MATERIALIZED_VIEW_NAMES = frozenset(
    {
        "allocation_snapshot_context",
        "balance_components",
        "regz_context",
    }
)

POLICY_TEXT = (
    "Regulatory requirements for this benchmark:\n\n"
    + (
        "Retain the completed payment allocation if and only if every applicable requirement\n"
        "below is satisfied. Amounts in regz_* tables are integer cents. The minimum\n"
        "is allocated to the separate MIN lot; the proposed excess allocation is the\n"
        "proposal whose batch_id equals the transaction selected by the SQL. Both\n"
        "candidate proposals are shown. No request is elected and no correction\n"
        "completion is proposed. Request and correction requirements are conditional.\n\n"
    )
    + EVIDENCE_SEMANTICS
    + "\n"
    + "\n".join(
        f"{spec.proposition_id}. {spec.name}: {spec.explanation}"
        + (f" Assumption/limitation: {spec.limitation}" if spec.limitation else "")
        + f" Decision condition: {policy_decision_text(spec)}"
        for spec in FULL_REGULATION_Z_53_POLICY_SPECS
    )
)

PROGRESS_FIELDNAMES = [
    "timestamp",
    "execution_number",
    "is_warmup",
    "approach",
    "model_id",
    "update_number",
    "num_updates",
    "allocation_id",
    "payment_transaction_id",
    "account_id",
    "case_type",
    "expected_selected_transaction_id",
    "expected_allow",
    "actual_allow",
    "correct",
    "elapsed_ms",
    "rewrite_ms",
    "llm_ms",
    "sql_execution_ms",
    "input_tokens",
    "output_tokens",
    "cache_read_input_tokens",
    "cache_write_input_tokens",
    "parse_failure",
    "request_id",
    "error",
    "raw_response",
    "prompt",
]


@dataclass(frozen=True)
class AllocationTransaction:
    transaction_id: str
    transaction_date: date
    post_date: date
    transaction_type: str
    balance_category: str
    amount: float
    merchant_name: str | None
    is_disputed: bool
    apr_applied: float = 25.0


@dataclass(frozen=True)
class PaymentAllocationCase:
    case_number: int
    account_id: str
    statement_id: str
    allocation_id: str
    payment_transaction_id: str
    disputed_transaction_id: str
    undisputed_transaction_id: str
    expected_selected_transaction_id: str
    cycle_start: date
    cycle_end: date
    payment_date: date
    apr_applied: float
    total_payment: float
    minimum_payment_portion: float
    transactions: tuple[AllocationTransaction, ...]
    case_type: str
    is_valid: bool
    sql: str


def build_regulation_z_53_policies() -> tuple[Policy, ...]:
    """Bind the full catalog's 31 unchanged predicates to the v3 update sink."""
    return tuple(
        Policy(
            sources=list(spec.sources),
            sink="payment_allocations",
            sink_alias="allocation",
            constraint=spec.constraint,
            on_fail=Resolution.REMOVE,
            description=f"{spec.proposition_id}: {spec.name}",
        )
        for spec in FULL_REGULATION_Z_53_POLICY_SPECS
        if spec.constraint is not None
    )


def build_disputed_balance_policy() -> Policy:
    """Return P17's protection check, independent of catalog position."""
    return next(
        policy
        for policy in build_regulation_z_53_policies()
        if policy.description.startswith("P17:")
    )


def _build_allocation_sql(case: PaymentAllocationCase) -> str:
    return (
        "UPDATE payment_allocations AS allocation "
        "SET cash_advance_allocation = LEAST("
        "allocation.total_payment - allocation.minimum_payment_portion, selected.amount) "
        "FROM transactions AS selected "
        "JOIN regz_context ON regz_context.batch_id = selected.transaction_id "
        "JOIN balance_components AS component "
        "ON component.component_id = selected.transaction_id "
        "JOIN statements AS statement_context "
        "ON statement_context.statement_id = selected.statement_id "
        "JOIN allocation_snapshot_context AS snapshot "
        "ON snapshot.account_id = selected.account_id "
        "AND snapshot.statement_id = selected.statement_id "
        f"WHERE allocation.allocation_id = '{case.allocation_id}' "
        "AND allocation.account_id = selected.account_id "
        "AND selected.transaction_id = ("
        "SELECT candidate.transaction_id "
        "FROM transactions AS candidate "
        "JOIN daily_balances AS balance "
        "ON balance.account_id = candidate.account_id "
        "AND balance.statement_id = candidate.statement_id "
        "AND balance.balance_category = candidate.balance_category "
        "WHERE candidate.account_id = allocation.account_id "
        "AND candidate.statement_id = allocation.statement_id "
        "AND candidate.transaction_type = 'cash_advance' "
        f"AND balance.balance_date = DATE '{case.cycle_end.isoformat()}' "
        "ORDER BY balance.apr_applied DESC "
        "LIMIT 1)"
    )


def _base_case(
    case_number: int, *, undisputed_selected: bool, no_ties: bool = False
) -> PaymentAllocationCase:
    account_id = f"ACC{case_number:07d}"
    statement_id = f"STM{case_number:07d}"
    allocation_id = f"ALLOC{case_number:07d}"
    payment_transaction_id = f"PAY{case_number:08d}"
    charge_a = f"CHG{case_number:07d}A"
    charge_b = f"CHG{case_number:07d}B"
    disputed_transaction_id = charge_a if case_number % 2 else charge_b
    undisputed_transaction_id = charge_b if case_number % 2 else charge_a
    cycle_start = date(2027, 1, 1) + timedelta(days=(case_number - 1) * 31)
    cycle_end = cycle_start + timedelta(days=29)
    payment_date = cycle_end + timedelta(days=10)
    charges = {
        disputed_transaction_id: AllocationTransaction(
            disputed_transaction_id,
            cycle_start + timedelta(days=4),
            cycle_start + timedelta(days=5),
            "cash_advance",
            "cash_advance",
            100.0,
            "Cash Advance",
            True,
        ),
        undisputed_transaction_id: AllocationTransaction(
            undisputed_transaction_id,
            cycle_start + timedelta(days=8),
            cycle_start + timedelta(days=9),
            "cash_advance",
            "cash_advance",
            100.0,
            "Cash Advance",
            False,
        ),
    }
    selected_transaction_id = (
        undisputed_transaction_id if undisputed_selected else disputed_transaction_id
    )
    other_transaction_id = (
        disputed_transaction_id if undisputed_selected else undisputed_transaction_id
    )
    if no_ties:
        # Separate rate buckets preserve the existing category-based balance join.
        charges[other_transaction_id] = replace(
            charges[other_transaction_id],
            apr_applied=24.0,
            balance_category="cash_advance_lower",
        )
    payment = AllocationTransaction(
        payment_transaction_id,
        payment_date,
        payment_date,
        "payment",
        "purchase",
        -125.0,
        None,
        False,
    )
    case = PaymentAllocationCase(
        case_number=case_number,
        account_id=account_id,
        statement_id=statement_id,
        allocation_id=allocation_id,
        payment_transaction_id=payment_transaction_id,
        disputed_transaction_id=disputed_transaction_id,
        undisputed_transaction_id=undisputed_transaction_id,
        expected_selected_transaction_id=selected_transaction_id,
        cycle_start=cycle_start,
        cycle_end=cycle_end,
        payment_date=payment_date,
        apr_applied=25.0,
        total_payment=125.0,
        minimum_payment_portion=25.0,
        transactions=(charges[selected_transaction_id], charges[other_transaction_id], payment),
        case_type=(
            "undisputed_balance_selected" if undisputed_selected else "disputed_balance_selected"
        ),
        is_valid=undisputed_selected,
        sql="",
    )
    return replace(case, sql=_build_allocation_sql(case))


def generate_payment_allocation_workload(
    *,
    num_accounts: int = DEFAULT_NUM_ACCOUNTS,
    num_updates: int = DEFAULT_NUM_UPDATES,
    workload_seed: int = DEFAULT_WORKLOAD_SEED,
    no_ties: bool = False,
) -> list[PaymentAllocationCase]:
    """Balance selected A/B and valid/invalid in each complete four-case block."""
    if num_updates < 1:
        raise ValueError("num_updates must be at least 1")
    if num_accounts < num_updates:
        raise ValueError("num_accounts must be at least num_updates")
    labels: list[bool] = []
    for block_start in range(0, num_accounts, 4):
        pair = [True, False]
        random.Random(f"{workload_seed}:{block_start // 4}").shuffle(pair)
        # A is disputed on odd cases and undisputed on even cases. Reversing
        # the second pair balances both selected identity and compliance.
        block = pair + list(reversed(pair))
        labels.extend(block[: min(4, num_accounts - block_start)])
    return [
        _base_case(index, undisputed_selected=labels[index - 1], no_ties=no_ties)
        for index in range(1, num_accounts + 1)
    ][:num_updates]


def _transaction_dict(case: PaymentAllocationCase, row: AllocationTransaction) -> dict[str, Any]:
    return {
        "transaction_id": row.transaction_id,
        "account_id": case.account_id,
        "statement_id": case.statement_id,
        "transaction_date": row.transaction_date.isoformat(),
        "post_date": row.post_date.isoformat(),
        "transaction_type": row.transaction_type,
        "balance_category": row.balance_category,
        "amount": row.amount,
        "merchant_name": row.merchant_name,
        "is_disputed": row.is_disputed,
    }


def _daily_balance_rows(case: PaymentAllocationCase) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for row in case.transactions:
        if row.transaction_type != "cash_advance":
            continue
        group = groups.setdefault(
            row.balance_category,
            {
                "account_id": case.account_id,
                "statement_id": case.statement_id,
                "balance_date": case.cycle_end.isoformat(),
                "balance_category": row.balance_category,
                "daily_balance": 0.0,
                "apr_applied": row.apr_applied,
                "daily_periodic_rate": row.apr_applied / 365.0,
            },
        )
        group["daily_balance"] += row.amount
    return [groups[key] for key in sorted(groups)]


def build_payment_allocation_prompt(case: PaymentAllocationCase) -> str:
    """Build the evidence available to a verifier without runtime provenance."""
    before = {
        "allocation_id": case.allocation_id,
        "account_id": case.account_id,
        "transaction_id": case.payment_transaction_id,
        "statement_id": case.statement_id,
        "payment_date": case.payment_date.isoformat(),
        "total_payment": case.total_payment,
        "minimum_payment_portion": case.minimum_payment_portion,
        "excess_payment_amount": case.total_payment - case.minimum_payment_portion,
        "purchase_allocation": 0.0,
        "cash_advance_allocation": 0.0,
        "balance_transfer_allocation": 0.0,
        "promo_allocation": 0.0,
    }
    after = {**before, "cash_advance_allocation": 100.0}
    evidence = {
        "statements": [
            {
                "statement_id": case.statement_id,
                "account_id": case.account_id,
                "cycle_start": case.cycle_start.isoformat(),
                "cycle_end": case.cycle_end.isoformat(),
                "minimum_payment": case.minimum_payment_portion,
            }
        ],
        "transactions": [
            _transaction_dict(case, row)
            for row in sorted(case.transactions, key=lambda transaction: transaction.transaction_id)
        ],
        "daily_balances": _daily_balance_rows(case),
        "balance_components": [
            {
                "component_id": row.transaction_id,
                "account_id": case.account_id,
                "statement_id": case.statement_id,
                "outstanding_amount": row.amount,
                "effective_apr": row.apr_applied,
            }
            for row in sorted(
                (row for row in case.transactions if row.transaction_type == "cash_advance"),
                key=lambda transaction: transaction.transaction_id,
            )
        ],
        "allocation_snapshot_context": [
            {
                "account_id": case.account_id,
                "statement_id": case.statement_id,
                "snapshot_date": case.cycle_end.isoformat(),
                "highest_effective_apr": case.apr_applied,
            }
        ],
        "payment_allocations_before": [before],
        "payment_allocations_after": [after],
    }
    with duckdb.connect(":memory:") as evidence_conn:
        reset_full_evidence(evidence_conn, [case])
        evidence.update(full_evidence_for_prompt(evidence_conn))
    return (
        f"{POLICY_TEXT}\n\n"
        "SQL that produced the completed allocation:\n"
        f"{case.sql}\n\n"
        "Database evidence:\n"
        f"{json.dumps(evidence, sort_keys=True, default=str)}\n\n"
        "Should this completed payment allocation be retained? "
        'Return JSON only: {"allow_update": true|false}\n'
    )


def reset_payment_allocation_database(
    conn: Any, workload: list[PaymentAllocationCase], *, materialize_context: bool = True
) -> None:
    """Create and populate the CCS-shaped payment-allocation database."""
    reset_full_evidence(conn, workload, materialize_context=materialize_context)
    for table in (
        "allocation_snapshot_context",
        "balance_components",
        "payment_allocations",
        "daily_balances",
        "transactions",
        "statements",
    ):
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.execute(
        "CREATE TABLE statements (statement_id VARCHAR PRIMARY KEY, account_id VARCHAR, "
        "cycle_start DATE, cycle_end DATE, minimum_payment DECIMAL(10, 2))"
    )
    conn.execute(
        "CREATE TABLE transactions (transaction_id VARCHAR PRIMARY KEY, account_id VARCHAR, "
        "statement_id VARCHAR, transaction_date DATE, post_date DATE, "
        "transaction_type VARCHAR, balance_category VARCHAR, amount DECIMAL(10, 2), "
        "merchant_name VARCHAR, is_disputed BOOLEAN)"
    )
    conn.execute(
        "CREATE TABLE daily_balances (account_id VARCHAR, statement_id VARCHAR, "
        "balance_date DATE, balance_category VARCHAR, daily_balance DECIMAL(10, 2), "
        "apr_applied DECIMAL(5, 3), daily_periodic_rate DECIMAL(8, 6), "
        "PRIMARY KEY (account_id, balance_date, balance_category))"
    )
    conn.execute(
        "CREATE TABLE payment_allocations (allocation_id VARCHAR PRIMARY KEY, "
        "account_id VARCHAR, transaction_id VARCHAR, statement_id VARCHAR, payment_date DATE, "
        "total_payment DECIMAL(10, 2), minimum_payment_portion DECIMAL(10, 2), "
        "excess_payment_amount DECIMAL(10, 2), "
        "purchase_allocation DECIMAL(10, 2), cash_advance_allocation DECIMAL(10, 2), "
        "balance_transfer_allocation DECIMAL(10, 2), promo_allocation DECIMAL(10, 2))"
    )
    # Passant validates policy sources as physical tables, so deterministic views are
    # materialized after the base rows are loaded.
    conn.execute(
        "CREATE TABLE balance_components (component_id VARCHAR PRIMARY KEY, "
        "account_id VARCHAR, statement_id VARCHAR, outstanding_amount DECIMAL(10, 2), "
        "effective_apr DECIMAL(8, 6))"
    )
    conn.execute(
        "CREATE TABLE allocation_snapshot_context (account_id VARCHAR, "
        "statement_id VARCHAR, snapshot_date DATE, "
        "highest_effective_apr DECIMAL(8, 6), "
        "PRIMARY KEY (account_id, statement_id))"
    )
    for case in workload:
        conn.execute(
            "INSERT INTO statements VALUES (?, ?, ?, ?, ?)",
            [
                case.statement_id,
                case.account_id,
                case.cycle_start,
                case.cycle_end,
                case.minimum_payment_portion,
            ],
        )
        conn.executemany(
            "INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    row.transaction_id,
                    case.account_id,
                    case.statement_id,
                    row.transaction_date,
                    row.post_date,
                    row.transaction_type,
                    row.balance_category,
                    row.amount,
                    row.merchant_name,
                    row.is_disputed,
                )
                for row in case.transactions
            ],
        )
        conn.executemany(
            "INSERT INTO daily_balances VALUES (?, ?, ?, ?, ?, ?, ?)",
            [list(row.values()) for row in _daily_balance_rows(case)],
        )
        conn.execute(
            "INSERT INTO payment_allocations VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0)",
            [
                case.allocation_id,
                case.account_id,
                case.payment_transaction_id,
                case.statement_id,
                case.payment_date,
                case.total_payment,
                case.minimum_payment_portion,
                case.total_payment - case.minimum_payment_portion,
            ],
        )
    conn.execute(
        "INSERT INTO balance_components "
        "SELECT transaction.transaction_id, transaction.account_id, "
        "transaction.statement_id, transaction.amount, balance.apr_applied "
        "FROM transactions AS transaction "
        "JOIN statements AS statement "
        "ON statement.statement_id = transaction.statement_id "
        "JOIN daily_balances AS balance "
        "ON balance.account_id = transaction.account_id "
        "AND balance.statement_id = transaction.statement_id "
        "AND balance.balance_category = transaction.balance_category "
        "AND balance.balance_date = statement.cycle_end "
        "WHERE transaction.amount > 0"
    )
    conn.execute(
        "INSERT INTO allocation_snapshot_context "
        "SELECT statement.account_id, statement.statement_id, balance.balance_date, "
        "MAX(balance.apr_applied) "
        "FROM statements AS statement "
        "JOIN daily_balances AS balance "
        "ON balance.account_id = statement.account_id "
        "AND balance.statement_id = statement.statement_id "
        "AND balance.balance_date = statement.cycle_end "
        "GROUP BY statement.account_id, statement.statement_id, "
        "statement.cycle_end, balance.balance_date"
    )


def selected_balance_transaction(conn: Any, case: PaymentAllocationCase) -> str:
    """Return the physical contributor selected by the tied top-1 expression."""
    return str(
        conn.execute(
            "SELECT candidate.transaction_id FROM transactions AS candidate "
            "JOIN daily_balances AS balance "
            "ON balance.account_id = candidate.account_id "
            "AND balance.statement_id = candidate.statement_id "
            "AND balance.balance_category = candidate.balance_category "
            "WHERE candidate.account_id = ? AND candidate.statement_id = ? "
            "AND candidate.transaction_type = 'cash_advance' "
            "AND balance.balance_date = ? "
            "ORDER BY balance.apr_applied DESC LIMIT 1",
            [case.account_id, case.statement_id, case.cycle_end],
        ).fetchone()[0]
    )


def audit_regulation_z_v3_policies(workload: list[PaymentAllocationCase]) -> list[dict[str, Any]]:
    """Execute each policy alone to establish which one actually blocks writes."""
    expected_blocked = {case.allocation_id for case in workload if not case.is_valid}
    results = []
    with duckdb.connect(":memory:") as conn:
        reset_payment_allocation_database(conn, workload)
        for policy in build_regulation_z_53_policies():
            conn.execute("UPDATE payment_allocations SET cash_advance_allocation = 0")
            rewriter = Connection(DuckDBAdapter(conn))
            rewriter.register_policy(policy)
            for case in workload:
                conn.execute(rewriter.transform_query(case.sql))
            blocked = {
                allocation_id
                for allocation_id, amount in conn.execute(
                    "SELECT allocation_id, cash_advance_allocation FROM payment_allocations"
                ).fetchall()
                if amount == 0
            }
            proposition_id = policy.description.split(":", 1)[0]
            expected = expected_blocked if proposition_id == "P17" else set()
            results.append(
                {
                    "proposition_id": proposition_id,
                    "policy": policy.description,
                    "executions": len(workload),
                    "allowed": len(workload) - len(blocked),
                    "blocked": len(blocked),
                    "expected_blocked": len(expected),
                    "matches_expected_ids": blocked == expected,
                }
            )
    return results


class RegulationZV3BenchmarkStrategy(_BenchmarkBase):
    """Compare provenance-aware DFC with model and no-policy allocation guards."""

    def setup(self, context: ExperimentContext) -> None:
        self.num_accounts = int(context.strategy_config.get("num_accounts", DEFAULT_NUM_ACCOUNTS))
        self.num_updates = int(context.strategy_config.get("num_updates", DEFAULT_NUM_UPDATES))
        if self.num_updates > self.num_accounts:
            raise ValueError("num_updates must not exceed num_accounts")
        self.max_concurrency = int(
            context.strategy_config.get("max_concurrency", DEFAULT_MAX_CONCURRENCY)
        )
        if not 1 <= self.max_concurrency <= DEFAULT_MAX_CONCURRENCY:
            raise ValueError(f"max_concurrency must be between 1 and {DEFAULT_MAX_CONCURRENCY}")
        self.workload_seed = int(
            context.strategy_config.get("workload_seed", DEFAULT_WORKLOAD_SEED)
        )
        self.no_ties = bool(context.strategy_config.get("no_ties", False))
        self.database_workload = generate_payment_allocation_workload(
            num_accounts=self.num_accounts,
            num_updates=self.num_accounts,
            workload_seed=self.workload_seed,
            no_ties=self.no_ties,
        )
        self.workload = self.database_workload[: self.num_updates]
        self.dfc_only = bool(context.strategy_config.get("dfc_only", False))
        self.model_specs = (
            []
            if self.dfc_only
            else [
                dict(spec)
                for spec in context.strategy_config.get("models", DEFAULT_V3_BEDROCK_MODEL_SPECS)
            ]
        )

        self.local_duckdb = duckdb
        self.no_policy_conn = self.local_duckdb.connect(":memory:")
        self.dfc_conn = self.local_duckdb.connect(":memory:")
        self.model_connections = {
            str(spec["key"]): self.local_duckdb.connect(":memory:") for spec in self.model_specs
        }
        for conn in [self.no_policy_conn, self.dfc_conn, *self.model_connections.values()]:
            self._reset_database(conn)
        self.dfc_rewriter = Connection(DuckDBAdapter(self.dfc_conn))
        for policy in build_regulation_z_53_policies():
            self.dfc_rewriter.register_policy(policy)

        supplied_client = context.strategy_config.get("bedrock_client")
        if supplied_client is None:
            import boto3
            from botocore.config import Config

            default_region = str(context.strategy_config.get("aws_region", DEFAULT_AWS_REGION))
            profile = context.strategy_config.get("aws_profile")
            clients_by_region: dict[str, Any] = {}
            for spec in self.model_specs:
                region = str(spec.get("aws_region", default_region))
                if region not in clients_by_region:
                    session = (
                        boto3.Session(profile_name=profile, region_name=region)
                        if profile
                        else boto3.Session(region_name=region)
                    )
                    clients_by_region[region] = session.client(
                        "bedrock-runtime",
                        config=Config(
                            read_timeout=600,
                            max_pool_connections=self.max_concurrency,
                            retries={"total_max_attempts": 1},
                        ),
                    )
            clients_by_key = {
                str(spec["key"]): clients_by_region[str(spec.get("aws_region", default_region))]
                for spec in self.model_specs
            }
        else:
            clients_by_key = {str(spec["key"]): supplied_client for spec in self.model_specs}
        self.guards = {
            str(spec["key"]): BedrockPolicyGuard(
                clients_by_key[str(spec["key"])],
                str(spec["model_id"]),
                str(spec.get("bedrock_provider", "")) or None,
            )
            for spec in self.model_specs
        }

        self.progress_file: TextIO | None = None
        self.progress_writer: csv.DictWriter | None = None
        progress_path_value = context.strategy_config.get("progress_path")
        if progress_path_value:
            progress_path = Path(str(progress_path_value))
            progress_path.parent.mkdir(parents=True, exist_ok=True)
            write_header = not progress_path.exists() or progress_path.stat().st_size == 0
            self.progress_file = progress_path.open("a", encoding="utf-8", newline="", buffering=1)
            self.progress_writer = csv.DictWriter(
                self.progress_file,
                fieldnames=PROGRESS_FIELDNAMES,
                extrasaction="ignore",
            )
            if write_header:
                self.progress_writer.writeheader()
                self.progress_file.flush()

    def _write_progress(self, context: ExperimentContext, **values: Any) -> None:
        if self.progress_writer is None or self.progress_file is None:
            return
        row = dict.fromkeys(PROGRESS_FIELDNAMES, "")
        row.update(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "execution_number": context.execution_number,
                "is_warmup": context.is_warmup,
                "num_updates": self.num_updates,
            }
        )
        row.update(values)
        self.progress_writer.writerow(row)
        self.progress_file.flush()

    def _reset_database(self, conn: Any) -> None:
        reset_payment_allocation_database(conn, self.database_workload)

    def _build_prompt(self, case: PaymentAllocationCase, nonce: str) -> str:
        prompt = build_payment_allocation_prompt(case)
        return f"Run nonce: {nonce}\n{prompt}" if nonce else prompt

    @staticmethod
    def _update_applied(conn: Any, case: PaymentAllocationCase) -> bool:
        allocation = conn.execute(
            "SELECT cash_advance_allocation FROM payment_allocations WHERE allocation_id = ?",
            [case.allocation_id],
        ).fetchone()[0]
        return float(allocation) > 0.0

    def _progress_identity(self, case: PaymentAllocationCase) -> dict[str, Any]:
        return {
            "allocation_id": case.allocation_id,
            "payment_transaction_id": case.payment_transaction_id,
            "account_id": case.account_id,
            "case_type": case.case_type,
            "expected_selected_transaction_id": case.expected_selected_transaction_id,
            "expected_allow": case.is_valid,
        }

    def _run_models_parallel(self, context: ExperimentContext) -> tuple[dict, float]:
        """Share one bounded pool, interleaving models; serialize SQL and output writes."""
        metrics = {}
        for spec in self.model_specs:
            key = str(spec["key"])
            self._reset_database(self.model_connections[key])
            metrics[key] = dict.fromkeys(
                (
                    "time_ms",
                    "llm_time_ms",
                    "sql_execution_ms",
                    "correct",
                    "false_positives",
                    "false_negatives",
                    "parse_failures",
                    "input_tokens",
                    "output_tokens",
                    "cache_read_input_tokens",
                    "cache_write_input_tokens",
                    "wall_time_ms",
                    "api_errors",
                    "completed_requests",
                ),
                0,
            )
        jobs = iter(
            (spec, number, case)
            for number, case in enumerate(self.workload, 1)
            for spec in self.model_specs
            if number
            not in context.strategy_config.get("skip_model_updates", {}).get(str(spec["key"]), [])
        )
        start = time.perf_counter()
        pending = {}
        if not self.model_specs:
            return metrics, 0.0
        with ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:

            def submit_next():
                job = next(jobs, None)
                if job is None:
                    return
                spec, number, case = job
                nonce = str(uuid.uuid4()) if spec.get("prompt_nonce", False) else ""
                prompt = self._build_prompt(case, nonce)
                future = executor.submit(self.guards[str(spec["key"])].invoke, prompt)
                pending[future] = (spec, number, case, prompt)

            for _ in range(self.max_concurrency):
                submit_next()
            while pending:
                completed, _ = wait(pending, return_when=FIRST_COMPLETED)
                for future in completed:
                    spec, number, case, prompt = pending.pop(future)
                    key = str(spec["key"])
                    identity = dict(
                        approach=key,
                        model_id=spec["model_id"],
                        update_number=number,
                        prompt=prompt,
                        **self._progress_identity(case),
                    )
                    try:
                        response = future.result()
                        sql_ms = 0.0
                        if response.allow_update:
                            sql_start = time.perf_counter()
                            self.model_connections[key].execute(case.sql)
                            sql_ms = (time.perf_counter() - sql_start) * 1000
                    except Exception as error:
                        self._write_progress(context, **identity, error=str(error))
                        metrics[key]["api_errors"] += 1
                        submit_next()
                        continue
                    allow = bool(response.allow_update)
                    values = {
                        "time_ms": response.runtime_ms + sql_ms,
                        "llm_time_ms": response.runtime_ms,
                        "sql_execution_ms": sql_ms,
                        "correct": int(
                            response.allow_update is not None and allow == case.is_valid
                        ),
                        "false_positives": int(response.allow_update is False and case.is_valid),
                        "false_negatives": int(response.allow_update is True and not case.is_valid),
                        "parse_failures": int(response.allow_update is None),
                        "input_tokens": response.input_tokens,
                        "output_tokens": response.output_tokens,
                        "cache_read_input_tokens": response.cache_read_input_tokens,
                        "cache_write_input_tokens": response.cache_write_input_tokens,
                    }
                    for name, value in values.items():
                        metrics[key][name] += value
                    metrics[key]["completed_requests"] += 1
                    metrics[key]["wall_time_ms"] = (time.perf_counter() - start) * 1000
                    self._write_progress(
                        context,
                        **identity,
                        actual_allow=allow,
                        correct=bool(values["correct"]),
                        elapsed_ms=values["time_ms"],
                        llm_ms=response.runtime_ms,
                        sql_execution_ms=sql_ms,
                        input_tokens=response.input_tokens,
                        output_tokens=response.output_tokens,
                        cache_read_input_tokens=response.cache_read_input_tokens,
                        cache_write_input_tokens=response.cache_write_input_tokens,
                        parse_failure=response.allow_update is None,
                        request_id=response.request_id,
                        raw_response=response.raw_response,
                    )
                    submit_next()
        return metrics, (time.perf_counter() - start) * 1000

    def execute(self, context: ExperimentContext) -> ExperimentResult:
        no_policy_ms, no_policy_correct = (
            (0.0, 0) if self.dfc_only else self._run_no_policy(context)
        )
        dfc_ms, dfc_rewrite_ms, dfc_execution_ms, dfc_correct = self._run_dfc(context)
        custom_metrics: dict[str, Any] = {
            "policy_count": len(build_regulation_z_53_policies()),
            "dfc_only": self.dfc_only,
            "no_ties": self.no_ties,
            "num_accounts": self.num_accounts,
            "num_updates": self.num_updates,
            "workload_seed": self.workload_seed,
            "compliant_executions": sum(case.is_valid for case in self.workload),
            "violating_executions": sum(not case.is_valid for case in self.workload),
            "no_policy_time_ms": no_policy_ms,
            "no_policy_correct": no_policy_correct,
            "no_policy_accuracy": no_policy_correct / self.num_updates,
            "dfc_time_ms": dfc_ms,
            "dfc_rewrite_time_ms": dfc_rewrite_ms,
            "dfc_execution_time_ms": dfc_execution_ms,
            "dfc_correct": dfc_correct,
            "dfc_accuracy": dfc_correct / self.num_updates,
        }
        if self.dfc_only:
            for key in ("no_policy_time_ms", "no_policy_correct", "no_policy_accuracy"):
                custom_metrics.pop(key)
        duration_ms = no_policy_ms + dfc_ms
        model_metrics, models_wall_ms = self._run_models_parallel(context)
        duration_ms += models_wall_ms
        for spec in self.model_specs:
            key = str(spec["key"])
            metrics = model_metrics[key]
            custom_metrics[f"{key}_display_name"] = spec["display_name"]
            custom_metrics[f"{key}_model_id"] = spec["model_id"]
            for metric_name, value in metrics.items():
                custom_metrics[f"{key}_{metric_name}"] = value
            completed = int(metrics["completed_requests"]) + int(metrics["api_errors"])
            custom_metrics[f"{key}_accuracy"] = (
                int(metrics["correct"]) / completed if completed else None
            )
            custom_metrics[f"{key}_cache_contaminated"] = bool(metrics["cache_read_input_tokens"])
        return ExperimentResult(duration_ms=duration_ms, custom_metrics=custom_metrics)


__all__ = [
    "AVAILABLE_V2_BEDROCK_MODEL_SPECS",
    "DEFAULT_AWS_REGION",
    "DEFAULT_MAX_CONCURRENCY",
    "DEFAULT_MEASURED_RUNS",
    "DEFAULT_NUM_ACCOUNTS",
    "DEFAULT_NUM_UPDATES",
    "DEFAULT_V2_BEDROCK_MODEL_SPECS",
    "DEFAULT_V3_BEDROCK_MODEL_SPECS",
    "DEFAULT_WARMUP_RUNS",
    "DEFAULT_WORKLOAD_SEED",
    "POLICY_TEXT",
    "PaymentAllocationCase",
    "RegulationZV3BenchmarkStrategy",
    "build_disputed_balance_policy",
    "build_payment_allocation_prompt",
    "build_regulation_z_53_policies",
    "generate_payment_allocation_workload",
    "reset_payment_allocation_database",
    "selected_balance_transaction",
]
