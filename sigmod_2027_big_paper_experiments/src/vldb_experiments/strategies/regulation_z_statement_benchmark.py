"""Regulation Z v2 credit-card statement reconciliation benchmark."""

from __future__ import annotations

import csv
from dataclasses import dataclass, replace
from datetime import date, timedelta
import json
from pathlib import Path
import random
from typing import TYPE_CHECKING, Any, TextIO

from sql_rewriter import DFCPolicy, Resolution, SQLRewriter

from vldb_experiments.strategies.regulation_z_benchmark import BedrockPolicyGuard
from vldb_experiments.strategies.regulation_z_v2_benchmark import (
    AVAILABLE_V2_BEDROCK_MODEL_SPECS,
    DEFAULT_AWS_REGION,
    DEFAULT_CATALOG_PATH,
    DEFAULT_MAX_CONCURRENCY,
    DEFAULT_MEASURED_RUNS,
    DEFAULT_NUM_ACCOUNTS,
    DEFAULT_NUM_UPDATES,
    DEFAULT_V2_BEDROCK_MODEL_SPECS,
    DEFAULT_WARMUP_RUNS,
    PROGRESS_FIELDNAMES,
)
from vldb_experiments.strategies.regulation_z_v2_benchmark import (
    RegulationZV2BenchmarkStrategy as _APRBenchmarkStrategy,
)
from vldb_experiments.strategies.regulation_z_v2_data import ProductTerm, load_product_terms
from vldb_experiments.strategies.tpch_strategy import _ensure_smokedduck

if TYPE_CHECKING:
    from experiment_harness import ExperimentContext

STATEMENT_POLICY_SPECS = (
    (
        "Publication identifies the reconciled statement and account",
        "The candidate publication must set status to published, its statement_id must identify "
        "the supplied draft statement, and its account_id must equal that statement's account_id.",
        "max(statement_reconciliation.statement_id) = pub.statement_id AND "
        "max(statement_reconciliation.account_id) = pub.account_id AND "
        "pub.status = 'published'",
    ),
    (
        "Account is active",
        "The statement's account must be active.",
        "max(statement_reconciliation.account_status) = 'active'",
    ),
    (
        "Card product is active",
        "The card product linked from the statement's account must be active.",
        "max(statement_reconciliation.product_status) = 'active'",
    ),
    (
        "Purchase APR configuration is approved",
        "The purchase-APR configuration linked to the account's product must have a non-null "
        "approved_by value.",
        "max(statement_reconciliation.config_approved_by) IS NOT NULL",
    ),
    (
        "Statement purchase total matches purchase transactions",
        "The statement's total_purchases must equal the sum of purchase transaction amounts, "
        "within $0.01.",
        "abs(max(statement_reconciliation.total_purchases) - "
        "max(statement_reconciliation.expected_total_purchases)) <= 0.01",
    ),
    (
        "Statement cash-advance total matches cash-advance transactions",
        "The statement's total_cash_advances must equal the sum of cash-advance transaction "
        "amounts, within $0.01.",
        "abs(max(statement_reconciliation.total_cash_advances) - "
        "max(statement_reconciliation.expected_total_cash_advances)) <= 0.01",
    ),
    (
        "Statement payment total matches payment transactions",
        "The statement's positive total_payments must equal the negative of the sum of signed "
        "payment transaction amounts, within $0.01.",
        "abs(max(statement_reconciliation.total_payments) - "
        "max(statement_reconciliation.expected_total_payments)) <= 0.01",
    ),
    (
        "Statement fee total matches fee transactions",
        "The statement's total_fees must equal the sum of fee transaction amounts, within $0.01.",
        "abs(max(statement_reconciliation.total_fees) - "
        "max(statement_reconciliation.expected_total_fees)) <= 0.01",
    ),
    (
        "Statement interest total matches interest transactions",
        "The statement's total_interest must equal the sum of interest transaction amounts, "
        "within $0.01.",
        "abs(max(statement_reconciliation.total_interest) - "
        "max(statement_reconciliation.expected_total_interest)) <= 0.01",
    ),
    (
        "New balance matches previous balance plus net activity",
        "The statement's new_balance must equal previous_balance plus the sum of every signed "
        "transaction amount, within $0.01.",
        "abs(max(statement_reconciliation.new_balance) - "
        "max(statement_reconciliation.expected_new_balance)) <= 0.01",
    ),
    (
        "Interest charge matches the daily-balance calculation",
        "The statement's total_interest must equal round(sum(daily_balance * "
        "daily_periodic_rate / 100), 2) across all supplied daily purchase balances, within $0.01.",
        "abs(max(statement_reconciliation.total_interest) - "
        "max(statement_reconciliation.expected_daily_interest)) <= 0.01",
    ),
    (
        "Daily balances completely cover the billing cycle",
        "There must be exactly one daily purchase-balance row for every calendar date from "
        "cycle_start through cycle_end, inclusive: the row count and distinct-date count must "
        "both equal the inclusive cycle length, and the minimum and maximum dates must equal the "
        "cycle boundaries.",
        "max(statement_reconciliation.daily_day_count) = "
        "max(statement_reconciliation.expected_day_count) AND "
        "max(statement_reconciliation.daily_distinct_day_count) = "
        "max(statement_reconciliation.expected_day_count) AND "
        "max(statement_reconciliation.first_daily_balance_date) = "
        "max(statement_reconciliation.cycle_start) AND "
        "max(statement_reconciliation.last_daily_balance_date) = "
        "max(statement_reconciliation.cycle_end)",
    ),
    (
        "Disclosed purchase APR matches the approved APR",
        "The purchase_apr disclosed on the statement must match the approved purchase APR for "
        "the account's product within 0.000001 percentage points.",
        "abs(max(statement_reconciliation.purchase_apr) - "
        "max(statement_reconciliation.config_purchase_apr)) <= 0.000001",
    ),
    (
        "Every daily purchase APR matches the approved APR",
        "Every supplied daily purchase-balance row's apr_applied must match the approved purchase "
        "APR for the account's product within 0.000001 percentage points.",
        "abs(max(statement_reconciliation.minimum_daily_purchase_apr) - "
        "max(statement_reconciliation.config_purchase_apr)) <= 0.000001 AND "
        "abs(max(statement_reconciliation.maximum_daily_purchase_apr) - "
        "max(statement_reconciliation.config_purchase_apr)) <= 0.000001",
    ),
)

POLICY_TEXT = "\n".join(
    [
        "A draft credit-card statement may be published only if every rule below passes. "
        "Each numbered rule corresponds to one independently enforced DFC policy:",
        *[
            f"{index}. {statement}"
            for index, (_, statement, _) in enumerate(STATEMENT_POLICY_SPECS, start=1)
        ],
        "Reject publication if any numbered rule fails.",
    ]
)

EVIDENCE_CONVENTIONS = """The evidence is complete: the transaction list contains all and only
transactions assigned to this statement, and the daily-purchase-balance list contains all and only
purchase daily-balance rows assigned to this statement. No relevant rows have been omitted.
Transaction amounts are signed: charges are positive and payments or credits are negative. The
statement reports total_payments as a positive amount. APRs and daily periodic rates are expressed
as percentages, so a daily periodic rate must be divided by 100 when calculating interest. The
supplied purchase-APR configuration is approved and effective for this product throughout the
statement's billing cycle. Evaluate only the stated policy requirements using the supplied rows."""

FULL_DATABASE_EVIDENCE_CONVENTIONS = """The database snapshot is complete. Locate the candidate
publication selected by the SQL WHERE clause, then follow statement_id, account_id, and product_code
relationships to identify its statement, account, product, approved purchase-APR configuration,
transactions, and daily balances. Rows for unrelated accounts and statements are irrelevant. For
the selected statement, the snapshot contains all and only its assigned transactions and daily
purchase-balance rows; no relevant rows have been omitted. Transaction amounts are signed: charges
are positive and payments or credits are negative. The statement reports total_payments as a
positive amount. APRs and daily periodic rates are percentages, so divide a daily periodic rate by
100 when calculating interest. Evaluate only the stated policy requirements using the supplied
database rows."""

INVALID_CASE_TYPES = (
    "purchase_total_mismatch",
    "cash_advance_total_mismatch",
    "payment_total_mismatch",
    "fee_total_mismatch",
    "interest_total_mismatch",
    "new_balance_mismatch",
    "purchase_apr_mismatch",
    "unposted_purchase",
    "applied_apr_mismatch",
    "missing_cycle_day",
)


@dataclass(frozen=True)
class TransactionRow:
    transaction_id: str
    transaction_date: date
    transaction_type: str
    balance_category: str
    amount: float


@dataclass(frozen=True)
class DailyBalanceRow:
    balance_date: date
    daily_balance: float
    apr_applied: float
    daily_periodic_rate: float


@dataclass(frozen=True)
class StatementCase:
    case_number: int
    publication_id: str
    statement_id: str
    account_id: str
    product_code: str
    account_status: str
    product_status: str
    cycle_start: date
    cycle_end: date
    statement_date: date
    source_record_id: str
    source_report_date: date
    source_purchase_apr: float
    source_url: str
    config_id: str
    config_approved_by: str | None
    config_approval_date: date
    approved_purchase_apr: float
    previous_balance: float
    total_purchases: float
    total_cash_advances: float
    total_payments: float
    total_fees: float
    total_interest: float
    new_balance: float
    purchase_apr: float
    transactions: tuple[TransactionRow, ...]
    daily_balances: tuple[DailyBalanceRow, ...]
    case_type: str
    is_valid: bool
    sql: str


def _statement_policy(description: str, constraint: str) -> DFCPolicy:
    return DFCPolicy(
        sources=["statement_reconciliation"],
        sink="statement_publications",
        sink_alias="pub",
        constraint=constraint,
        on_fail=Resolution.REMOVE,
        description=description,
    )


def build_regulation_z_statement_policies() -> list[DFCPolicy]:
    """Build independently reportable controls for statement publication."""
    return [
        _statement_policy(description, constraint)
        for description, _, constraint in STATEMENT_POLICY_SPECS
    ]


def _build_publish_sql(case: StatementCase) -> str:
    return (
        "UPDATE statement_publications AS pub "
        "SET status = 'published', "
        f"published_at = TIMESTAMP '{case.statement_date.isoformat()} 09:00:00' "
        "FROM statement_reconciliation "
        f"WHERE pub.publication_id = '{case.publication_id}' "
        "AND statement_reconciliation.statement_id = pub.statement_id "
        "AND statement_reconciliation.account_id = pub.account_id"
    )


def _base_case(case_number: int, term: ProductTerm) -> StatementCase:
    statement_id = f"STM{case_number:08d}"
    account_id = f"A{case_number:07d}"
    product_code = f"P{case_number:07d}"
    cycle_start = date(2027, 1, 1) + timedelta(days=(case_number - 1) * 31)
    cycle_end = cycle_start + timedelta(days=29)
    statement_date = cycle_end + timedelta(days=1)
    apr = term.purchase_apr
    previous_balance = round(425.0 + (case_number % 17) * 23.75, 2)
    purchase_one = round(42.5 + (case_number % 13) * 3.25, 2)
    purchase_two = round(18.75 + (case_number % 9) * 2.5, 2)
    cash_advance = round(25.0 + (case_number % 5) * 5.0, 2)
    payment = round(75.0 + (case_number % 11) * 4.0, 2)
    fee = 5.0 if case_number % 3 == 0 else 0.0
    daily_balances = tuple(
        DailyBalanceRow(
            balance_date=cycle_start + timedelta(days=day),
            daily_balance=round(previous_balance + day * 1.75, 2),
            apr_applied=apr,
            daily_periodic_rate=apr / 365.0,
        )
        for day in range(30)
    )
    interest = round(
        sum(row.daily_balance * row.daily_periodic_rate / 100.0 for row in daily_balances),
        2,
    )
    transactions = (
        TransactionRow(
            f"TX{case_number:07d}A",
            cycle_start + timedelta(days=2),
            "purchase",
            "purchase",
            purchase_one,
        ),
        TransactionRow(
            f"TX{case_number:07d}B",
            cycle_start + timedelta(days=8),
            "purchase",
            "purchase",
            purchase_two,
        ),
        TransactionRow(
            f"TX{case_number:07d}C",
            cycle_start + timedelta(days=12),
            "cash_advance",
            "cash_advance",
            cash_advance,
        ),
        TransactionRow(
            f"TX{case_number:07d}D",
            cycle_start + timedelta(days=18),
            "payment",
            "purchase",
            -payment,
        ),
        TransactionRow(
            f"TX{case_number:07d}E", cycle_start + timedelta(days=24), "fee", "purchase", fee
        ),
        TransactionRow(f"TX{case_number:07d}F", cycle_end, "interest", "purchase", interest),
    )
    total_purchases = round(purchase_one + purchase_two, 2)
    new_balance = round(previous_balance + sum(row.amount for row in transactions), 2)
    case = StatementCase(
        case_number=case_number,
        publication_id=f"PUB{case_number:08d}",
        statement_id=statement_id,
        account_id=account_id,
        product_code=product_code,
        account_status="active",
        product_status="active",
        cycle_start=cycle_start,
        cycle_end=cycle_end,
        statement_date=statement_date,
        source_record_id=term.source_record_id,
        source_report_date=term.report_date,
        source_purchase_apr=term.purchase_apr,
        source_url=term.source_url,
        config_id=f"CFG{case_number:07d}",
        config_approved_by="pricing-approver",
        config_approval_date=cycle_start - timedelta(days=30),
        approved_purchase_apr=apr,
        previous_balance=previous_balance,
        total_purchases=total_purchases,
        total_cash_advances=cash_advance,
        total_payments=payment,
        total_fees=fee,
        total_interest=interest,
        new_balance=new_balance,
        purchase_apr=apr,
        transactions=transactions,
        daily_balances=daily_balances,
        case_type="valid_statement",
        is_valid=True,
        sql="",
    )
    return replace(case, sql=_build_publish_sql(case))


def _make_invalid(case: StatementCase, case_type: str) -> StatementCase:
    changes: dict[str, Any] = {"case_type": case_type, "is_valid": False}
    if case_type == "purchase_total_mismatch":
        changes["total_purchases"] = case.total_purchases + 19.0
    elif case_type == "cash_advance_total_mismatch":
        changes["total_cash_advances"] = case.total_cash_advances + 15.0
    elif case_type == "payment_total_mismatch":
        changes["total_payments"] = case.total_payments - 12.0
    elif case_type == "fee_total_mismatch":
        changes["total_fees"] = case.total_fees + 8.0
    elif case_type == "interest_total_mismatch":
        changes["total_interest"] = case.total_interest + 4.25
    elif case_type == "new_balance_mismatch":
        changes["new_balance"] = case.new_balance + 25.0
    elif case_type == "purchase_apr_mismatch":
        changes["purchase_apr"] = case.purchase_apr + 1.5
    elif case_type == "unposted_purchase":
        changes["transactions"] = (
            *case.transactions,
            TransactionRow(
                f"TX{case.case_number:07d}G",
                case.cycle_end - timedelta(days=1),
                "purchase",
                "purchase",
                31.75,
            ),
        )
    elif case_type == "applied_apr_mismatch":
        wrong_apr = case.purchase_apr + 2.0
        changes["daily_balances"] = tuple(
            replace(
                row,
                apr_applied=wrong_apr,
                daily_periodic_rate=wrong_apr / 365.0,
            )
            for row in case.daily_balances
        )
    elif case_type == "missing_cycle_day":
        changes["daily_balances"] = tuple(
            replace(row, balance_date=case.daily_balances[9].balance_date) if index == 10 else row
            for index, row in enumerate(case.daily_balances)
        )
    else:
        raise ValueError(f"Unknown invalid case type: {case_type}")
    invalid = replace(case, **changes)
    return replace(invalid, sql=_build_publish_sql(invalid))


def generate_statement_workload(
    product_terms: list[ProductTerm],
    *,
    num_accounts: int = DEFAULT_NUM_ACCOUNTS,
    num_updates: int = DEFAULT_NUM_UPDATES,
) -> list[StatementCase]:
    """Generate balanced valid and invalid statement-publication attempts."""
    if num_updates < 1:
        raise ValueError("num_updates must be at least 1")
    if num_accounts < num_updates:
        raise ValueError("num_accounts must be at least num_updates")
    if not product_terms:
        raise ValueError("product_terms must not be empty")
    case_types = tuple(
        case_type
        for invalid_case_type in INVALID_CASE_TYPES
        for case_type in ("valid_statement", invalid_case_type)
    )
    cases = []
    for index in range(num_updates):
        base = _base_case(index + 1, product_terms[index % len(product_terms)])
        case_type = case_types[index % len(case_types)]
        cases.append(base if case_type == "valid_statement" else _make_invalid(base, case_type))
    return cases


def build_statement_policy_prompt(case: StatementCase) -> str:
    """Build the exact evidence prompt supplied to each Bedrock model."""
    context = {
        "statement_publication": {
            "publication_id": case.publication_id,
            "statement_id": case.statement_id,
            "account_id": case.account_id,
            "status": "draft",
            "published_at": None,
        },
        "draft_statement": {
            "statement_id": case.statement_id,
            "account_id": case.account_id,
            "cycle_start": case.cycle_start.isoformat(),
            "cycle_end": case.cycle_end.isoformat(),
            "previous_balance": case.previous_balance,
            "new_balance": case.new_balance,
            "total_purchases": case.total_purchases,
            "total_cash_advances": case.total_cash_advances,
            "total_payments": case.total_payments,
            "total_fees": case.total_fees,
            "total_interest": case.total_interest,
            "purchase_apr": case.purchase_apr,
        },
        "account": {
            "account_id": case.account_id,
            "product_code": case.product_code,
            "status": case.account_status,
        },
        "product": {"product_code": case.product_code, "status": case.product_status},
        "approved_purchase_apr": {
            "config_id": case.config_id,
            "product_code": case.product_code,
            "rate_value": case.approved_purchase_apr,
            "approved_by": case.config_approved_by,
            "approval_date": case.config_approval_date.isoformat(),
        },
        "transactions": [
            {
                "transaction_id": row.transaction_id,
                "account_id": case.account_id,
                "statement_id": case.statement_id,
                "transaction_date": row.transaction_date.isoformat(),
                "transaction_type": row.transaction_type,
                "balance_category": row.balance_category,
                "amount": row.amount,
            }
            for row in case.transactions
        ],
    }
    daily_balance_table = "\n".join(
        [
            "balance_date\tdaily_balance\tapr_applied\tdaily_periodic_rate",
            *[
                f"{row.balance_date.isoformat()}\t{row.daily_balance}\t{row.apr_applied}\t"
                f"{row.daily_periodic_rate}"
                for row in case.daily_balances
            ],
        ]
    )
    return (
        "Policy:\n"
        f"{POLICY_TEXT}\n\n"
        "Evidence conventions:\n"
        f"{EVIDENCE_CONVENTIONS}\n\n"
        "Candidate SQL publication update:\n"
        f"{case.sql}\n\n"
        "Relevant database rows:\n"
        f"{json.dumps(context, sort_keys=True)}\n\n"
        "Daily purchase balances (tab-separated). Every row has "
        f"account_id={case.account_id}, statement_id={case.statement_id}, and "
        "balance_category=purchase:\n"
        f"{daily_balance_table}\n\n"
        "Should the candidate statement be published? "
        'Return JSON only: {"allow_update": true|false}\n'
    )


def _statement_database_tables(
    workload: list[StatementCase],
) -> dict[str, tuple[tuple[str, ...], list[tuple[Any, ...]]]]:
    product_terms: dict[str, tuple[Any, ...]] = {}
    for case in workload:
        product_terms.setdefault(
            case.source_record_id,
            (
                case.source_record_id,
                case.source_report_date,
                case.source_purchase_apr,
                case.source_url,
            ),
        )
    return {
        "public_product_terms": (
            ("source_record_id", "report_date", "purchase_apr", "source_url"),
            list(product_terms.values()),
        ),
        "products": (
            ("product_code", "product_name", "status"),
            [
                (
                    case.product_code,
                    f"Card Product {case.case_number:04d}",
                    case.product_status,
                )
                for case in workload
            ],
        ),
        "accounts": (
            ("account_id", "product_code", "status"),
            [(case.account_id, case.product_code, case.account_status) for case in workload],
        ),
        "apr_config": (
            (
                "config_id",
                "product_code",
                "apr_type",
                "rate_value",
                "approved_by",
                "approval_date",
            ),
            [
                (
                    case.config_id,
                    case.product_code,
                    "purchase",
                    case.approved_purchase_apr,
                    case.config_approved_by,
                    case.config_approval_date,
                )
                for case in workload
            ],
        ),
        "statements": (
            (
                "statement_id",
                "account_id",
                "cycle_start",
                "cycle_end",
                "statement_date",
                "previous_balance",
                "new_balance",
                "total_purchases",
                "total_cash_advances",
                "total_payments",
                "total_fees",
                "total_interest",
                "purchase_apr",
            ),
            [
                (
                    case.statement_id,
                    case.account_id,
                    case.cycle_start,
                    case.cycle_end,
                    case.statement_date,
                    case.previous_balance,
                    case.new_balance,
                    case.total_purchases,
                    case.total_cash_advances,
                    case.total_payments,
                    case.total_fees,
                    case.total_interest,
                    case.purchase_apr,
                )
                for case in workload
            ],
        ),
        "transactions": (
            (
                "transaction_id",
                "account_id",
                "statement_id",
                "transaction_date",
                "transaction_type",
                "balance_category",
                "amount",
            ),
            [
                (
                    row.transaction_id,
                    case.account_id,
                    case.statement_id,
                    row.transaction_date,
                    row.transaction_type,
                    row.balance_category,
                    row.amount,
                )
                for case in workload
                for row in case.transactions
            ],
        ),
        "daily_balances": (
            (
                "account_id",
                "statement_id",
                "balance_date",
                "balance_category",
                "daily_balance",
                "apr_applied",
                "daily_periodic_rate",
            ),
            [
                (
                    case.account_id,
                    case.statement_id,
                    row.balance_date,
                    "purchase",
                    row.daily_balance,
                    row.apr_applied,
                    row.daily_periodic_rate,
                )
                for case in workload
                for row in case.daily_balances
            ],
        ),
        "statement_publications": (
            ("publication_id", "statement_id", "account_id", "status", "published_at"),
            [
                (
                    case.publication_id,
                    case.statement_id,
                    case.account_id,
                    "draft",
                    None,
                )
                for case in workload
            ],
        ),
    }


def _tabular_value(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def build_full_database_statement_policy_prompt(
    case: StatementCase,
    database_workload: list[StatementCase],
    *,
    shuffle_seed: int = 0,
) -> str:
    """Build a prompt containing every base-table row in independently shuffled order."""
    table_sections = []
    for table_name, (columns, source_rows) in _statement_database_tables(database_workload).items():
        rows = list(source_rows)
        column_header = "\t".join(columns)
        random.Random(f"{shuffle_seed}:{case.publication_id}:{table_name}").shuffle(rows)
        table_sections.append(
            f"TABLE {table_name}\n"
            f"{column_header}\n"
            + "\n".join("\t".join(_tabular_value(value) for value in row) for row in rows)
        )
    database_text = "\n\n".join(table_sections)
    return (
        "Policy:\n"
        f"{POLICY_TEXT}\n\n"
        "Evidence conventions:\n"
        f"{FULL_DATABASE_EVIDENCE_CONVENTIONS}\n\n"
        "Candidate SQL publication update:\n"
        f"{case.sql}\n\n"
        "Complete input database snapshot (tab-separated; NULL means SQL NULL). "
        "Rows within each table have been independently shuffled:\n"
        f"{database_text}\n\n"
        "Should the candidate statement be published? "
        'Return JSON only: {"allow_update": true|false}\n'
    )


def reset_statement_database(conn: Any, workload: list[StatementCase]) -> None:
    """Create and populate the CCS-shaped statement reconciliation database."""
    conn.execute("DROP VIEW IF EXISTS statement_reconciliation")
    for table in (
        "statement_publications",
        "daily_balances",
        "transactions",
        "statements",
        "apr_config",
        "accounts",
        "products",
        "public_product_terms",
    ):
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.execute(
        "CREATE TABLE public_product_terms (source_record_id VARCHAR, report_date DATE, "
        "purchase_apr DOUBLE, source_url VARCHAR)"
    )
    conn.execute(
        "CREATE TABLE products (product_code VARCHAR PRIMARY KEY, product_name VARCHAR, "
        "status VARCHAR)"
    )
    conn.execute(
        "CREATE TABLE accounts (account_id VARCHAR PRIMARY KEY, product_code VARCHAR, status VARCHAR)"
    )
    conn.execute(
        "CREATE TABLE apr_config (config_id VARCHAR PRIMARY KEY, product_code VARCHAR, "
        "apr_type VARCHAR, rate_value DOUBLE, approved_by VARCHAR, approval_date DATE)"
    )
    conn.execute(
        "CREATE TABLE statements (statement_id VARCHAR PRIMARY KEY, account_id VARCHAR, "
        "cycle_start DATE, cycle_end DATE, statement_date DATE, previous_balance DOUBLE, "
        "new_balance DOUBLE, total_purchases DOUBLE, total_cash_advances DOUBLE, "
        "total_payments DOUBLE, total_fees DOUBLE, total_interest DOUBLE, purchase_apr DOUBLE)"
    )
    conn.execute(
        "CREATE TABLE transactions (transaction_id VARCHAR PRIMARY KEY, account_id VARCHAR, "
        "statement_id VARCHAR, transaction_date DATE, transaction_type VARCHAR, "
        "balance_category VARCHAR, amount DOUBLE)"
    )
    conn.execute(
        "CREATE TABLE daily_balances (account_id VARCHAR, statement_id VARCHAR, "
        "balance_date DATE, balance_category VARCHAR, daily_balance DOUBLE, "
        "apr_applied DOUBLE, daily_periodic_rate DOUBLE)"
    )
    conn.execute(
        "CREATE TABLE statement_publications (publication_id VARCHAR PRIMARY KEY, "
        "statement_id VARCHAR, account_id VARCHAR, status VARCHAR, published_at TIMESTAMP)"
    )
    product_terms: dict[str, StatementCase] = {}
    for case in workload:
        product_terms.setdefault(case.source_record_id, case)
    for case in product_terms.values():
        conn.execute(
            "INSERT INTO public_product_terms VALUES (?, ?, ?, ?)",
            [
                case.source_record_id,
                case.source_report_date,
                case.source_purchase_apr,
                case.source_url,
            ],
        )
    for case in workload:
        conn.execute(
            "INSERT INTO products VALUES (?, ?, ?)",
            [case.product_code, f"Card Product {case.case_number:04d}", case.product_status],
        )
        conn.execute(
            "INSERT INTO accounts VALUES (?, ?, ?)",
            [case.account_id, case.product_code, case.account_status],
        )
        conn.execute(
            "INSERT INTO apr_config VALUES (?, ?, 'purchase', ?, ?, ?)",
            [
                case.config_id,
                case.product_code,
                case.approved_purchase_apr,
                case.config_approved_by,
                case.config_approval_date,
            ],
        )
        conn.execute(
            "INSERT INTO statements VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                case.statement_id,
                case.account_id,
                case.cycle_start,
                case.cycle_end,
                case.statement_date,
                case.previous_balance,
                case.new_balance,
                case.total_purchases,
                case.total_cash_advances,
                case.total_payments,
                case.total_fees,
                case.total_interest,
                case.purchase_apr,
            ],
        )
        conn.executemany(
            "INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    row.transaction_id,
                    case.account_id,
                    case.statement_id,
                    row.transaction_date,
                    row.transaction_type,
                    row.balance_category,
                    row.amount,
                )
                for row in case.transactions
            ],
        )
        conn.executemany(
            "INSERT INTO daily_balances VALUES (?, ?, ?, 'purchase', ?, ?, ?)",
            [
                (
                    case.account_id,
                    case.statement_id,
                    row.balance_date,
                    row.daily_balance,
                    row.apr_applied,
                    row.daily_periodic_rate,
                )
                for row in case.daily_balances
            ],
        )
        conn.execute(
            "INSERT INTO statement_publications VALUES (?, ?, ?, 'draft', NULL)",
            [case.publication_id, case.statement_id, case.account_id],
        )
    conn.execute(
        "CREATE VIEW statement_reconciliation AS "
        "WITH transaction_totals AS ("
        "SELECT statement_id, account_id, "
        "sum(CASE WHEN transaction_type = 'purchase' THEN amount ELSE 0 END) "
        "AS expected_total_purchases, "
        "sum(CASE WHEN transaction_type = 'cash_advance' THEN amount ELSE 0 END) "
        "AS expected_total_cash_advances, "
        "sum(CASE WHEN transaction_type = 'payment' THEN -amount ELSE 0 END) "
        "AS expected_total_payments, "
        "sum(CASE WHEN transaction_type = 'fee' THEN amount ELSE 0 END) "
        "AS expected_total_fees, "
        "sum(CASE WHEN transaction_type = 'interest' THEN amount ELSE 0 END) "
        "AS expected_total_interest, sum(amount) AS net_activity "
        "FROM transactions GROUP BY statement_id, account_id), "
        "daily_totals AS ("
        "SELECT statement_id, account_id, count(*) AS daily_day_count, "
        "count(DISTINCT balance_date) AS daily_distinct_day_count, "
        "min(balance_date) AS first_daily_balance_date, "
        "max(balance_date) AS last_daily_balance_date, "
        "round(sum(daily_balance * daily_periodic_rate / 100.0), 2) "
        "AS expected_daily_interest, min(apr_applied) AS minimum_daily_purchase_apr, "
        "max(apr_applied) AS maximum_daily_purchase_apr "
        "FROM daily_balances WHERE balance_category = 'purchase' "
        "GROUP BY statement_id, account_id) "
        "SELECT s.*, a.status AS account_status, p.status AS product_status, "
        "cfg.rate_value AS config_purchase_apr, cfg.approved_by AS config_approved_by, "
        "tx.expected_total_purchases, tx.expected_total_cash_advances, "
        "tx.expected_total_payments, tx.expected_total_fees, tx.expected_total_interest, "
        "round(s.previous_balance + tx.net_activity, 2) AS expected_new_balance, "
        "date_diff('day', s.cycle_start, s.cycle_end) + 1 AS expected_day_count, "
        "daily.daily_day_count, daily.daily_distinct_day_count, "
        "daily.first_daily_balance_date, daily.last_daily_balance_date, "
        "daily.expected_daily_interest, "
        "daily.minimum_daily_purchase_apr, daily.maximum_daily_purchase_apr "
        "FROM statements AS s "
        "JOIN accounts AS a ON a.account_id = s.account_id "
        "JOIN products AS p ON p.product_code = a.product_code "
        "JOIN apr_config AS cfg ON cfg.product_code = a.product_code "
        "AND cfg.apr_type = 'purchase' "
        "JOIN transaction_totals AS tx ON tx.statement_id = s.statement_id "
        "AND tx.account_id = s.account_id "
        "JOIN daily_totals AS daily ON daily.statement_id = s.statement_id "
        "AND daily.account_id = s.account_id"
    )


class RegulationZStatementBenchmarkStrategy(_APRBenchmarkStrategy):
    """Compare DFC, Bedrock guards, and no policy on statement publication."""

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
        catalog_path = Path(str(context.strategy_config.get("catalog_path", DEFAULT_CATALOG_PATH)))
        self.product_terms = load_product_terms(catalog_path)
        self.database_workload = generate_statement_workload(
            self.product_terms,
            num_accounts=self.num_accounts,
            num_updates=self.num_accounts,
        )
        self.workload = self.database_workload[: self.num_updates]
        self.prompt_scope = str(context.strategy_config.get("prompt_scope", "relevant_rows"))
        if self.prompt_scope not in {"relevant_rows", "full_database"}:
            raise ValueError("prompt_scope must be 'relevant_rows' or 'full_database'")
        self.prompt_shuffle_seed = int(context.strategy_config.get("prompt_shuffle_seed", 0))
        self.model_specs = [
            dict(spec)
            for spec in context.strategy_config.get("models", DEFAULT_V2_BEDROCK_MODEL_SPECS)
        ]
        self.local_duckdb = _ensure_smokedduck()
        self.no_policy_conn = self.local_duckdb.connect(":memory:")
        self.dfc_conn = self.local_duckdb.connect(":memory:")
        self.model_connections = {
            str(spec["key"]): self.local_duckdb.connect(":memory:") for spec in self.model_specs
        }
        for conn in [self.no_policy_conn, self.dfc_conn, *self.model_connections.values()]:
            self._reset_database(conn)
        self.dfc_rewriter = SQLRewriter(conn=self.dfc_conn)
        for policy in build_regulation_z_statement_policies():
            self.dfc_rewriter.register_policy(policy)

        supplied_client = context.strategy_config.get("bedrock_client")
        if supplied_client is None:
            import boto3

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
                    clients_by_region[region] = session.client("bedrock-runtime")
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

    def _reset_database(self, conn: Any) -> None:
        reset_statement_database(conn, self.database_workload)

    def _build_prompt(self, case: StatementCase, nonce: str) -> str:
        if self.prompt_scope == "full_database":
            prompt = build_full_database_statement_policy_prompt(
                case,
                self.database_workload,
                shuffle_seed=self.prompt_shuffle_seed,
            )
        else:
            prompt = build_statement_policy_prompt(case)
        return f"Run nonce: {nonce}\n{prompt}" if nonce else prompt

    @staticmethod
    def _update_applied(conn: Any, case: StatementCase) -> bool:
        status = conn.execute(
            "SELECT status FROM statement_publications WHERE publication_id = ?",
            [case.publication_id],
        ).fetchone()[0]
        return status == "published"

    def _progress_identity(self, case: StatementCase) -> dict[str, Any]:
        return {
            "publication_id": case.publication_id,
            "statement_id": case.statement_id,
            "account_id": case.account_id,
            "case_type": case.case_type,
            "source_record_id": case.source_record_id,
            "expected_allow": case.is_valid,
        }


RegulationZV2BenchmarkStrategy = RegulationZStatementBenchmarkStrategy

__all__ = [
    "AVAILABLE_V2_BEDROCK_MODEL_SPECS",
    "DEFAULT_AWS_REGION",
    "DEFAULT_CATALOG_PATH",
    "DEFAULT_MAX_CONCURRENCY",
    "DEFAULT_MEASURED_RUNS",
    "DEFAULT_NUM_ACCOUNTS",
    "DEFAULT_NUM_UPDATES",
    "DEFAULT_V2_BEDROCK_MODEL_SPECS",
    "DEFAULT_WARMUP_RUNS",
    "EVIDENCE_CONVENTIONS",
    "FULL_DATABASE_EVIDENCE_CONVENTIONS",
    "INVALID_CASE_TYPES",
    "POLICY_TEXT",
    "STATEMENT_POLICY_SPECS",
    "RegulationZStatementBenchmarkStrategy",
    "RegulationZV2BenchmarkStrategy",
    "StatementCase",
    "build_full_database_statement_policy_prompt",
    "build_regulation_z_statement_policies",
    "build_statement_policy_prompt",
    "generate_statement_workload",
    "reset_statement_database",
]
