"""Trusted batch adapter for the 40-proposition catalog.

Raw evidence is relational. Deterministic derivations below produce numeric facts,
not caller-supplied compliance verdicts. Passant guards one acceptance row; only
then are all proposed allocation rows published in the same transaction.

This is a case-study adapter, not a permission system: callers must not have an
alternate write path to context, acceptances, posted allocations, or heads.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from data_flow_control.adapters.duckdb import DuckDBAdapter
from data_flow_control.connection import Connection

from vldb_experiments.strategies.regulation_z_53_full_policy_catalog import (
    build_full_regulation_z_53_policies,
)


class EvidenceError(ValueError):
    """Missing evidence or an unsupported input, not a regulatory verdict."""


SCHEMA_DDL = (
    "CREATE TABLE regz_program_classifications (batch_id VARCHAR NOT NULL, program_id VARCHAR NOT NULL, qualifies BOOLEAN NOT NULL, PRIMARY KEY(batch_id, program_id))",
    "CREATE TABLE regz_posted_classifications (batch_id VARCHAR NOT NULL, program_id VARCHAR NOT NULL, qualifies BOOLEAN NOT NULL, PRIMARY KEY(batch_id, program_id))",
    "CREATE TABLE regz_accounts (account_id VARCHAR PRIMARY KEY, credit_card BOOLEAN NOT NULL, consumer_plan BOOLEAN NOT NULL, open_end BOOLEAN NOT NULL, home_secured BOOLEAN NOT NULL)",
    "CREATE TABLE regz_payments (payment_id VARCHAR PRIMARY KEY, account_id VARCHAR NOT NULL, credited_date DATE NOT NULL, cycle_end DATE NOT NULL, total_cents BIGINT NOT NULL, minimum_cents BIGINT NOT NULL)",
    "CREATE TABLE regz_snapshot_sets (account_id VARCHAR, snapshot_date DATE, PRIMARY KEY(account_id, snapshot_date))",
    "CREATE TABLE regz_balances (account_id VARCHAR NOT NULL, balance_id VARCHAR NOT NULL, snapshot_date DATE NOT NULL, amount_cents BIGINT NOT NULL, apr DECIMAL(12,6) NOT NULL, protected_cents BIGINT NOT NULL, secured BOOLEAN NOT NULL, program_id VARCHAR, PRIMARY KEY(account_id, balance_id, snapshot_date))",
    "CREATE TABLE regz_cycles (account_id VARCHAR NOT NULL, start_date DATE NOT NULL, end_date DATE NOT NULL, due_date DATE NOT NULL, PRIMARY KEY(account_id, start_date))",
    "CREATE TABLE regz_programs (program_id VARCHAR PRIMARY KEY, account_id VARCHAR NOT NULL, start_date DATE NOT NULL, expiration_date DATE NOT NULL, interest_accrues BOOLEAN NOT NULL, waived_on_payoff BOOLEAN NOT NULL, grace_period BOOLEAN NOT NULL, temporary_zero BOOLEAN NOT NULL, retroactive_interest BOOLEAN NOT NULL, interim_payment_required BOOLEAN NOT NULL)",
    "CREATE TABLE regz_practices (account_id VARCHAR NOT NULL, effective_date DATE NOT NULL, convention VARCHAR NOT NULL, offset_days INTEGER NOT NULL, authorization_evidence VARCHAR, PRIMARY KEY(account_id, effective_date))",
    "CREATE TABLE regz_requests (request_id VARCHAR PRIMARY KEY, payment_id VARCHAR NOT NULL, submitted_date DATE, basis VARCHAR NOT NULL, channel VARCHAR NOT NULL, affirmative BOOLEAN NOT NULL, form_completed BOOLEAN NOT NULL, required_condition BOOLEAN NOT NULL, issuer_elected BOOLEAN NOT NULL, supersedes_id VARCHAR)",
    "CREATE TABLE regz_request_lines (request_id VARCHAR NOT NULL, balance_id VARCHAR NOT NULL, amount_cents BIGINT NOT NULL, PRIMARY KEY(request_id, balance_id))",
    "CREATE TABLE regz_proposals (batch_id VARCHAR PRIMARY KEY, payment_id VARCHAR NOT NULL, snapshot_date DATE NOT NULL, decision_date DATE NOT NULL, method VARCHAR NOT NULL, request_id VARCHAR, replaces_batch_id VARCHAR)",
    "CREATE TABLE regz_proposal_lines (batch_id VARCHAR NOT NULL, balance_id VARCHAR NOT NULL, minimum_cents BIGINT NOT NULL, excess_cents BIGINT NOT NULL, effective_apr DECIMAL(12,6) NOT NULL, window_start DATE, PRIMARY KEY(batch_id, balance_id))",
    "CREATE TABLE regz_acceptances (batch_id VARCHAR PRIMARY KEY, kind VARCHAR NOT NULL)",
    "CREATE TABLE regz_posted_allocations (batch_id VARCHAR NOT NULL, payment_id VARCHAR NOT NULL, balance_id VARCHAR NOT NULL, minimum_cents BIGINT NOT NULL, excess_cents BIGINT NOT NULL, effective_apr DECIMAL(12,6) NOT NULL, window_start DATE, method VARCHAR NOT NULL, decision_date DATE NOT NULL, PRIMARY KEY(batch_id, balance_id))",
    "CREATE TABLE regz_payment_heads (payment_id VARCHAR PRIMARY KEY, batch_id VARCHAR NOT NULL)",
    "CREATE TABLE regz_error_obligations (error_id VARCHAR PRIMARY KEY, determination_date DATE NOT NULL, deadline DATE NOT NULL, recipient VARCHAR NOT NULL)",
    "CREATE TABLE regz_error_items (error_id VARCHAR NOT NULL, item_id VARCHAR NOT NULL, required_cents BIGINT NOT NULL, PRIMARY KEY(error_id, item_id))",
    "CREATE TABLE regz_credits (credit_id VARCHAR PRIMARY KEY, error_id VARCHAR NOT NULL, item_id VARCHAR NOT NULL, amount_cents BIGINT NOT NULL, posted_date DATE NOT NULL)",
    "CREATE TABLE regz_dispatches (dispatch_id VARCHAR PRIMARY KEY, error_id VARCHAR NOT NULL, recipient VARCHAR NOT NULL, notice_type VARCHAR NOT NULL, sent_date DATE NOT NULL, content_evidence VARCHAR NOT NULL)",
    "CREATE TABLE regz_correction_completions (error_id VARCHAR PRIMARY KEY, checkpoint_date DATE NOT NULL)",
)

_ZERO_FIELDS = [
    "highest_violations",
    "remaining_violations",
    "ordinary_violations",
    "deferred_violations",
    "snapshot_violations",
    "early_expiry_violations",
    "active_programs",
    "window_programs",
    "unauthorized_changes",
    "protected_reduction",
    "unavoidable_protected_reduction",
    "other_capacity_remaining",
    "effective_apr_mismatches",
    "window_start_mismatches",
    "request_mismatches",
    "unsecured_request_targets",
    "request_feasibility_errors",
    "superseding_requests",
    "instruction_count",
    "missing_credit_cents",
    "notice_count",
    "classification_mismatches",
    "interim_payment_omissions",
    "grace_classifications",
    "zero_offer_classifications",
]
_BOOL_FIELDS = [
    "issuer_elected",
    "request_present",
    "affirmative",
    "contact_channel",
    "form_completed",
    "prior_default_allocation",
    "required_condition",
]
_DATE_FIELDS = ["snapshot_date", "cycle_end", "credited_date", "expected_snapshot_date"]
_CONTEXT_TYPES = {
    "batch_id": "VARCHAR",
    "kind": "VARCHAR",
    "method": "VARCHAR",
    "request_basis": "VARCHAR",
    **dict.fromkeys(_ZERO_FIELDS, "BIGINT"),
    **dict.fromkeys(_BOOL_FIELDS, "BOOLEAN"),
    **dict.fromkeys(_DATE_FIELDS, "DATE"),
}


def create_schema(conn: Any) -> None:
    for ddl in SCHEMA_DDL:
        conn.execute(ddl)
    columns = ", ".join(f"{name} {sql_type} NOT NULL" for name, sql_type in _CONTEXT_TYPES.items())
    conn.execute(f"CREATE TABLE regz_context ({columns}, PRIMARY KEY(batch_id))")


def _rows(conn: Any, query: str, parameters: list | None = None) -> list[dict]:
    cursor = conn.execute(query, parameters or [])
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _one(conn: Any, query: str, parameters: list) -> dict:
    rows = _rows(conn, query, parameters)
    if len(rows) != 1:
        raise EvidenceError("Expected exactly one evidence row")
    return rows[0]


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise EvidenceError(message)


def _context(batch_id: str, kind: str) -> dict:
    return {
        "batch_id": batch_id,
        "kind": kind,
        "method": "default",
        "request_basis": "none",
        **dict.fromkeys(_ZERO_FIELDS, 0),
        **dict.fromkeys(_BOOL_FIELDS, False),
        **dict.fromkeys(_DATE_FIELDS, date(1900, 1, 1)),
    }


def _qualifies(program: dict) -> bool:
    return (
        program["interest_accrues"]
        and program["waived_on_payoff"]
        and not program["grace_period"]
        and (not program["temporary_zero"] or program["retroactive_interest"])
    )


def _window(conn: Any, account: str, expiration: date) -> tuple[date, date | None]:
    cycles = _rows(
        conn, "SELECT * FROM regz_cycles WHERE account_id = ? ORDER BY start_date", [account]
    )
    containing = [c for c in cycles if c["start_date"] <= expiration <= c["end_date"]]
    _require(len(containing) == 1, "Need the actual expiration billing cycle")
    for previous, following in zip(cycles, cycles[1:]):
        _require(
            previous["end_date"] + timedelta(days=1) == following["start_date"],
            "Billing cycles must be contiguous and nonoverlapping",
        )
    eligible = [c for c in cycles if c["start_date"] <= expiration and c["due_date"] <= expiration]
    _require(len(eligible) >= 2, "Need the two actual billing cycles preceding expiration")
    early_start = containing[0]["start_date"] if expiration < containing[0]["due_date"] else None
    return eligible[-2]["start_date"], early_start


def _practice(conn: Any, payment: dict, ctx: dict) -> None:
    practices = _rows(
        conn,
        "SELECT * FROM regz_practices WHERE account_id = ? AND effective_date <= ? ORDER BY effective_date",
        [payment["account_id"], payment["credited_date"]],
    )
    _require(bool(practices), "Missing effective snapshot convention")
    for index, practice in enumerate(practices):
        _require(
            practice["convention"] in {"cycle_end", "credit_date", "offset"},
            "Unsupported snapshot convention",
        )
        changed = index > 0 and (practice["convention"], practice["offset_days"]) != (
            practices[index - 1]["convention"],
            practices[index - 1]["offset_days"],
        )
        ctx["unauthorized_changes"] += int(changed and not practice["authorization_evidence"])
    practice = practices[-1]
    if practice["convention"] == "credit_date":
        expected = payment["credited_date"]
    else:
        expected = payment["cycle_end"] + timedelta(
            days=practice["offset_days"] if practice["convention"] == "offset" else 0
        )
    ctx["expected_snapshot_date"] = expected


def _request_facts(
    conn: Any, proposal: dict, payment: dict, balances: dict, lines: dict, ctx: dict
) -> None:
    if proposal["request_id"] is None or proposal["method"] == "default":
        # Declining a request must not require interpreting or validating it.
        return
    request = _one(
        conn, "SELECT * FROM regz_requests WHERE request_id = ?", [proposal["request_id"]]
    )
    _require(
        request["payment_id"] == payment["payment_id"], "Request belongs to a different payment"
    )
    _require(
        request["basis"]
        in {
            "contact",
            "form",
            "reallocation",
            "agreement_default",
            "application_default",
            "coupon_default",
        },
        "Unmodeled request evidence needs review, not automatic rejection as illegal",
    )
    ctx.update(
        request_basis=request["basis"],
        issuer_elected=request["issuer_elected"],
        affirmative=request["affirmative"],
        form_completed=request["form_completed"],
        required_condition=request["required_condition"],
        contact_channel=request["channel"] in {"oral", "electronic", "written"},
        request_present=request["submitted_date"] is not None
        and request["submitted_date"] <= proposal["decision_date"],
    )
    instructions = _rows(
        conn, "SELECT * FROM regz_request_lines WHERE request_id = ?", [request["request_id"]]
    )
    ctx["instruction_count"] = len(instructions)
    target = {row["balance_id"]: row["amount_cents"] for row in instructions}
    ctx["request_feasibility_errors"] = int(not instructions)
    ctx["request_feasibility_errors"] += int(
        sum(target.values()) != payment["total_cents"] - payment["minimum_cents"]
    )
    for balance_id, amount in target.items():
        invalid = balance_id not in balances or amount < 0
        if balance_id in balances:
            invalid |= (
                amount > balances[balance_id]["amount_cents"] - lines[balance_id]["minimum_cents"]
            )
            ctx["unsecured_request_targets"] += int(
                amount > 0 and not balances[balance_id]["secured"]
            )
        ctx["request_feasibility_errors"] += int(invalid)
    ctx["request_mismatches"] = sum(
        int(lines[key]["excess_cents"] != target.get(key, 0)) for key in lines
    )
    ctx["request_mismatches"] += len(set(target) - set(lines))
    # Supersession is an evidence link, not an assumption that the latest arrival
    # necessarily cancels every previous instruction.
    replacements = _rows(
        conn,
        "SELECT request_id FROM regz_requests WHERE payment_id = ? AND supersedes_id = ? AND submitted_date <= ?",
        [payment["payment_id"], request["request_id"], proposal["decision_date"]],
    )
    ctx["superseding_requests"] = len(replacements)
    if proposal["replaces_batch_id"] and request["submitted_date"]:
        prior = _rows(
            conn,
            "SELECT DISTINCT method, decision_date FROM regz_posted_allocations WHERE batch_id = ? AND payment_id = ?",
            [proposal["replaces_batch_id"], payment["payment_id"]],
        )
        ctx["prior_default_allocation"] = bool(prior) and all(
            row["method"] == "default" and row["decision_date"] <= request["submitted_date"]
            for row in prior
        )


def derive_allocation_context(conn: Any, batch_id: str) -> dict:
    """Recompute all facts from complete source snapshots and proposed line items."""
    proposal = _one(conn, "SELECT * FROM regz_proposals WHERE batch_id = ?", [batch_id])
    payment = _one(
        conn, "SELECT * FROM regz_payments WHERE payment_id = ?", [proposal["payment_id"]]
    )
    account = _one(
        conn, "SELECT * FROM regz_accounts WHERE account_id = ?", [payment["account_id"]]
    )
    _require(
        account["credit_card"]
        and account["consumer_plan"]
        and account["open_end"]
        and not account["home_secured"],
        "Out-of-scope accounts need another workflow; this is not a legal rejection",
    )
    _require(
        proposal["method"] in {"default", "deferred_request", "secured_request"},
        "Unknown allocation method",
    )
    _require(
        proposal["decision_date"] >= payment["credited_date"], "Decision cannot precede crediting"
    )
    _require(
        0 <= payment["minimum_cents"] <= payment["total_cents"],
        "Invalid externally supplied minimum portion",
    )
    for snapshot in {proposal["snapshot_date"], payment["credited_date"]}:
        _one(
            conn,
            "SELECT * FROM regz_snapshot_sets WHERE account_id = ? AND snapshot_date = ?",
            [payment["account_id"], snapshot],
        )
    current = _rows(
        conn,
        "SELECT * FROM regz_balances WHERE account_id = ? AND snapshot_date = ?",
        [payment["account_id"], payment["credited_date"]],
    )
    historical = {
        row["balance_id"]: row
        for row in _rows(
            conn,
            "SELECT * FROM regz_balances WHERE account_id = ? AND snapshot_date = ?",
            [payment["account_id"], proposal["snapshot_date"]],
        )
    }
    balances = {row["balance_id"]: row for row in current}
    lines = {
        row["balance_id"]: row
        for row in _rows(conn, "SELECT * FROM regz_proposal_lines WHERE batch_id = ?", [batch_id])
    }
    _require(
        bool(balances) and set(lines) == set(balances),
        "Proposal must enumerate every current balance, including zero allocations",
    )
    _require(
        sum(row["minimum_cents"] for row in lines.values()) == payment["minimum_cents"],
        "Minimum portions do not conserve the supplied minimum",
    )
    _require(
        sum(row["excess_cents"] for row in lines.values())
        == payment["total_cents"] - payment["minimum_cents"],
        "Excess allocations must conserve the whole excess",
    )
    heads = _rows(
        conn, "SELECT * FROM regz_payment_heads WHERE payment_id = ?", [payment["payment_id"]]
    )
    _require(
        (not heads and proposal["replaces_batch_id"] is None)
        or (len(heads) == 1 and heads[0]["batch_id"] == proposal["replaces_batch_id"]),
        "Duplicate payment or stale replacement",
    )
    ctx = _context(batch_id, "allocation")
    ctx.update(
        method=proposal["method"],
        snapshot_date=proposal["snapshot_date"],
        cycle_end=payment["cycle_end"],
        credited_date=payment["credited_date"],
    )
    _practice(conn, payment, ctx)
    declarations = {
        row["program_id"]: row["qualifies"]
        for row in _rows(
            conn, "SELECT * FROM regz_program_classifications WHERE batch_id = ?", [batch_id]
        )
    }
    referenced_programs = {
        row["program_id"] for row in balances.values() if row["program_id"] is not None
    }
    _require(
        set(declarations) == referenced_programs,
        "Every referenced program needs a proposed classification",
    )
    slices = []
    active_programs, window_programs = set(), set()
    total_undisputed = 0
    for balance_id, balance in balances.items():
        line = lines[balance_id]
        amount, protected = balance["amount_cents"], balance["protected_cents"]
        minimum, excess = line["minimum_cents"], line["excess_cents"]
        _require(
            0 <= protected <= amount
            and minimum >= 0
            and excess >= 0
            and minimum + excess <= amount,
            "Negative amounts or allocation beyond balance capacity",
        )
        old = historical.get(balance_id)
        # Atomic lots never increase: new principal must have a new lot ID. This
        # prevents silently applying an old APR to later additions in one bucket.
        if old:
            _require(
                amount <= old["amount_cents"], "Growing balance IDs require explicit lot splitting"
            )
        apr = old["apr"] if old else balance["apr"]
        _require(apr >= 0, "Negative APR outside the modeled domain")
        window_start, in_window, early = None, False, False
        if balance["program_id"] is not None:
            program = _one(
                conn, "SELECT * FROM regz_programs WHERE program_id = ?", [balance["program_id"]]
            )
            _require(
                program["account_id"] == payment["account_id"], "Program belongs to another account"
            )
            _require(program["start_date"] <= program["expiration_date"], "Invalid program dates")
            declared = declarations[program["program_id"]]
            qualifies = _qualifies(program)
            ctx["classification_mismatches"] += int(declared != qualifies)
            ctx["interim_payment_omissions"] += int(
                program["interim_payment_required"] and qualifies and not declared
            )
            ctx["grace_classifications"] += int(program["grace_period"] and declared)
            ctx["zero_offer_classifications"] += int(
                program["temporary_zero"] and not program["retroactive_interest"] and declared
            )
            active = (
                _qualifies(program)
                and program["start_date"] <= payment["credited_date"] <= program["expiration_date"]
            )
            if active:
                apr = Decimal(0)
                active_programs.add(program["program_id"])
                window_start, expiration_cycle_start = _window(
                    conn, payment["account_id"], program["expiration_date"]
                )
                in_window = window_start <= payment["credited_date"] <= program["expiration_date"]
                early = (
                    in_window
                    and expiration_cycle_start is not None
                    and expiration_cycle_start <= payment["credited_date"]
                )
                if in_window:
                    window_programs.add(program["program_id"])
        ctx["effective_apr_mismatches"] += int(line["effective_apr"] != apr)
        ctx["window_start_mismatches"] += int(line["window_start"] != window_start)
        undisputed = amount - protected
        total_undisputed += undisputed
        ctx["protected_reduction"] += max(0, minimum + excess - undisputed)
        ctx["other_capacity_remaining"] += max(0, undisputed - minimum - excess)
        # Minimum allocation is free of APR priority. Within each atomic lot,
        # consume its undisputed portion before its disputed portion.
        available_u = max(0, undisputed - minimum)
        available_p = protected - max(0, minimum - undisputed)
        for disputed, capacity, allocated in (
            (False, available_u, min(excess, available_u)),
            (True, available_p, max(0, excess - available_u)),
        ):
            if capacity:
                slices.append(
                    {
                        "disputed": disputed,
                        "capacity": capacity,
                        "allocated": allocated,
                        "window": in_window,
                        "early": early,
                        "phase": int(old is None),
                        "apr": apr,
                    }
                )
    ctx["active_programs"], ctx["window_programs"] = len(active_programs), len(window_programs)
    ctx["unavoidable_protected_reduction"] = max(0, payment["total_cents"] - total_undisputed)
    _priority_facts(slices, ctx)
    _request_facts(conn, proposal, payment, balances, lines, ctx)
    return ctx


def _priority_facts(slices: list[dict], ctx: dict) -> None:
    """Pairwise exhaustion permits every split within genuinely equal priorities."""
    for high in slices:
        for low in slices:
            if (
                high["disputed"] != low["disputed"]
                or high["allocated"] >= high["capacity"]
                or low["allocated"] == 0
            ):
                continue
            if high["window"] and not low["window"]:
                ctx["deferred_violations"] += 1
                ctx["early_expiry_violations"] += int(high["early"])
            elif not high["window"] and not low["window"]:
                if high["phase"] < low["phase"]:
                    ctx["snapshot_violations"] += 1
                elif high["phase"] == low["phase"] and high["apr"] > low["apr"]:
                    peers = [
                        s["apr"]
                        for s in slices
                        if not s["window"]
                        and s["phase"] == high["phase"]
                        and s["disputed"] == high["disputed"]
                    ]
                    ctx["ordinary_violations"] += 1
                    field = (
                        "highest_violations"
                        if high["apr"] == max(peers)
                        else "remaining_violations"
                    )
                    ctx[field] += 1


def derive_correction_context(conn: Any, error_id: str, checkpoint_date: date) -> dict:
    obligation = _one(conn, "SELECT * FROM regz_error_obligations WHERE error_id = ?", [error_id])
    _require(
        checkpoint_date >= obligation["determination_date"], "Checkpoint precedes determination"
    )
    ctx = _context(f"correction:{error_id}", "correction")
    cutoff = min(checkpoint_date, obligation["deadline"])
    items = _rows(conn, "SELECT * FROM regz_error_items WHERE error_id = ?", [error_id])
    _require(bool(items), "Missing required correction items")
    for item in items:
        _require(item["required_cents"] >= 0, "Invalid correction amount")
        credits = _rows(
            conn,
            "SELECT amount_cents FROM regz_credits WHERE error_id = ? AND item_id = ? AND posted_date BETWEEN ? AND ?",
            [error_id, item["item_id"], obligation["determination_date"], cutoff],
        )
        # Negative rows represent reversals and reduce the actual credit.
        credited = sum(row["amount_cents"] for row in credits)
        ctx["missing_credit_cents"] += max(0, item["required_cents"] - credited)
    notices = _rows(
        conn,
        "SELECT dispatch_id FROM regz_dispatches WHERE error_id = ? AND recipient = ? AND notice_type = 'correction' AND sent_date BETWEEN ? AND ? AND length(trim(content_evidence)) > 0",
        [error_id, obligation["recipient"], obligation["determination_date"], cutoff],
    )
    ctx["notice_count"] = len(notices)
    return ctx


def _accept(conn: Any, ctx: dict) -> bool:
    columns = list(_CONTEXT_TYPES)
    conn.execute(
        f"INSERT INTO regz_context ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
        [ctx[key] for key in columns],
    )
    rewriter = Connection(DuckDBAdapter(conn))
    for policy in build_full_regulation_z_53_policies():
        rewriter.register_policy(policy)
    # IDs are data, never SQL fragments. Escaping is needed because the rewriter
    # transforms SQL text before execution; no shell interpolation is involved.
    literal = "'" + ctx["batch_id"].replace("'", "''") + "'"
    query = (
        "INSERT INTO regz_acceptances SELECT regz_context.batch_id, regz_context.kind FROM regz_context WHERE regz_context.batch_id = "
        + literal
    )
    conn.execute(rewriter.transform_query(query))
    return bool(
        conn.execute(
            "SELECT 1 FROM regz_acceptances WHERE batch_id = ?", [ctx["batch_id"]]
        ).fetchall()
    )


def submit_allocation(conn: Any, batch_id: str) -> bool:
    """Own the transaction. False means all publication was rolled back."""
    conn.execute("BEGIN TRANSACTION")
    try:
        ctx = derive_allocation_context(conn, batch_id)
        if not _accept(conn, ctx):
            conn.execute("ROLLBACK")
            return False
        conn.execute(
            "INSERT INTO regz_posted_allocations SELECT p.batch_id, p.payment_id, l.balance_id, l.minimum_cents, l.excess_cents, l.effective_apr, l.window_start, p.method, p.decision_date FROM regz_proposals p JOIN regz_proposal_lines l ON p.batch_id = l.batch_id WHERE p.batch_id = ?",
            [batch_id],
        )
        conn.execute(
            "INSERT INTO regz_posted_classifications SELECT * FROM regz_program_classifications WHERE batch_id = ?",
            [batch_id],
        )
        conn.execute(
            "DELETE FROM regz_payment_heads WHERE payment_id = (SELECT payment_id FROM regz_proposals WHERE batch_id = ?)",
            [batch_id],
        )
        conn.execute(
            "INSERT INTO regz_payment_heads SELECT payment_id, batch_id FROM regz_proposals WHERE batch_id = ?",
            [batch_id],
        )
        conn.execute("COMMIT")
        return True
    except Exception:
        conn.execute("ROLLBACK")
        raise


def complete_correction(conn: Any, error_id: str, checkpoint_date: date) -> bool:
    """Check actual credit/dispatch evidence; no action scheduling is implied."""
    conn.execute("BEGIN TRANSACTION")
    try:
        ctx = derive_correction_context(conn, error_id, checkpoint_date)
        if not _accept(conn, ctx):
            conn.execute("ROLLBACK")
            return False
        conn.execute(
            "INSERT INTO regz_correction_completions VALUES (?, ?)", [error_id, checkpoint_date]
        )
        conn.execute("COMMIT")
        return True
    except Exception:
        conn.execute("ROLLBACK")
        raise
