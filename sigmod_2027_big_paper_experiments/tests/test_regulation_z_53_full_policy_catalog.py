from __future__ import annotations

from datetime import date

from data_flow_control.adapters.duckdb import DuckDBAdapter
from data_flow_control.connection import Connection
import duckdb
import pytest

from vldb_experiments.strategies.regulation_z_53_full_policy_catalog import (
    FULL_REGULATION_Z_53_POLICY_SPECS,
    build_full_regulation_z_53_policies,
)
from vldb_experiments.strategies.regulation_z_53_runtime import (
    EvidenceError,
    complete_correction,
    create_schema,
    derive_allocation_context,
    submit_allocation,
)
from vldb_experiments.strategies.regulation_z_payment_allocation_benchmark import (
    build_regulation_z_53_policies,
)


@pytest.fixture
def conn():
    database = duckdb.connect(":memory:")
    create_schema(database)
    yield database
    database.close()


def _base(conn, *, total=80000, close="2026-01-31", credited="2026-02-25"):
    conn.execute("INSERT INTO regz_accounts VALUES ('A', true, true, true, false)")
    conn.execute(
        "INSERT INTO regz_payments VALUES ('PAY', 'A', ?, ?, ?, 0)", [credited, close, total]
    )
    conn.execute("INSERT INTO regz_practices VALUES ('A', DATE '2026-01-01', 'cycle_end', 0, NULL)")
    for snapshot in {close, credited}:
        conn.execute("INSERT INTO regz_snapshot_sets VALUES ('A', ?)", [snapshot])
        conn.execute(
            "INSERT INTO regz_balances VALUES ('A', 'H', ?, 50000, 20, 0, false, NULL), ('A', 'L', ?, 100000, 15, 0, false, NULL)",
            [snapshot, snapshot],
        )
    conn.execute(
        "INSERT INTO regz_proposals VALUES ('B', 'PAY', ?, ?, 'default', NULL, NULL)",
        [close, credited],
    )
    conn.execute(
        "INSERT INTO regz_proposal_lines VALUES ('B', 'H', 0, 50000, 20, NULL), ('B', 'L', 0, ?, 15, NULL)",
        [total - 50000],
    )


def _allocate(conn, high, low):
    conn.execute(
        "UPDATE regz_proposal_lines SET excess_cents = CASE balance_id WHEN 'H' THEN ? ELSE ? END",
        [high, low],
    )


def _program(conn, *, expiry="2026-06-30", balance="H", program="PROMO"):
    conn.execute(
        "INSERT INTO regz_programs VALUES (?, 'A', DATE '2026-01-01', ?, true, true, false, false, true, false)",
        [program, expiry],
    )
    conn.execute("UPDATE regz_balances SET program_id = ? WHERE balance_id = ?", [program, balance])
    if not conn.execute("SELECT 1 FROM regz_cycles").fetchall():
        for month, end in ((1, 31), (2, 28), (3, 31), (4, 30), (5, 31), (6, 30)):
            conn.execute(
                "INSERT INTO regz_cycles VALUES ('A', ?, ?, ?)",
                [date(2026, month, 1), date(2026, month, end), date(2026, month, 25)],
            )
    conn.execute("INSERT INTO regz_program_classifications VALUES ('B', ?, true)", [program])
    start = "2026-04-01" if expiry == "2026-06-15" else "2026-05-01"
    conn.execute(
        "UPDATE regz_proposal_lines SET effective_apr = 0, window_start = ? WHERE balance_id = ?",
        [start, balance],
    )


def _request(conn, *, basis="contact", method="deferred_request", high=50000, low=30000):
    conn.execute(
        "INSERT INTO regz_requests SELECT 'R', payment_id, decision_date, ?, 'oral', true, true, false, true, NULL FROM regz_proposals WHERE batch_id = 'B'",
        [basis],
    )
    conn.execute("INSERT INTO regz_request_lines VALUES ('R', 'H', ?), ('R', 'L', ?)", [high, low])
    conn.execute(
        "UPDATE regz_proposals SET method = ?, request_id = 'R' WHERE batch_id = 'B'", [method]
    )


def test_catalog_preserves_40_ids_without_fake_policies(conn):
    assert [spec.proposition_id for spec in FULL_REGULATION_Z_53_POLICY_SPECS] == [
        f"P{i:02d}" for i in range(1, 41)
    ]
    assert [
        spec.number for spec in FULL_REGULATION_Z_53_POLICY_SPECS if spec.constraint is None
    ] == [9, 10, 11, 12, 21, 22, 24, 30, 31]
    policies = build_full_regulation_z_53_policies()
    assert len(policies) == 31
    assert len(build_regulation_z_53_policies()) == 31
    rewriter = Connection(DuckDBAdapter(conn))
    for policy in policies:
        rewriter.register_policy(policy)
    for spec in FULL_REGULATION_Z_53_POLICY_SPECS:
        if spec.constraint is None:
            with pytest.raises(ValueError, match="no standalone PGN"):
                spec.to_pgn()
        else:
            assert (
                spec.to_pgn()
                == f"SOURCE regz_context\nSINK regz_acceptances\nCONSTRAINT\n    {spec.constraint}\nON FAIL REMOVE"
            )


@pytest.mark.parametrize(("high", "low", "accepted"), [(50000, 30000, True), (40000, 40000, False)])
def test_highest_priority_and_atomic_rejection(conn, high, low, accepted):
    _base(conn)
    _allocate(conn, high, low)
    assert submit_allocation(conn, "B") is accepted
    assert conn.execute("SELECT count(*) FROM regz_posted_allocations").fetchone()[0] == (
        2 if accepted else 0
    )
    assert conn.execute("SELECT count(*) FROM regz_acceptances").fetchone()[0] == int(accepted)
    assert conn.execute("SELECT count(*) FROM regz_context").fetchone()[0] == int(accepted)


def test_remainder_checks_rates_below_the_highest(conn):
    _base(conn, total=40000)
    conn.execute("UPDATE regz_balances SET amount_cents = 10000, apr = 25 WHERE balance_id = 'H'")
    conn.execute(
        "INSERT INTO regz_balances SELECT account_id, 'M', snapshot_date, 20000, 20, 0, false, NULL FROM regz_balances WHERE balance_id = 'H'"
    )
    conn.execute(
        "UPDATE regz_proposal_lines SET effective_apr = 25, excess_cents = 10000 WHERE balance_id = 'H'"
    )
    conn.execute("UPDATE regz_proposal_lines SET excess_cents = 20000 WHERE balance_id = 'L'")
    conn.execute("INSERT INTO regz_proposal_lines VALUES ('B', 'M', 0, 10000, 20, NULL)")
    facts = derive_allocation_context(conn, "B")
    assert facts["highest_violations"] == 0
    assert facts["remaining_violations"] == 1
    assert submit_allocation(conn, "B") is False


def test_ties_and_minimum_allocation_are_free(conn):
    _base(conn)
    conn.execute("UPDATE regz_balances SET apr = 20")
    conn.execute("UPDATE regz_proposal_lines SET effective_apr = 20")
    conn.execute("UPDATE regz_payments SET minimum_cents = 20000")
    conn.execute(
        "UPDATE regz_proposal_lines SET minimum_cents = 20000, excess_cents = 60000 WHERE balance_id = 'L'"
    )
    conn.execute("UPDATE regz_proposal_lines SET excess_cents = 0 WHERE balance_id = 'H'")
    assert submit_allocation(conn, "B") is True


def test_minimum_may_go_to_lower_rate(conn):
    _base(conn)
    conn.execute("UPDATE regz_payments SET minimum_cents = 30000")
    conn.execute(
        "UPDATE regz_proposal_lines SET minimum_cents = 30000, excess_cents = 0 WHERE balance_id = 'L'"
    )
    assert submit_allocation(conn, "B") is True


@pytest.mark.parametrize(
    "change",
    [
        "DELETE FROM regz_proposal_lines WHERE balance_id = 'H'",
        "UPDATE regz_proposal_lines SET excess_cents = 0",
        "UPDATE regz_proposal_lines SET excess_cents = -1 WHERE balance_id = 'H'",
        "DELETE FROM regz_snapshot_sets",
    ],
)
def test_missing_or_malformed_batch_fails_closed(conn, change):
    _base(conn)
    conn.execute(change)
    with pytest.raises(EvidenceError):
        submit_allocation(conn, "B")
    assert conn.execute("SELECT count(*) FROM regz_acceptances").fetchone()[0] == 0


@pytest.mark.parametrize(("high", "low", "accepted"), [(50000, 30000, True), (40000, 40000, False)])
def test_historical_snapshot_residual(conn, high, low, accepted):
    _base(conn)
    conn.execute(
        "DELETE FROM regz_balances WHERE balance_id = 'L' AND snapshot_date = DATE '2026-01-31'"
    )
    conn.execute("UPDATE regz_balances SET apr = 30 WHERE balance_id = 'L'")
    conn.execute("UPDATE regz_proposal_lines SET effective_apr = 30 WHERE balance_id = 'L'")
    _allocate(conn, high, low)
    assert submit_allocation(conn, "B") is accepted


@pytest.mark.parametrize(
    ("total", "high", "low", "accepted"),
    [(90000, 30000, 60000, True), (90000, 50000, 40000, False), (140000, 40000, 100000, True)],
)
def test_disputed_minimization_and_unavoidable_residual(conn, total, high, low, accepted):
    _base(conn, total=total)
    conn.execute("UPDATE regz_balances SET protected_cents = 20000 WHERE balance_id = 'H'")
    _allocate(conn, high, low)
    assert submit_allocation(conn, "B") is accepted


def test_dispute_protection_includes_minimum(conn):
    _base(conn)
    conn.execute("UPDATE regz_balances SET protected_cents = 50000 WHERE balance_id = 'H'")
    conn.execute("UPDATE regz_payments SET minimum_cents = 10000")
    conn.execute(
        "UPDATE regz_proposal_lines SET minimum_cents = 10000, excess_cents = 0 WHERE balance_id = 'H'"
    )
    conn.execute("UPDATE regz_proposal_lines SET excess_cents = 70000 WHERE balance_id = 'L'")
    assert submit_allocation(conn, "B") is False


@pytest.mark.parametrize(
    ("credited", "close", "high", "low", "accepted"),
    [
        ("2026-02-25", "2026-01-31", 0, 80000, True),
        ("2026-02-25", "2026-01-31", 50000, 30000, False),
        ("2026-05-25", "2026-04-30", 50000, 30000, True),
        ("2026-05-25", "2026-04-30", 40000, 40000, False),
    ],
)
def test_program_default_before_and_during_window(conn, credited, close, high, low, accepted):
    _base(conn, credited=credited, close=close)
    _program(conn)
    _allocate(conn, high, low)
    assert submit_allocation(conn, "B") is accepted


def test_early_expiration_window_is_computed(conn):
    _base(conn, credited="2026-06-10", close="2026-05-31")
    _program(conn, expiry="2026-06-15")
    facts = derive_allocation_context(conn, "B")
    assert facts["window_start_mismatches"] == 0
    assert facts["window_programs"] == 1
    _allocate(conn, 40000, 40000)
    assert derive_allocation_context(conn, "B")["early_expiry_violations"] == 1
    assert submit_allocation(conn, "B") is False
    _allocate(conn, 50000, 30000)
    assert submit_allocation(conn, "B") is True


def test_window_and_apr_cannot_be_falsified(conn):
    _base(conn)
    _program(conn)
    _allocate(conn, 0, 80000)
    conn.execute(
        "UPDATE regz_proposal_lines SET window_start = DATE '2026-06-01', effective_apr = 20 WHERE balance_id = 'H'"
    )
    facts = derive_allocation_context(conn, "B")
    assert (facts["window_start_mismatches"], facts["effective_apr_mismatches"]) == (1, 1)
    assert submit_allocation(conn, "B") is False


def test_multiple_deferred_programs_share_priority(conn):
    _base(conn, credited="2026-05-25", close="2026-04-30")
    _program(conn)
    _program(conn, balance="L", program="SECOND")
    _allocate(conn, 0, 80000)
    assert submit_allocation(conn, "B") is True


@pytest.mark.parametrize(
    ("field", "value"),
    [("grace_period", True), ("waived_on_payoff", False), ("interest_accrues", False)],
)
def test_program_exclusions_restore_actual_apr(conn, field, value):
    _base(conn)
    _program(conn)
    conn.execute(f"UPDATE regz_programs SET {field} = ?", [value])
    conn.execute("UPDATE regz_program_classifications SET qualifies = false")
    conn.execute(
        "UPDATE regz_proposal_lines SET window_start = NULL, effective_apr = 20 WHERE balance_id = 'H'"
    )
    assert submit_allocation(conn, "B") is True


def test_temporary_zero_and_interim_payment_classification(conn):
    _base(conn)
    _program(conn)
    conn.execute(
        "UPDATE regz_programs SET temporary_zero = true, retroactive_interest = false, interim_payment_required = true"
    )
    conn.execute(
        "UPDATE regz_proposal_lines SET window_start = NULL, effective_apr = 20 WHERE balance_id = 'H'"
    )
    assert derive_allocation_context(conn, "B")["active_programs"] == 0
    conn.execute("UPDATE regz_programs SET retroactive_interest = true")
    assert derive_allocation_context(conn, "B")["active_programs"] == 1


@pytest.mark.parametrize(("authorized", "accepted"), [(None, False), ("recorded-approval", True)])
def test_snapshot_adjustment_requires_explicit_evidence(conn, authorized, accepted):
    _base(conn)
    conn.execute(
        "INSERT INTO regz_practices VALUES ('A', DATE '2026-02-01', 'credit_date', 0, ?)",
        [authorized],
    )
    conn.execute("UPDATE regz_proposals SET snapshot_date = DATE '2026-02-25'")
    assert submit_allocation(conn, "B") is accepted


def test_snapshot_must_follow_convention(conn):
    _base(conn)
    conn.execute("UPDATE regz_proposals SET snapshot_date = DATE '2026-02-25'")
    assert submit_allocation(conn, "B") is False


@pytest.mark.parametrize(
    ("basis", "accepted"),
    [
        ("contact", True),
        ("form", True),
        ("agreement_default", False),
        ("application_default", False),
        ("coupon_default", False),
    ],
)
def test_request_routes_preserve_default_and_explicit_choice(conn, basis, accepted):
    _base(conn)
    _program(conn)
    _request(conn, basis=basis)
    assert submit_allocation(conn, "B") is accepted


def test_deferred_request_can_target_ordinary_balance_in_window(conn):
    _base(conn, credited="2026-05-25", close="2026-04-30")
    _program(conn)
    _allocate(conn, 0, 80000)
    _request(conn, high=0, low=80000)
    assert submit_allocation(conn, "B") is True


@pytest.mark.parametrize(
    "change",
    [
        "UPDATE regz_requests SET required_condition = true",
        "UPDATE regz_requests SET issuer_elected = false",
        "UPDATE regz_requests SET affirmative = false",
        "UPDATE regz_requests SET submitted_date = NULL",
        "UPDATE regz_request_lines SET amount_cents = 40000 WHERE balance_id = 'L'",
        "UPDATE regz_requests SET basis = 'form', form_completed = false",
    ],
)
def test_invalid_request_evidence_cannot_enable_exception(conn, change):
    _base(conn)
    _program(conn)
    _request(conn)
    conn.execute(change)
    assert submit_allocation(conn, "B") is False


def test_request_does_not_override_dispute_protection(conn):
    _base(conn)
    _program(conn)
    _request(conn)
    conn.execute("UPDATE regz_balances SET protected_cents = 50000 WHERE balance_id = 'H'")
    assert submit_allocation(conn, "B") is False


def test_missing_request_cannot_enable_exception(conn):
    _base(conn)
    _program(conn)
    conn.execute("UPDATE regz_proposals SET method = 'deferred_request'")
    assert submit_allocation(conn, "B") is False


def test_invalid_request_can_be_declined(conn):
    _base(conn)
    _request(conn, method="default", high=90000, low=0)
    assert submit_allocation(conn, "B") is True


def test_secured_request(conn):
    _base(conn, total=40000)
    _allocate(conn, 40000, 0)
    conn.execute("UPDATE regz_balances SET apr = 5, secured = true WHERE balance_id = 'H'")
    conn.execute("UPDATE regz_proposal_lines SET effective_apr = 5 WHERE balance_id = 'H'")
    _request(conn, method="secured_request", high=40000, low=0)
    assert submit_allocation(conn, "B") is True


def test_superseded_request_cannot_be_honored(conn):
    _base(conn)
    _program(conn)
    _request(conn)
    conn.execute(
        "INSERT INTO regz_requests SELECT 'R2', payment_id, submitted_date, basis, channel, affirmative, form_completed, required_condition, issuer_elected, 'R' FROM regz_requests WHERE request_id = 'R'"
    )
    assert submit_allocation(conn, "B") is False
    conn.execute(
        "INSERT INTO regz_request_lines SELECT 'R2', balance_id, amount_cents FROM regz_request_lines WHERE request_id = 'R'"
    )
    conn.execute("UPDATE regz_proposals SET request_id = 'R2'")
    assert submit_allocation(conn, "B") is True


def test_reallocation_replaces_head_but_retains_audit_history(conn):
    _base(conn)
    _program(conn)
    _allocate(conn, 0, 80000)
    assert submit_allocation(conn, "B") is True
    conn.execute(
        "INSERT INTO regz_proposals VALUES ('B2', 'PAY', DATE '2026-01-31', DATE '2026-02-26', 'deferred_request', 'R', 'B')"
    )
    conn.execute(
        "INSERT INTO regz_proposal_lines SELECT 'B2', balance_id, 0, CASE balance_id WHEN 'H' THEN 50000 ELSE 30000 END, effective_apr, window_start FROM regz_proposal_lines WHERE batch_id = 'B'"
    )
    conn.execute(
        "INSERT INTO regz_requests VALUES ('R', 'PAY', DATE '2026-02-26', 'reallocation', 'oral', true, false, false, true, NULL)"
    )
    conn.execute("INSERT INTO regz_request_lines VALUES ('R', 'H', 50000), ('R', 'L', 30000)")
    conn.execute(
        "INSERT INTO regz_program_classifications SELECT 'B2', program_id, qualifies FROM regz_program_classifications WHERE batch_id = 'B'"
    )
    assert submit_allocation(conn, "B2") is True
    assert conn.execute("SELECT * FROM regz_payment_heads").fetchall() == [("PAY", "B2")]
    assert conn.execute("SELECT count(*) FROM regz_posted_allocations").fetchone()[0] == 4


def test_correction_checks_each_credit_and_actual_notice(conn):
    conn.execute(
        "INSERT INTO regz_error_obligations VALUES ('ERR', DATE '2026-01-01', DATE '2026-02-01', 'consumer')"
    )
    conn.execute(
        "INSERT INTO regz_error_items VALUES ('ERR', 'principal', 20000), ('ERR', 'charge', 1000)"
    )
    assert complete_correction(conn, "ERR", date(2026, 1, 20)) is False
    conn.execute(
        "INSERT INTO regz_credits VALUES ('C1', 'ERR', 'principal', 21000, DATE '2026-01-10')"
    )
    conn.execute(
        "INSERT INTO regz_dispatches VALUES ('N', 'ERR', 'consumer', 'correction', DATE '2026-01-10', 'content-reviewed')"
    )
    # Overcrediting principal cannot substitute for correcting the related charge.
    assert complete_correction(conn, "ERR", date(2026, 1, 20)) is False
    conn.execute("INSERT INTO regz_credits VALUES ('C2', 'ERR', 'charge', 1000, DATE '2026-01-11')")
    conn.execute("UPDATE regz_dispatches SET recipient = 'someone-else'")
    assert complete_correction(conn, "ERR", date(2026, 1, 20)) is False
    conn.execute("UPDATE regz_dispatches SET recipient = 'consumer'")
    assert complete_correction(conn, "ERR", date(2026, 1, 20)) is True


def test_late_dispatch_does_not_satisfy_deadline(conn):
    conn.execute(
        "INSERT INTO regz_error_obligations VALUES ('ERR', DATE '2026-01-01', DATE '2026-02-01', 'consumer')"
    )
    conn.execute("INSERT INTO regz_error_items VALUES ('ERR', 'principal', 20000)")
    conn.execute(
        "INSERT INTO regz_credits VALUES ('C', 'ERR', 'principal', 20000, DATE '2026-01-10')"
    )
    conn.execute(
        "INSERT INTO regz_dispatches VALUES ('N', 'ERR', 'consumer', 'correction', DATE '2026-02-02', 'reviewed')"
    )
    assert complete_correction(conn, "ERR", date(2026, 2, 3)) is False


@pytest.mark.parametrize(
    ("change", "field"),
    [
        ("UPDATE regz_program_classifications SET qualifies = false", "classification_mismatches"),
        (
            "UPDATE regz_programs SET interim_payment_required = true; UPDATE regz_program_classifications SET qualifies = false",
            "interim_payment_omissions",
        ),
        ("UPDATE regz_programs SET grace_period = true", "grace_classifications"),
        (
            "UPDATE regz_programs SET temporary_zero = true, retroactive_interest = false",
            "zero_offer_classifications",
        ),
    ],
)
def test_proposed_classifications_are_checked_from_terms(conn, change, field):
    _base(conn)
    _program(conn)
    _allocate(conn, 0, 80000)
    conn.execute(change)
    assert derive_allocation_context(conn, "B")[field] == 1
    assert submit_allocation(conn, "B") is False
    assert conn.execute("SELECT count(*) FROM regz_posted_classifications").fetchone()[0] == 0


def test_complete_snapshot_uses_both_selected_rates_and_balances(conn):
    _base(conn)
    conn.execute("INSERT INTO regz_snapshot_sets VALUES ('A', DATE '2026-02-10')")
    conn.execute(
        "INSERT INTO regz_balances SELECT account_id, balance_id, DATE '2026-02-10', amount_cents, CASE balance_id WHEN 'H' THEN 10 ELSE 30 END, protected_cents, secured, program_id FROM regz_balances WHERE snapshot_date = DATE '2026-01-31'"
    )
    conn.execute("UPDATE regz_practices SET convention = 'offset', offset_days = 10")
    conn.execute("UPDATE regz_proposals SET snapshot_date = DATE '2026-02-10'")
    _allocate(conn, 0, 80000)
    conn.execute(
        "UPDATE regz_proposal_lines SET effective_apr = CASE balance_id WHEN 'H' THEN 10 ELSE 30 END"
    )
    assert submit_allocation(conn, "B") is True


def test_snapshot_outside_interval_rejected_even_if_practice_matches(conn):
    _base(conn)
    conn.execute("INSERT INTO regz_snapshot_sets VALUES ('A', DATE '2026-01-30')")
    conn.execute(
        "INSERT INTO regz_balances SELECT account_id, balance_id, DATE '2026-01-30', amount_cents, apr, protected_cents, secured, program_id FROM regz_balances WHERE snapshot_date = DATE '2026-01-31'"
    )
    conn.execute("UPDATE regz_practices SET convention = 'offset', offset_days = -1")
    conn.execute("UPDATE regz_proposals SET snapshot_date = DATE '2026-01-30'")
    assert submit_allocation(conn, "B") is False


def test_missing_program_cannot_enable_deferred_request(conn):
    _base(conn)
    _request(conn)
    assert submit_allocation(conn, "B") is False


def test_unsecured_target_cannot_enable_secured_request(conn):
    _base(conn)
    _request(conn, method="secured_request")
    assert submit_allocation(conn, "B") is False


def test_matching_total_does_not_substitute_for_matching_request(conn):
    _base(conn)
    _program(conn)
    _request(conn, high=40000, low=40000)
    assert derive_allocation_context(conn, "B")["request_feasibility_errors"] == 0
    assert submit_allocation(conn, "B") is False


def test_reallocation_needs_prior_default_allocation(conn):
    _base(conn)
    _program(conn)
    _request(conn, basis="reallocation")
    assert submit_allocation(conn, "B") is False


def test_default_does_not_require_interpreting_a_declined_request(conn):
    _base(conn)
    _request(conn, method="default")
    conn.execute("UPDATE regz_requests SET basis = 'uninterpreted_conversation'")
    assert submit_allocation(conn, "B") is True


def test_default_after_rejected_exception_can_be_retried_atomically(conn):
    _base(conn)
    _program(conn)
    _request(conn)
    conn.execute("UPDATE regz_requests SET required_condition = true")
    assert submit_allocation(conn, "B") is False
    conn.execute("UPDATE regz_proposals SET method = 'default'")
    _allocate(conn, 0, 80000)
    assert submit_allocation(conn, "B") is True
    with pytest.raises(EvidenceError, match="Duplicate payment or stale replacement"):
        submit_allocation(conn, "B")
    assert conn.execute("SELECT count(*) FROM regz_posted_allocations").fetchone()[0] == 2
