"""Inline evidence computations for the existing 31 Regulation Z predicates.

CTEs are local to each constraint, never installed as views or materialized tables.
This preserves the catalog's operational meanings, not a new legal interpretation.
"""

from __future__ import annotations

import re

from data_flow_control import Policy, Resolution

from vldb_experiments.strategies.regulation_z_53_full_policy_catalog import (
    FULL_REGULATION_Z_53_POLICY_SPECS,
)

# Dependency pruning keeps each exported policy self-contained without embedding
# unrelated calculations (e.g. notice matching in an APR comparison).
CTES = {
    "base": """
SELECT p.*, pay.account_id, pay.credited_date, pay.cycle_end,
       pay.total_cents, pay.minimum_cents
FROM regz_proposals p JOIN regz_payments pay ON pay.payment_id = p.payment_id
""",
    "program_terms": """
SELECT g.*, (g.interest_accrues AND g.waived_on_payoff AND NOT g.grace_period
       AND (NOT g.temporary_zero OR g.retroactive_interest)) AS qualifies
FROM regz_programs g
""",
    "lot_terms": """
SELECT b.batch_id, b.account_id, b.credited_date, b.snapshot_date,
       bal.balance_id, bal.amount_cents, bal.protected_cents, bal.secured,
       bal.program_id, l.minimum_cents, l.excess_cents,
       l.effective_apr AS proposed_apr, l.window_start AS proposed_window,
       CASE WHEN old.balance_id IS NULL THEN 1 ELSE 0 END AS phase,
       COALESCE(old.apr, bal.apr) AS reference_apr,
       COALESCE(g.qualifies, false) AS qualifies,
       COALESCE(g.qualifies AND b.credited_date BETWEEN g.start_date
                AND g.expiration_date, false) AS active,
       g.expiration_date, g.grace_period, g.temporary_zero,
       g.retroactive_interest, g.interim_payment_required,
       d.qualifies AS declared
FROM base b JOIN regz_balances bal
  ON bal.account_id = b.account_id AND bal.snapshot_date = b.credited_date
JOIN regz_proposal_lines l ON l.batch_id = b.batch_id AND l.balance_id = bal.balance_id
LEFT JOIN regz_balances old ON old.account_id = b.account_id
  AND old.balance_id = bal.balance_id AND old.snapshot_date = b.snapshot_date
LEFT JOIN program_terms g ON g.program_id = bal.program_id
LEFT JOIN regz_program_classifications d
  ON d.batch_id = b.batch_id AND d.program_id = bal.program_id
""",
    "lots": """
SELECT t.*, CASE WHEN t.active THEN 0 ELSE t.reference_apr END AS effective_apr,
 CASE WHEN t.active THEN (
   SELECT MIN(last_two.start_date) FROM (
     SELECT c.start_date FROM regz_cycles c
     WHERE c.account_id = t.account_id AND c.start_date <= t.expiration_date
       AND c.due_date <= t.expiration_date
     ORDER BY c.start_date DESC LIMIT 2
   ) last_two
 ) ELSE NULL END AS window_start,
 CASE WHEN t.active THEN (
   SELECT c.start_date FROM regz_cycles c WHERE c.account_id = t.account_id
     AND t.expiration_date BETWEEN c.start_date AND c.end_date
     AND t.expiration_date < c.due_date
 ) ELSE NULL END AS early_start
FROM lot_terms t
""",
    "slices": """
SELECT l.batch_id, l.balance_id, part.disputed, l.phase, l.effective_apr,
 COALESCE(l.active AND l.credited_date BETWEEN l.window_start
          AND l.expiration_date, false) AS in_window,
 COALESCE(l.active AND l.credited_date BETWEEN l.window_start AND l.expiration_date
          AND l.credited_date >= l.early_start, false) AS early,
 CASE WHEN part.disputed THEN
   l.protected_cents - GREATEST(0, l.minimum_cents - (l.amount_cents-l.protected_cents))
 ELSE GREATEST(0, l.amount_cents-l.protected_cents-l.minimum_cents) END AS capacity,
 CASE WHEN part.disputed THEN
   GREATEST(0, l.excess_cents-GREATEST(0,l.amount_cents-l.protected_cents-l.minimum_cents))
 ELSE LEAST(l.excess_cents,GREATEST(0,l.amount_cents-l.protected_cents-l.minimum_cents))
 END AS allocated
FROM lots l CROSS JOIN (VALUES (false), (true)) part(disputed)
""",
    "pairs": """
SELECT h.batch_id,
 h.in_window AND NOT lo.in_window AS deferred,
 h.in_window AND NOT lo.in_window AND h.early AS early,
 NOT h.in_window AND NOT lo.in_window AND h.phase < lo.phase AS historical,
 NOT h.in_window AND NOT lo.in_window AND h.phase = lo.phase
   AND h.effective_apr > lo.effective_apr AS ordinary,
 h.effective_apr = (
   SELECT MAX(peer.effective_apr) FROM slices peer
   WHERE peer.batch_id = h.batch_id AND peer.disputed = h.disputed
     AND peer.phase = h.phase AND NOT peer.in_window AND peer.capacity > 0
 ) AS highest
FROM slices h JOIN slices lo ON lo.batch_id = h.batch_id AND lo.disputed = h.disputed
WHERE h.capacity > 0 AND lo.capacity > 0 AND h.allocated < h.capacity AND lo.allocated > 0
""",
    "request": """
SELECT b.batch_id, r.* FROM base b JOIN regz_requests r ON r.request_id = b.request_id
WHERE b.method <> 'default'
""",
    "request_targets": """
SELECT r.batch_id, i.*, l.balance_id AS existing_balance,
       l.amount_cents AS capacity, l.minimum_cents, l.secured
FROM request r JOIN regz_request_lines i ON i.request_id=r.request_id
LEFT JOIN lot_terms l ON l.batch_id=r.batch_id AND l.balance_id=i.balance_id
""",
    "request_allocations": """
SELECT r.batch_id, l.balance_id, l.excess_cents, COALESCE(i.amount_cents,0) AS requested
FROM request r JOIN regz_proposal_lines l ON l.batch_id=r.batch_id
LEFT JOIN regz_request_lines i ON i.request_id=r.request_id AND i.balance_id=l.balance_id
""",
    "practice_history": """
SELECT p.*, LAG(p.convention) OVER w AS previous_convention,
 LAG(p.offset_days) OVER w AS previous_offset
FROM regz_practices p
WINDOW w AS (PARTITION BY p.account_id ORDER BY p.effective_date)
""",
}


def _count(table: str, condition: str) -> str:
    return f"(SELECT COUNT(*) FROM {table} x WHERE x.batch_id = b.batch_id AND ({condition}))"


def _request_value(column: str, default: str) -> str:
    return f"COALESCE((SELECT r.{column} FROM request r WHERE r.batch_id=b.batch_id), {default})"


FIELDS = {
    "kind": "'allocation'",
    "method": "b.method",
    "snapshot_date": "b.snapshot_date",
    "cycle_end": "b.cycle_end",
    "credited_date": "b.credited_date",
    "request_basis": _request_value("basis", "'none'"),
    "issuer_elected": _request_value("issuer_elected", "false"),
    "affirmative": _request_value("affirmative", "false"),
    "form_completed": _request_value("form_completed", "false"),
    "required_condition": _request_value("required_condition", "false"),
    "contact_channel": "COALESCE((SELECT r.channel IN ('oral','electronic','written') FROM request r WHERE r.batch_id=b.batch_id), false)",
    "request_present": "EXISTS (SELECT 1 FROM request r WHERE r.batch_id=b.batch_id AND r.submitted_date <= b.decision_date)",
    "instruction_count": "(SELECT COUNT(*) FROM request r JOIN regz_request_lines i ON i.request_id=r.request_id WHERE r.batch_id=b.batch_id)",
    "superseding_requests": "(SELECT COUNT(*) FROM request r JOIN regz_requests replacement ON replacement.supersedes_id=r.request_id AND replacement.payment_id=b.payment_id WHERE r.batch_id=b.batch_id AND replacement.submitted_date <= b.decision_date)",
    "prior_default_allocation": "EXISTS (SELECT 1 FROM request r WHERE r.batch_id=b.batch_id AND r.submitted_date IS NOT NULL AND EXISTS (SELECT 1 FROM regz_posted_allocations p WHERE p.batch_id=b.replaces_batch_id AND p.payment_id=b.payment_id) AND NOT EXISTS (SELECT 1 FROM regz_posted_allocations p WHERE p.batch_id=b.replaces_batch_id AND p.payment_id=b.payment_id AND (p.method <> 'default' OR p.decision_date > r.submitted_date)))",
    "active_programs": "(SELECT COUNT(DISTINCT x.program_id) FROM lots x WHERE x.batch_id=b.batch_id AND x.active)",
    "window_programs": "(SELECT COUNT(DISTINCT x.program_id) FROM lots x WHERE x.batch_id=b.batch_id AND x.active AND x.credited_date BETWEEN x.window_start AND x.expiration_date)",
    "classification_mismatches": _count(
        "lot_terms", "x.program_id IS NOT NULL AND x.declared IS DISTINCT FROM x.qualifies"
    ),
    "interim_payment_omissions": _count(
        "lot_terms", "x.interim_payment_required AND x.qualifies AND NOT x.declared"
    ),
    "grace_classifications": _count("lot_terms", "x.grace_period AND x.declared"),
    "zero_offer_classifications": _count(
        "lot_terms", "x.temporary_zero AND NOT x.retroactive_interest AND x.declared"
    ),
    "effective_apr_mismatches": _count("lots", "x.proposed_apr IS DISTINCT FROM x.effective_apr"),
    "window_start_mismatches": _count("lots", "x.proposed_window IS DISTINCT FROM x.window_start"),
    "highest_violations": _count("pairs", "x.ordinary AND x.highest"),
    "remaining_violations": _count("pairs", "x.ordinary AND NOT x.highest"),
    "ordinary_violations": _count("pairs", "x.ordinary"),
    "deferred_violations": _count("pairs", "x.deferred"),
    "snapshot_violations": _count("pairs", "x.historical"),
    "early_expiry_violations": _count("pairs", "x.early"),
    "protected_reduction": "(SELECT COALESCE(SUM(GREATEST(0, x.minimum_cents+x.excess_cents-(x.amount_cents-x.protected_cents))),0) FROM lot_terms x WHERE x.batch_id=b.batch_id)",
    "unavoidable_protected_reduction": "GREATEST(0,b.total_cents-(SELECT COALESCE(SUM(x.amount_cents-x.protected_cents),0) FROM lot_terms x WHERE x.batch_id=b.batch_id))",
    "other_capacity_remaining": "(SELECT COALESCE(SUM(GREATEST(0,x.amount_cents-x.protected_cents-x.minimum_cents-x.excess_cents)),0) FROM lot_terms x WHERE x.batch_id=b.batch_id)",
    "expected_snapshot_date": "(SELECT CASE p.convention WHEN 'credit_date' THEN b.credited_date WHEN 'offset' THEN b.cycle_end + CAST(p.offset_days AS INTEGER) ELSE b.cycle_end END FROM regz_practices p WHERE p.account_id=b.account_id AND p.effective_date <= b.credited_date ORDER BY p.effective_date DESC LIMIT 1)",
    "unauthorized_changes": "(SELECT COUNT(*) FROM practice_history p WHERE p.account_id=b.account_id AND p.effective_date <= b.credited_date AND p.previous_convention IS NOT NULL AND (p.convention <> p.previous_convention OR p.offset_days <> p.previous_offset) AND (p.authorization_evidence IS NULL OR p.authorization_evidence=''))",
    "missing_credit_cents": "0",
    "notice_count": "0",
    "request_mismatches": """(SELECT COUNT(*) FROM request_allocations x
      WHERE x.batch_id=b.batch_id AND x.excess_cents <> x.requested)
      + (SELECT COUNT(*) FROM request_targets x WHERE x.batch_id=b.batch_id
         AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines l
             WHERE l.batch_id=x.batch_id AND l.balance_id=x.balance_id))""",
    "unsecured_request_targets": """(SELECT COUNT(*) FROM request r
      JOIN regz_request_lines i ON i.request_id=r.request_id
      JOIN lot_terms l ON l.batch_id=b.batch_id AND l.balance_id=i.balance_id
      WHERE r.batch_id=b.batch_id AND i.amount_cents > 0 AND NOT l.secured)""",
    "request_feasibility_errors": """COALESCE((SELECT
      CAST(NOT EXISTS (SELECT 1 FROM regz_request_lines i WHERE i.request_id=r.request_id) AS INTEGER)
      + CAST((SELECT COALESCE(SUM(i.amount_cents),0) FROM regz_request_lines i
              WHERE i.request_id=r.request_id) <> b.total_cents-b.minimum_cents AS INTEGER)
      + (SELECT COUNT(*) FROM request_targets x
         WHERE x.batch_id=b.batch_id AND (x.existing_balance IS NULL OR x.amount_cents < 0
            OR x.amount_cents > x.capacity-x.minimum_cents))
      FROM request r WHERE r.batch_id=b.batch_id),0)""",
}


def _with_dependencies(body: str) -> str:
    needed = set()

    def visit(text: str) -> None:
        for name, sql in CTES.items():
            if name not in needed and re.search(rf"\b{re.escape(name)}\b", text):
                needed.add(name)
                visit(sql)

    visit(body)
    definitions = [f"{name} AS (\n{sql.strip()}\n)" for name, sql in CTES.items() if name in needed]
    return "WITH " + ",\n".join(definitions) + "\n" + body


def allocation_facts_sql(fields: list[str] | None = None) -> str:
    """Reference SQL for differential testing; policies embed the selected fields."""
    names = list(FIELDS) if fields is None else fields
    select = ",\n       ".join(f"{FIELDS[name]} AS {name}" for name in names)
    return _with_dependencies(f"SELECT b.batch_id, {select}\nFROM base b")


def decision_sql(number: int) -> str:
    spec = next(s for s in FULL_REGULATION_Z_53_POLICY_SPECS if s.number == number)
    if spec.constraint is None:
        raise ValueError(f"{spec.proposition_id} has no independent constraint")
    fields = sorted(set(re.findall(r"regz_context\.(\w+)", spec.constraint)))
    facts = allocation_facts_sql(fields)
    predicate = spec.constraint.replace("regz_context.", "e.")
    query = f"SELECT e.batch_id FROM (\n{facts}\n) e WHERE {predicate}"
    if number in (19, 20):
        query += "\nUNION ALL\n" + correction_decision_sql(number)
    return query


def correction_facts_sql() -> str:
    """Compute credits/notices from actual evidence at a staged completion checkpoint.

    Separate upper bounds avoid this DuckDB build's incorrect optimization of
    a LEFT JOIN range ending at LEAST(checkpoint_date, deadline).
    """
    return """
WITH item_credits AS (
 SELECT k.error_id, i.item_id, i.required_cents,
        COALESCE(SUM(c.amount_cents),0) AS credited_cents
 FROM regz_correction_completions k
 JOIN regz_error_obligations o ON o.error_id=k.error_id
 JOIN regz_error_items i ON i.error_id=k.error_id
 LEFT JOIN regz_credits c ON c.error_id=i.error_id AND c.item_id=i.item_id
   AND c.posted_date >= o.determination_date
   AND c.posted_date <= k.checkpoint_date AND c.posted_date <= o.deadline
 GROUP BY k.error_id, i.item_id, i.required_cents
)
SELECT 'correction:' || o.error_id AS batch_id,
 (SELECT SUM(GREATEST(0, i.required_cents-i.credited_cents))
  FROM item_credits i WHERE i.error_id=o.error_id) AS missing_credit_cents,
 (SELECT COUNT(*) FROM regz_dispatches d
  WHERE d.error_id=o.error_id AND d.recipient=o.recipient AND d.notice_type='correction'
    AND d.sent_date BETWEEN o.determination_date AND LEAST(k.checkpoint_date,o.deadline)
    AND LENGTH(TRIM(d.content_evidence)) > 0) AS notice_count
FROM regz_correction_completions k JOIN regz_error_obligations o ON o.error_id=k.error_id
WHERE k.checkpoint_date >= o.determination_date
""".strip()


def correction_decision_sql(number: int) -> str:
    condition = "c.missing_credit_cents = 0" if number == 19 else "c.notice_count > 0"
    return f"SELECT c.batch_id FROM (\n{correction_facts_sql()}\n) c WHERE {condition}"


# Bind the evaluated proposal to the proposed sink values, not only the contributor
# identity. This is a v3 input-contract guard, not an additional legal proposition.
UPDATE_BINDING = """(
 allocation.cash_advance_allocation*100 = (
   SELECT line.excess_cents FROM regz_proposal_lines line
   WHERE line.batch_id=selected.transaction_id AND line.balance_id=selected.transaction_id
 ) AND EXISTS (
   SELECT 1 FROM regz_proposals bp JOIN regz_payments pay ON pay.payment_id=bp.payment_id
   WHERE bp.batch_id=selected.transaction_id AND pay.payment_id=allocation.transaction_id
     AND pay.account_id=allocation.account_id AND pay.credited_date=allocation.payment_date
     AND pay.total_cents=allocation.total_payment*100
     AND pay.minimum_cents=allocation.minimum_payment_portion*100
     AND NOT EXISTS (SELECT 1 FROM regz_proposal_lines other_line
       WHERE other_line.batch_id=bp.batch_id AND other_line.balance_id<>bp.batch_id
         AND other_line.excess_cents<>0)
 ) AND allocation.purchase_allocation=0 AND allocation.balance_transfer_allocation=0
   AND allocation.promo_allocation=0
)"""


def build_expanded_policies(*, binding: str = "v3") -> tuple[Policy, ...]:
    """All 31 predicates, reading live evidence inside each allocation constraint."""
    if binding not in {"v3", "allocation", "correction"}:
        raise ValueError("Unknown expanded policy binding")
    if binding == "correction":
        return tuple(
            Policy(
                sources=["regz_correction_completions"],
                source_aliases={"completion": "regz_correction_completions"},
                sink="regz_acceptances",
                constraint="(('correction:' || completion.error_id) IN (\n"
                + correction_decision_sql(number)
                + "\n))",
                on_fail=Resolution.REMOVE,
                description=f"P{number}: expanded correction evidence",
            )
            for number in (19, 20)
        )
    contributor = "selected.transaction_id" if binding == "v3" else "proposal.batch_id"
    return tuple(
        Policy(
            sources=["transactions" if binding == "v3" else "regz_proposals"],
            source_aliases={"selected": "transactions"}
            if binding == "v3"
            else {"proposal": "regz_proposals"},
            sink="payment_allocations" if binding == "v3" else "regz_acceptances",
            sink_alias="allocation" if binding == "v3" else None,
            constraint=f"({contributor} IN (\n{decision_sql(spec.number)}\n)"
            + (f" AND {UPDATE_BINDING}" if binding == "v3" else "")
            + ")",
            on_fail=Resolution.REMOVE,
            description=f"{spec.proposition_id}: {spec.name} (expanded)",
        )
        for spec in FULL_REGULATION_Z_53_POLICY_SPECS
        if spec.constraint is not None
    )


def expanded_update(sql: str) -> str:
    """Remove only redundant materialized-evidence joins; preserve selection/order."""
    for join in (
        "JOIN regz_context ON regz_context.batch_id = selected.transaction_id ",
        "JOIN balance_components AS component ON component.component_id = selected.transaction_id ",
        "JOIN statements AS statement_context ON statement_context.statement_id = selected.statement_id ",
        "JOIN allocation_snapshot_context AS snapshot ON snapshot.account_id = selected.account_id AND snapshot.statement_id = selected.statement_id ",
    ):
        if sql.count(join) != 1:
            raise ValueError("Expected the unchanged v3 UPDATE joins")
        sql = sql.replace(join, "")
    return sql
